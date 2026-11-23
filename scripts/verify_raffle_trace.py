"""Trace an actual GitOps raffle request through MySQL, Kafka and local Iceberg."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
from http.cookiejar import CookieJar
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, Request, build_opener


ROOT = Path(__file__).resolve().parents[1]
TOPIC = "raffle.entry.accepted.v1"


def git_revision(path: Path) -> str:
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"],
                                   timeout=30, text=True).strip()


def data_source_digest() -> str:
    digest = hashlib.sha256()
    inputs = [ROOT / "compose.raffle-trace.yaml", Path(__file__),
              ROOT / "scripts/raffle-trace.Dockerfile", ROOT / "app/raffle_outbox_relay.py",
              ROOT / "d2c_contract.py", ROOT / "raffle_contract.py",
              *sorted((ROOT / "lakehouse").glob("*.py"))]
    for path in inputs:
        digest.update(str(path.relative_to(ROOT)).encode() + b"\x00" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def image_id(image: str) -> str:
    return subprocess.check_output(["docker", "image", "inspect", "--format", "{{.Id}}", image],
                                   text=True, timeout=30).strip()


class TraceStack:
    def __init__(self, writer: Path):
        self.project = "raffle-trace-" + secrets.token_hex(6)
        self.env = dict(os.environ, RAFFLE_GITOPS_ROOT=str(writer))
        for name in ("ROOT_PASSWORD", "DB_PASSWORD", "RELAY_PASSWORD", "SESSION_SECRET"):
            self.env["RAFFLE_" + name] = secrets.token_hex(24)

    def compose(self, *args: str, stdin: str | None = None, timeout: int = 180) -> str:
        command = ["docker", "compose", "-p", self.project, "-f", str(ROOT / "compose.raffle-trace.yaml"), *args]
        run = subprocess.run(command, env=self.env, cwd=ROOT, input=stdin, text=True,
                             capture_output=True, timeout=timeout)
        if run.returncode:
            output = run.stdout + "\n" + run.stderr
            causes = [line for line in output.splitlines() if re.search(
                r"Caused by:|Exception:|Error:|AccessDenied|ClassNotFound|NoClassDefFound", line)]
            logs = "\n".join(dict.fromkeys(causes))[:5000] + "\n" + output[-6000:]
            for name in ("ROOT_PASSWORD", "DB_PASSWORD", "RELAY_PASSWORD", "SESSION_SECRET"):
                logs = logs.replace(self.env["RAFFLE_" + name], "[redacted]")
            raise RuntimeError(f"compose {args[0]} failed ({run.returncode})\n{logs}")
        return run.stdout.strip()

    def sql(self, statement: str) -> str:
        return self.compose("exec", "-T", "mysql", "sh", "-ec",
                            'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names raffle_db',
                            stdin=statement)

    def url(self, service: str) -> str:
        return "http://" + self.compose("port", service, "8080")


def apply_requests(stack: TraceStack, item: int, fault_item: int) -> dict:
    client = build_opener(HTTPCookieProcessor(CookieJar()))
    api = stack.url("api")
    fault_api = stack.url("api-failure")
    with client.open(api + "/signup", timeout=10) as page:
        match = re.search(r'<meta name="csrf-token" content="([^"]+)"', page.read().decode())
    if not match:
        raise ValueError("writer did not supply a CSRF token")
    token = match.group(1)

    def post(url: str, data: dict, expected_status: int) -> dict:
        request = Request(url, data=json.dumps(data).encode(), headers={
            "Content-Type": "application/json", "X-CSRFToken": token,
        })
        try:
            response = client.open(request, timeout=15)
        except HTTPError as error:
            response = error
        with response:
            if response.status != expected_status:
                raise ValueError(f"{request.selector} returned HTTP {response.status}, expected {expected_status}")
            result = json.load(response)
            expected = "success" if expected_status == 200 else "error"
            if result.get("status") != expected:
                raise ValueError(f"{request.selector} returned unexpected application status")
            return result

    credentials = {"username": "trace_" + secrets.token_hex(6), "password": secrets.token_urlsafe(24)}
    post(api + "/api/signup", credentials, 200)
    token = post(api + "/api/login", credentials, 200)["csrf_token"]
    post(api + "/api/apply", {"item_id": item}, 200)
    duplicate = post(api + "/api/apply", {"item_id": item}, 400)
    if "이미 응모" not in duplicate.get("message", ""):
        raise ValueError("writer did not identify the repeated application")
    post(fault_api + "/api/apply", {"item_id": fault_item}, 503)
    return {"accepted": 200, "duplicate": 400, "outbox_fault": 503}


def read_broker_record(stack: TraceStack, source: dict) -> tuple[int, int]:
    output = stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                           "--bootstrap-server", "kafka:9092", "--topic", TOPIC,
                           "--from-beginning", "--max-messages", "1", "--timeout-ms", "15000",
                           "--property", "print.partition=true", "--property", "print.offset=true",
                           "--property", "print.key=true", "--property", "print.value=true")
    partition, offset, key, value = output.split("\t", 3)
    if key != str(source["data"]["entry_id"]) or json.loads(value) != source:
        raise ValueError("Kafka record key or serialized event differs from the writer outbox")
    return int(partition.removeprefix("Partition:")), int(offset.removeprefix("Offset:"))


def broker_size(stack: TraceStack) -> int:
    # This run owns a fresh topic. Every partition starts at offset zero.
    offsets = stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                            "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--time", "latest")
    return sum(int(line.rsplit(":", 1)[1]) for line in offsets.splitlines())


def inspect_iceberg(stack: TraceStack, expected: dict) -> dict:
    args = []
    for key in ("event_id", "entry_id", "item_id", "partition", "offset"):
        args.extend(["-e", f"RAFFLE_EXPECT_{key.upper()}={expected[key]}"])
    output = stack.compose("run", "-T", "--no-deps", "--rm", *args, "iceberg", timeout=300)
    prefix = "RAFFLE_TRACE_RESULT="
    (result,) = [json.loads(line[len(prefix):]) for line in output.splitlines() if line.startswith(prefix)]
    for key, value in expected.items():
        if result.get(key) != value:
            raise ValueError(f"Iceberg result differs from the independent source for {key}")
    if result.get("snapshot_id") is None:
        raise ValueError("no Iceberg snapshot found")
    return result


def start_infrastructure(stack: TraceStack) -> None:
    stack.compose("build", "migrate", timeout=360)
    stack.compose("up", "-d", "--wait", "--wait-timeout", "130", "mysql", "kafka")
    stack.compose("run", "-T", "--no-deps", "--rm", "migrate")
    relay_password = stack.env["RAFFLE_RELAY_PASSWORD"]
    stack.sql(f"CREATE USER 'raffle_relay'@'%' IDENTIFIED BY '{relay_password}'; "
              "GRANT SELECT, UPDATE (published_at, publish_attempts, last_error) "
              "ON raffle_db.raffle_outbox_events TO 'raffle_relay'@'%';")
    stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092",
                  "--create", "--topic", TOPIC, "--partitions", "3", "--replication-factor", "1")


def run_trace(stack: TraceStack) -> dict:
    start_infrastructure(stack)
    stack.compose("up", "-d", "--wait", "--wait-timeout", "80", "--no-build", "--pull", "never", "api", "api-failure")
    item, fault_item = map(int, stack.sql("SELECT id FROM raffle_items ORDER BY id LIMIT 2;").splitlines())
    requests = apply_requests(stack, item, fault_item)

    counts = stack.sql("SELECT (SELECT COUNT(*) FROM raffle_entries), "
                       "(SELECT COUNT(*) FROM raffle_outbox_events), "
                       "(SELECT COUNT(*) FROM raffle_outbox_events WHERE published_at IS NULL);")
    if counts.split() != ["1", "1", "1"]:
        raise ValueError(f"expected one accepted application and one pending outbox row, got {counts}")
    raw = stack.sql("SELECT o.payload FROM raffle_entries e JOIN raffle_outbox_events o "
                    "ON o.aggregate_id=e.id AND o.event_type='raffle.entry.accepted.v1' "
                    f"WHERE e.item_id={item} AND o.aggregate_type='raffle_entry' "
                    "AND e.id=JSON_EXTRACT(o.payload,'$.data.entry_id') "
                    "AND e.user_id=JSON_EXTRACT(o.payload,'$.data.user_id') "
                    "AND e.item_id=JSON_EXTRACT(o.payload,'$.data.item_id') "
                    "AND o.event_id=JSON_UNQUOTE(JSON_EXTRACT(o.payload,'$.event_id'));")
    source = json.loads(raw)
    stack.compose("run", "-T", "--no-deps", "--rm", "relay")
    if stack.sql("SELECT COUNT(*) FROM raffle_outbox_events WHERE published_at IS NOT NULL;") != "1":
        raise ValueError("original outbox event was not marked published")
    stack.compose("run", "-T", "--no-deps", "--rm", "relay")
    if broker_size(stack) != 1:
        raise ValueError("unexpected Kafka record count after a second relay run on the isolated topic")
    partition, offset = read_broker_record(stack, source)
    occurred = datetime.fromisoformat(source["occurred_at"].replace("Z", "+00:00")).astimezone(timezone.utc)
    start = occurred.replace(minute=5 * (occurred.minute // 5), second=0, microsecond=0)
    end = start + timedelta(minutes=5)
    event_bytes = json.dumps(source, sort_keys=True, separators=(",", ":")).encode()
    expected = {"event_id": source["event_id"], "entry_id": source["data"]["entry_id"],
                "item_id": item, "topic": TOPIC, "partition": partition, "offset": offset,
                "payload_sha256": hashlib.sha256(event_bytes).hexdigest(), "accepted_count": 1,
                "window_start_utc": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "window_end_utc": end.strftime("%Y-%m-%dT%H:%M:%SZ")}
    first = inspect_iceberg(stack, expected)
    # Model at-least-once delivery at the broker. The same business event now has
    # a second physical record, while the source DB and first snapshot stay intact.
    stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                  "--bootstrap-server", "kafka:9092", "--topic", TOPIC,
                  "--property", "parse.key=true", "--producer-property", "acks=all",
                  stdin=f"{source['data']['entry_id']}\t{event_bytes.decode()}\n")
    replay_records = broker_size(stack)
    if replay_records != 2:
        raise ValueError("the re-delivery drill did not append a second physical record")
    second = inspect_iceberg(stack, expected)
    if second["snapshot_id"] != first["snapshot_id"]:
        raise ValueError("unchanged replay unexpectedly created a new Iceberg snapshot")
    return {"http": requests, "writer_entries": 1, "published_outbox": 1,
            "broker_records_before_redelivery": 1, "broker_records_after_redelivery": replay_records,
            "topic": TOPIC, "event_sha256": expected["payload_sha256"],
            "first_read": first, "replay": second}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writer", type=Path, required=True, help="checkout of aws-data-platform-gitops")
    parser.add_argument("--evidence", type=Path, help="new local JSON file for a successful trace")
    args = parser.parse_args()
    writer = args.writer.resolve(strict=True)
    if not (writer / "app" / "migrate.py").is_file():
        parser.error("writer checkout does not contain app/migrate.py")
    if args.evidence and args.evidence.exists():
        parser.error("choose a new evidence file")
    stack = TraceStack(writer)
    spark_image = os.environ.get("RAFFLE_SPARK_IMAGE", "d2c-iceberg-stream:local")
    writer_changes = subprocess.check_output(["git", "-C", str(writer), "status", "--porcelain", "--", "app"],
                                            text=True, timeout=30)
    metadata = {"writer_sha": git_revision(writer), "writer_app_dirty": bool(writer_changes.strip()),
                "data_base_sha": git_revision(ROOT), "spark_image_id": image_id(spark_image),
                "data_source_sha256": data_source_digest(),
                "project": stack.project, "started_at": datetime.now(timezone.utc).isoformat()}

    def stop(_signum, _frame):
        raise SystemExit("trace interrupted")

    signal.signal(signal.SIGTERM, stop)
    try:
        result = run_trace(stack)
        metadata["writer_runtime_image_id"] = image_id(f"{stack.project}-runtime:local")
    finally:
        stack.compose("down", "--volumes", "--remove-orphans", timeout=120)
        # This tag belongs to this uniquely named run. Infrastructure base images are shared.
        subprocess.run(["docker", "image", "rm", f"{stack.project}-runtime:local"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    report = dict(metadata, **result, finished_at=datetime.now(timezone.utc).isoformat())
    encoded = json.dumps(report, ensure_ascii=False, indent=2)
    if args.evidence:
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        with args.evidence.open("x", encoding="utf-8") as handle:
            handle.write(encoded + "\n")
    print(encoded)


if __name__ == "__main__":
    main()
