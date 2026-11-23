"""Crash a real relay between Kafka ACK and MySQL mark, then replay a bounded offset range twice."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
from tempfile import TemporaryDirectory
import time
from uuid import UUID

from raffle_recovery_source import seed_and_apply
from verify_raffle_trace import ROOT, TOPIC, TraceStack, git_revision, image_id, start_infrastructure

RELAY_SCRIPT = ROOT / "scripts/raffle_recovery_relay.py"
OLDEST = "(SELECT published_at, created_at FROM raffle_outbox_events ORDER BY created_at,event_id LIMIT 500) held"


def check(condition, message):
    if not condition:
        raise ValueError(message)


def log(message):
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {message}", flush=True)


def docker(*args, timeout=45):
    return subprocess.check_output(["docker", *args], text=True, timeout=timeout).strip()


def offsets(stack, expected_total):
    result = stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                           "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--time", "latest")
    values = {}
    for line in result.splitlines():
        topic, partition, offset = line.split(":")
        check(topic == TOPIC, "unexpected topic")
        values[partition] = int(offset)
    check(set(values) == {"0", "1", "2"} and sum(values.values()) == expected_total,
          f"unexpected offsets for expected total {expected_total}: {values}")
    return values


def published(stack):
    return int(stack.sql("SELECT COUNT(*) FROM raffle_outbox_events WHERE published_at IS NOT NULL"))


def canonical_hash(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def source_ledger(stack, path):
    counts = stack.sql("SELECT (SELECT COUNT(*) FROM raffle_entries), (SELECT COUNT(*) FROM raffle_outbox_events), "
                       "(SELECT COUNT(*) FROM raffle_outbox_events WHERE published_at IS NULL)")
    check(counts.split() == ["10000"] * 3, f"unexpected source counts {counts}")
    rows = stack.sql("SELECT e.id,e.item_id,e.user_id,o.event_id,o.payload FROM raffle_entries e "
                     "JOIN raffle_outbox_events o ON o.aggregate_id=e.id AND o.aggregate_type='raffle_entry' "
                     f"AND o.event_type='{TOPIC}' ORDER BY e.id").splitlines()
    events = {}
    entries = set()
    with path.open("x") as file:
        for row in rows:
            entry, item, user, event_id, raw = row.split("\t", 4)
            keys = dict(entry_id=int(entry), item_id=int(item), user_id=int(user))
            payload = json.loads(raw)
            check(keys == payload["data"] and event_id == payload["event_id"], "source payload mismatch")
            check(event_id not in events and entry not in entries, "source identity not unique")
            entries.add(entry)
            record = dict(keys, event_id=event_id, occurred_at=payload["occurred_at"], payload_sha256=canonical_hash(payload))
            events[event_id] = record
            file.write(json.dumps(record) + "\n")
    check(len(events) == 10000, "source join lost entries")
    path.chmod(0o644)
    return events


def helper_args(*command):
    return ["-e", "PYTHONPATH=/trace", "-v", f"{RELAY_SCRIPT}:/trace/drill.py:ro",
            "relay", "python", "/trace/drill.py", *command]


def run_helper(stack, *command):
    return stack.compose("run", "-T", "--rm", "--no-deps", *helper_args(*command), timeout=600)


def start_helper(stack, owned, command, marker):
    name = f"{stack.project}-{command}"
    stack.compose("run", "-d", "--no-deps", "--name", name, *helper_args(command))
    owned.add(name)
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        output = subprocess.run(["docker", "logs", name], text=True, capture_output=True, timeout=10)
        for line in output.stdout.splitlines():
            if line.startswith(marker):
                return name, json.loads(line[len(marker):])
        if docker("inspect", "--format", "{{.State.Running}}", name) != "true":
            raise RuntimeError(f"{command} exited before {marker}: {output.stderr[-1500:]}")
        time.sleep(0.25)
    raise TimeoutError(f"timed out waiting for {marker}")


def kill_helper(name):
    docker("kill", "--signal", "KILL", name)
    docker("wait", name)
    state = json.loads(docker("inspect", "--format", "{{json .State}}", name))
    check(state["ExitCode"] == 137 and not state["OOMKilled"], f"unexpected {name} exit")
    return {"exit_code": state["ExitCode"], "oom_killed": state["OOMKilled"]}


def read_ack(stack, partition, offset, expected):
    result = stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                           "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--partition", str(partition),
                           "--offset", str(offset), "--max-messages", "1", "--timeout-ms", "15000",
                           "--property", "print.offset=true", "--property", "print.key=true")
    coord, key, value = result.split("\t", 2)
    check(coord == f"Offset:{offset}" and key == str(expected["entry_id"]) and
          canonical_hash(json.loads(value)) == expected["payload_sha256"], "ACK readback differs from source")


def exercise(stack, folder, owned):
    start_infrastructure(stack)
    stack.compose("up", "-d", "--wait", "--no-build", "--pull", "never", "api")
    log("Sending 10,000 applications to the writer")
    http = seed_and_apply(stack)
    manifest = folder / "source.jsonl"
    events = source_ledger(stack, manifest)
    bounds = {"start": offsets(stack, 0)}
    log("Source reconciled. Holding the 500 oldest events while publishing 8,000")
    hold, hold_proof = start_helper(stack, owned, "hold", "HOLD_PROOF=")
    begin_hold = time.monotonic()
    run_helper(stack, "drain", "8000")
    bounds["lower"] = offsets(stack, 8000)
    check(published(stack) == 8000, "healthy prefix not fully marked")

    range_start = stack.sql("SELECT UTC_TIMESTAMP(6)")
    crash, ack = start_helper(stack, owned, "crash", "ACK_PROOF=")
    event_id = str(UUID(ack["event_id"]))
    expected = events[event_id]
    lookup = ("SELECT published_at IS NULL,publish_attempts FROM raffle_outbox_events "
              f"WHERE event_id='{event_id}'")
    before = stack.sql(lookup)
    check(before.split() == ["1", "0"], "fault hook missed the pre-mark window")
    # The hook blocks all progress after ACK. SIGKILL here cannot run Python cleanup or commit.
    state = kill_helper(crash)
    after = stack.sql(lookup)
    check(after == before, "outbox changed while terminating relay")
    partition, offset = ack["partition"], ack["offset"]
    check(bounds["lower"][str(partition)] == offset, "unexpected first offset in failed range")
    read_ack(stack, partition, offset, expected)
    log(f"SIGKILL confirmed. Kafka {partition}/{offset} readable, MySQL row still pending")
    run_helper(stack, "drain", "1000")
    recovered = stack.sql(lookup)
    check(recovered.split() == ["0", "1"], "retry did not mark crash row once")
    read_ack(stack, partition, offset + 1, expected)
    check(published(stack) == 9000 and stack.sql("SELECT COUNT(*) FROM " + OLDEST +
          " WHERE published_at IS NULL") == "500", "oldest cohort was not held through the newer publications")
    kill_helper(hold)
    held_seconds = round(time.monotonic() - begin_hold, 3)
    run_helper(stack, "drain", "500")
    delay = stack.sql("SELECT COUNT(published_at),MIN(TIMESTAMPDIFF(MICROSECOND,created_at,published_at)),"
                      "MAX(TIMESTAMPDIFF(MICROSECOND,created_at,published_at)) FROM " + OLDEST).split()
    check(delay[0] == "500" and published(stack) == 9500, "delayed cohort not published")
    # Compare original event time with the instant the late cohort was finally marked.
    late_rows = stack.sql("SELECT event_id,published_at FROM raffle_outbox_events "
                          "ORDER BY created_at,event_id LIMIT 500").splitlines()
    after_window = 0
    for row in late_rows:
        held_id, marked = row.split("\t")
        occurred_at = datetime.fromisoformat(events[held_id]["occurred_at"].replace("Z", "+00:00"))
        marked_at = datetime.fromisoformat(marked).replace(tzinfo=timezone.utc)
        after_window += int(marked_at.timestamp() // 300 > occurred_at.timestamp() // 300)
    run_helper(stack, "duplicate", "--range-start", range_start)
    bounds["upper"] = offsets(stack, 10501)
    run_helper(stack, "drain", "500")
    bounds["end"] = offsets(stack, 11001)
    check(published(stack) == 10000, "outbox not fully marked")
    log(f"Kafka has 11,001 records. Running Iceberg reconciliation for {bounds['lower']} .. {bounds['upper']}")
    bounds_file = folder / "bounds.json"
    bounds_file.write_text(json.dumps(bounds))
    bounds_file.chmod(0o644)
    output = stack.compose("run", "-T", "--no-deps", "--rm", "-v", f"{manifest}:/drill/source.jsonl:ro",
                           "-v", f"{bounds_file}:/drill/bounds.json:ro", "iceberg",
                           "/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "640m",
                           "--conf", "spark.ui.enabled=false", "/opt/d2c-lakehouse/lakehouse/raffle_recovery.py",
                           "--manifest", "/drill/source.jsonl", "--bounds", "/drill/bounds.json", timeout=900)
    (spark,) = [json.loads(line.split("=", 1)[1]) for line in output.splitlines()
                if line.startswith("RAFFLE_RECOVERY_RESULT=")]
    return dict(http=http, mysql_entries=len(events), mysql_published=published(stack),
                source_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(), offsets=bounds,
                crash=dict(ack=ack, **state, pending_and_attempts_before_kill=before.split(),
                           pending_and_attempts_after_kill=after.split(), pending_and_attempts_after_retry=recovered.split(),
                           redelivery_offset=offset + 1),
                delay=dict(lock=hold_proof, marked_in_later_five_minute_window=after_window, held_seconds=held_seconds, overtaken_by=9000,
                           min_publish_delay_seconds=int(delay[1])/1e6, max_publish_delay_seconds=int(delay[2])/1e6),
                iceberg=spark)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writer", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    writer = args.writer.resolve(strict=True)
    check((writer / "app/migrate.py").is_file(), "writer checkout missing")
    check(not args.evidence.exists(), "choose a new evidence path")
    stack = TraceStack(writer)
    started = datetime.now(timezone.utc)
    spark_image = image_id(os.environ.get("RAFFLE_SPARK_IMAGE", "d2c-iceberg-stream:local"))
    components = ["scripts/verify_raffle_trace.py", "scripts/verify_raffle_recovery.py",
                  "scripts/raffle_recovery_source.py", "scripts/raffle_recovery_relay.py",
                  "scripts/raffle-trace.Dockerfile", "app/raffle_outbox_relay.py", "raffle_contract.py",
                  "d2c_contract.py", "compose.raffle-trace.yaml", "lakehouse/d2c_iceberg_stream.py",
                  "lakehouse/raffle_iceberg.py", "lakehouse/raffle_recovery.py"]
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in components}

    def stop(signum, frame):
        raise SystemExit("drill interrupted")
    signal.signal(signal.SIGTERM, stop)
    owned = set()
    log(f"Starting isolated stack {stack.project}")
    try:
        with TemporaryDirectory(prefix="raffle-recovery-") as tmp:
            result = exercise(stack, Path(tmp), owned)
        runtime_image = image_id(f"{stack.project}-runtime:local")
    finally:
        for name in owned:
            docker("rm", "-f", name)
        stack.compose("down", "--volumes", "--remove-orphans", timeout=120)
        tag = f"{stack.project}-runtime:local"
        if docker("image", "ls", "-q", tag):
            docker("image", "rm", tag)
        label = "label=com.docker.compose.project=" + stack.project
        for kind, command in [("containers", ["ps", "-aq"]), ("volumes", ["volume", "ls", "-q"]),
                              ("networks", ["network", "ls", "-q"])]:
            check(not docker(*command, "--filter", label), f"leftover {kind} in {stack.project}")
        log(f"Teardown verified for {stack.project}")
    report = dict(project=stack.project, started_at=started.isoformat(), finished_at=datetime.now(timezone.utc).isoformat(),
                  writer_sha=git_revision(writer), data_base_sha=git_revision(ROOT), writer_runtime_image_id=runtime_image,
                  spark_image_id=spark_image, source_file_sha256=hashes, teardown_verified=True, **result)
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    with args.evidence.open("x") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)
        file.write("\n")
    log(f"Evidence saved to {args.evidence}")


if __name__ == "__main__":
    main()
