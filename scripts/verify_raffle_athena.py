"""Verify two event-time windows against MySQL using Kafka, Spark and optionally Athena."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from verify_raffle_trace import ROOT, TraceStack, image_id

SQL = ROOT / "sql" / "raffle_athena_mysql"
TOPIC = "raffle.athena.fixture.v1"
STAGES = (
    ("initial", "10_initial.sql", ("A", "B"), 9980),
    ("sealed", "20_sealed.sql", ("B",), 9980),
    ("backfill", "30_backfill.sql", ("A",), 10000),
    ("retry", "30_backfill.sql", ("A",), 10000),
)
BUNDLES = {
    "1.11.0": "38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f",
    "1.12.0": "dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957",
}


def require(test, message):
    if not test:
        raise ValueError(message)


def columns(text):
    return [row.split("\t") for row in text.splitlines()]


def oracle(stack):
    rows = []
    for window, product, state, _revision, _cutoff, count in columns(stack.sql((SQL / "oracle.sql").read_text())):
        rows.append({"window_key": window, "product_id": int(product), "window_state": state,
                     "entry_count": None if count == "NULL" else int(count)})
    return rows


def by_key(rows):
    values = {(r["window_key"], int(r["product_id"])): (r["window_state"], r["entry_count"]) for r in rows}
    keys = {(w, p) for w in ("A", "B") for p in range(1, 101)}
    require(len(rows) == len(values) and set(values) == keys, "missing, extra or duplicate product/window keys")
    return values


def validate_stage(stage, rows, prior):
    actual = by_key(rows)
    totals = {}
    for window in ("A", "B"):
        values = [actual[window, p] for p in range(1, 101)]
        if window == "B" and stage == "initial":
            require(all(v == ("OPEN", None) for v in values), "open B must not publish final counts")
            totals[window] = None
        else:
            require(all(state == "CLOSED" and type(n) is int and n >= 0 for state, n in values),
                    "closed windows require a confirmed numeric count")
            require(actual[window, 100] == ("CLOSED", 0), "missing confirmed zero")
            totals[window] = sum(n for _, n in values)
    expected = {"A": 5000 if stage in ("backfill", "retry") else 4980,
                "B": None if stage == "initial" else 5000}
    require(totals == expected, f"{stage}: unexpected window totals {totals}")
    require(actual["A", 99][1] == (10 if stage in ("backfill", "retry") else 0), "late-only product mismatch")
    if prior is not None:
        old = by_key(prior)
        changed = {k for k in actual if actual[k] != old[k]}
        allowed = ({("B", p) for p in range(1, 101)} if stage == "sealed" else
                   {("A", 1), ("A", 99)} if stage == "backfill" else set())
        require(changed == allowed, f"{stage}: unexpected set of changed window/product values")
    return totals


def publish_and_bound(stack, last_id, new_total):
    if new_total > last_id:
        rows = columns(stack.sql((SQL / "events.sql").read_text()))
        require([int(r[0]) for r in rows] == list(range(1, new_total + 1)), "unexpected source event IDs")
        lines = []
        for eid, pid, occurred in rows[last_id:]:
            payload = dict(entry_id=int(eid), product_id=int(pid), occurred_at=occurred)
            lines.append(eid + "\t" + json.dumps(payload, separators=(",", ":")))
        stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                      "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--property", "parse.key=true",
                      "--producer-property", "acks=all", stdin="\n".join(lines) + "\n")
    result = stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                           "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--time", "latest")
    offsets = {}
    for line in result.splitlines():
        topic, partition, position = line.rsplit(":", 2)
        require(topic == TOPIC and partition not in offsets, "wrong Kafka topic or duplicate offset")
        offsets[partition] = int(position)
    require(set(offsets) == {"0", "1", "2"} and sum(offsets.values()) == new_total, "Kafka bounds differ from source")
    return {TOPIC: offsets}


def checked_bundle(image, path):
    path = path.resolve(strict=True)
    files = subprocess.check_output(["docker", "run", "--pull", "never", "--rm", "--entrypoint", "ls",
                                    image, "/opt/spark/jars"], text=True, timeout=45)
    versions = re.findall(r"iceberg-spark-runtime-[^\n]+?-(\d+\.\d+\.\d+)(?:-d2c-security\d+)?\.jar$", files, re.M)
    require(len(versions) == 1 and versions[0] in BUNDLES, f"unsupported Iceberg versions {versions}")
    require(hashlib.sha256(path.read_bytes()).hexdigest() == BUNDLES[versions[0]], "AWS bundle does not match runtime")
    return path


def start_services(stack):
    stack.compose("up", "-d", "--wait", "--wait-timeout", "150", "--no-build", "mysql", "kafka")
    stack.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092",
                  "--create", "--topic", TOPIC, "--partitions", "3", "--replication-factor", "1")
    stack.sql((SQL / "00_schema.sql").read_text())


def stop_services(stack):
    stack.compose("down", "--volumes", "--remove-orphans", timeout=120)
    for args in (["ps", "-aq"], ["network", "ls", "-q"], ["volume", "ls", "-q"]):
        left = subprocess.check_output(["docker", *args, "--filter", f"label=com.docker.compose.project={stack.project}"],
                                      text=True, timeout=30).strip()
        require(not left, "run-owned Docker resources remain")


def save_report(path, data):
    scratch = path.with_suffix(".part")
    scratch.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    scratch.replace(path)

def spark_stage(stack, offsets, selected, table, warehouse, bundle=None, credentials=None):
    control = {}
    for key, state, cutoff in columns(stack.sql(
            "SELECT window_key,window_state,cutoff_entry_id FROM raffle_windows ORDER BY window_key")):
        if key in selected:
            control[key] = {"state": state, "source_entry_id_cutoff": None if cutoff == "NULL" else int(cutoff)}
    require(len(control) == len(selected), "incomplete MySQL closure control")
    container_options = []
    options = ["--master", "local[1]", "--driver-memory", "640m", "--conf", "spark.ui.enabled=false",
               "--conf", "spark.sql.shuffle.partitions=2",
               "--conf", "spark.driver.extraJavaOptions=-XX:ActiveProcessorCount=2"]
    if bundle:
        # The preinstalled Iceberg runtime loads Glue through Spark's parent classloader.
        container_options.extend(["-v", f"{bundle}:/opt/spark/jars/iceberg-aws-bundle.jar:ro"])
    env = {"AWS_REGION": "ap-northeast-2", "AWS_DEFAULT_REGION": "ap-northeast-2",
           "AWS_EC2_METADATA_DISABLED": "true", **(credentials or {})} if bundle else {}
    saved = {name: stack.env.get(name) for name in env}
    try:
        for name, value in env.items():
            stack.env[name] = value
            container_options.extend(["-e", name])
        output = stack.compose("run", "-T", "--rm", "--no-deps", *container_options, "iceberg",
                               "/opt/spark/bin/spark-submit", *options,
                               "/opt/d2c-lakehouse/lakehouse/raffle_athena_mart.py",
                               "--bootstrap-servers", "kafka:9092", "--topic", TOPIC,
                               "--ending-offsets-json", json.dumps(offsets),
                               "--windows-json", json.dumps(control), "--table", table, "--warehouse", warehouse,
                               "--catalog-type", "glue" if bundle else "hadoop", timeout=360)
    except RuntimeError as err:
        message = str(err)
        for secret in (credentials or {}).values():
            message = message.replace(secret, "[redacted]")
        raise RuntimeError(message) from None
    finally:
        for name, original in saved.items():
            if original is None:
                stack.env.pop(name, None)
            else:
                stack.env[name] = original
    prefix = "RAFFLE_ATHENA_MART_RESULT="
    result, = [json.loads(line[len(prefix):]) for line in output.splitlines() if line.startswith(prefix)]
    return result


def verify_spark(stage, result, expected, prior_id, total):
    require(by_key(result["visible_rows"]) == by_key(expected), "Spark snapshot differs from MySQL")
    require(result["kafka_records"] == result["unique_entry_ids"] == total, "Spark input count differs from Kafka")
    current = result["snapshot_id_after"]
    require(current is not None, "no committed Iceberg snapshot")
    if prior_id is not None:
        require(result["snapshot_id_before"] == prior_id, "snapshot lineage changed between stages")
        require((current == prior_id) == (stage == "retry"), "unexpected snapshot change or missing commit")
    changed = {"initial": 200, "sealed": 100, "backfill": 2, "retry": 0}[stage]
    require(result["changed_rows"] == changed, f"{stage}: unexpected Iceberg change count")
    return current

def run_stages(stack, cloud, bundle, report, output):
    table = (cloud.database if cloud else "raffle") + ".raffle_mart"
    warehouse = cloud.warehouse_uri if cloud else "file:///warehouse"
    prior_rows, prior_id, sent = None, None, 0
    sealed = None
    previous_offsets = None
    for stage, filename, selected, total in STAGES:
        started = time.monotonic()
        stack.sql((SQL / filename).read_text())
        rows = oracle(stack)
        totals = validate_stage(stage, rows, prior_rows)
        offsets = publish_and_bound(stack, sent, total)
        if sent == total:
            require(offsets == previous_offsets, "fixed Kafka boundary moved with no new events")
        credentials = cloud.writer_env() if cloud else None
        result = spark_stage(stack, offsets, selected, table, warehouse, bundle, credentials)
        snapshot = verify_spark(stage, result, rows, prior_id, total)
        record = {"stage": stage, "source_entries": total, "confirmed": totals, "kafka_ending_offsets": offsets,
                  "mysql_oracle": rows, "spark": result}
        report["stages"].append(record)
        if cloud:
            record["athena"] = cloud.validate(snapshot, rows, observation=stage)
        record["elapsed_seconds"] = round(time.monotonic() - started, 3)
        save_report(output, report)
        print(stage, totals, "snapshot", snapshot, "changes", result["changed_rows"], flush=True)
        if stage == "sealed":
            sealed = (snapshot, rows)
        prior_rows, prior_id, sent, previous_offsets = rows, snapshot, total, offsets
    if cloud:
        old_snapshot, old_rows = sealed
        report["historical"] = cloud.validate(old_snapshot, old_rows, observation="historical")
        ids = [stage["athena"]["query_id"] for stage in report["stages"]] + [report["historical"]["query_id"]]
        require(len(set(ids)) == 5, "each observation requires its own Athena execution")
        save_report(output, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True, help="new private local directory")
    parser.add_argument("--spark-image", default=os.environ.get("RAFFLE_SPARK_IMAGE", "d2c-iceberg-stream:local"))
    parser.add_argument("--aws-profile", help="use a temporary AWS profile in Seoul; omit for local verification")
    parser.add_argument("--aws-bundle", type=Path, help="matching Maven Central iceberg-aws-bundle JAR")
    args = parser.parse_args()
    if bool(args.aws_profile) != bool(args.aws_bundle):
        parser.error("supply --aws-profile and --aws-bundle together")
    image = image_id(args.spark_image)
    bundle = checked_bundle(args.spark_image, args.aws_bundle) if args.aws_bundle else None
    directory = args.evidence_dir.resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    journal, output = directory / "cloud-state.json", directory / "result.json"
    report = {"spark_image_id": image, "mode": "aws" if bundle else "local", "stages": []}
    stack = TraceStack(ROOT)
    stack.env["RAFFLE_SPARK_IMAGE"] = args.spark_image
    cloud = session = None
    try:
        start_services(stack)
        if bundle:
            import boto3
            from raffle_athena_cloud import CloudRun, REGION
            session = boto3.Session(profile_name=args.aws_profile, region_name=REGION)
            cloud = CloudRun.create(journal, session=session, price_notice_ack=True)
            report.update(database=cloud.database, region=REGION, workgroup=cloud.workgroup)
        run_stages(stack, cloud, bundle, report, output)
    finally:
        try:
            stop_services(stack)
            report["local_teardown"] = True
        finally:
            try:
                if bundle and journal.exists():
                    active = cloud or CloudRun.open(journal, session=session)
                    report["cloud_teardown"] = active.cleanup()
            finally:
                save_report(output, report)
    print("Verified both windows. Evidence:", output, flush=True)


if __name__ == "__main__":
    main()
