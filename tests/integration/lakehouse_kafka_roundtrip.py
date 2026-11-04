"""One real Kafka/checkpoint phase, run by a separate guarded spark-submit.

This script writes only the orchestrator's new /state volume. The only fault
injection is a test callback after the unchanged production writer returns.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import uuid4

import d2c_contract
from d2c_contract import build_application_approved_event, canonicalize_application_approved_event
from lakehouse.d2c_iceberg_stream import (
    build_parsed_stream, create_spark_session, create_tables,
    settings_from_environment, write_batch,
)
from lakehouse.validate_d2c_iceberg import validate


PHASES = ("initial", "resume", "crash", "recover")
TOPIC = "d2c.application.approved.v1"
DELIVERIES = {"initial": ("A", "A", "invalid"), "resume": ("B", "A"),
              "crash": ("C",), "recover": ()}
OFFSETS = {"initial": [0, 1, 2], "resume": [3, 4], "crash": [5], "recover": [5]}
EXPECTED = {"initial": (1, 1, 1), "resume": (2, 1, 2),
            "crash": (3, 1, 3), "recover": (3, 1, 3)}
ERROR_PREFIX = "D2C_TEST_ONLY_AFTER_REAL_WRITE:"
PROOF_PREFIX = "D2C_STREAM_PROOF="


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def read_json(path: Path) -> dict:
    require(path.is_file() and path.stat().st_size <= 1_048_576, f"Missing or oversized test state: {path.name}")
    return json.loads(path.read_text())


def atomic_json(path: Path, value: dict) -> None:
    require(not path.exists(), f"Test evidence already exists: {path.name}")
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("x") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def checkpoint_state(checkpoint: Path) -> dict:
    def logs(kind: str) -> list[int]:
        directory = checkpoint / kind
        return sorted(int(path.name) for path in directory.iterdir()
                      if path.is_file() and path.name.isdigit()) if directory.exists() else []

    return {"offsets": logs("offsets"), "commits": logs("commits")}


def manifest_digest(manifest: dict) -> str:
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def verify_checkpoint_window(checkpoint: Path, phase: str) -> dict:
    result = checkpoint_state(checkpoint)
    expected_offsets = [0] if phase == "initial" else [0, 1] if phase == "resume" else [0, 1, 2]
    expected_commits = [0] if phase == "initial" else [0, 1] if phase in ("resume", "crash") else [0, 1, 2]
    require(result == {"offsets": expected_offsets, "commits": expected_commits},
            f"Unexpected {phase} checkpoint commit window: {result}")
    return result


def load_manifest(state: Path, phase: str, owner: str, image_id: str) -> tuple[dict, list[dict]]:
    path = state / "manifest.json"
    previous = []
    if phase == "initial":
        require(not path.exists(), "Initial phase refuses an existing manifest")
        require(checkpoint_state(state / "checkpoint") == {"offsets": [], "commits": []}, "Initial checkpoint is not fresh")
        require(not any((state / "warehouse").iterdir()), "Initial warehouse is not fresh")
        require(not any((state / "proofs").iterdir()), "Initial proofs directory is not fresh")
        events = {label: build_application_approved_event(application_id=42 + index, user_id=7 + index, campaign_id=3)
                  for index, label in enumerate(("A", "B", "C"))}
        payloads = {label: canonicalize_application_approved_event(event) for label, event in events.items()}
        payloads["invalid"] = json.dumps({**events["A"], "event_id": "not-a-uuid"}, sort_keys=True, separators=(",", ":"))
        contract = Path(d2c_contract.__file__).resolve()
        require(contract.is_relative_to(Path("/opt/d2c-lakehouse")), "Contract module did not come from the final image")
        manifest = {"schema_version": 1, "owner": owner, "image_id": image_id,
                    "events": events, "payloads": payloads, "kafka_base_timestamp_ms": int(time.time() * 1000),
                    "production_contract_sha256": hashlib.sha256(contract.read_bytes()).hexdigest()}
        atomic_json(path, manifest)
    else:
        manifest = read_json(path)
        require(manifest.get("owner") == owner and manifest.get("image_id") == image_id, "State belongs to another image or test owner")
        for preceding in PHASES[:PHASES.index(phase)]:
            proof = read_json(state / "proofs" / f"{preceding}.json")
            require(proof.get("phase") == preceding and proof.get("status") == "PASS"
                    and proof.get("owner") == owner and proof.get("image_id") == image_id,
                    "Previous phase proof identity or result changed")
            require(proof.get("manifest_sha256") == manifest_digest(manifest), "Input manifest changed after a preceding phase")
            previous.append(proof)
        verify_checkpoint_window(state / "checkpoint", PHASES[PHASES.index(phase) - 1])
    return manifest, previous


def publish(spark: Any, bootstrap: str, manifest: dict, phase: str) -> dict:
    jvm = spark._jvm
    properties = jvm.java.util.Properties()
    for name, value in {
        "bootstrap.servers": bootstrap, "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer", "compression.type": "lz4",
        "acks": "all", "enable.idempotence": "true", "max.block.ms": "15000",
        "delivery.timeout.ms": "30000", "request.timeout.ms": "10000", "retries": "2",
        "client.id": "d2c-security-roundtrip",
    }.items():
        properties.setProperty(name, value)
    producer = jvm.org.apache.kafka.clients.producer.KafkaProducer(properties)
    delivered = []
    try:
        partitions = producer.partitionsFor(TOPIC)
        require(partitions.size() == 1 and partitions.get(0).partition() == 0,
                "The fresh application topic must have exactly partition zero")
        for label, offset in zip(DELIVERIES[phase], OFFSETS[phase]):
            payload = manifest["payloads"][label]
            key = "invalid-uuid" if label == "invalid" else manifest["events"][label]["event_id"]
            # Millisecond timestamps exceed Integer.MAX_VALUE, so Py4J supplies
            # the Long required by ProducerRecord(String,Integer,Long,K,V).
            record = jvm.org.apache.kafka.clients.producer.ProducerRecord(
                TOPIC, 0, int(manifest["kafka_base_timestamp_ms"]) + offset, key, payload)
            metadata = producer.send(record).get(35, jvm.java.util.concurrent.TimeUnit.SECONDS)
            require(metadata.topic() == TOPIC and metadata.partition() == 0 and metadata.offset() == offset,
                    "Kafka assigned unexpected source coordinates; topic was not fresh")
            delivered.append({"label": label, "partition": 0, "offset": offset,
                              "payload_sha256": hashlib.sha256(payload.encode()).hexdigest()})
        producer.flush()
        factory = jvm.net.jpountz.lz4.LZ4Factory.fastestJavaInstance()
        return {"compression": "lz4", "records": delivered,
                "kafka_version": jvm.org.apache.kafka.common.utils.AppInfoParser.getVersion(),
                "producer_code_source": producer.getClass().getProtectionDomain().getCodeSource().getLocation().toString(),
                "lz4_code_source": factory.getClass().getProtectionDomain().getCodeSource().getLocation().toString()}
    finally:
        producer.close(jvm.java.time.Duration.ofSeconds(5))


def tables(settings: Any) -> tuple[tuple[str, str], ...]:
    return (("approvals", settings.approvals_table), ("quarantine", settings.dlq_table),
            ("audit", settings.batch_audit_table))


def snapshots(spark: Any, settings: Any) -> dict:
    result = {}
    for label, table in tables(settings):
        spark.catalog.refreshTable(table)
        result[label] = sorted(int(row["snapshot_id"]) for row in
                               spark.table(f"{table}.snapshots").select("snapshot_id").collect())
    return result


def checked_state(spark: Any, settings: Any, manifest: dict, phase: str) -> dict:
    expected_approvals, expected_quarantined, expected_audits = EXPECTED[phase]
    for _, table in tables(settings):
        spark.catalog.refreshTable(table)
    result = validate(spark, settings, {"D2C_LAKEHOUSE_EXPECTED_APPROVALS": str(expected_approvals),
                                        "D2C_LAKEHOUSE_EXPECTED_QUARANTINED": str(expected_quarantined)})
    require(result["batch_audit_rows"] == expected_audits, "Incorrect durable batch audit count")
    require(all(result[name] == 0 for name in ("duplicate_event_ids", "invalid_audit_rows", "duplicate_audit_ranges")),
            "Production reconciliation reported a violation")
    expected_facts = {}
    for label, offset in (("A", 0), ("B", 3), ("C", 5))[:expected_approvals]:
        event = manifest["events"][label]
        expected_facts[event["event_id"]] = {"payload_json": manifest["payloads"][label], "kafka_topic": TOPIC,
                                            "kafka_partition": 0, "kafka_offset": offset, **event["data"]}
    selected = ("event_id", "payload_json", "kafka_topic", "kafka_partition", "kafka_offset", "application_id", "user_id", "campaign_id")
    actual_facts = {row["event_id"]: {key: row[key] for key in selected if key != "event_id"}
                    for row in spark.table(settings.approvals_table).select(*selected).collect()}
    require(actual_facts == expected_facts, "Durable facts differ from immutable event payloads or expected offsets A0/B3/C5")
    quarantined = [row.asDict() for row in spark.table(settings.dlq_table).select(
        "kafka_topic", "kafka_partition", "kafka_offset", "raw_payload", "failure_code").collect()]
    require(quarantined == [{"kafka_topic": TOPIC, "kafka_partition": 0, "kafka_offset": 2,
                              "raw_payload": manifest["payloads"]["invalid"], "failure_code": "contract_violation"}],
            "Quarantine lost the invalid UUID payload or source offset two")
    audit = sorted((row.asDict() for row in spark.table(settings.batch_audit_table).select(
        "stream_id", "spark_batch_id", "source_records", "valid_records", "quarantined_records",
        "duplicate_records", "source_offset_ranges").collect()), key=lambda row: row["spark_batch_id"])
    for row, (batch_id, start, end, valid, invalid, within_duplicates) in zip(audit, (
            (0, 0, 2, 2, 1, 1), (1, 3, 4, 2, 0, 0), (2, 5, 5, 1, 0, 0))):
        require(row["stream_id"] == settings.stream_id and row["spark_batch_id"] == batch_id,
                "Durable audit batch/stream identity changed")
        require((row["source_records"], row["valid_records"], row["quarantined_records"], row["duplicate_records"])
                == (end - start + 1, valid, invalid, within_duplicates), "Audit source classification differs")
        require(json.loads(row["source_offset_ranges"]) == [{"topic": TOPIC, "partition": 0,
                "start_offset": start, "end_offset": end, "records": end - start + 1}], "Audit offsets differ")
    source_records = sum(int(row["source_records"]) for row in audit)
    valid_deliveries = sum(int(row["valid_records"]) for row in audit)
    invalid_deliveries = sum(int(row["quarantined_records"]) for row in audit)
    within_duplicates = sum(int(row["duplicate_records"]) for row in audit)
    duplicate_deliveries = valid_deliveries - expected_approvals
    require(source_records == expected_approvals + invalid_deliveries + duplicate_deliveries,
            "Source audit does not reconcile distinct valid, invalid and duplicate deliveries")
    return {"reconciliation": result, "facts": actual_facts, "audit": audit,
            "accounting": {"source_records": source_records, "valid_deliveries": valid_deliveries,
                           "distinct_valid_events": expected_approvals, "invalid_deliveries": invalid_deliveries,
                           "duplicate_deliveries": duplicate_deliveries, "within_batch_duplicates": within_duplicates,
                           "cross_batch_duplicates": duplicate_deliveries - within_duplicates}}


def physical_parquet(spark: Any, settings: Any, warehouse: Path, manifest: dict) -> dict:
    result = {}
    for label, table in tables(settings):
        files = spark.table(f"{table}.files").select("file_path", "file_format", "content").collect()
        require(bool(files), f"No physical data files for {label}")
        paths = []
        for row in files:
            location = urlparse(row["file_path"])
            require(row["file_format"] == "PARQUET" and int(row["content"]) == 0,
                    "Physical readback requires the committed data-only Parquet files")
            require(location.scheme == "file" and not location.netloc
                    and Path(unquote(location.path)).resolve().is_relative_to(warehouse.resolve()),
                    "Physical data file escaped the dedicated test warehouse")
            paths.append(row["file_path"])
        frame = spark.read.parquet(*paths)
        count = frame.count()
        require(count == {"approvals": 3, "quarantine": 1, "audit": 3}[label], f"Physical Parquet row count differs for {label}")
        if label == "approvals":
            found = {row["event_id"]: (row["payload_json"], int(row["kafka_offset"]))
                     for row in frame.select("event_id", "payload_json", "kafka_offset").collect()}
            expected = {manifest["events"][name]["event_id"]: (manifest["payloads"][name], offset)
                        for name, offset in (("A", 0), ("B", 3), ("C", 5))}
            require(found == expected, "Physical Parquet payload or source offset differs")
        result[label] = {"rows": count, "committed_files": len(paths)}
    return result


def run_phase(args: argparse.Namespace) -> dict:
    state = args.state_dir
    image_id, owner = os.environ.get("D2C_SECURITY_IMAGE_ID", ""), os.environ.get("D2C_SECURITY_OWNER", "")
    require(state == Path("/state") and state.is_dir(), "Only the newly mounted /state volume is supported")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is not None, "Final immutable image identity is required")
    require(re.fullmatch(r"[a-zA-Z0-9_-]{8,128}", owner) is not None, "Dedicated owner token is required")
    require(args.bootstrap_server == "broker:9092", "Only this test network's broker alias is supported")
    require(10 <= args.query_timeout_seconds <= 600, "Query timeout must be 10..600 seconds")
    manifest, previous = load_manifest(state, args.phase, owner, image_id)
    settings = settings_from_environment({"KAFKA_BOOTSTRAP_SERVERS": args.bootstrap_server,
        "D2C_LAKEHOUSE_CATALOG": "d2c_security", "D2C_LAKEHOUSE_NAMESPACE": "roundtrip",
        "D2C_LAKEHOUSE_STREAM_ID": "d2c-kafka-checkpoint-regression",
        "D2C_LAKEHOUSE_WAREHOUSE": (state / "warehouse").as_uri(),
        "D2C_LAKEHOUSE_CHECKPOINT": str(state / "checkpoint")})
    spark = create_spark_session(settings, local_master="local[1]")
    query = None
    try:
        require(spark.sparkContext.master == "local[1]", "Regression requires the bounded local[1] master")
        spark_conf_master = spark.sparkContext.getConf().get("spark.master")
        require(spark_conf_master == "local[1]", "Regression requires spark.master to remain local[1]")
        spark.sparkContext.setLogLevel("WARN")
        create_tables(spark, settings)
        marker_path = state / "proofs/crash-marker.json"
        marker = read_json(marker_path) if args.phase == "recover" else None
        if marker is not None:
            require(marker.get("owner") == owner and marker.get("batch_id") == 2
                    and marker.get("image_id") == image_id and marker.get("simulation") == ERROR_PREFIX + owner,
                    "Crash marker does not belong to this recovery")
            require(snapshots(spark, settings) == marker["snapshots"], "State changed between failed and recovery processes")
        publishing = publish(spark, args.bootstrap_server, manifest, args.phase)
        observed = []
        simulation = ERROR_PREFIX + owner

        def consume(batch: Any, batch_id: int) -> None:
            expected_batch = 0 if args.phase == "initial" else 1 if args.phase == "resume" else 2
            require(int(batch_id) == expected_batch, "Unexpected callback batch ID")
            coordinates = sorted((row["kafka_topic"], int(row["kafka_partition"]), int(row["kafka_offset"]))
                                 for row in batch.select("kafka_topic", "kafka_partition", "kafka_offset").collect())
            require(coordinates == [(TOPIC, 0, offset) for offset in OFFSETS[args.phase]], "Real Kafka source delivered unexpected offsets")
            write_batch(batch, int(batch_id), settings)
            observed.append({"batch_id": int(batch_id), "offsets": [item[2] for item in coordinates]})
            if args.phase == "crash":
                atomic_json(marker_path, {"owner": owner, "image_id": image_id, "batch_id": int(batch_id),
                    "simulation": simulation, "snapshots": snapshots(batch.sparkSession, settings),
                    "state": checked_state(batch.sparkSession, settings, manifest, "crash")})
                raise RuntimeError(simulation)

        query = (build_parsed_stream(spark, settings).writeStream.queryName(settings.stream_id)
                 .option("checkpointLocation", settings.checkpoint_location).foreachBatch(consume)
                 .trigger(availableNow=True).start())
        caught = None
        try:
            finished = query.awaitTermination(args.query_timeout_seconds)
            require(finished, "Structured Streaming query exceeded its phase budget")
        except Exception as error:
            if args.phase != "crash":
                raise
            caught = error
        if args.phase == "crash":
            require(caught is not None and f"RuntimeError: {simulation}" in str(caught), "Query did not fail for the exact test-only simulation")
            marker = read_json(marker_path)
            require(marker.get("simulation") == simulation and marker.get("image_id") == image_id,
                    "A durable marker from the completed production write is required")
        require(not query.isActive and len(observed) == 1, "Expected exactly one completed production callback")
        commit_window = verify_checkpoint_window(state / "checkpoint", args.phase)
        checked = checked_state(spark, settings, manifest, args.phase)
        snapshot_ids = snapshots(spark, settings)
        if args.phase == "recover":
            require(checked == marker["state"] and snapshot_ids == marker["snapshots"], "Uncommitted offset replay changed rows, audit or snapshots")
            require(checked["accounting"] == {"source_records": 6, "valid_deliveries": 5,
                "distinct_valid_events": 3, "invalid_deliveries": 1, "duplicate_deliveries": 2,
                "within_batch_duplicates": 1, "cross_batch_duplicates": 1}, "Final six-source-event reconciliation differs")
        identity = {"query_id": str(query.id), "run_id": str(query.runId),
                    "spark_application_id": spark.sparkContext.applicationId}
        if previous:
            require(identity["query_id"] == previous[0]["identity"]["query_id"], "Checkpoint query identity did not survive the process restart")
            for proof in previous:
                require(identity["run_id"] != proof["identity"]["run_id"]
                        and identity["spark_application_id"] != proof["identity"]["spark_application_id"],
                        "Each phase must use a fresh query run and Spark application process")
        proof = {"schema_version": 1, "test": "d2c_real_kafka_checkpoint_roundtrip", "phase": args.phase,
            "status": "PASS", "owner": owner, "image_id": image_id, "identity": identity,
            "manifest_sha256": manifest_digest(manifest),
            "spark_version": spark.version, "java_version": spark._jvm.java.lang.System.getProperty("java.version"),
            "machine": os.uname().machine, "master": spark.sparkContext.master, "spark_conf_master": spark_conf_master,
            "publishing": publishing, "observed_batches": observed, "checkpoint": commit_window,
            "state": checked, "snapshots": snapshot_ids,
            "scope": "Real Kafka/LZ4 and four separate Spark processes; test-only failure after production sink write and before checkpoint commit"}
        if caught is not None:
            text = str(caught)
            location = text.index(f"RuntimeError: {simulation}")
            proof["expected_failure"] = {"exception_type": type(caught).__name__,
                                         "cause_excerpt": text[max(0, location - 200):location + 1000],
                                         "full_message_sha256": hashlib.sha256(text.encode()).hexdigest(),
                                         "exact_simulation": simulation, "production_write_completed": True}
        if args.phase == "recover":
            proof["physical_parquet"] = physical_parquet(spark, settings, state / "warehouse", manifest)
        atomic_json(state / "proofs" / f"{args.phase}.json", proof)
        return proof
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=PHASES, required=True)
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--query-timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    try:
        proof = run_phase(args)
    except Exception as error:
        print(PROOF_PREFIX + json.dumps({"schema_version": 1, "phase": args.phase, "status": "FAIL",
              "owner": os.environ.get("D2C_SECURITY_OWNER"), "image_id": os.environ.get("D2C_SECURITY_IMAGE_ID"),
              "exception_type": type(error).__name__, "reason": str(error)[:2000]}, sort_keys=True), flush=True)
        raise
    print(PROOF_PREFIX + json.dumps(proof, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
