"""Offline harness checks; these do not execute Spark, Kafka or Docker."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch


PHASE_FILE = Path(__file__).parent / "integration/lakehouse_kafka_roundtrip.py"
SPEC = importlib.util.spec_from_file_location("lakehouse_kafka_roundtrip", PHASE_FILE)
phase = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase)
IID = "sha256:" + "1" * 64
OWNER = "testowner01234567"


def manifest():
    events = {label: phase.build_application_approved_event(application_id=42 + index, user_id=7 + index, campaign_id=3)
              for index, label in enumerate(("A", "B", "C"))}
    payloads = {label: phase.canonicalize_application_approved_event(event) for label, event in events.items()}
    payloads["invalid"] = json.dumps({**events["A"], "event_id": "not-a-uuid"}, sort_keys=True, separators=(",", ":"))
    return {"events": events, "payloads": payloads, "kafka_base_timestamp_ms": 1_793_750_000_000,
            "owner": OWNER, "image_id": IID}


def checkpoint(root, offsets, commits):
    for kind, batches in (("offsets", offsets), ("commits", commits)):
        directory = root / "checkpoint" / kind
        directory.mkdir(parents=True, exist_ok=True)
        for batch in batches:
            (directory / str(batch)).write_text("v1\n{}\n")


class Row(dict):
    def asDict(self):
        return dict(self)


class Frame:
    def __init__(self, rows):
        self.rows = [Row(row) for row in rows]

    def select(self, *names):
        return Frame([{name: row[name] for name in names} for row in self.rows])

    def collect(self):
        return self.rows


class DedicatedState:
    """Map the container-only /state contract to an offline temporary directory."""
    def __init__(self, root):
        self.root = root

    def __eq__(self, other):
        return other == Path("/state")

    def is_dir(self):
        return self.root.is_dir()

    def __truediv__(self, child):
        return self.root / child


class PhaseBoundaryTest(unittest.TestCase):
    def test_every_phase_selects_and_checks_the_exact_bounded_master_before_sink_or_kafka_io(self):
        for label in phase.PHASES:
            for effective, configured in (("local[1]", "local[1]"), ("local[2]", "local[1]"),
                                          ("local[1]", "local[*]"), ("local[1]", "yarn")):
                with self.subTest(phase=label, effective=effective, configured=configured), TemporaryDirectory() as directory:
                    spark = MagicMock()
                    spark.sparkContext.master = effective
                    spark.sparkContext.getConf.return_value.get.return_value = configured
                    args = SimpleNamespace(state_dir=DedicatedState(Path(directory)), phase=label,
                                           bootstrap_server="broker:9092", query_timeout_seconds=30)
                    with patch.dict(phase.os.environ, {"D2C_SECURITY_IMAGE_ID": IID, "D2C_SECURITY_OWNER": OWNER}), \
                            patch.object(phase, "load_manifest", return_value=(manifest(), [])), \
                            patch.object(phase, "create_spark_session", return_value=spark) as factory, \
                            patch.object(phase, "create_tables", side_effect=RuntimeError("stop after master guards")) as tables, \
                            patch.object(phase, "publish") as publish:
                        if effective == configured == "local[1]":
                            with self.assertRaisesRegex(RuntimeError, "stop after master guards"):
                                phase.run_phase(args)
                            tables.assert_called_once()
                        else:
                            with self.assertRaisesRegex(AssertionError, "local\\[1\\]"):
                                phase.run_phase(args)
                            tables.assert_not_called()
                        self.assertEqual(factory.call_args.kwargs, {"local_master": "local[1]"})
                        publish.assert_not_called()
                    spark.stop.assert_called_once()

    def test_uncommitted_batch_two_is_required_then_recovered(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            checkpoint(root, [0, 1, 2], [0, 1])
            (root / "checkpoint/offsets/.2.crc").write_bytes(b"not a log")
            self.assertEqual(phase.verify_checkpoint_window(root / "checkpoint", "crash")["commits"], [0, 1])
            with self.assertRaisesRegex(AssertionError, "commit window"):
                phase.verify_checkpoint_window(root / "checkpoint", "recover")
            (root / "checkpoint/commits/2").write_text("v1\n{}\n")
            self.assertEqual(phase.verify_checkpoint_window(root / "checkpoint", "recover")["commits"], [0, 1, 2])
            with self.assertRaisesRegex(AssertionError, "commit window"):
                phase.verify_checkpoint_window(root / "checkpoint", "crash")

    def test_durable_evidence_refuses_overwriting_a_previous_phase(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "proof.json"
            phase.atomic_json(path, {"status": "PASS"})
            self.assertEqual(phase.read_json(path), {"status": "PASS"})
            with self.assertRaisesRegex(AssertionError, "already exists"):
                phase.atomic_json(path, {"status": "FAIL"})

    def test_resume_refuses_other_owners_state(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            phase.atomic_json(root / "manifest.json", manifest())
            with self.assertRaisesRegex(AssertionError, "another image or test owner"):
                phase.load_manifest(root, "resume", "another-owner", IID)

    def test_resume_refuses_payload_manifest_changes_after_initial_proof(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "proofs").mkdir()
            data = manifest()
            phase.atomic_json(root / "manifest.json", data)
            phase.atomic_json(root / "proofs/initial.json", {"phase": "initial", "status": "PASS",
                "owner": OWNER, "image_id": IID, "manifest_sha256": phase.manifest_digest(data)})
            data["payloads"]["B"] = "unexpected changed event"
            (root / "manifest.json").write_text(json.dumps(data))
            with self.assertRaisesRegex(AssertionError, "manifest changed"):
                phase.load_manifest(root, "resume", OWNER, IID)

    def sink_fixture(self):
        data = manifest()
        settings = phase.settings_from_environment({})
        facts = [{"event_id": data["events"][name]["event_id"], "payload_json": data["payloads"][name],
                  "kafka_topic": phase.TOPIC, "kafka_partition": 0, "kafka_offset": offset,
                  **data["events"][name]["data"]} for name, offset in (("A", 0), ("B", 3), ("C", 5))]
        quarantine = [{"kafka_topic": phase.TOPIC, "kafka_partition": 0, "kafka_offset": 2,
                       "raw_payload": data["payloads"]["invalid"], "failure_code": "contract_violation"}]
        audit = [{"stream_id": settings.stream_id, "spark_batch_id": batch,
                  "source_records": end - start + 1, "valid_records": valid, "quarantined_records": invalid,
                  "duplicate_records": duplicates, "source_offset_ranges": json.dumps([{
                      "topic": phase.TOPIC, "partition": 0, "start_offset": start,
                      "end_offset": end, "records": end - start + 1}])}
                 for batch, start, end, valid, invalid, duplicates in
                 ((0, 0, 2, 2, 1, 1), (1, 3, 4, 2, 0, 0), (2, 5, 5, 1, 0, 0))]
        frames = {settings.approvals_table: Frame(facts), settings.dlq_table: Frame(quarantine),
                  settings.batch_audit_table: Frame(audit)}
        spark = MagicMock()
        spark.table.side_effect = frames.__getitem__
        validated = {"approvals": 3, "quarantined": 1, "duplicate_event_ids": 0,
                     "invalid_audit_rows": 0, "duplicate_audit_ranges": 0, "batch_audit_rows": 3}
        return spark, settings, data, frames, validated

    def test_six_source_records_reconcile_cross_batch_and_within_batch_duplicates(self):
        spark, settings, data, _frames, validated = self.sink_fixture()
        with patch.object(phase, "validate", return_value=validated) as validator:
            result = phase.checked_state(spark, settings, data, "recover")
        self.assertEqual(result["accounting"], {"source_records": 6, "valid_deliveries": 5,
            "distinct_valid_events": 3, "invalid_deliveries": 1, "duplicate_deliveries": 2,
            "within_batch_duplicates": 1, "cross_batch_duplicates": 1})
        validator.assert_called_once()

    def test_immutable_payload_and_expected_kafka_offset_must_both_match(self):
        for column, wrong in (("payload_json", "changed payload"), ("kafka_offset", 99)):
            with self.subTest(column=column):
                spark, settings, data, frames, validated = self.sink_fixture()
                frames[settings.approvals_table].rows[0][column] = wrong
                with patch.object(phase, "validate", return_value=validated), self.assertRaisesRegex(AssertionError, "immutable event payloads"):
                    phase.checked_state(spark, settings, data, "recover")

    def kafka(self, offsets):
        spark = MagicMock()
        jvm = spark._jvm
        producer = jvm.org.apache.kafka.clients.producer.KafkaProducer.return_value
        producer.partitionsFor.return_value.size.return_value = 1
        producer.partitionsFor.return_value.get.return_value.partition.return_value = 0
        futures = []
        for offset in offsets:
            future = MagicMock()
            future.get.return_value.topic.return_value = phase.TOPIC
            future.get.return_value.partition.return_value = 0
            future.get.return_value.offset.return_value = offset
            futures.append(future)
        producer.send.side_effect = futures
        jvm.org.apache.kafka.common.utils.AppInfoParser.getVersion.return_value = "3.9.2"
        producer.getClass.return_value.getProtectionDomain.return_value.getCodeSource.return_value.getLocation.return_value.toString.return_value = "file:/opt/spark/jars/kafka-clients.jar"
        factory = jvm.net.jpountz.lz4.LZ4Factory.fastestJavaInstance.return_value
        factory.getClass.return_value.getProtectionDomain.return_value.getCodeSource.return_value.getLocation.return_value.toString.return_value = "file:/opt/spark/jars/lz4-java.jar"
        return spark, producer

    def test_java_producer_uses_lz4_and_real_expected_kafka_coordinates(self):
        spark, producer = self.kafka([0, 1, 2])
        events = manifest()
        proof = phase.publish(spark, "broker:9092", events, "initial")
        properties = dict(call.args for call in spark._jvm.java.util.Properties.return_value.setProperty.call_args_list)
        self.assertEqual(properties["compression.type"], "lz4")
        self.assertEqual(properties["key.serializer"], "org.apache.kafka.common.serialization.StringSerializer")
        self.assertEqual([row["offset"] for row in proof["records"]], [0, 1, 2])
        records = spark._jvm.org.apache.kafka.clients.producer.ProducerRecord.call_args_list
        self.assertEqual(records[0].args[2], events["kafka_base_timestamp_ms"])
        self.assertEqual(records[2].args[-1], events["payloads"]["invalid"])
        producer.close.assert_called_once()

    def test_recovery_publishes_no_new_message(self):
        spark, producer = self.kafka([])
        proof = phase.publish(spark, "broker:9092", manifest(), "recover")
        producer.send.assert_not_called()
        self.assertEqual(proof["records"], [])
        producer.close.assert_called_once()

    def test_wrong_source_offsets_fail_and_close_the_producer(self):
        spark, producer = self.kafka([9])
        with self.assertRaisesRegex(AssertionError, "source coordinates"):
            phase.publish(spark, "broker:9092", manifest(), "initial")
        producer.close.assert_called_once()

    def crash_fixture(self, root, unexpected_failure=False):
        (root / "proofs").mkdir()
        checkpoint(root, [0, 1, 2], [0, 1])
        spark = MagicMock()
        spark.sparkContext.master = "local[1]"
        spark.sparkContext.getConf.return_value.get.return_value = "local[1]"
        spark.sparkContext.applicationId = "local-new-process"
        spark.version = "4.2.1"
        spark._jvm.java.lang.System.getProperty.return_value = "21.0.13"
        query = MagicMock()
        query.id, query.runId = "stable-checkpoint-query", "fresh-run-id"
        query.isActive = True
        batch = MagicMock()
        batch.select.return_value.collect.return_value = [{"kafka_topic": phase.TOPIC, "kafka_partition": 0, "kafka_offset": 5}]
        parsed = MagicMock()
        writer = parsed.writeStream.queryName.return_value
        writer.option.return_value = writer
        writer.foreachBatch.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = query
        callback = None

        def bind(consume):
            nonlocal callback
            callback = consume
            return writer

        writer.foreachBatch.side_effect = bind

        def await_query(_timeout):
            query.isActive = False
            if unexpected_failure:
                raise RuntimeError("unrelated Kafka failure")
            try:
                callback(batch, 2)
            except RuntimeError as error:
                raise RuntimeError("RuntimeError: " + str(error)) from error
            return True

        query.awaitTermination.side_effect = await_query
        identity = {"query_id": query.id, "run_id": "previous-run", "spark_application_id": "local-previous-process"}
        previous = [{"identity": identity}]
        args = SimpleNamespace(state_dir=DedicatedState(root), phase="crash", bootstrap_server="broker:9092", query_timeout_seconds=30)
        return args, spark, parsed, batch, previous

    def test_only_the_exact_after_write_simulation_passes_crash_phase(self):
        for unexpected in (False, True):
            with self.subTest(unexpected=unexpected), TemporaryDirectory() as directory:
                root = Path(directory)
                args, spark, parsed, batch, previous = self.crash_fixture(root, unexpected)
                with patch.dict(phase.os.environ, {"D2C_SECURITY_IMAGE_ID": IID, "D2C_SECURITY_OWNER": OWNER}), \
                        patch.object(phase, "load_manifest", return_value=(manifest(), previous)), \
                        patch.object(phase, "create_spark_session", return_value=spark), \
                        patch.object(phase, "create_tables"), patch.object(phase, "publish", return_value={"compression": "lz4"}), \
                        patch.object(phase, "build_parsed_stream", return_value=parsed), \
                        patch.object(phase, "write_batch") as writer, \
                        patch.object(phase, "checked_state", return_value={"verified_state": True}), \
                        patch.object(phase, "snapshots", return_value={"approvals": [1], "quarantine": [2], "audit": [3]}):
                    if unexpected:
                        with self.assertRaisesRegex(AssertionError, "exact test-only simulation"):
                            phase.run_phase(args)
                        writer.assert_not_called()
                    else:
                        proof = phase.run_phase(args)
                        self.assertEqual(proof["status"], "PASS")
                        self.assertEqual(proof["spark_conf_master"], "local[1]")
                        self.assertTrue(proof["expected_failure"]["production_write_completed"])
                        self.assertEqual(proof["observed_batches"], [{"batch_id": 2, "offsets": [5]}])
                        self.assertEqual(writer.call_args.args[:2], (batch, 2))
                        self.assertEqual(proof["checkpoint"], {"offsets": [0, 1, 2], "commits": [0, 1]})
                        self.assertTrue((root / "proofs/crash.json").is_file())
                spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
