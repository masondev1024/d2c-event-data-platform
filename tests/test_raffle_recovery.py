"""Bounded read contract, reconciliation gates and opt-in Spark/Iceberg recovery drill."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from collections import Counter
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch


from lakehouse import raffle_recovery as job


def bounds_fixture():
    return {
        "start": {"0": 7, "1": 0, "2": 5}, "lower": {"0": 17, "1": 12, "2": 5},
        "upper": {"0": 2517, "1": 12, "2": 6}, "end": {"0": 2517, "1": 19, "2": 9},
    }


class RangeTests(unittest.TestCase):
    def test_bounds_are_explicit_ordered_int64s_and_fixed_target_size(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "bounds.json")
            valid = bounds_fixture()
            path.write_text(json.dumps(valid))
            self.assertEqual(job.load_bounds(path), valid)
            for edge, change in (
                ("lower", {"1": 13}), ("end", {"2": 5}), ("start", {"1": True}),
                ("start", {"1": -1}), ("end", {"1": 2**63}), ("end", {"3": 1}),
                ("start", {"0": "7"}), ("end", {"2": None}), ("upper", {"0": 18}),
            ):
                with self.subTest(edge=edge, change=change):
                    bad = bounds_fixture()
                    bad[edge].update(change)
                    path.write_text(json.dumps(bad))
                    with self.assertRaises(ValueError):
                        job.load_bounds(path)
            del valid["upper"]["1"]
            path.write_text(json.dumps(valid))
            with self.assertRaises(ValueError):
                job.load_bounds(path)

    def test_reader_never_subscribes_or_uses_kafka_sentinels(self):
        spark = Mock()
        reader = spark.read.format.return_value
        reader.option.return_value = reader
        settings = job.raffle_settings({"KAFKA_BOOTSTRAP_SERVERS": "broker:1234"})
        bounds = bounds_fixture()
        job.read_range(spark, settings, bounds["lower"], bounds["upper"])
        spark.read.format.assert_called_once_with("kafka")
        options = dict(c.args for c in reader.option.call_args_list)
        self.assertEqual(set(options), {"assign", "startingOffsets", "endingOffsets",
                                       "failOnDataLoss", "kafka.bootstrap.servers"})
        self.assertEqual(json.loads(options["assign"]), {job.TOPIC: [0, 1, 2]})
        for option, edge in (("startingOffsets", "lower"), ("endingOffsets", "upper")):
            self.assertEqual(json.loads(options[option]), {job.TOPIC: bounds[edge]})
        self.assertEqual(options["failOnDataLoss"], "true")
        self.assertEqual(options["kafka.bootstrap.servers"], "broker:1234")

    def test_rejects_missing_duplicate_shifted_and_foreign_coordinates(self):
        bounds = bounds_fixture()
        a = dict(topic=job.TOPIC, partition=0, n=2500, distinct_offsets=2500, first_offset=17, last_offset=2516)
        b = dict(topic=job.TOPIC, partition=2, n=1, distinct_offsets=1, first_offset=5, last_offset=5)
        def check(rows):
            return job.verify_coordinates(rows, bounds["lower"], bounds["upper"])

        self.assertEqual(check([a, b]), {"0": 2500, "1": 0, "2": 1})
        for rows in ([a], [b, {**a, "distinct_offsets": 2499}], [b, {**a, "n": 2501}],
                     [b, {**a, "first_offset": 18, "last_offset": 2517}],
                     [b, {**a, "partition": 3}], [b, {**a, "topic": "other"}]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                check(rows)


class AggregateTests(unittest.TestCase):
    def test_full_key_join_catches_compensating_window_errors_and_duplicates(self):
        # Execute the same full-join aggregate locally on already bucketed inputs.
        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE expected(item_id INT, window_start_utc INT)")
            db.execute("CREATE TABLE observed AS SELECT * FROM expected")
            db.execute("INSERT INTO expected VALUES (1,0),(1,0),(2,0),(1,300)")
            db.execute("INSERT INTO observed VALUES (1,0),(2,0),(2,0),(2,300)")

            def sql(query, **tables):
                cursor = db.execute(query.format(**tables))
                row = dict(zip((column[0] for column in cursor.description), cursor.fetchone()))
                return Mock(first=lambda: SimpleNamespace(**row))

            result = job.grouped_counts(SimpleNamespace(sql=sql), "expected", "observed",
                                        ["item_id", "window_start_utc"])
            self.assertEqual(vars(result), dict(source_keys=3, actual_keys=3, actual_rows=4,
                                                missing_keys=1, extra_keys=1, duplicates=1,
                                                different_keys=4, absolute_delta=4))

    def run_stages(self, *, gap=1500, retry_id=22, removed=0):
        bounds = bounds_fixture()
        spark = Mock()
        spark.catalog.tableExists.return_value = False
        settings = job.raffle_settings({})
        source = Mock()
        before, first, retry = Mock(), Mock(), Mock()
        before.where.return_value.count.return_value = 8500
        before.exceptAll.return_value.count.return_value = removed
        for frame in (first, retry):
            frame.where.return_value.count.return_value = 1500
            frame.where.return_value.exceptAll.return_value.count.return_value = 0
        ranges = []

        @contextmanager
        def checked(spark, settings, lo, hi):
            ranges.append((lo, hi))
            yield Mock(), {p: hi[p] - lo[p] for p in lo}

        def reconciliation(spark, source, facts):
            missing = gap if facts is before else 0
            return dict(fact_count=8500 if facts is before else 10000,
                        missing_event_ids=missing, missing_entry_ids=missing, absolute_window_count_delta=missing,
                        extra_event_ids=0, extra_entry_ids=0, duplicate_event_ids=0, duplicate_entry_ids=0,
                        payload_mismatches=0, missing_windows=0, extra_windows=0, mismatched_windows=int(missing > 0))

        with (patch.object(job, "validate_manifest", return_value=10000),
              patch.object(job, "checked_range", checked), patch.object(job, "range_predicate", return_value=MagicMock()),
              patch.object(job, "target_id_counts", return_value=dict(target_unique_event_ids=1500, target_unique_entry_ids=1500)),
              patch.object(job, "write_facts") as write, patch.object(job, "reconcile", side_effect=reconciliation),
              patch.object(job, "current_snapshot", side_effect=[(11, before), (22, first), (retry_id, retry)])):
            try:
                return job.run_drill(spark, settings, source, bounds)
            finally:
                self.ranges = ranges
                self.writes = write.call_count

    def test_gap_is_measured_before_reading_target_and_each_replay_is_isolated(self):
        result = self.run_stages()
        b = bounds_fixture()
        target = (b["lower"], b["upper"])
        self.assertEqual(self.ranges, [(b["start"], b["lower"]), (b["upper"], b["end"]), target, target])
        self.assertEqual(self.writes, 3)
        self.assertEqual(result["before"]["missing_event_ids"], 1500)
        self.assertEqual(result["after_first"], result["after_retry"])
        self.assertEqual(result["after_retry"]["target_physical_records"], 2501)
        with self.assertRaisesRegex(ValueError, "before"):
            self.run_stages(gap=0)
        self.assertEqual(len(self.ranges), 2)
        self.assertEqual(self.writes, 1)
        with self.assertRaisesRegex(ValueError, "after_first"):
            self.run_stages(removed=1)
        self.assertEqual(self.writes, 2)
        with self.assertRaisesRegex(ValueError, "retry changed"):
            self.run_stages(retry_id=23)


@unittest.skipUnless(os.getenv("RAFFLE_TEST_SPARK") == "1", "requires local Spark with Iceberg")
class SparkRecoveryTest(unittest.TestCase):
    def test_real_iceberg_writes_and_keyed_reconciliation(self):
        # Only Kafka transport is replaced. Feed the production writer physical duplicates
        # across three partitions, then time travel to its real committed snapshots.
        with tempfile.TemporaryDirectory() as directory:
            settings = job.raffle_settings({"D2C_LAKEHOUSE_WAREHOUSE": Path(directory, "wh").as_uri()})
            spark = job.create_spark_session(settings)
            self.addCleanup(spark.stop)
            spark.sparkContext.setLogLevel("ERROR")
            records = []
            manifest = Path(directory, "source.jsonl")
            payloads = {}
            with manifest.open("w", encoding="utf-8") as output:
                for n in range(1, 10001):
                    data = dict(entry_id=n, item_id=n % 17 + 1, user_id=n)
                    stamp = "2026-11-21T18:05:00+09:00" if n % 2 else "2026-11-21T09:04:59.999999Z"
                    event_id = f"aaaaaaaa-0000-4000-8000-{n:012x}"
                    body = dict(event_id=event_id, event_type=job.TOPIC, event_version=1, data=data, occurred_at=stamp)
                    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
                    payloads[n] = raw
                    output.write(json.dumps(dict(data, event_id=event_id, occurred_at=stamp,
                                                 payload_sha256=hashlib.sha256(raw).hexdigest())) + "\n")
            offsets = Counter({str(p): 0 for p in job.PARTITIONS})
            bounds = {"start": dict(offsets)}
            for edge, ids in (("lower", range(1, 8001)), ("upper", [*range(8001, 9501), *range(8001, 9002)]),
                              ("end", range(9501, 10001))):
                for n in ids:
                    p = str(n % 3)
                    records.append((job.TOPIC, int(p), offsets[p], payloads[n]))
                    offsets[p] += 1
                bounds[edge] = dict(offsets)

            def read_range(spark, settings, lo, hi):
                rows = [r for r in records if lo[str(r[1])] <= r[2] < hi[str(r[1])]]
                return spark.createDataFrame(rows, "topic STRING, partition INT, offset BIGINT, value BINARY")

            source = job.read_manifest(spark, manifest)
            try:
                with patch.object(job, "read_range", read_range):
                    result = job.run_drill(spark, settings, source, bounds)
                self.assertEqual(result["after_first"], result["after_retry"])
                self.assertEqual(result["after_first"]["source_windows"], 34)

                # Preserve the fact count while corrupting one identity, product/window
                # and fingerprint. Full-join metrics must expose the absent and extra key.
                facts = spark.table(job.fact_table(settings)).where("entry_id <= 2")
                import pyspark.sql.functions as F

                corrupt = (facts.where("entry_id = 1")
                           .withColumn("item_id", F.lit(777))
                           .withColumn("payload_sha256", F.lit("0" * 64)))
                report = job.reconcile(spark, source.where("entry_id <= 2"), corrupt.unionByName(corrupt))
                for name in ("missing_event_ids", "missing_entry_ids", "duplicate_event_ids", "duplicate_entry_ids"):
                    self.assertEqual(report[name], 1)
                self.assertEqual((report["payload_mismatches"], report["missing_windows"], report["extra_windows"]),
                                 (2, 2, 1))
            finally:
                source.unpersist()


if __name__ == "__main__":
    unittest.main()
