"""Prove bounded Kafka range recovery against an independent MySQL JSONL export.

Run in an isolated, single-writer warehouse. Each stage reconciles a pinned Iceberg
snapshot. Only aggregate evidence leaves the job.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path

from lakehouse.raffle_iceberg import create_spark_session, fact_table, raffle_settings, write_facts
from raffle_contract import TOPIC


PARTITIONS = (0, 1, 2)
SOURCE_ROWS = 10_000
NEW_FACTS = 1_500
TARGET_RECORDS = 2_501
MANIFEST_SCHEMA = """
    event_id STRING, entry_id BIGINT, item_id BIGINT, user_id BIGINT,
    occurred_at STRING, payload_sha256 STRING, _corrupt_record STRING
"""


def load_bounds(path):
    with open(path, encoding="utf-8") as handle:
        bounds = json.load(handle)
    edges = ("start", "lower", "upper", "end")
    if not isinstance(bounds, dict) or set(bounds) != set(edges):
        raise ValueError("bounds require start, lower, upper and end")
    for edge in edges:
        offsets = bounds[edge]
        if not isinstance(offsets, dict) or set(offsets) != {str(p) for p in PARTITIONS}:
            raise ValueError(f"{edge} requires offsets for exactly three partitions")
        if any(type(n) is not int or not 0 <= n < 2**63 for n in offsets.values()):
            raise ValueError(f"{edge} requires nonnegative int64 offsets")
    for p in PARTITIONS:
        values = [bounds[edge][str(p)] for edge in edges]
        if values != sorted(values):
            raise ValueError("partition bounds must satisfy start <= lower <= upper <= end")
    if sum(bounds["upper"][str(p)] - bounds["lower"][str(p)] for p in PARTITIONS) != TARGET_RECORDS:
        raise ValueError("target bounds must span 2501 physical records")
    return bounds


def read_range(spark, settings, start, end):
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("assign", json.dumps({TOPIC: PARTITIONS}))
        .option("startingOffsets", json.dumps({TOPIC: start}))
        .option("endingOffsets", json.dumps({TOPIC: end}))
        .option("failOnDataLoss", "true")
        .load().select("topic", "partition", "offset", "value")
    )


def verify_coordinates(groups, start, end):
    """Count, distinct count and extrema together prove dense [start, end) coverage."""
    counts = {str(p): 0 for p in PARTITIONS}
    for row in groups:
        p = str(row["partition"])
        if row["topic"] != TOPIC or p not in counts:
            raise ValueError("unexpected Kafka topic or partition")
        expected = end[p] - start[p]
        if (row["n"], row["distinct_offsets"], row["first_offset"], row["last_offset"]) != (
            expected, expected, start[p], end[p] - 1
        ):
            raise ValueError(f"incomplete or unexpected Kafka coordinates for partition {p}")
        counts[p] = row["n"]
    if any(counts[p] != end[p] - start[p] for p in counts):
        raise ValueError("missing Kafka partition records")
    return counts


@contextmanager
def checked_range(spark, settings, start, end):
    from pyspark.sql import functions as F

    batch = read_range(spark, settings, start, end).persist()
    try:
        groups = batch.groupBy("topic", "partition").agg(
            F.count("*").alias("n"), F.countDistinct("offset").alias("distinct_offsets"),
            F.min("offset").alias("first_offset"), F.max("offset").alias("last_offset"),
        ).collect()
        yield batch, verify_coordinates(groups, start, end)
    finally:
        batch.unpersist()


def target_id_counts(batch):
    from pyspark.sql import functions as F

    # Count identities in the physical read before the insert-only writer can
    # discard redeliveries or matches to existing outside facts.
    value = F.col("value").cast("string")
    return batch.agg(
        F.countDistinct(F.get_json_object(value, "$.event_id")).alias("target_unique_event_ids"),
        F.countDistinct(F.get_json_object(value, "$.data.entry_id")).alias("target_unique_entry_ids"),
    ).first().asDict()


def read_manifest(spark, path):
    from pyspark.sql import functions as F

    # Keep corrupt rows countable without including their payloads in a parser error.
    return (
        spark.read.schema(MANIFEST_SCHEMA).json(str(path))
        .withColumn("occurred_at", F.expr("try_cast(occurred_at AS TIMESTAMP)"))
        .persist()
    )


def validate_manifest(spark, source):
    counts = spark.sql("""
        SELECT count(*) AS rows, count(DISTINCT event_id) AS events,
               count(DISTINCT entry_id) AS entries,
               count_if(_corrupt_record IS NOT NULL OR event_id IS NULL OR entry_id IS NULL
                        OR entry_id <= 0 OR item_id IS NULL OR item_id <= 0
                        OR user_id IS NULL OR user_id <= 0 OR occurred_at IS NULL
                        OR payload_sha256 IS NULL OR length(payload_sha256) <> 64
                        OR payload_sha256 RLIKE '[^0-9a-f]')
                   AS invalid
        FROM {source}
    """, source=source).first()
    if (counts.rows, counts.events, counts.entries, counts.invalid) != (SOURCE_ROWS, SOURCE_ROWS, SOURCE_ROWS, 0):
        raise ValueError(f"invalid source manifest aggregates: {counts.asDict()}")
    return counts.rows


def grouped_counts(spark, source, actual, keys):
    # Both sides aggregate their own complete inputs; the full join retains keys
    # present on either side. Duplicate counts are surplus rows per identity.
    columns = ", ".join(keys)
    return spark.sql(f"""
        WITH s AS (SELECT {columns}, count(*) AS n FROM {{source}} GROUP BY {columns}),
             a AS (SELECT {columns}, count(*) AS n FROM {{actual}} GROUP BY {columns})
        SELECT count(s.n) AS source_keys, count(a.n) AS actual_keys,
               coalesce(sum(a.n), 0) AS actual_rows,
               count(CASE WHEN a.n IS NULL THEN 1 END) AS missing_keys,
               count(CASE WHEN s.n IS NULL THEN 1 END) AS extra_keys,
               coalesce(sum(a.n - 1), 0) AS duplicates,
               count(CASE WHEN coalesce(s.n, 0) <> coalesce(a.n, 0) THEN 1 END) AS different_keys,
               coalesce(sum(abs(coalesce(s.n, 0) - coalesce(a.n, 0))), 0) AS absolute_delta
        FROM s FULL OUTER JOIN a USING ({columns})
    """, source=source, actual=actual).first()


def reconcile(spark, source, facts):
    from pyspark.sql import functions as F

    events = grouped_counts(spark, source, facts, ["event_id"])
    entries = grouped_counts(spark, source, facts, ["entry_id"])
    # Compare the stored columns as well as the producer's canonical fingerprint.
    mismatches = spark.sql("""
        SELECT count(*) AS n FROM {source} s JOIN {facts} a USING (event_id)
        WHERE s.entry_id IS DISTINCT FROM a.entry_id OR s.item_id IS DISTINCT FROM a.item_id
           OR s.user_id IS DISTINCT FROM a.user_id OR s.occurred_at IS DISTINCT FROM a.occurred_at
           OR s.payload_sha256 IS DISTINCT FROM a.payload_sha256
    """, source=source, facts=facts).first().n
    window = F.window("occurred_at", "5 minutes").start.alias("window_start_utc")
    windows = grouped_counts(spark, source.select("item_id", window), facts.select("item_id", window),
                             ["item_id", "window_start_utc"])
    return {
        "fact_count": events.actual_rows,
        "missing_event_ids": events.missing_keys, "extra_event_ids": events.extra_keys,
        "duplicate_event_ids": events.duplicates,
        "missing_entry_ids": entries.missing_keys, "extra_entry_ids": entries.extra_keys,
        "duplicate_entry_ids": entries.duplicates, "payload_mismatches": mismatches,
        "source_windows": windows.source_keys, "actual_windows": windows.actual_keys,
        "missing_windows": windows.missing_keys, "extra_windows": windows.extra_keys,
        "mismatched_windows": windows.different_keys, "absolute_window_count_delta": windows.absolute_delta,
    }


def range_predicate(start, end):
    from pyspark.sql import functions as F

    in_partition = F.lit(False)
    for p in PARTITIONS:
        in_partition |= ((F.col("kafka_partition") == p)
                         & (F.col("kafka_offset") >= start[str(p)])
                         & (F.col("kafka_offset") < end[str(p)]))
    return (F.col("kafka_topic") == TOPIC) & in_partition


def current_snapshot(spark, table):
    ref = spark.table(f"{table}.refs").where("name = 'main'").select("snapshot_id").first()
    if ref is None:
        raise ValueError("raffle table has no committed main snapshot")
    snapshot_id = int(ref.snapshot_id)
    return snapshot_id, spark.sql(f"SELECT * FROM {table} VERSION AS OF {snapshot_id}")


def require_counts(stage, report, **expected):
    if any(report[key] != value for key, value in expected.items()):
        raise ValueError(f"{stage} failed recovery invariants: {json.dumps(report, sort_keys=True)}")


def run_drill(spark, settings, source, bounds):
    source_rows = validate_manifest(spark, source)
    table = fact_table(settings)
    if spark.catalog.tableExists(table) and not spark.table(table).isEmpty():
        raise ValueError("recovery drill requires an initially empty raffle table")

    start, lower, upper, end = (bounds[k] for k in ("start", "lower", "upper", "end"))
    with (
        checked_range(spark, settings, start, lower) as (prefix, prefix_counts),
        checked_range(spark, settings, upper, end) as (suffix, suffix_counts),
    ):
        write_facts(prefix.unionByName(suffix), settings)

    before_id, baseline = current_snapshot(spark, table)
    outside = range_predicate(start, lower) | range_predicate(upper, end)
    target = range_predicate(lower, upper)
    before = reconcile(spark, source, baseline)
    before.update(snapshot_id=before_id, outside_fact_count=baseline.where(outside).count())
    clean = dict(extra_event_ids=0, extra_entry_ids=0, duplicate_event_ids=0, duplicate_entry_ids=0,
                 payload_mismatches=0, extra_windows=0)
    outside_count = SOURCE_ROWS - NEW_FACTS
    require_counts("before", before, **clean, fact_count=outside_count, outside_fact_count=outside_count,
                   missing_event_ids=NEW_FACTS, missing_entry_ids=NEW_FACTS, absolute_window_count_delta=NEW_FACTS)

    result = {"source_rows": source_rows, "prefix_partition_counts": prefix_counts,
              "suffix_partition_counts": suffix_counts, "before": before}
    for stage in ("after_first", "after_retry"):
        with checked_range(spark, settings, lower, upper) as (batch, counts):
            identities = target_id_counts(batch)
            require_counts(stage, identities, target_unique_event_ids=NEW_FACTS, target_unique_entry_ids=NEW_FACTS)
            write_facts(batch, settings)
        snapshot_id, facts = current_snapshot(spark, table)
        report = reconcile(spark, source, facts)
        current_outside = facts.where(outside)
        # Compare multisets of complete stored rows, including coordinates and fingerprint.
        report.update(
            snapshot_id=snapshot_id, target_physical_records=sum(counts.values()),
            target_partition_counts=counts, target_fact_count=facts.where(target).count(), **identities,
            outside_rows_removed=baseline.exceptAll(current_outside).count(),
            outside_rows_added=current_outside.exceptAll(baseline).count(),
        )
        require_counts(stage, report, **clean, fact_count=SOURCE_ROWS, target_physical_records=TARGET_RECORDS,
                       target_fact_count=NEW_FACTS, missing_event_ids=0, missing_entry_ids=0,
                       mismatched_windows=0, missing_windows=0, absolute_window_count_delta=0,
                       outside_rows_removed=0, outside_rows_added=0)
        if stage == "after_first" and snapshot_id == before_id:
            raise ValueError("first recovery did not advance the Iceberg snapshot")
        if stage == "after_retry" and snapshot_id != result["after_first"]["snapshot_id"]:
            raise ValueError("retry changed the Iceberg snapshot")
        result[stage] = report
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--bounds", required=True, type=Path)
    args = parser.parse_args()
    bounds = load_bounds(args.bounds)
    settings = raffle_settings(os.environ)
    spark = create_spark_session(settings)
    try:
        manifest = read_manifest(spark, args.manifest)
        try:
            result = run_drill(spark, settings, manifest, bounds)
        finally:
            manifest.unpersist()
        print("RAFFLE_RECOVERY_RESULT=" + json.dumps(result, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
