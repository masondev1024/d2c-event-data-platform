"""Materialize selected five-minute raffle windows from a bounded Kafka batch.

Run one writer at a time against the lab table. Keep its unique topic and its
full history through the retry. The controls contain no oracle counts.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit


WINDOW_BOUNDS = {
    "A": ("2026-01-01 00:00:00", "2026-01-01 00:05:00"),
    "B": ("2026-01-01 00:05:00", "2026-01-01 00:10:00"),
}
PRODUCTS = 100
MAX_BIGINT = 2**63 - 1
MART_COLUMNS = ("window_key", "product_id", "window_state", "entry_count")
MART_SCHEMA = "window_key STRING, product_id BIGINT, window_state STRING, entry_count BIGINT"
UTC_STAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?(?:Z|\+00:00)"
)


def unique_fields(pairs):
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ValueError("duplicate JSON object key")
    return fields


def bigint(value, minimum=0):
    return type(value) is int and minimum <= value <= MAX_BIGINT


def parse_offsets(topic, raw):
    if not re.fullmatch(r"[a-zA-Z0-9._-]{1,249}", topic) or topic in (".", ".."):
        raise ValueError("expected a single Kafka topic name")
    bounds = json.loads(raw, object_pairs_hook=unique_fields)
    if not isinstance(bounds, dict) or set(bounds) != {topic}:
        raise ValueError("ending offsets must specify exactly the selected topic")
    partitions = bounds[topic]
    if not isinstance(partitions, dict) or not partitions:
        raise ValueError("ending offsets must include all topic partitions")
    for partition, end in partitions.items():
        if not re.fullmatch(r"0|[1-9][0-9]{0,9}", partition) or int(partition) >= 2**31:
            raise ValueError("invalid Kafka partition number")
        if not bigint(end):
            raise ValueError("expected explicit, nonnegative exclusive end offsets")
    return bounds


def parse_controls(raw):
    controls = json.loads(raw, object_pairs_hook=unique_fields)
    if not isinstance(controls, dict) or not controls or not controls.keys() <= WINDOW_BOUNDS.keys():
        raise ValueError("select one or both of windows A and B")
    for window, control in controls.items():
        if not isinstance(control, dict) or set(control) != {"state", "source_entry_id_cutoff"}:
            raise ValueError(f"{window} requires state and source_entry_id_cutoff")
        state, cutoff = control["state"], control["source_entry_id_cutoff"]
        if state not in ("OPEN", "CLOSED"):
            raise ValueError(f"{window} requires state OPEN or CLOSED")
        if not bigint(cutoff) and not (state == "OPEN" and cutoff is None):
            raise ValueError(f"{window} requires a nonnegative BIGINT cutoff or null for OPEN")
    return controls


def parse_table(name):
    parts = name.split(".")
    if len(parts) == 2:
        parts.insert(0, "mart")
    if len(parts) != 3 or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError("expected [catalog.]namespace.table with simple SQL identifiers")
    return parts


def warehouse_uri(kind, location):
    uri = urlsplit(location)
    if kind == "hadoop":
        if not uri.scheme and Path(location).is_absolute():
            return Path(location).as_uri()
        if uri.scheme == "file" and not uri.netloc and uri.path.startswith("/"):
            return location
        raise ValueError("the Hadoop warehouse requires an absolute local path or file:/// URI")
    if uri.scheme != "s3" or not uri.netloc:
        raise ValueError("the Glue warehouse requires an s3:// URI")
    return location


def spark_session(catalog, kind, warehouse):
    from pyspark.sql import SparkSession

    prefix = f"spark.sql.catalog.{catalog}"
    builder = (
        SparkSession.builder.appName("raffle-athena-mart")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.warehouse", warehouse)
        .config(f"{prefix}.cache-enabled", "false")
    )
    if kind == "hadoop":
        builder = builder.config(f"{prefix}.type", "hadoop")
    else:
        builder = (
            builder.config(f"{prefix}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        )
    return builder.getOrCreate()


def read_kafka(spark, servers, topic, offsets):
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", json.dumps(offsets))
        .option("failOnDataLoss", "true")
        .load()
    )


def decode_payload(value):
    """Return null for malformed records so the driver can reject the whole batch."""
    if value is None:
        return None
    try:
        event = json.loads(value.decode("utf-8"), object_pairs_hook=unique_fields)
        if not isinstance(event, dict) or not bigint(event.get("entry_id"), 1):
            return None
        product = event.get("product_id")
        if not bigint(product, 1) or product > PRODUCTS:
            return None
        stamp = event.get("occurred_at")
        if not isinstance(stamp, str) or not UTC_STAMP.fullmatch(stamp):
            return None
        instant = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        # Hash the full JSON object. Key order and insignificant whitespace are
        # immaterial, but a changed field on an existing ID is a conflict.
        canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(canonical.encode("ascii")).hexdigest()
        return event["entry_id"], product, instant, digest
    except (ValueError, OverflowError, RecursionError):
        return None


def group_entries(batch):
    from pyspark.sql import functions as F

    parse = F.udf(decode_payload, "entry_id LONG, product_id LONG, occurred_at TIMESTAMP, digest STRING",
                  useArrow=False)
    parsed = batch.select(parse("value").alias("e"), "partition", "offset")
    return parsed.groupBy(F.col("e.entry_id").alias("entry_id")).agg(
        F.count("*").alias("deliveries"),
        F.min("e.product_id").alias("product_id"),
        F.min("e.occurred_at").alias("occurred_at"),
        (F.min("e.digest") != F.max("e.digest")).alias("conflict"),
        F.min(F.struct("partition", "offset")).alias("first_coordinate"),
    )


def source_metrics(spark, entries):
    summary = spark.sql("""
        SELECT coalesce(sum(deliveries), 0) AS kafka_records, count(entry_id) AS unique_entry_ids,
               coalesce(sum(CASE WHEN entry_id IS NULL THEN deliveries END), 0) AS invalid_records,
               min(CASE WHEN entry_id IS NULL THEN first_coordinate END) AS first_invalid,
               count_if(conflict) AS conflicting_ids,
               min(CASE WHEN conflict THEN entry_id END) AS first_conflict
        FROM {entries}
    """, entries=entries).first()
    if summary.invalid_records or summary.conflicting_ids:
        raise ValueError(f"Kafka validation failed: {summary.asDict(recursive=True)}")
    return {"kafka_records": summary.kafka_records, "unique_entry_ids": summary.unique_entry_ids,
            "duplicate_records": summary.kafka_records - summary.unique_entry_ids}


def selected_rows(spark, entries, controls):
    rows = [(key, control["state"], control["source_entry_id_cutoff"], *WINDOW_BOUNDS[key])
            for key, control in sorted(controls.items())]
    windows = spark.createDataFrame(rows, "window_key STRING, window_state STRING, cutoff LONG, lo STRING, hi STRING")
    return spark.sql("""
        WITH counts AS (
            SELECT w.window_key, e.product_id, count(*) AS n
            FROM {entries} e JOIN {windows} w
              ON w.window_state = 'CLOSED' AND e.entry_id <= w.cutoff
             AND e.occurred_at >= CAST(w.lo AS TIMESTAMP) AND e.occurred_at < CAST(w.hi AS TIMESTAMP)
            GROUP BY w.window_key, e.product_id
        )
        SELECT w.window_key, p.id AS product_id, w.window_state,
               CASE WHEN w.window_state = 'CLOSED' THEN coalesce(c.n, CAST(0 AS BIGINT)) END AS entry_count
        FROM {windows} w CROSS JOIN {products} p
        LEFT JOIN counts c ON c.window_key = w.window_key AND c.product_id = p.id
    """, entries=entries, windows=windows, products=spark.range(1, PRODUCTS + 1)).collect()


def current_snapshot(spark, table):
    ref = spark.table(f"{table}.refs").where("name = 'main'").select("snapshot_id").first()
    return ref.snapshot_id if ref else None


def snapshot_rows(spark, table, snapshot):
    if snapshot is None:
        return {}
    # A valid table has at most 200 keys. Bound driver memory on a corrupt target.
    rows = spark.sql(f"SELECT {', '.join(MART_COLUMNS)} FROM {table} VERSION AS OF {int(snapshot)} "
                     f"LIMIT {len(WINDOW_BOUNDS) * PRODUCTS + 1}").collect()
    result = {}
    for row in rows:
        key = (row.window_key, row.product_id)
        if (key in result or row.window_key not in WINDOW_BOUNDS or not bigint(row.product_id, 1)
                or row.product_id > PRODUCTS):
            raise ValueError("invalid or duplicate Iceberg mart key")
        if not ((row.window_state == "OPEN" and row.entry_count is None)
                or (row.window_state == "CLOSED" and bigint(row.entry_count))):
            raise ValueError(f"invalid Iceberg mart value at {key}")
        result[key] = row
    return result


def publish(spark, parts, kind, desired):
    namespace = ".".join(f"`{p}`" for p in parts[:-1])
    table = f"{namespace}.`{parts[-1]}`"
    if kind == "hadoop":
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} ({MART_SCHEMA}) USING iceberg
        PARTITIONED BY (window_key)
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.merge.mode'='copy-on-write')
    """)
    before = current_snapshot(spark, table)
    previous = snapshot_rows(spark, table, before)
    expected = previous.copy()
    changes = []
    for row in desired:
        key = (row.window_key, row.product_id)
        if previous.get(key) != row:
            changes.append(row)
        expected[key] = row

    if changes:
        delta = spark.createDataFrame(changes, MART_SCHEMA)
        delta.createOrReplaceTempView("raffle_mart_changes")
        try:
            spark.sql(f"""
                MERGE INTO {table} t USING raffle_mart_changes s
                   ON t.window_key = s.window_key AND t.product_id = s.product_id
                WHEN MATCHED THEN UPDATE SET window_state = s.window_state, entry_count = s.entry_count
                WHEN NOT MATCHED THEN INSERT (window_key, product_id, window_state, entry_count)
                    VALUES (s.window_key, s.product_id, s.window_state, s.entry_count)
            """)
        finally:
            spark.catalog.dropTempView("raffle_mart_changes")
    after = current_snapshot(spark, table)
    if bool(changes) != (after != before):
        raise RuntimeError("unexpected Iceberg main snapshot transition")
    visible = snapshot_rows(spark, table, after)
    if visible != expected:
        raise RuntimeError("committed Iceberg snapshot differs from the selected update")
    return {"snapshot_id_before": before, "snapshot_id_after": after, "changed_rows": len(changes),
            "visible_rows": [visible[key].asDict() for key in sorted(visible)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--ending-offsets-json", required=True, help='e.g. {"my-lab-topic":{"0":100}}')
    parser.add_argument("--windows-json", required=True, help='e.g. {"B":{"state":"OPEN","source_entry_id_cutoff":null}}')
    parser.add_argument("--table", required=True, help="[catalog.]namespace.table")
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--catalog-type", required=True, choices=("hadoop", "glue"))
    args = parser.parse_args()
    try:
        offsets = parse_offsets(args.topic, args.ending_offsets_json)
        controls = parse_controls(args.windows_json)
        parts = parse_table(args.table)
        warehouse = warehouse_uri(args.catalog_type, args.warehouse)
    except ValueError as error:
        parser.error(str(error))

    spark = spark_session(parts[0], args.catalog_type, warehouse)
    try:
        batch = read_kafka(spark, args.bootstrap_servers, args.topic, offsets)
        entries = group_entries(batch).persist()
        try:
            metrics = source_metrics(spark, entries)
            desired = selected_rows(spark, entries, controls)
        finally:
            entries.unpersist()
        result = publish(spark, parts, args.catalog_type, desired)
        print("RAFFLE_ATHENA_MART_RESULT=" + json.dumps(
            {**metrics, **result, "ending_offsets": offsets, "windows": controls}, sort_keys=True
        ), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
