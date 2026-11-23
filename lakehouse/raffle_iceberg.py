"""Single-writer bounded raffle ingest. Pass --proof-only to read the Iceberg trace.

Without expectations the trace describes the most recent event by occurrence time,
or returns null trace fields and a zero count for an empty table.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from lakehouse.d2c_iceberg_stream import (
    LakehouseSettings,
    create_spark_session,
    settings_from_environment,
)
from raffle_contract import TOPIC, decode_event, is_canonical_uuid


_EVENT_SCHEMA = """
    event_id STRING NOT NULL, event_type STRING NOT NULL, event_version INT NOT NULL,
    occurred_at TIMESTAMP NOT NULL, entry_id BIGINT NOT NULL, item_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL, payload_sha256 STRING NOT NULL
"""
_TRACE_FIELDS = (
    "event_id", "entry_id", "item_id", "topic", "partition", "offset", "payload_sha256",
    "window_start_utc", "window_end_utc",
)


def raffle_settings(environment: Mapping[str, str]) -> LakehouseSettings:
    shared_keys = (
        "KAFKA_BOOTSTRAP_SERVERS", "D2C_LAKEHOUSE_WAREHOUSE",
        "D2C_LAKEHOUSE_CATALOG", "D2C_LAKEHOUSE_NAMESPACE",
    )
    shared = {key: environment[key] for key in shared_keys if key in environment}
    return replace(settings_from_environment(shared), topic=TOPIC, stream_id="raffle-entries-bounded")


def fact_table(settings: LakehouseSettings) -> str:
    return f"{settings.catalog}.{settings.namespace}.raffle_entries"


def parse_event(value: bytes | bytearray | None) -> dict[str, Any] | None:
    """Parse a GitOps outbox value. A null result marks the entire batch invalid."""
    if value is None:
        return None
    try:
        event = decode_event(value)
        instant = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
        canonical = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return {
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "event_version": event["event_version"],
            "occurred_at": instant.astimezone(timezone.utc),
            **event["data"],
            "payload_sha256": hashlib.sha256(canonical).hexdigest(),
        }
    except (ValueError, OverflowError, RecursionError):
        return None


def read_kafka(spark: Any, settings: LakehouseSettings) -> Any:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .load()
    )


def write_facts(source: Any, settings: LakehouseSettings) -> None:
    """Validate the whole bounded input before publishing one insert-only MERGE."""
    from pyspark.sql import functions as F

    spark = source.sparkSession
    table = fact_table(settings)
    parser = F.udf(parse_event, _EVENT_SCHEMA, useArrow=False)
    valid_origin = (F.col("topic") == TOPIC) & (F.col("partition") >= 0) & (F.col("offset") >= 0)
    parsed = source.select(
        F.when(valid_origin, parser("value")).alias("event"),
        F.struct(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        ).alias("origin"),
    )
    # Identical deliveries collapse together. Keep a deterministic observed coordinate.
    incoming = (
        parsed.groupBy("event").agg(F.min("origin").alias("origin"))
        .select("event.*", "origin.*").persist()
    )
    try:
        invalid = incoming.where("event_id IS NULL").select("kafka_partition", "kafka_offset").first()
        if invalid is not None:
            raise ValueError(f"invalid raffle record at partition={invalid[0]} offset={invalid[1]}")

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {settings.catalog}.{settings.namespace}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {_EVENT_SCHEMA},
                kafka_topic STRING NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT NOT NULL
            ) USING iceberg PARTITIONED BY (days(occurred_at))
            TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet')
        """)
        stored = spark.table(table)
        # The outbox also has UNIQUE(event_type, aggregate_id): one acceptance per entry.
        for key in ("event_id", "entry_id"):
            hashes = incoming.groupBy(key).agg(
                F.min("payload_sha256").alias("first_hash"),
                F.max("payload_sha256").alias("last_hash"),
            )
            existing = stored.select(key, F.col("payload_sha256").alias("stored_hash"))
            conflicts = hashes.join(existing, key, "left").where(
                "first_hash <> last_hash OR first_hash <> stored_hash"
            )
            if not conflicts.isEmpty():
                raise ValueError(f"conflicting raffle payloads for the same {key}")

        if incoming.join(stored.select("event_id"), "event_id", "left_anti").isEmpty():
            return
        incoming.createOrReplaceTempView("raffle_iceberg_incoming")
        try:
            spark.sql(f"""
                MERGE INTO {table} AS t USING raffle_iceberg_incoming AS s
                ON t.event_id = s.event_id
                WHEN NOT MATCHED THEN INSERT *
            """)
        finally:
            spark.catalog.dropTempView("raffle_iceberg_incoming")
    finally:
        incoming.unpersist()


def trace_expectations(environment: Mapping[str, str]) -> dict[str, str | int]:
    expected: dict[str, str | int] = {}
    for field in ("event_id", "entry_id", "item_id", "partition", "offset"):
        key = f"RAFFLE_EXPECT_{field.upper()}"
        if key not in environment:
            continue
        value = environment[key].strip()
        if field == "event_id":
            if not is_canonical_uuid(value):
                raise ValueError(f"{key} must be a canonical UUID")
            expected[field] = value
        else:
            minimum = 0 if field in ("partition", "offset") else 1
            maximum = 2**31 - 1 if field == "partition" else 2**63 - 1
            if not re.fullmatch(r"[0-9]{1,19}", value) or not minimum <= int(value) <= maximum:
                raise ValueError(f"{key} must be an integer between {minimum} and {maximum}")
            expected[field] = int(value)
    return expected


def prove(spark: Any, settings: LakehouseSettings, expected: Mapping[str, str | int]) -> dict[str, Any]:
    """Read a fact and its complete UTC item/window group at the current main snapshot."""
    from pyspark.sql import functions as F

    table = fact_table(settings)
    ref = spark.table(f"{table}.refs").where("name = 'main'").select("snapshot_id").first()
    snapshot_id = ref["snapshot_id"] if ref else None
    empty = {**dict.fromkeys(_TRACE_FIELDS), "snapshot_id": snapshot_id, "accepted_count": 0}
    if snapshot_id is None:
        if expected:
            raise ValueError("expected a raffle fact but the table has no committed snapshot")
        return empty

    facts = spark.sql(f"SELECT * FROM {table} VERSION AS OF {int(snapshot_id)}")
    bucket = F.window("occurred_at", "5 minutes")
    utc_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    traces = facts.select(
        "event_id", "entry_id", "item_id", "occurred_at", "payload_sha256",
        F.col("kafka_topic").alias("topic"), F.col("kafka_partition").alias("partition"),
        F.col("kafka_offset").alias("offset"),
        F.date_format(bucket.start, utc_format).alias("window_start_utc"),
        F.date_format(bucket.end, utc_format).alias("window_end_utc"),
    )
    if expected:
        # Check uniqueness by event_id before checking its other expected attributes.
        criteria = {"event_id": expected["event_id"]} if "event_id" in expected else expected
        for field, value in criteria.items():
            traces = traces.where(F.col(field) == value)
        matches = traces.select(*_TRACE_FIELDS).limit(2).collect()
        if len(matches) != 1:
            raise ValueError("expected exactly one matching raffle fact in the snapshot")
        row = matches[0]
    else:
        row = traces.orderBy(F.desc("occurred_at"), "event_id").select(*_TRACE_FIELDS).first()
        if row is None:
            return empty
    for field, value in expected.items():
        if row[field] != value:
            raise ValueError(f"raffle fact does not match expected {field}")
    if row["topic"] != TOPIC:
        raise ValueError("raffle fact has an unexpected Kafka topic")

    # Count every accepted fact in the selected group, including its other event IDs.
    group = facts.where(
        (F.col("item_id") == row["item_id"])
        & (F.col("occurred_at") >= F.lit(row["window_start_utc"]).cast("timestamp"))
        & (F.col("occurred_at") < F.lit(row["window_end_utc"]).cast("timestamp"))
    )
    accepted_count = group.count()
    if accepted_count < 1:
        raise ValueError("the raffle fact has no positive five-minute item group")
    return {**row.asDict(), "snapshot_id": snapshot_id, "accepted_count": accepted_count}


def main() -> None:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--proof-only", action="store_true", help="query the committed Iceberg snapshot")
    args = cli.parse_args()
    settings = raffle_settings(os.environ)
    expected = trace_expectations(os.environ)
    spark = create_spark_session(settings)
    try:
        if not args.proof_only:
            write_facts(read_kafka(spark, settings), settings)
        report = prove(spark, settings, expected)
        print("RAFFLE_TRACE_RESULT=" + json.dumps(report, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
