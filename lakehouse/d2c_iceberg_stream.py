"""Replay-safe D2C Kafka to Iceberg Structured Streaming job.

The job relies on Spark's checkpoint to resume Kafka offsets and uses immutable
``event_id`` / Kafka source coordinates as idempotency keys at the Iceberg
boundary.  It intentionally provides at-least-once source semantics; a replay
after a checkpoint failure is reconciled by the table-level MERGE operations.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from d2c_contract import (
    D2C_EVENT_TYPE,
    canonicalize_application_approved_event,
    validate_application_approved_event,
)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("d2c-iceberg-stream")

_IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_STREAM_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
_VALID_TRIGGERS = frozenset({"available_now", "processing"})


@dataclass(frozen=True)
class LakehouseSettings:
    """Validated configuration for the local Iceberg implementation."""

    bootstrap_servers: str
    topic: str
    checkpoint_location: str
    warehouse: str
    catalog: str
    namespace: str
    stream_id: str
    starting_offsets: str
    max_offsets_per_trigger: int
    trigger: str
    processing_time: str

    @property
    def approvals_table(self) -> str:
        return f"{self.catalog}.{self.namespace}.application_approvals"

    @property
    def dlq_table(self) -> str:
        return f"{self.catalog}.{self.namespace}.application_approval_dlq"

    @property
    def batch_audit_table(self) -> str:
        return f"{self.catalog}.{self.namespace}.application_approval_stream_batches"


def _environment_value(
    environment: Mapping[str, str], key: str, default: str
) -> str:
    value = environment.get(key, default).strip()
    if not value:
        raise ValueError(f"{key} must not be empty")
    return value


def _identifier(environment: Mapping[str, str], key: str, default: str) -> str:
    value = _environment_value(environment, key, default)
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{key} must be a lower-case SQL identifier")
    return value


def _absolute_checkpoint(environment: Mapping[str, str]) -> str:
    checkpoint = _environment_value(
        environment,
        "D2C_LAKEHOUSE_CHECKPOINT",
        "/checkpoints/d2c-application-approvals-v1",
    )
    if not checkpoint.startswith("/") or ".." in checkpoint.split("/"):
        raise ValueError("D2C_LAKEHOUSE_CHECKPOINT must be an absolute non-traversing path")
    return checkpoint.rstrip("/")


def _local_warehouse(environment: Mapping[str, str]) -> str:
    warehouse = _environment_value(
        environment,
        "D2C_LAKEHOUSE_WAREHOUSE",
        "file:///warehouse",
    )
    parsed = urlparse(warehouse)
    if parsed.scheme != "file" or parsed.netloc or not parsed.path.startswith("/"):
        raise ValueError(
            "D2C_LAKEHOUSE_WAREHOUSE must be an absolute local file:// URI for this profile"
        )
    if ".." in parsed.path.split("/"):
        raise ValueError("D2C_LAKEHOUSE_WAREHOUSE must not contain path traversal")
    return warehouse.rstrip("/")


def settings_from_environment(environment: Mapping[str, str]) -> LakehouseSettings:
    """Load fail-closed settings without importing PySpark for unit testability."""

    topic = _environment_value(environment, "D2C_EVENT_TOPIC", D2C_EVENT_TYPE)
    if topic != D2C_EVENT_TYPE:
        raise ValueError(
            "D2C_EVENT_TOPIC must match the supported d2c.application.approved.v1 contract"
        )

    stream_id = _environment_value(
        environment,
        "D2C_LAKEHOUSE_STREAM_ID",
        "d2c-application-approvals-v1",
    )
    if not _STREAM_ID_PATTERN.fullmatch(stream_id):
        raise ValueError("D2C_LAKEHOUSE_STREAM_ID must be a lower-case stable identifier")

    starting_offsets = _environment_value(
        environment,
        "D2C_LAKEHOUSE_STARTING_OFFSETS",
        "earliest",
    )
    if starting_offsets not in {"earliest", "latest"}:
        raise ValueError("D2C_LAKEHOUSE_STARTING_OFFSETS must be earliest or latest")

    raw_max_offsets = _environment_value(
        environment,
        "D2C_LAKEHOUSE_MAX_OFFSETS_PER_TRIGGER",
        "1000",
    )
    try:
        max_offsets_per_trigger = int(raw_max_offsets)
    except ValueError as error:
        raise ValueError("D2C_LAKEHOUSE_MAX_OFFSETS_PER_TRIGGER must be an integer") from error
    if not 1 <= max_offsets_per_trigger <= 50_000:
        raise ValueError("D2C_LAKEHOUSE_MAX_OFFSETS_PER_TRIGGER must be between 1 and 50000")

    trigger = _environment_value(environment, "D2C_LAKEHOUSE_TRIGGER", "available_now")
    if trigger not in _VALID_TRIGGERS:
        raise ValueError("D2C_LAKEHOUSE_TRIGGER must be available_now or processing")

    return LakehouseSettings(
        bootstrap_servers=_environment_value(
            environment, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
        ),
        topic=topic,
        checkpoint_location=_absolute_checkpoint(environment),
        warehouse=_local_warehouse(environment),
        catalog=_identifier(environment, "D2C_LAKEHOUSE_CATALOG", "d2c_lakehouse"),
        namespace=_identifier(environment, "D2C_LAKEHOUSE_NAMESPACE", "d2c"),
        stream_id=stream_id,
        starting_offsets=starting_offsets,
        max_offsets_per_trigger=max_offsets_per_trigger,
        trigger=trigger,
        processing_time=_environment_value(
            environment, "D2C_LAKEHOUSE_PROCESSING_TIME", "30 seconds"
        ),
    )


def parse_d2c_kafka_payload(raw_payload: str | None) -> dict[str, object | None]:
    """Parse one Kafka value into a strict D2C event or a quarantine record."""

    if raw_payload is None or not raw_payload.strip():
        return {
            "event_id": None,
            "event_type": None,
            "event_version": None,
            "occurred_at": None,
            "application_id": None,
            "user_id": None,
            "campaign_id": None,
            "payload_json": None,
            "failure_code": "empty_payload",
            "failure_message": "Kafka message value is empty",
        }

    try:
        event = json.loads(raw_payload)
    except json.JSONDecodeError as error:
        return {
            "event_id": None,
            "event_type": None,
            "event_version": None,
            "occurred_at": None,
            "application_id": None,
            "user_id": None,
            "campaign_id": None,
            "payload_json": None,
            "failure_code": "invalid_json",
            "failure_message": str(error)[:500],
        }

    try:
        validated = validate_application_approved_event(event)
        occurred_at = datetime.fromisoformat(
            str(validated["occurred_at"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        data = validated["data"]
        assert isinstance(data, dict)
        return {
            "event_id": str(validated["event_id"]),
            "event_type": str(validated["event_type"]),
            "event_version": int(validated["event_version"]),
            "occurred_at": occurred_at,
            "application_id": int(data["application_id"]),
            "user_id": int(data["user_id"]),
            "campaign_id": int(data["campaign_id"]),
            "payload_json": canonicalize_application_approved_event(validated),
            "failure_code": None,
            "failure_message": None,
        }
    except (AssertionError, TypeError, ValueError) as error:
        return {
            "event_id": None,
            "event_type": None,
            "event_version": None,
            "occurred_at": None,
            "application_id": None,
            "user_id": None,
            "campaign_id": None,
            "payload_json": None,
            "failure_code": "contract_violation",
            "failure_message": str(error)[:500],
        }


def canonical_source_ranges(
    ranges: Iterable[Mapping[str, object]],
) -> tuple[str, str]:
    """Return a deterministic batch identity from Kafka source offset ranges."""

    normalized: list[dict[str, object]] = []
    seen: set[tuple[str, int]] = set()
    for source_range in ranges:
        topic = source_range.get("topic")
        partition = source_range.get("partition")
        start_offset = source_range.get("start_offset")
        end_offset = source_range.get("end_offset")
        records = source_range.get("records")
        if not isinstance(topic, str) or not topic:
            raise ValueError("source range topic is required")
        if any(isinstance(value, bool) for value in (partition, start_offset, end_offset, records)):
            raise ValueError("source range values must be integers")
        if not all(isinstance(value, int) for value in (partition, start_offset, end_offset, records)):
            raise ValueError("source range values must be integers")
        assert isinstance(partition, int)
        assert isinstance(start_offset, int)
        assert isinstance(end_offset, int)
        assert isinstance(records, int)
        if partition < 0 or start_offset < 0 or end_offset < start_offset or records <= 0:
            raise ValueError("source range values are invalid")
        if records != end_offset - start_offset + 1:
            raise ValueError("source range record count does not match offset span")
        key = (topic, partition)
        if key in seen:
            raise ValueError("source ranges must be unique per topic and partition")
        seen.add(key)
        normalized.append(
            {
                "topic": topic,
                "partition": partition,
                "start_offset": start_offset,
                "end_offset": end_offset,
                "records": records,
            }
        )

    if not normalized:
        raise ValueError("source ranges must not be empty")
    normalized.sort(key=lambda item: (str(item["topic"]), int(item["partition"])))
    source_ranges_json = json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(source_ranges_json.encode("utf-8")).hexdigest(), source_ranges_json


def create_spark_session(settings: LakehouseSettings) -> Any:
    """Build a UTC Spark session with the only Iceberg catalog used locally."""

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(settings.stream_id)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{settings.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{settings.catalog}.warehouse", settings.warehouse)
        .config("spark.sql.defaultCatalog", settings.catalog)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def create_tables(spark: Any, settings: LakehouseSettings) -> None:
    """Create the fact, quarantine, and reconciliation tables idempotently."""

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {settings.catalog}.{settings.namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.approvals_table} (
            event_id STRING NOT NULL,
            event_type STRING NOT NULL,
            event_version INT NOT NULL,
            occurred_at TIMESTAMP NOT NULL,
            application_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            campaign_id BIGINT NOT NULL,
            kafka_topic STRING NOT NULL,
            kafka_partition INT NOT NULL,
            kafka_offset BIGINT NOT NULL,
            kafka_timestamp TIMESTAMP,
            payload_json STRING NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (days(occurred_at))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.dlq_table} (
            kafka_topic STRING NOT NULL,
            kafka_partition INT NOT NULL,
            kafka_offset BIGINT NOT NULL,
            kafka_timestamp TIMESTAMP,
            raw_payload STRING,
            raw_payload_base64 STRING,
            failure_code STRING NOT NULL,
            failure_message STRING NOT NULL,
            failed_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (days(failed_at))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.batch_audit_table} (
            stream_id STRING NOT NULL,
            source_fingerprint STRING NOT NULL,
            spark_batch_id BIGINT NOT NULL,
            source_records BIGINT NOT NULL,
            valid_records BIGINT NOT NULL,
            quarantined_records BIGINT NOT NULL,
            duplicate_records BIGINT NOT NULL,
            source_offset_ranges STRING NOT NULL,
            committed_at TIMESTAMP NOT NULL
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )


def _parsed_schema() -> Any:
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_version", IntegerType(), True),
            StructField("occurred_at", TimestampType(), True),
            StructField("application_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("campaign_id", LongType(), True),
            StructField("payload_json", StringType(), True),
            StructField("failure_code", StringType(), True),
            StructField("failure_message", StringType(), True),
        ]
    )


def build_parsed_stream(spark: Any, settings: LakehouseSettings) -> Any:
    """Read bounded Kafka micro-batches and attach contract-validation results."""

    from pyspark.sql import functions as functions

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
        .option("failOnDataLoss", "true")
        .load()
        .select(
            functions.col("topic").alias("kafka_topic"),
            functions.col("partition").alias("kafka_partition"),
            functions.col("offset").alias("kafka_offset"),
            functions.col("timestamp").alias("kafka_timestamp"),
            functions.expr("CAST(value AS STRING)").alias("raw_payload"),
            functions.base64(functions.col("value")).alias("raw_payload_base64"),
        )
    )
    parser = functions.udf(parse_d2c_kafka_payload, _parsed_schema())
    return (
        source.withColumn("parsed", parser(functions.col("raw_payload")))
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_payload",
            "raw_payload_base64",
            functions.col("parsed.event_id").alias("event_id"),
            functions.col("parsed.event_type").alias("event_type"),
            functions.col("parsed.event_version").alias("event_version"),
            functions.col("parsed.occurred_at").alias("occurred_at"),
            functions.col("parsed.application_id").alias("application_id"),
            functions.col("parsed.user_id").alias("user_id"),
            functions.col("parsed.campaign_id").alias("campaign_id"),
            functions.col("parsed.payload_json").alias("payload_json"),
            functions.col("parsed.failure_code").alias("failure_code"),
            functions.col("parsed.failure_message").alias("failure_message"),
            functions.current_timestamp().alias("ingested_at"),
        )
    )


def _batch_source_ranges(batch: Any) -> list[dict[str, object]]:
    from pyspark.sql import functions as functions

    rows = (
        batch.groupBy("kafka_topic", "kafka_partition")
        .agg(
            functions.min("kafka_offset").alias("start_offset"),
            functions.max("kafka_offset").alias("end_offset"),
            functions.count("*").alias("records"),
        )
        .orderBy("kafka_topic", "kafka_partition")
        .collect()
    )
    return [
        {
            "topic": row["kafka_topic"],
            "partition": row["kafka_partition"],
            "start_offset": row["start_offset"],
            "end_offset": row["end_offset"],
            "records": row["records"],
        }
        for row in rows
    ]


def _merge_approval_rows(spark: Any, settings: LakehouseSettings) -> None:
    spark.sql(
        f"""
        MERGE INTO {settings.approvals_table} AS target
        USING d2c_iceberg_valid_batch AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id, event_type, event_version, occurred_at,
            application_id, user_id, campaign_id,
            kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
            payload_json, ingested_at
        ) VALUES (
            source.event_id, source.event_type, source.event_version, source.occurred_at,
            source.application_id, source.user_id, source.campaign_id,
            source.kafka_topic, source.kafka_partition, source.kafka_offset, source.kafka_timestamp,
            source.payload_json, source.ingested_at
        )
        """
    )


def _merge_quarantine_rows(spark: Any, settings: LakehouseSettings) -> None:
    spark.sql(
        f"""
        MERGE INTO {settings.dlq_table} AS target
        USING d2c_iceberg_invalid_batch AS source
        ON target.kafka_topic = source.kafka_topic
           AND target.kafka_partition = source.kafka_partition
           AND target.kafka_offset = source.kafka_offset
        WHEN NOT MATCHED THEN INSERT (
            kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
            raw_payload, raw_payload_base64,
            failure_code, failure_message, failed_at
        ) VALUES (
            source.kafka_topic, source.kafka_partition, source.kafka_offset, source.kafka_timestamp,
            source.raw_payload, source.raw_payload_base64,
            source.failure_code, source.failure_message, source.failed_at
        )
        """
    )


def _record_batch_audit(
    spark: Any,
    settings: LakehouseSettings,
    *,
    source_fingerprint: str,
    spark_batch_id: int,
    source_records: int,
    valid_records: int,
    quarantined_records: int,
    duplicate_records: int,
    source_offset_ranges: str,
) -> bool:
    """Record a source-range audit once, refusing conflicting history.

    A source range can be replayed after a checkpoint recovery.  Avoiding an
    otherwise empty Iceberg MERGE keeps replay protection from generating
    metadata-only snapshots while preserving a fail-closed audit invariant.
    """

    from pyspark.sql import functions as functions

    expected_audit = {
        "source_records": source_records,
        "valid_records": valid_records,
        "quarantined_records": quarantined_records,
        "duplicate_records": duplicate_records,
        "source_offset_ranges": source_offset_ranges,
    }
    existing_rows = (
        spark.table(settings.batch_audit_table)
        .where(
            (functions.col("stream_id") == settings.stream_id)
            & (functions.col("source_fingerprint") == source_fingerprint)
        )
        .select(*expected_audit)
        .limit(2)
        .collect()
    )
    if existing_rows:
        if len(existing_rows) != 1 or existing_rows[0].asDict() != expected_audit:
            raise RuntimeError("D2C source-range audit conflicts with persisted history")
        return False

    audit_row = spark.createDataFrame(
        [
            (
                settings.stream_id,
                source_fingerprint,
                spark_batch_id,
                source_records,
                valid_records,
                quarantined_records,
                duplicate_records,
                source_offset_ranges,
                datetime.now(timezone.utc),
            )
        ],
        [
            "stream_id",
            "source_fingerprint",
            "spark_batch_id",
            "source_records",
            "valid_records",
            "quarantined_records",
            "duplicate_records",
            "source_offset_ranges",
            "committed_at",
        ],
    )
    audit_row.createOrReplaceTempView("d2c_iceberg_batch_audit")
    spark.sql(
        f"""
        MERGE INTO {settings.batch_audit_table} AS target
        USING d2c_iceberg_batch_audit AS source
        ON target.stream_id = source.stream_id
           AND target.source_fingerprint = source.source_fingerprint
        WHEN NOT MATCHED THEN INSERT (
            stream_id, source_fingerprint, spark_batch_id,
            source_records, valid_records, quarantined_records, duplicate_records,
            source_offset_ranges, committed_at
        ) VALUES (
            source.stream_id, source.source_fingerprint, source.spark_batch_id,
            source.source_records, source.valid_records, source.quarantined_records,
            source.duplicate_records, source.source_offset_ranges, source.committed_at
        )
        """
    )
    return True


def write_batch(batch: Any, batch_id: int, settings: LakehouseSettings) -> None:
    """Persist one micro-batch, refusing immutable-ID conflicts before commits.

    Spark gives each ``foreachBatch`` callback a session scoped to that batch.
    Temporary views must be registered and queried through this session rather
    than the parent streaming session.
    """

    from pyspark import StorageLevel
    from pyspark.sql import functions as functions
    from pyspark.sql.window import Window

    spark = batch.sparkSession
    batch.persist(StorageLevel.MEMORY_AND_DISK)
    valid_candidates = None
    unique_valid = None
    new_valid = None
    new_quarantined = None
    try:
        source_ranges = _batch_source_ranges(batch)
        if not source_ranges:
            LOGGER.info("d2c_lakehouse_empty_batch batch_id=%s", batch_id)
            return
        source_fingerprint, source_ranges_json = canonical_source_ranges(source_ranges)
        source_records = sum(int(source_range["records"]) for source_range in source_ranges)

        valid_input = batch.where(functions.col("failure_code").isNull())
        invalid_input = batch.where(functions.col("failure_code").isNotNull())
        valid_records = valid_input.count()
        quarantined_records = invalid_input.count()
        if source_records != valid_records + quarantined_records:
            raise RuntimeError("D2C source-to-sink record classification is inconsistent")

        valid_candidates = valid_input.select(
            "event_id",
            "event_type",
            "event_version",
            "occurred_at",
            "application_id",
            "user_id",
            "campaign_id",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "payload_json",
            "ingested_at",
        ).persist(StorageLevel.MEMORY_AND_DISK)
        conflicting_input_ids = (
            valid_candidates.groupBy("event_id")
            .agg(functions.countDistinct("payload_json").alias("payload_variants"))
            .where(functions.col("payload_variants") > 1)
            .limit(1)
            .count()
        )
        if conflicting_input_ids:
            raise RuntimeError("D2C batch has different payloads for the same immutable event_id")

        deterministic_event_order = Window.partitionBy("event_id").orderBy(
            functions.col("kafka_timestamp").asc_nulls_last(),
            functions.col("kafka_topic").asc(),
            functions.col("kafka_partition").asc(),
            functions.col("kafka_offset").asc(),
        )
        unique_valid = (
            valid_candidates.withColumn(
                "event_row_number", functions.row_number().over(deterministic_event_order)
            )
            .where(functions.col("event_row_number") == 1)
            .drop("event_row_number")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        unique_valid_records = unique_valid.count()
        duplicate_records = valid_records - unique_valid_records

        if unique_valid_records:
            unique_valid.createOrReplaceTempView("d2c_iceberg_valid_batch")
            immutable_id_conflicts = spark.sql(
                f"""
                SELECT COUNT(*) AS conflicts
                FROM d2c_iceberg_valid_batch AS incoming
                INNER JOIN {settings.approvals_table} AS persisted
                    ON incoming.event_id = persisted.event_id
                WHERE incoming.payload_json <> persisted.payload_json
                """
            ).first()["conflicts"]
            if int(immutable_id_conflicts) > 0:
                raise RuntimeError("D2C immutable event_id conflicts with an existing Iceberg fact")
            new_valid = spark.sql(
                f"""
                SELECT incoming.*
                FROM d2c_iceberg_valid_batch AS incoming
                LEFT ANTI JOIN {settings.approvals_table} AS persisted
                    ON incoming.event_id = persisted.event_id
                """
            ).persist(StorageLevel.MEMORY_AND_DISK)
            new_valid_records = new_valid.count()
            if new_valid_records:
                new_valid.createOrReplaceTempView("d2c_iceberg_valid_batch")
                _merge_approval_rows(spark, settings)
        else:
            new_valid_records = 0

        if quarantined_records:
            invalid_input.select(
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "raw_payload",
                "raw_payload_base64",
                "failure_code",
                "failure_message",
                functions.col("ingested_at").alias("failed_at"),
            ).createOrReplaceTempView("d2c_iceberg_invalid_batch")
            new_quarantined = spark.sql(
                f"""
                SELECT incoming.*
                FROM d2c_iceberg_invalid_batch AS incoming
                LEFT ANTI JOIN {settings.dlq_table} AS persisted
                    ON incoming.kafka_topic = persisted.kafka_topic
                   AND incoming.kafka_partition = persisted.kafka_partition
                   AND incoming.kafka_offset = persisted.kafka_offset
                """
            ).persist(StorageLevel.MEMORY_AND_DISK)
            new_quarantined_records = new_quarantined.count()
            if new_quarantined_records:
                new_quarantined.createOrReplaceTempView("d2c_iceberg_invalid_batch")
                _merge_quarantine_rows(spark, settings)
        else:
            new_quarantined_records = 0

        audit_written = _record_batch_audit(
            spark,
            settings,
            source_fingerprint=source_fingerprint,
            spark_batch_id=batch_id,
            source_records=source_records,
            valid_records=valid_records,
            quarantined_records=quarantined_records,
            duplicate_records=duplicate_records,
            source_offset_ranges=source_ranges_json,
        )
        LOGGER.info(
            "d2c_lakehouse_batch_committed batch_id=%s source_fingerprint=%s source_records=%s valid_records=%s quarantined_records=%s new_valid_records=%s new_quarantined_records=%s duplicate_records=%s audit_written=%s",
            batch_id,
            source_fingerprint,
            source_records,
            valid_records,
            quarantined_records,
            new_valid_records,
            new_quarantined_records,
            duplicate_records,
            audit_written,
        )
    finally:
        if new_quarantined is not None:
            new_quarantined.unpersist()
        if new_valid is not None:
            new_valid.unpersist()
        if unique_valid is not None:
            unique_valid.unpersist()
        if valid_candidates is not None:
            valid_candidates.unpersist()
        batch.unpersist()
        for view in (
            "d2c_iceberg_valid_batch",
            "d2c_iceberg_invalid_batch",
            "d2c_iceberg_batch_audit",
        ):
            spark.catalog.dropTempView(view)


def run(settings: LakehouseSettings) -> None:
    """Start and wait for the configured Spark Structured Streaming query."""

    spark = create_spark_session(settings)
    try:
        create_tables(spark, settings)
        parsed_stream = build_parsed_stream(spark, settings)
        writer = (
            parsed_stream.writeStream.queryName(settings.stream_id)
            .option("checkpointLocation", settings.checkpoint_location)
            .foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, settings))
        )
        if settings.trigger == "available_now":
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=settings.processing_time)
        LOGGER.info(
            "d2c_lakehouse_stream_starting topic=%s checkpoint=%s warehouse=%s trigger=%s",
            settings.topic,
            settings.checkpoint_location,
            settings.warehouse,
            settings.trigger,
        )
        query = writer.start()
        query.awaitTermination()
    finally:
        spark.stop()


def main() -> None:
    run(settings_from_environment(os.environ))


if __name__ == "__main__":
    main()
