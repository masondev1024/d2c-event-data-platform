"""Data-quality gates for canonical Parquet lake outputs.

The gate is deliberately independent from the Kafka consumer. A streaming job
can be healthy while a malformed or duplicated file has already landed in the
lake, so promotion and release evidence need a second, data-aware check.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .contract import CANONICAL_SCHEMA_VERSION

CANONICAL_COLUMNS = (
    "event_id",
    "schema_version",
    "event_time",
    "ingested_at",
    "sensor_id",
    "temperature",
    "humidity",
    "status",
    "source_topic",
    "source_partition",
    "source_offset",
    "event_date",
)
AWS_BRONZE_COLUMNS = (
    "event_id",
    "schema_version",
    "event_time",
    "ingested_at",
    "sensor_id",
    "temperature",
    "humidity",
    "status",
    "source",
)

LOCAL_QUALITY_QUERY = """
    SELECT
        COUNT(*)::BIGINT AS row_count,
        COUNT(DISTINCT event_id)::BIGINT AS unique_event_ids,
        (COUNT(*) - COUNT(DISTINCT event_id))::BIGINT AS duplicate_rows,
        SUM(CASE WHEN event_id IS NULL OR trim(event_id) = ''
                 OR NOT regexp_matches(event_id, '^[a-f0-9]{64}$') THEN 1 ELSE 0 END)::BIGINT
            AS invalid_event_id_rows,
        SUM(CASE WHEN event_id IS NULL OR schema_version IS NULL OR event_time IS NULL
                 OR ingested_at IS NULL OR sensor_id IS NULL OR trim(sensor_id) = ''
                 OR temperature IS NULL OR humidity IS NULL OR status IS NULL
                 OR trim(status) = '' OR source_topic IS NULL OR trim(source_topic) = ''
                 OR source_partition IS NULL OR source_offset IS NULL OR event_date IS NULL
                 THEN 1 ELSE 0 END)::BIGINT AS null_required_rows,
        SUM(CASE WHEN schema_version IS NULL OR schema_version <> ? THEN 1 ELSE 0 END)::BIGINT
            AS invalid_schema_version_rows,
        SUM(CASE WHEN temperature IS NULL OR NOT isfinite(temperature)
                 OR temperature < -40 OR temperature > 150 THEN 1 ELSE 0 END)::BIGINT
            AS invalid_temperature_rows,
        SUM(CASE WHEN humidity IS NULL OR NOT isfinite(humidity)
                 OR humidity < 0 OR humidity > 100 THEN 1 ELSE 0 END)::BIGINT
            AS invalid_humidity_rows,
        SUM(CASE WHEN status IS NULL OR status NOT IN ('RUNNING', 'IDLE', 'STOPPED', 'ERROR')
                 THEN 1 ELSE 0 END)::BIGINT AS invalid_status_rows,
        SUM(CASE WHEN source_topic IS NULL OR trim(source_topic) = ''
                 OR source_partition IS NULL OR source_partition < 0
                 OR source_offset IS NULL OR source_offset < 0 THEN 1 ELSE 0 END)::BIGINT
            AS invalid_lineage_rows,
        SUM(CASE WHEN event_time IS NULL OR event_date IS NULL
                 OR CAST(event_time AS DATE) <> event_date THEN 1 ELSE 0 END)::BIGINT
            AS partition_mismatch_rows,
        MAX(event_time) AS max_event_time
    FROM read_parquet(?)
    """

AWS_BRONZE_QUALITY_QUERY = """
    SELECT
        COUNT(*)::BIGINT AS row_count,
        COUNT(DISTINCT event_id)::BIGINT AS unique_event_ids,
        (COUNT(*) - COUNT(DISTINCT event_id))::BIGINT AS duplicate_rows,
        SUM(CASE WHEN event_id IS NULL OR trim(event_id) = ''
                 OR NOT regexp_matches(event_id, '^[a-f0-9]{64}$') THEN 1 ELSE 0 END)::BIGINT
            AS invalid_event_id_rows,
        SUM(CASE WHEN event_id IS NULL OR schema_version IS NULL OR event_time IS NULL
                 OR ingested_at IS NULL OR sensor_id IS NULL OR trim(sensor_id) = ''
                 OR temperature IS NULL OR humidity IS NULL OR status IS NULL
                 OR trim(status) = '' OR source."topic" IS NULL OR trim(source."topic") = ''
                 OR source."partition" IS NULL OR source."offset" IS NULL
                 THEN 1 ELSE 0 END)::BIGINT AS null_required_rows,
        SUM(CASE WHEN schema_version IS NULL OR schema_version <> ? THEN 1 ELSE 0 END)::BIGINT
            AS invalid_schema_version_rows,
        SUM(CASE WHEN temperature IS NULL OR NOT isfinite(temperature)
                 OR temperature < -40 OR temperature > 150 THEN 1 ELSE 0 END)::BIGINT
            AS invalid_temperature_rows,
        SUM(CASE WHEN humidity IS NULL OR NOT isfinite(humidity)
                 OR humidity < 0 OR humidity > 100 THEN 1 ELSE 0 END)::BIGINT
            AS invalid_humidity_rows,
        SUM(CASE WHEN status IS NULL OR status NOT IN ('RUNNING', 'IDLE', 'STOPPED', 'ERROR')
                 THEN 1 ELSE 0 END)::BIGINT AS invalid_status_rows,
        SUM(CASE WHEN source."topic" IS NULL OR trim(source."topic") = ''
                 OR source."partition" IS NULL OR source."partition" < 0
                 OR source."offset" IS NULL OR source."offset" < 0 THEN 1 ELSE 0 END)::BIGINT
            AS invalid_lineage_rows,
        0::BIGINT AS partition_mismatch_rows,
        MAX(event_time) AS max_event_time
    FROM read_parquet(?)
    """


class LakeQualityError(ValueError):
    """Raised when a lake quality check cannot be executed safely."""


@dataclass(frozen=True)
class LakeQualityReport:
    """Serializable quality evidence for one Parquet path."""

    path: str
    files: tuple[str, ...]
    layout: str
    evaluated_at: str
    expected_schema_version: str
    row_count: int
    unique_event_ids: int
    duplicate_rows: int
    null_required_rows: int
    invalid_event_id_rows: int
    invalid_schema_version_rows: int
    invalid_temperature_rows: int
    invalid_humidity_rows: int
    invalid_status_rows: int
    invalid_lineage_rows: int
    partition_mismatch_rows: int
    max_event_age_seconds: float | None
    freshness_limit_seconds: float | None
    missing_columns: tuple[str, ...]
    passed: bool
    failures: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        """Return JSON-compatible evidence without leaking implementation objects."""

        return {
            "path": self.path,
            "files": list(self.files),
            "layout": self.layout,
            "evaluated_at": self.evaluated_at,
            "expected_schema_version": self.expected_schema_version,
            "row_count": self.row_count,
            "unique_event_ids": self.unique_event_ids,
            "duplicate_rows": self.duplicate_rows,
            "null_required_rows": self.null_required_rows,
            "invalid_event_id_rows": self.invalid_event_id_rows,
            "invalid_schema_version_rows": self.invalid_schema_version_rows,
            "invalid_temperature_rows": self.invalid_temperature_rows,
            "invalid_humidity_rows": self.invalid_humidity_rows,
            "invalid_status_rows": self.invalid_status_rows,
            "invalid_lineage_rows": self.invalid_lineage_rows,
            "partition_mismatch_rows": self.partition_mismatch_rows,
            "max_event_age_seconds": self.max_event_age_seconds,
            "freshness_limit_seconds": self.freshness_limit_seconds,
            "missing_columns": list(self.missing_columns),
            "passed": self.passed,
            "failures": list(self.failures),
        }

    def as_json(self) -> str:
        """Return stable, human-readable JSON for CI artifacts."""

        return json.dumps(self.as_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _parquet_files(path: str | Path) -> tuple[Path, ...]:
    target = Path(path).expanduser()
    if not target.exists():
        raise LakeQualityError(f"lake path does not exist: {target}")
    if target.is_file():
        if target.suffix != ".parquet":
            raise LakeQualityError(f"lake path is not a Parquet file: {target}")
        return (target.resolve(),)
    files = tuple(sorted(item.resolve() for item in target.rglob("*.parquet")))
    if not files:
        raise LakeQualityError(f"lake path contains no Parquet files: {target}")
    return files


def _isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _failure_names(report_values: dict[str, int], row_count: int, allow_empty: bool) -> list[str]:
    failures = [name for name, value in report_values.items() if value > 0]
    if row_count == 0 and not allow_empty:
        failures.append("empty_lake")
    return failures


def validate_lake_path(
    path: str | Path,
    *,
    expected_schema_version: str = CANONICAL_SCHEMA_VERSION,
    allow_empty: bool = False,
    max_event_age_seconds: float | None = None,
    reference_time: datetime | None = None,
) -> LakeQualityReport:
    """Validate canonical Parquet files and return a release-gate report.

    The check covers the data contract fields emitted by ``lake_sink.py``:
    nulls, event-id uniqueness, schema version, measurement ranges, status,
    Kafka lineage, and event-date partition consistency. ``reference_time`` is
    injectable so freshness tests remain deterministic.
    """

    if not expected_schema_version.strip():
        raise LakeQualityError("expected_schema_version must not be empty")
    if max_event_age_seconds is not None and max_event_age_seconds < 0:
        raise LakeQualityError("max_event_age_seconds must be non-negative")

    files = _parquet_files(path)
    evaluated_at = _isoformat(reference_time or datetime.now(timezone.utc))
    connection = duckdb.connect(":memory:")
    try:
        file_arguments = [str(file) for file in files]
        columns = {
            row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [file_arguments]
            ).fetchall()
        }
        if set(CANONICAL_COLUMNS).issubset(columns):
            layout = "local_event_time"
            quality_query = LOCAL_QUALITY_QUERY
        elif set(AWS_BRONZE_COLUMNS).issubset(columns):
            layout = "aws_bronze"
            # Firehose partitions by arrival time, not event time. The Athena
            # table exposes those partition keys outside the Parquet payload,
            # so an event-date equality check is not meaningful here.
            quality_query = AWS_BRONZE_QUALITY_QUERY
        else:
            missing_columns = tuple(
                column for column in CANONICAL_COLUMNS if column not in columns
            )
            return LakeQualityReport(
                path=str(Path(path).expanduser().resolve()),
                files=tuple(str(file) for file in files),
                layout="unknown",
                evaluated_at=evaluated_at,
                expected_schema_version=expected_schema_version,
                row_count=0,
                unique_event_ids=0,
                duplicate_rows=0,
                null_required_rows=0,
                invalid_event_id_rows=0,
                invalid_schema_version_rows=0,
                invalid_temperature_rows=0,
                invalid_humidity_rows=0,
                invalid_status_rows=0,
                invalid_lineage_rows=0,
                partition_mismatch_rows=0,
                max_event_age_seconds=None,
                freshness_limit_seconds=max_event_age_seconds,
                missing_columns=missing_columns,
                passed=False,
                failures=("missing_columns",),
            )

        row = connection.execute(
            quality_query,
            [expected_schema_version, file_arguments],
        ).fetchone()
        if row is None:
            raise LakeQualityError("quality query returned no result")

        row_count = int(row[0] or 0)
        unique_event_ids = int(row[1] or 0)
        duplicate_rows = int(row[2] or 0)
        invalid_event_id_rows = int(row[3] or 0)
        null_required_rows = int(row[4] or 0)
        invalid_schema_version_rows = int(row[5] or 0)
        invalid_temperature_rows = int(row[6] or 0)
        invalid_humidity_rows = int(row[7] or 0)
        invalid_status_rows = int(row[8] or 0)
        invalid_lineage_rows = int(row[9] or 0)
        partition_mismatch_rows = int(row[10] or 0)
        max_event_time = row[11]
        age_seconds: float | None = None
        if max_event_time is not None:
            if max_event_time.tzinfo is None:
                max_event_time = max_event_time.replace(tzinfo=timezone.utc)
            reference_utc = (reference_time or datetime.now(timezone.utc)).astimezone(timezone.utc)
            age_seconds = max(
                0.0,
                (reference_utc - max_event_time.astimezone(timezone.utc)).total_seconds(),
            )

        check_values = {
            "duplicate_rows": duplicate_rows,
            "null_required_rows": null_required_rows,
            "invalid_event_id_rows": invalid_event_id_rows,
            "invalid_schema_version_rows": invalid_schema_version_rows,
            "invalid_temperature_rows": invalid_temperature_rows,
            "invalid_humidity_rows": invalid_humidity_rows,
            "invalid_status_rows": invalid_status_rows,
            "invalid_lineage_rows": invalid_lineage_rows,
            "partition_mismatch_rows": partition_mismatch_rows,
        }
        failures = _failure_names(check_values, row_count, allow_empty)
        if (
            max_event_age_seconds is not None
            and age_seconds is not None
            and age_seconds > max_event_age_seconds
        ):
            failures.append("freshness")

        return LakeQualityReport(
            path=str(Path(path).expanduser().resolve()),
            files=tuple(str(file) for file in files),
            layout=layout,
            evaluated_at=evaluated_at,
            expected_schema_version=expected_schema_version,
            row_count=row_count,
            unique_event_ids=unique_event_ids,
            duplicate_rows=duplicate_rows,
            null_required_rows=null_required_rows,
            invalid_event_id_rows=invalid_event_id_rows,
            invalid_schema_version_rows=invalid_schema_version_rows,
            invalid_temperature_rows=invalid_temperature_rows,
            invalid_humidity_rows=invalid_humidity_rows,
            invalid_status_rows=invalid_status_rows,
            invalid_lineage_rows=invalid_lineage_rows,
            partition_mismatch_rows=partition_mismatch_rows,
            max_event_age_seconds=age_seconds,
            freshness_limit_seconds=max_event_age_seconds,
            missing_columns=(),
            passed=not failures,
            failures=tuple(failures),
        )
    finally:
        connection.close()
