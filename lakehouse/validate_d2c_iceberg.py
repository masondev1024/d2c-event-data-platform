"""Read-only reconciliation gate for the D2C local Iceberg lakehouse."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

from lakehouse.d2c_iceberg_stream import (
    LakehouseSettings,
    create_spark_session,
    create_tables,
    settings_from_environment,
)


def _optional_expected_count(environment: Mapping[str, str], key: str) -> int | None:
    raw_value = environment.get(key)
    if raw_value is None or not raw_value.strip():
        return None
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{key} must be a non-negative integer") from error
    if value < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return value


def validate(spark: Any, settings: LakehouseSettings, environment: Mapping[str, str]) -> dict[str, int]:
    """Prove table uniqueness, batch parity, and Iceberg snapshot creation."""

    approvals = int(spark.table(settings.approvals_table).count())
    quarantined = int(spark.table(settings.dlq_table).count())
    duplicate_event_ids = int(
        spark.sql(
            f"""
            SELECT COUNT(*) AS duplicate_groups
            FROM (
                SELECT event_id
                FROM {settings.approvals_table}
                GROUP BY event_id
                HAVING COUNT(*) > 1
            )
            """
        ).first()["duplicate_groups"]
    )
    batch_audit_rows = int(spark.table(settings.batch_audit_table).count())
    invalid_audit_rows = int(
        spark.sql(
            f"""
            SELECT COUNT(*) AS invalid_rows
            FROM {settings.batch_audit_table}
            WHERE source_records <> valid_records + quarantined_records
               OR duplicate_records < 0
            """
        ).first()["invalid_rows"]
    )
    duplicate_audit_ranges = int(
        spark.sql(
            f"""
            SELECT COUNT(*) AS duplicate_groups
            FROM (
                SELECT stream_id, source_fingerprint
                FROM {settings.batch_audit_table}
                GROUP BY stream_id, source_fingerprint
                HAVING COUNT(*) > 1
            )
            """
        ).first()["duplicate_groups"]
    )
    approval_snapshots = int(spark.table(f"{settings.approvals_table}.snapshots").count())
    dlq_snapshots = int(spark.table(f"{settings.dlq_table}.snapshots").count())
    batch_audit_snapshots = int(
        spark.table(f"{settings.batch_audit_table}.snapshots").count()
    )

    if duplicate_event_ids:
        raise RuntimeError(f"Iceberg approvals table contains {duplicate_event_ids} duplicate event IDs")
    if invalid_audit_rows:
        raise RuntimeError(f"Iceberg batch audit contains {invalid_audit_rows} parity violations")
    if duplicate_audit_ranges:
        raise RuntimeError(
            f"Iceberg batch audit contains {duplicate_audit_ranges} duplicate source ranges"
        )
    if approvals and not approval_snapshots:
        raise RuntimeError("Iceberg approvals table has data without a committed snapshot")
    if quarantined and not dlq_snapshots:
        raise RuntimeError("Iceberg quarantine table has data without a committed snapshot")

    expected_approvals = _optional_expected_count(
        environment, "D2C_LAKEHOUSE_EXPECTED_APPROVALS"
    )
    expected_quarantined = _optional_expected_count(
        environment, "D2C_LAKEHOUSE_EXPECTED_QUARANTINED"
    )
    if expected_approvals is not None and approvals != expected_approvals:
        raise RuntimeError(
            f"expected {expected_approvals} approvals, found {approvals} in Iceberg"
        )
    if expected_quarantined is not None and quarantined != expected_quarantined:
        raise RuntimeError(
            f"expected {expected_quarantined} quarantined records, found {quarantined} in Iceberg"
        )

    return {
        "approvals": approvals,
        "quarantined": quarantined,
        "duplicate_event_ids": duplicate_event_ids,
        "invalid_audit_rows": invalid_audit_rows,
        "batch_audit_rows": batch_audit_rows,
        "duplicate_audit_ranges": duplicate_audit_ranges,
        "approval_snapshots": approval_snapshots,
        "dlq_snapshots": dlq_snapshots,
        "batch_audit_snapshots": batch_audit_snapshots,
    }


def main() -> None:
    settings = settings_from_environment(os.environ)
    spark = create_spark_session(settings)
    try:
        create_tables(spark, settings)
        print(json.dumps(validate(spark, settings, os.environ), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
