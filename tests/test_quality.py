from __future__ import annotations

import unittest
from datetime import datetime, timezone
from tempfile import TemporaryDirectory

import duckdb

from services.lake_sink import write_canonical_events
from services.quality import validate_lake_path


def _event(event_id: str, event_time: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "schema_version": "factory-sensor.v1",
        "event_time": event_time,
        "ingested_at": event_time,
        "sensor_id": "AI-FACTORY-001",
        "temperature": 87.5,
        "humidity": 42.4,
        "status": "RUNNING",
        "source": {"topic": "factory.sensor.clean.v1", "partition": 0, "offset": 1},
    }


class LakeQualityTest(unittest.TestCase):
    def test_valid_lake_passes_contract_and_lineage_checks(self) -> None:
        with TemporaryDirectory() as directory:
            write_canonical_events(
                [
                    _event("a" * 64, "2026-09-06T00:00:00Z"),
                    _event("b" * 64, "2026-09-06T00:01:00Z"),
                ],
                directory,
                "quality-pass",
            )

            report = validate_lake_path(
                directory,
                reference_time=datetime(2026, 9, 6, 0, 2, tzinfo=timezone.utc),
            )

            self.assertTrue(report.passed)
            self.assertEqual(report.row_count, 2)
            self.assertEqual(report.unique_event_ids, 2)
            self.assertEqual(report.failures, ())

    def test_invalid_id_and_stale_event_fail_the_gate(self) -> None:
        with TemporaryDirectory() as directory:
            write_canonical_events(
                [_event("not-a-sha256", "2026-09-03T00:00:00Z")],
                directory,
                "quality-fail",
            )

            report = validate_lake_path(
                directory,
                max_event_age_seconds=60,
                reference_time=datetime(2026, 9, 6, 0, 0, tzinfo=timezone.utc),
            )

            self.assertFalse(report.passed)
            self.assertIn("invalid_event_id_rows", report.failures)
            self.assertIn("freshness", report.failures)

    def test_aws_bronze_nested_source_layout_passes_without_event_partition_check(self) -> None:
        with TemporaryDirectory() as directory:
            parquet_path = f"{directory}/bronze.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    """
                    CREATE TABLE aws_events (
                        event_id VARCHAR,
                        schema_version VARCHAR,
                        event_time TIMESTAMPTZ,
                        ingested_at TIMESTAMPTZ,
                        sensor_id VARCHAR,
                        temperature DOUBLE,
                        humidity DOUBLE,
                        status VARCHAR,
                        source STRUCT(topic VARCHAR, "partition" INTEGER, "offset" BIGINT)
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO aws_events VALUES (
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        'factory-sensor.v1',
                        '2026-09-06T00:00:00Z',
                        '2026-09-06T00:00:01Z',
                        'AI-FACTORY-001',
                        87.5,
                        42.4,
                        'RUNNING',
                        {'topic': 'factory.sensor.clean.v1', 'partition': 0, 'offset': 1}
                    )
                    """
                )
                connection.table("aws_events").write_parquet(parquet_path)
            finally:
                connection.close()

            report = validate_lake_path(parquet_path)

            self.assertTrue(report.passed)
            self.assertEqual(report.layout, "aws_bronze")
            self.assertEqual(report.partition_mismatch_rows, 0)


if __name__ == "__main__":
    unittest.main()
