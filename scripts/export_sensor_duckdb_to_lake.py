"""Export the local canonical DuckDB sink into the idempotent Parquet lake sink."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Support ``python scripts/export_sensor_duckdb_to_lake.py`` from any directory.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from services.lake_sink import write_canonical_events


def _isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def read_sensor_events(database_path: str | Path) -> list[dict[str, object]]:
    """Read the sink table without mutating the source database."""

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT event_id, schema_version, event_time, ingested_at, sensor_id,
                   temperature, humidity, status, source_topic,
                   source_partition, source_offset
            FROM sensor_events
            ORDER BY event_time, event_id
            """
        ).fetchall()
    finally:
        connection.close()

    return [
        {
            "event_id": row[0],
            "schema_version": row[1],
            "event_time": _isoformat(row[2]),
            "ingested_at": _isoformat(row[3]),
            "sensor_id": row[4],
            "temperature": row[5],
            "humidity": row[6],
            "status": row[7],
            "source": {
                "topic": row[8],
                "partition": row[9],
                "offset": row[10],
            },
        }
        for row in rows
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="data/sensor.duckdb")
    parser.add_argument("--output", default="data/lake")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    events = read_sensor_events(args.database)
    result = write_canonical_events(events, args.output, args.batch_id)
    print(json.dumps(result.__dict__, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
