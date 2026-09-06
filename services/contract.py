"""Data contract and normalization for factory sensor events.

The raw topics intentionally accept more than one input shape because the
collector path contains both JSON and text/logstash producers. Everything
after the normalizer uses the versioned canonical contract below.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


TEXT_EVENT_PATTERN = re.compile(
    r"\[(?P<event_time>[^\]]+)\]\s+ID=(?P<sensor_id>[^|]+)\s+\|\s*"
    r"TEMP:(?P<temperature>[-+]?\d+(?:\.\d+)?)\s+\|\s*"
    r"HUMI:(?P<humidity>[-+]?\d+(?:\.\d+)?)\s+\|\s*"
    r"STAT:(?P<status>[A-Za-z_]+)"
)
CANONICAL_SCHEMA_VERSION = "factory-sensor.v1"
DLQ_SCHEMA_VERSION = "factory-sensor-dlq.v1"


class DataQualityError(ValueError):
    """Raised when a source record violates the canonical contract."""


@dataclass(frozen=True)
class NormalizedSensorEvent:
    schema_version: str
    event_id: str
    event_time: str
    ingested_at: str
    sensor_id: str
    temperature: float
    humidity: float
    status: str

    def as_dict(self, source_topic: str, source_partition: int, source_offset: int) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_time": self.event_time,
            "ingested_at": self.ingested_at,
            "sensor_id": self.sensor_id,
            "temperature": self.temperature,
            "humidity": self.humidity,
            "status": self.status,
            "source": {
                "topic": source_topic,
                "partition": source_partition,
                "offset": source_offset,
            },
        }


def _parse_event_time(value: Any) -> str:
    if value is None or str(value).strip() == "":
        raise DataQualityError("event_time is required")

    text = str(value).strip().strip('"')
    parsed: datetime
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
        else:
            raise DataQualityError(f"unsupported event_time format: {text}") from None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_float(value: Any, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise DataQualityError(f"{field_name} must be numeric") from exc
    if not math.isfinite(parsed):
        raise DataQualityError(f"{field_name} must be finite")
    return parsed


def _decode_text_fields(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise DataQualityError("record must be a JSON object or text")

    candidate = value.strip().strip('"')
    match = TEXT_EVENT_PATTERN.search(candidate)
    if not match:
        raise DataQualityError("text record does not match the sensor log contract")
    return match.groupdict()


def _unwrap_record(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        record = raw
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return _decode_text_fields(raw)
        return _unwrap_record(parsed)
    else:
        raise DataQualityError("record must be a JSON object")

    # Logstash/HTTP can wrap the original text in message or log. Prefer the
    # already extracted fields, then fall back to parsing the text payload.
    if {"sensor_id", "temperature", "humidity", "status"}.issubset(record):
        return record
    if {"device_id", "temp", "humi", "status"}.issubset(record):
        return {
            **record,
            "sensor_id": record["device_id"],
            "temperature": record["temp"],
            "humidity": record["humi"],
            "event_time": record.get("log_time") or record.get("timestamp"),
        }

    for field_name in ("log", "message", "event.original"):
        if field_name in record:
            return {**record, **_decode_text_fields(record[field_name])}
    return record


def normalize_sensor_event(raw: Any, ingested_at: datetime | None = None) -> NormalizedSensorEvent:
    """Normalize a raw JSON/text record and enforce the sensor contract."""

    record = _unwrap_record(raw)
    event_time = _parse_event_time(
        record.get("event_time")
        or record.get("timestamp")
        or record.get("log_time")
        or record.get("@timestamp")
    )
    sensor_id = str(record.get("sensor_id", "")).strip()
    status = str(record.get("status", "")).strip().upper()

    if not sensor_id:
        raise DataQualityError("sensor_id is required")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{2,63}", sensor_id):
        raise DataQualityError("sensor_id has an invalid format")

    temperature = _as_float(record.get("temperature"), "temperature")
    humidity = _as_float(record.get("humidity"), "humidity")
    if not -40.0 <= temperature <= 150.0:
        raise DataQualityError("temperature is outside the allowed range [-40, 150]")
    if not 0.0 <= humidity <= 100.0:
        raise DataQualityError("humidity is outside the allowed range [0, 100]")
    if status not in {"RUNNING", "IDLE", "STOPPED", "ERROR"}:
        raise DataQualityError(f"unsupported status: {status}")

    identity = json.dumps(
        {
            "event_time": event_time,
            "humidity": humidity,
            "sensor_id": sensor_id,
            "status": status,
            "temperature": temperature,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    event_id = hashlib.sha256(identity).hexdigest()
    ingestion_time = (ingested_at or datetime.now(timezone.utc)).astimezone(timezone.utc)

    return NormalizedSensorEvent(
        schema_version=CANONICAL_SCHEMA_VERSION,
        event_id=event_id,
        event_time=event_time,
        ingested_at=ingestion_time.isoformat().replace("+00:00", "Z"),
        sensor_id=sensor_id,
        temperature=temperature,
        humidity=humidity,
        status=status,
    )


def validate_canonical_sensor_event(raw: Any) -> None:
    """Validate an already-normalized event before a downstream write.

    The processor validates source records, but every downstream boundary must
    still fail closed because a replay, manual publish, or producer regression
    can bypass that process. The event id is recomputed to protect idempotency
    from a payload whose identity was altered after normalization.
    """

    if not isinstance(raw, dict):
        raise DataQualityError("canonical event must be a JSON object")

    required = {
        "schema_version",
        "event_id",
        "event_time",
        "ingested_at",
        "sensor_id",
        "temperature",
        "humidity",
        "status",
        "source",
    }
    missing = sorted(field for field in required if raw.get(field) in (None, ""))
    if missing:
        raise DataQualityError(f"canonical event is missing fields: {missing}")
    unknown = sorted(set(raw) - required)
    if unknown:
        raise DataQualityError(f"canonical event has unsupported fields: {unknown}")
    if raw["schema_version"] != CANONICAL_SCHEMA_VERSION:
        raise DataQualityError(
            f"unsupported canonical schema_version: {raw['schema_version']}"
        )

    source = raw["source"]
    if not isinstance(source, dict):
        raise DataQualityError("canonical event source must be an object")
    if set(source) != {"topic", "partition", "offset"}:
        raise DataQualityError("canonical event source must contain only topic, partition and offset")
    if not isinstance(source["topic"], str) or not source["topic"].strip():
        raise DataQualityError("canonical event source.topic is required")
    if (
        isinstance(source["partition"], bool)
        or not isinstance(source["partition"], int)
        or source["partition"] < 0
    ):
        raise DataQualityError("canonical event source.partition must be a non-negative integer")
    if (
        isinstance(source["offset"], bool)
        or not isinstance(source["offset"], int)
        or source["offset"] < 0
    ):
        raise DataQualityError("canonical event source.offset must be a non-negative integer")

    event = normalize_sensor_event(
        {
            "timestamp": raw["event_time"],
            "sensor_id": raw["sensor_id"],
            "temperature": raw["temperature"],
            "humidity": raw["humidity"],
            "status": raw["status"],
        }
    )
    if not isinstance(raw["event_id"], str) or raw["event_id"] != event.event_id:
        raise DataQualityError("event_id does not match the canonical payload")
    if raw["ingested_at"] in (None, ""):
        raise DataQualityError("ingested_at is required")
    _parse_event_time(raw["ingested_at"])
