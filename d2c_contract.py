"""Shared D2C approval event contract used by producers and consumers."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4


D2C_EVENT_TYPE = "d2c.application.approved.v1"
D2C_EVENT_VERSION = 1
_RFC3339_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


def _parse_occurred_at(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("D2C event occurred_at must be an RFC 3339 timestamp")
    if not _RFC3339_TIMESTAMP_PATTERN.fullmatch(value):
        raise ValueError("D2C event occurred_at must be an RFC 3339 timestamp")
    try:
        occurred_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("D2C event occurred_at must be an RFC 3339 timestamp") from error
    if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
        raise ValueError("D2C event occurred_at must include a timezone")
    return occurred_at.astimezone(timezone.utc)


def validate_application_approved_event(event: object) -> dict[str, object]:
    """Validate the executable form of the published D2C approval contract.

    The JSON Schema remains the external contract. This validator is deliberately
    shared by the operational consumer and the lakehouse writer so each durable
    sink rejects the same malformed or unsupported event before persistence.
    """

    if not isinstance(event, dict):
        raise ValueError("D2C event must be a JSON object")

    required = {"event_id", "event_type", "event_version", "occurred_at", "data"}
    if set(event) != required:
        raise ValueError("D2C event has missing or unsupported fields")
    if event["event_type"] != D2C_EVENT_TYPE:
        raise ValueError("unsupported D2C event type")
    if event["event_version"] != D2C_EVENT_VERSION:
        raise ValueError("unsupported D2C event version")

    try:
        UUID(str(event["event_id"]))
    except (TypeError, ValueError) as error:
        raise ValueError("D2C event_id must be a UUID") from error
    _parse_occurred_at(event["occurred_at"])

    data = event["data"]
    if not isinstance(data, dict) or set(data) != {
        "application_id",
        "user_id",
        "campaign_id",
    }:
        raise ValueError("D2C event data is invalid")
    for field in ("application_id", "user_id", "campaign_id"):
        value = data[field]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"D2C event data.{field} must be a positive integer")

    return event


def canonicalize_application_approved_event(event: object) -> str:
    """Return the stable payload representation used for immutable-ID checks."""

    validated = validate_application_approved_event(event)
    return json.dumps(validated, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def build_application_approved_event(
    *, application_id: int, user_id: int, campaign_id: int
) -> dict[str, object]:
    """Build the immutable event stored in the transactional outbox."""

    return {
        "event_id": str(uuid4()),
        "event_type": D2C_EVENT_TYPE,
        "event_version": D2C_EVENT_VERSION,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "application_id": application_id,
            "user_id": user_id,
            "campaign_id": campaign_id,
        },
    }
