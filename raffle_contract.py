"""Wire contract of the GitOps writer's raffle.entry.accepted.v1 outbox payload."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
from typing import Any
from uuid import UUID


TOPIC = "raffle.entry.accepted.v1"
_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])"
)


def is_canonical_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ValueError("duplicate JSON field")
    return fields


def decode_event(raw: bytes | bytearray | str | dict) -> dict:
    """Validate without changing the producer's UUID, timestamp or business identifiers."""
    if isinstance(raw, (str, bytes, bytearray)):
        try:
            text = raw if isinstance(raw, str) else raw.decode("utf-8")
            event = json.loads(text, object_pairs_hook=_unique_fields)
        except (ValueError, RecursionError):
            raise ValueError("invalid raffle JSON") from None
    else:
        event = raw
    if not isinstance(event, dict) or event.keys() != {"event_id", "event_type", "event_version", "occurred_at", "data"}:
        raise ValueError("invalid raffle event envelope")
    if not is_canonical_uuid(event["event_id"]):
        raise ValueError("invalid event_id")
    if event["event_type"] != TOPIC or type(event["event_version"]) is not int or event["event_version"] != 1:
        raise ValueError("unsupported raffle event type or version")
    stamp = event["occurred_at"]
    if not isinstance(stamp, str) or not _TIMESTAMP.fullmatch(stamp):
        raise ValueError("invalid occurred_at")
    try:
        datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise ValueError("invalid occurred_at") from None
    data = event["data"]
    if not isinstance(data, dict) or data.keys() != {"entry_id", "item_id", "user_id"}:
        raise ValueError("invalid raffle entry fields")
    for key, value in data.items():
        if type(value) is not int or not 0 < value <= 2**63 - 1:
            raise ValueError(f"invalid {key}")
    return event
