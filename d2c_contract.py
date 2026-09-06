"""Shared D2C approval event contract used by producers and consumers."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


D2C_EVENT_TYPE = "d2c.application.approved.v1"
D2C_EVENT_VERSION = 1


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
