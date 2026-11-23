"""Publish MySQL raffle outbox events to Kafka in bounded transactions.

Run with python -m app.raffle_outbox_relay. Runtime clients: PyMySQL and
kafka-python. Delivery is at least once: consumers can deduplicate by event_id.
The published count covers committed outbox marks and is null on an unknown
commit outcome. Deferred counts only selected rows left untouched this run.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack, closing, suppress
from functools import cache
import json
import math
import os
from time import monotonic, sleep
from typing import Any

from raffle_contract import TOPIC, decode_event, is_canonical_uuid


DEFAULT_BATCH_SIZE = 50
MAX_BATCH_SIZE = 500
ACK_TIMEOUT_SECONDS = 10
BATCH_WORK_SECONDS = 30

# InnoDB appends the event_id primary key to the (published_at, created_at)
# index. Walk it in lock order, including a deterministic tie-break for equal times.
SELECT_PENDING = """
    SELECT event_id, aggregate_type, aggregate_id, event_type, event_version, payload
    FROM raffle_outbox_events FORCE INDEX (idx_raffle_outbox_unpublished)
    WHERE published_at IS NULL AND event_type = %s
    ORDER BY created_at, event_id
    LIMIT %s FOR UPDATE SKIP LOCKED
"""
MARK_PUBLISHED = """
    UPDATE raffle_outbox_events
    SET published_at = UTC_TIMESTAMP(6), publish_attempts = publish_attempts + 1,
        last_error = NULL
    WHERE event_id = %s AND published_at IS NULL
"""
RECORD_FAILURE = """
    UPDATE raffle_outbox_events
    SET publish_attempts = publish_attempts + 1, last_error = %s
    WHERE event_id = %s AND published_at IS NULL
"""


class RelayError(ValueError):
    """A relay diagnostic safe to include in operator output."""


class CommitOutcomeUnknown(RelayError):
    """MySQL may have committed before the connection reported a failure."""


def _validated_event(row: dict[str, Any]) -> dict[str, Any]:
    event = decode_event(row["payload"])
    if row["event_id"] != event["event_id"]:
        raise RelayError("event_id differs from the outbox row")
    if row["event_type"] != event["event_type"]:
        raise RelayError("event_type differs from the outbox row")
    if type(row["event_version"]) is not int or row["event_version"] != event["event_version"]:
        raise RelayError("event_version differs from the outbox row")
    if row["aggregate_type"] != "raffle_entry":
        raise RelayError("unexpected outbox aggregate_type")
    if type(row["aggregate_id"]) is not int or row["aggregate_id"] != event["data"]["entry_id"]:
        raise RelayError("entry_id differs from the outbox aggregate_id")
    return event


def _publish(producer, event: dict[str, Any], deadline: float) -> dict[str, Any]:
    key = str(event["data"]["entry_id"]).encode("ascii")
    value = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    future = producer.send(TOPIC, key=key, value=value)
    metadata = future.get(timeout=max(0, min(ACK_TIMEOUT_SECONDS, deadline - monotonic())))
    if getattr(metadata, "topic", None) != TOPIC:
        raise RelayError("missing acknowledgement for the raffle topic")
    for field in ("partition", "offset"):
        coordinate = getattr(metadata, field, None)
        if type(coordinate) is not int or coordinate < 0:
            raise RelayError(f"invalid acknowledged Kafka {field}")
    return {
        "event_id": event["event_id"],
        "entry_id": event["data"]["entry_id"],
        "item_id": event["data"]["item_id"],
        "topic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
    }


def _update_pending(cursor, statement: str, event_id: str, *values) -> None:
    cursor.execute(statement, (*values, event_id))
    if cursor.rowcount != 1:
        raise RelayError("expected to update exactly one locked pending outbox row")


def _safe_error(error: Exception) -> str:
    # Driver exception messages may contain SQL parameters or serialized records.
    return str(error) if isinstance(error, RelayError) else type(error).__name__


def _record_failure(cursor, event_id: str, stage: str, error: Exception) -> dict:
    reason = f"{stage}: {_safe_error(error)}"
    _update_pending(cursor, RECORD_FAILURE, event_id, reason)
    # A malformed row key is untrusted output too. Emit only a UUID or JSON null.
    return {"event_id": event_id if is_canonical_uuid(event_id) else None, "error": reason}


def publish_batch(connection, producer_factory, batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
    """Own one transaction on a dedicated connection. The caller owns the producer."""
    if type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise RelayError(f"batch size must be between 1 and {MAX_BATCH_SIZE}")
    coordinates = []
    failures = []
    producer = None
    deadline = monotonic() + BATCH_WORK_SECONDS
    connection.begin()
    try:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_PENDING, (TOPIC, batch_size))
            rows = cursor.fetchall()
            for row in rows:
                if monotonic() >= deadline:
                    break
                event_id = row["event_id"]
                try:
                    event = _validated_event(row)
                except ValueError as error:
                    failures.append(_record_failure(cursor, event_id, "validation", error))
                    continue
                try:
                    if producer is None:
                        producer = producer_factory()
                        if producer.config["acks"] not in ("all", -1):
                            raise RelayError("the relay requires acks=all")
                    coordinate = _publish(producer, event, deadline)
                except Exception as error:
                    failures.append(_record_failure(cursor, event_id, "kafka", error))
                    break
                _update_pending(cursor, MARK_PUBLISHED, event_id)
                coordinates.append(coordinate)
        try:
            connection.commit()
        except Exception as error:
            raise CommitOutcomeUnknown("MySQL commit outcome is unknown") from error
    except BaseException:
        # A lost socket can fail rollback too. Preserve the original failure.
        with suppress(Exception):
            connection.rollback()
        raise
    return {
        "topic": TOPIC,
        "published": len(coordinates),
        "coordinates": coordinates,
        "failures": failures,
        "deferred": len(rows) - len(coordinates) - len(failures),
    }


def _environment(name: str) -> str:
    if name not in os.environ:
        raise RelayError(f"{name} must be configured")
    return os.environ[name]


def _connect():
    import pymysql

    return pymysql.connect(
        host=_environment("MYSQL_HOST"),
        database=_environment("MYSQL_DATABASE"),
        user=_environment("MYSQL_USER"),
        password=_environment("MYSQL_PASSWORD"),
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
        init_command="SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
    )


def _open_producer(resources: ExitStack, servers: str):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=servers,
        acks="all",
        enable_idempotence=True,
        bootstrap_timeout_ms=5000,
        max_block_ms=5000,
        request_timeout_ms=5000,
        delivery_timeout_ms=ACK_TIMEOUT_SECONDS * 1000,
    )
    resources.callback(producer.close, timeout=ACK_TIMEOUT_SECONDS)
    return producer


def _run_batches(connection, producer_factory, batch_size: int, follow: bool, poll_seconds: float) -> dict:
    while True:
        batch = publish_batch(connection, producer_factory, batch_size)
        if not follow or batch["failures"]:
            return batch
        if batch["published"]:
            print(json.dumps(batch, ensure_ascii=False), flush=True)
        if batch["published"] < batch_size and not batch["deferred"]:
            sleep(poll_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relay pending raffle entries to Kafka.")
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"maximum rows to lock (1–{MAX_BATCH_SIZE}, default {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument("--follow", action="store_true", help="continue polling for pending rows")
    parser.add_argument("--poll-seconds", type=float, default=1, help="delay after draining pending rows (default 1)")
    args = parser.parse_args(argv)
    if not math.isfinite(args.poll_seconds) or args.poll_seconds <= 0:
        parser.error("--poll-seconds must be finite and positive")
    summary = {"topic": TOPIC, "published": 0, "coordinates": [], "failures": []}
    try:
        servers = _environment("KAFKA_BOOTSTRAP_SERVERS")
        with ExitStack() as resources:
            connection = resources.enter_context(closing(_connect()))

            @cache
            def producer():
                return _open_producer(resources, servers)

            try:
                summary = _run_batches(connection, producer, args.batch_size, args.follow, args.poll_seconds)
            except CommitOutcomeUnknown as error:
                # Preserve uncertainty even if closing either client also fails.
                summary.update(published=None, error=str(error))
    except Exception as error:
        summary.setdefault("error", _safe_error(error))
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return int("error" in summary or bool(summary["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
