"""Recovery drill CLI. Run as python /trace/drill.py in a one-shot relay process."""

from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import hashlib
import json
import os
import signal
import sys
from time import monotonic, sleep
from unittest.mock import patch

from app import raffle_outbox_relay as relay
from raffle_contract import TOPIC, is_canonical_uuid


HOLD_COUNT = 500
DUPLICATE_COUNT = 1000
SEND_WINDOW = 100
# verify_raffle_recovery.exercise captures bounds["lower"] on its fresh topic
# after exactly this many committed publications, before starting the crash relay.
HEALTHY_PREFIX_COUNT = 8000

LOCK_OLDEST = """
    SELECT event_id FROM raffle_outbox_events FORCE INDEX (idx_raffle_outbox_unpublished)
    WHERE published_at IS NULL AND event_type = %s
    ORDER BY created_at, event_id LIMIT %s FOR UPDATE
"""
SELECT_RANGE_BOUNDARY = """
    SELECT published_at FROM raffle_outbox_events
    WHERE event_type = %s AND published_at IS NOT NULL
    ORDER BY published_at, created_at, event_id LIMIT 1 OFFSET %s
"""
SELECT_PUBLISHED = """
    SELECT event_id, aggregate_type, aggregate_id, event_type, event_version, payload
    FROM raffle_outbox_events
    WHERE event_type = %s AND published_at > %s
    ORDER BY published_at DESC, created_at DESC, event_id DESC LIMIT %s
"""


class DrillError(Exception):
    """Messages must be safe for the orchestrator log."""


def proof(name, body):
    print(f"{name}=" + json.dumps(body, separators=(",", ":"), allow_nan=False), flush=True)


def drain(connection, producer, count):
    published = 0
    batches = 0
    first = last = None
    while published < count:
        size = min(count - published, relay.MAX_BATCH_SIZE)
        batch = relay.publish_batch(connection, lambda: producer, size)
        points = batch["coordinates"]
        marked = batch["published"]
        if batch["failures"] or not (0 < marked <= size and len(points) == marked):
            raise DrillError(f"drain stopped after {published + marked} committed marks")
        published += marked
        batches += 1
        first = first or points[0]
        last = points[-1]
    return {"published": published, "batches": batches, "first": first, "last": last}


def crash(connection, producer):
    publish = relay._publish

    def stop_after_ack(client, event, deadline):
        coordinate = publish(client, event, deadline)
        proof("ACK_PROOF", coordinate)
        signal.alarm(0)
        os.kill(os.getpid(), signal.SIGSTOP)
        # A stray SIGCONT must never let publish_batch mark or commit the row.
        os._exit(1)

    with patch.object(relay, "_publish", stop_after_ack):
        relay.publish_batch(connection, lambda: producer, 1)
    raise DrillError("crash batch finished without reaching the ACK boundary")


def hold(connection):
    # relay._connect opens a dedicated writer session at READ COMMITTED.
    # Match the relay index walk so another relay can SKIP LOCKED past this prefix.
    connection.begin()
    with connection.cursor() as cursor:
        cursor.execute(LOCK_OLDEST, (TOPIC, HOLD_COUNT))
        ids = [row["event_id"] for row in cursor.fetchall()]
    if len(set(ids)) != HOLD_COUNT or not all(map(is_canonical_uuid, ids)):
        raise DrillError("expected 500 distinct pending event UUIDs")
    digest = hashlib.sha256("\n".join(sorted(ids)).encode("ascii")).hexdigest()
    proof("HOLD_PROOF", {"count": len(ids), "event_ids_sha256": digest, "anchor_event_id": ids[0]})
    while True:
        sleep(5)
        connection.ping(reconnect=False)  # A replacement session would have lost the locks.


def duplicate(connection, producer, start):
    with connection.cursor() as cursor:
        if start is None:
            cursor.execute(SELECT_RANGE_BOUNDARY, (TOPIC, HEALTHY_PREFIX_COUNT - 1))
            boundary = cursor.fetchone()
            if not boundary:
                raise DrillError("missing the recovery drill's 8000-row healthy prefix")
            start = boundary["published_at"]
        cursor.execute(SELECT_PUBLISHED, (TOPIC, start, DUPLICATE_COUNT))
        rows = cursor.fetchall()
    connection.rollback()  # End the read transaction before sending to Kafka.
    events = [relay._validated_event(row) for row in rows]
    if len(events) != DUPLICATE_COUNT or len({e["event_id"] for e in events}) != DUPLICATE_COUNT:
        raise DrillError("range must contain 1000 distinct published events")

    coordinates = []
    seen = set()
    for index in range(0, DUPLICATE_COUNT, SEND_WINDOW):
        futures = []
        for event in events[index:index + SEND_WINDOW]:
            # Match relay._publish wire bytes, including JSON field order.
            key = str(event["data"]["entry_id"]).encode("ascii")
            value = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            futures.append((event["event_id"], producer.send(TOPIC, key=key, value=value)))

        deadline = monotonic() + relay.ACK_TIMEOUT_SECONDS
        for event_id, future in futures:
            ack = future.get(timeout=max(0, deadline - monotonic()))
            if ack.topic != TOPIC or any(type(n) is not int or n < 0 for n in (ack.partition, ack.offset)):
                raise DrillError("invalid duplicate acknowledgement")
            point = (ack.partition, ack.offset)
            if point in seen:
                raise DrillError("repeated duplicate acknowledgement coordinate")
            seen.add(point)
            coordinates.append({"event_id": event_id, "partition": ack.partition, "offset": ack.offset})
    return {"topic": TOPIC, "published": len(coordinates), "coordinates": coordinates,
            "range_start_utc": start.isoformat(timespec="microseconds") + "Z"}


def utc_start(value):
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(timezone.utc).replace(tzinfo=None)
        return stamp  # MySQL stores the relay's UTC_TIMESTAMP(6) in a DATETIME(6).
    except (ValueError, OverflowError):
        raise argparse.ArgumentTypeError("expected an ISO 8601 range start in UTC") from None


def deadline_expired(signum, frame):
    # Never unwind through the relay's ordinary Kafka failure/DB commit path.
    os.write(2, b"drill deadline expired; inspect durable marks and broker offsets before retry\n")
    os._exit(124)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    drain_cli = commands.add_parser("drain", help="relay exactly N pending rows")
    drain_cli.add_argument("count", metavar="N", type=int)
    crash_cli = commands.add_parser("crash", help="stop after broker ACK and before the outbox mark")
    commands.add_parser("hold", help="lock the oldest 500 pending rows until killed")
    duplicate_cli = commands.add_parser("duplicate", help="replay 1000 published rows")
    duplicate_cli.add_argument("--range-start", default=os.getenv("RAFFLE_RANGE_START"), type=utc_start,
                               help="exclusive UTC lower bound (or RAFFLE_RANGE_START); "
                                    "defaults to the drill's 8000th publication")
    for command in (drain_cli, crash_cli, duplicate_cli):
        command.add_argument("--deadline-seconds", type=int, default=600,
                             help="whole-process wall time limit (default 600)")
    args = parser.parse_args(argv)
    if args.command == "drain" and args.count < 0:
        parser.error("N must be nonnegative")
    if args.command != "hold":
        if not 1 <= args.deadline_seconds <= 86400:
            parser.error("--deadline-seconds must be between 1 and 86400")
        if args.command == "crash" and os.getpid() == 1:
            # Container PID 1 has special signal handling. Stop a normal child
            # with its DB transaction open; killing PID 1 also kills the child.
            child = os.fork()
            if child:
                _, status = os.waitpid(child, 0)
                return 0 if status == 0 else 1
        signal.signal(signal.SIGALRM, deadline_expired)
        signal.alarm(args.deadline_seconds)
    try:
        if args.command == "drain" and args.count == 0:
            proof("DRAIN_PROOF", drain(None, None, 0))
            return 0
        with ExitStack() as resources:
            connection = resources.enter_context(closing(relay._connect()))
            if args.command == "hold":
                return hold(connection)
            producer = relay._open_producer(resources, relay._environment("KAFKA_BOOTSTRAP_SERVERS"))
            if args.command == "crash":
                return crash(connection, producer)
            if args.command == "drain":
                result = drain(connection, producer, args.count)
            else:
                result = duplicate(connection, producer, args.range_start)
        proof(args.command.upper() + "_PROOF", result)
        return 0
    except Exception as error:
        message = str(error) if isinstance(error, DrillError) else type(error).__name__
        print(f"{args.command} failed: {message}", file=sys.stderr, flush=True)
        return 1
    finally:
        if args.command != "hold":
            signal.alarm(0)


if __name__ == "__main__":
    raise SystemExit(main())
