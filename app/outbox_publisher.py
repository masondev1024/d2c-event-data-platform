"""At-least-once transactional-outbox publisher for D2C approval events."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

import psycopg
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Gauge, start_http_server
from psycopg.rows import dict_row


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("d2c-outbox-publisher")
STOP_REQUESTED = False

OUTBOX_PUBLISHES = Counter(
    "d2c_outbox_publisher_events_total",
    "Transactional outbox publish attempts by outcome.",
    ("result",),
)
OUTBOX_BACKLOG = Gauge(
    "d2c_outbox_backlog",
    "Number of unpublished D2C outbox events.",
)
OUTBOX_OLDEST_AGE = Gauge(
    "d2c_outbox_oldest_age_seconds",
    "Age of the oldest unpublished D2C outbox event.",
)
OUTBOX_HEARTBEAT = Gauge(
    "d2c_outbox_publisher_heartbeat_unixtime",
    "Unix timestamp of the last outbox publisher loop.",
)
DB_READINESS = Gauge(
    "d2c_outbox_publisher_db_readiness",
    "Whether the outbox publisher can reach PostgreSQL.",
)


def _stop_handler(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _connect():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be configured")
    return psycopg.connect(database_url, connect_timeout=5, row_factory=dict_row)


def _batch_size() -> int:
    value = int(os.environ.get("OUTBOX_BATCH_SIZE", "50"))
    if not 1 <= value <= 500:
        raise ValueError("OUTBOX_BATCH_SIZE must be between 1 and 500")
    return value


def _poll_interval() -> float:
    value = float(os.environ.get("OUTBOX_POLL_INTERVAL_SECONDS", "1"))
    if value <= 0:
        raise ValueError("OUTBOX_POLL_INTERVAL_SECONDS must be greater than zero")
    return value


def _record_backlog(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS pending,
                COALESCE(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(created_at))), 0)
                    AS oldest_age_seconds
            FROM d2c_outbox_events
            WHERE published_at IS NULL
            """
        )
        result = cursor.fetchone()
    OUTBOX_BACKLOG.set(int(result["pending"]))
    OUTBOX_OLDEST_AGE.set(float(result["oldest_age_seconds"]))
    DB_READINESS.set(1)


def publish_batch(connection, producer: KafkaProducer, topic: str, batch_size: int) -> tuple[int, int]:
    """Publish and mark a batch; a crash before the mark intentionally replays."""

    published = 0
    failed = 0
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT event_id, aggregate_id, payload
            FROM d2c_outbox_events
            WHERE published_at IS NULL
            ORDER BY created_at, event_id
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (batch_size,),
        )
        rows = cursor.fetchall()
        for row in rows:
            try:
                producer.send(
                    topic,
                    key=str(row["aggregate_id"]).encode("utf-8"),
                    value=row["payload"],
                ).get(timeout=10)
            except KafkaError as error:
                failed += 1
                OUTBOX_PUBLISHES.labels("failed").inc()
                cursor.execute(
                    """
                    UPDATE d2c_outbox_events
                    SET publish_attempts = publish_attempts + 1,
                        last_error = LEFT(%s, 1024)
                    WHERE event_id = %s
                    """,
                    (str(error), row["event_id"]),
                )
                LOGGER.warning("outbox_publish_failed event_id=%s error=%s", row["event_id"], error)
                continue

            published += 1
            OUTBOX_PUBLISHES.labels("published").inc()
            cursor.execute(
                """
                UPDATE d2c_outbox_events
                SET published_at = CURRENT_TIMESTAMP,
                    publish_attempts = publish_attempts + 1,
                    last_error = NULL
                WHERE event_id = %s
                """,
                (row["event_id"],),
            )
    connection.commit()
    return published, failed


def run() -> None:
    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)
    topic = os.environ.get("OUTBOX_TOPIC", "d2c.application.approved.v1")
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    batch_size = _batch_size()
    poll_interval = _poll_interval()
    metrics_port = int(os.environ.get("METRICS_PORT", "9102"))
    start_http_server(metrics_port)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=10,
        linger_ms=10,
        compression_type="gzip",
        value_serializer=_json_bytes,
    )
    LOGGER.info(
        "outbox_publisher_started topic=%s bootstrap=%s batch_size=%s",
        topic,
        bootstrap_servers,
        batch_size,
    )
    try:
        while not STOP_REQUESTED:
            OUTBOX_HEARTBEAT.set(time.time())
            connection = None
            try:
                connection = _connect()
                _record_backlog(connection)
                publish_batch(connection, producer, topic, batch_size)
                _record_backlog(connection)
            except (psycopg.Error, KafkaError, OSError, RuntimeError) as error:
                DB_READINESS.set(0)
                LOGGER.warning("outbox_publisher_loop_failed error=%s", error)
                if connection is not None:
                    try:
                        connection.rollback()
                    except psycopg.Error:
                        LOGGER.exception("outbox_publisher_rollback_failed")
            finally:
                if connection is not None:
                    connection.close()
            time.sleep(poll_interval)
    finally:
        producer.flush(timeout=10)
        producer.close()
        LOGGER.info("outbox_publisher_stopped")


if __name__ == "__main__":
    run()
