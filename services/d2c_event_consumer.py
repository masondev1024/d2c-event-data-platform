"""Idempotent Kafka consumer for transactionally approved D2C events."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import duckdb
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from d2c_contract import D2C_EVENT_TYPE, D2C_EVENT_VERSION
from .metrics import MetricsRegistry, metrics_port_from_env, start_metrics_server


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("d2c-event-consumer")
STOP_REQUESTED = False
def _stop_handler(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _decode(raw: bytes | None) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode("utf-8", errors="replace")


def _validate_event(event: Any) -> None:
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
        datetime.fromisoformat(str(event["occurred_at"]).replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise ValueError("D2C event id or occurred_at is invalid") from error
    data = event["data"]
    if not isinstance(data, dict) or set(data) != {"application_id", "user_id", "campaign_id"}:
        raise ValueError("D2C event data is invalid")
    for field in ("application_id", "user_id", "campaign_id"):
        if isinstance(data[field], bool) or not isinstance(data[field], int) or data[field] <= 0:
            raise ValueError(f"D2C event data.{field} must be a positive integer")


def _commit_message(consumer: KafkaConsumer, message: Any) -> None:
    topic_partition = TopicPartition(message.topic, message.partition)
    consumer.commit(offsets={topic_partition: OffsetAndMetadata(message.offset + 1, None)})


def _build_dlq(raw: Any, error: Exception, message: Any) -> dict[str, Any]:
    return {
        "schema_version": "d2c-event-consumer-dlq.v1",
        "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "source": {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        },
        "raw_event": raw,
    }


def _update_lag(consumer: KafkaConsumer, metrics: MetricsRegistry) -> None:
    try:
        assigned = consumer.assignment()
        if not assigned:
            return
        end_offsets = consumer.end_offsets(assigned)
        for topic_partition in assigned:
            position = consumer.position(topic_partition)
            if position is None:
                continue
            metrics.set_gauge(
                "d2c_event_consumer_lag",
                max(0, end_offsets.get(topic_partition, position) - position),
                labels={"topic": topic_partition.topic, "partition": topic_partition.partition},
                help_text="Current Kafka lag for the D2C event consumer.",
            )
    except Exception:
        metrics.inc(
            "d2c_event_consumer_lag_errors_total",
            help_text="Number of failures while collecting D2C consumer lag.",
        )
        LOGGER.warning("d2c_consumer_lag_collection_failed", exc_info=True)


def _initialize_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS d2c_application_events (
            event_id VARCHAR PRIMARY KEY,
            event_type VARCHAR NOT NULL,
            event_version INTEGER NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            application_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            campaign_id BIGINT NOT NULL,
            payload JSON NOT NULL,
            consumed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def run() -> None:
    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("D2C_EVENT_TOPIC", D2C_EVENT_TYPE)
    dlq_topic = os.getenv("D2C_EVENT_DLQ_TOPIC", "d2c.application.consumer.dlq.v1")
    group_id = os.getenv("D2C_EVENT_CONSUMER_GROUP", "d2c-application-event-consumer-v1")
    database_path = os.getenv("D2C_DUCKDB_PATH", "/data/d2c_events.duckdb")
    lag_interval_seconds = float(os.getenv("LAG_INTERVAL_SECONDS", "10"))
    if lag_interval_seconds <= 0:
        raise ValueError("LAG_INTERVAL_SECONDS must be greater than zero")

    os.makedirs(os.path.dirname(database_path) or ".", exist_ok=True)
    metrics = MetricsRegistry()
    metrics_server = start_metrics_server(metrics, metrics_port_from_env(9103))
    connection: duckdb.DuckDBPyConnection | None = None
    consumer: KafkaConsumer | None = None
    producer: KafkaProducer | None = None
    next_lag_check = 0.0

    try:
        connection = duckdb.connect(database_path)
        _initialize_schema(connection)
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=50,
            value_deserializer=_decode,
        )
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=10,
            linger_ms=10,
            compression_type="gzip",
            value_serializer=_json_bytes,
        )
        LOGGER.info(
            "d2c_event_consumer_started topic=%s dlq_topic=%s group_id=%s database=%s",
            topic,
            dlq_topic,
            group_id,
            database_path,
        )
        while not STOP_REQUESTED:
            records = consumer.poll(timeout_ms=1_000, max_records=50)
            metrics.set_gauge(
                "d2c_event_consumer_heartbeat_unixtime",
                time.time(),
                help_text="Unix timestamp of the last completed D2C consumer poll.",
            )
            if time.monotonic() >= next_lag_check:
                _update_lag(consumer, metrics)
                next_lag_check = time.monotonic() + lag_interval_seconds
            for _tp, messages in records.items():
                for message in messages:
                    raw = message.value
                    try:
                        _validate_event(raw)
                        assert connection is not None
                        data = raw["data"]
                        inserted = connection.execute(
                            """
                            INSERT INTO d2c_application_events (
                                event_id, event_type, event_version, occurred_at,
                                application_id, user_id, campaign_id, payload
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                            """,
                            [
                                raw["event_id"],
                                raw["event_type"],
                                raw["event_version"],
                                raw["occurred_at"],
                                data["application_id"],
                                data["user_id"],
                                data["campaign_id"],
                                json.dumps(raw, separators=(",", ":")),
                            ],
                        ).fetchone()
                        connection.commit()
                    except (TypeError, ValueError, KeyError) as error:
                        if producer is None:
                            raise RuntimeError("D2C event DLQ producer is not initialized") from error
                        try:
                            producer.send(dlq_topic, value=_build_dlq(raw, error, message)).get(timeout=10)
                        except KafkaError:
                            metrics.inc(
                                "d2c_event_consumer_quarantine_failures_total",
                                help_text="D2C invalid events that could not be quarantined.",
                            )
                            raise
                        metrics.inc(
                            "d2c_event_consumer_records_total",
                            labels={"result": "quarantined"},
                            help_text="D2C event consumer outcomes.",
                        )
                        _commit_message(consumer, message)
                        LOGGER.error(
                            "d2c_event_quarantined topic=%s partition=%s offset=%s error=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                            error,
                        )
                        continue
                    except KafkaError:
                        LOGGER.exception("d2c_event_consume_failed; offset will be retried")
                        raise
                    except Exception:
                        assert connection is not None
                        connection.rollback()
                        metrics.inc(
                            "d2c_event_consumer_failures_total",
                            help_text="D2C event records that failed before durable commit.",
                        )
                        LOGGER.exception(
                            "d2c_event_store_failed topic=%s partition=%s offset=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                        )
                        raise

                    result = "stored" if inserted else "duplicate"
                    metrics.inc(
                        "d2c_event_consumer_records_total",
                        labels={"result": result},
                        help_text="D2C event consumer outcomes.",
                    )
                    _commit_message(consumer, message)
                    LOGGER.info(
                        "d2c_event_%s event_id=%s kafka_offset=%s",
                        result,
                        raw["event_id"],
                        message.offset,
                    )
    finally:
        if producer is not None:
            producer.flush(timeout=10)
            producer.close()
        if consumer is not None:
            consumer.close()
        if connection is not None:
            connection.close()
        if metrics_server is not None:
            metrics_server.shutdown()
            metrics_server.server_close()
        LOGGER.info("d2c_event_consumer_stopped")


if __name__ == "__main__":
    run()
