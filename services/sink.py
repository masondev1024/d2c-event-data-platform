"""Idempotent local sink for the normalized Kafka topic.

DuckDB is used only as a local analytical sink for the PoC. The unique
event_id constraint demonstrates the same protection a production lakehouse
sink needs when an at-least-once consumer retries after a crash.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

import duckdb
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from .contract import DataQualityError, validate_canonical_sensor_event
from .metrics import MetricsRegistry, metrics_port_from_env, start_metrics_server

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("sensor-sink")
STOP_REQUESTED = False


def _stop_handler(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def _initialize_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_events (
            event_id VARCHAR PRIMARY KEY,
            schema_version VARCHAR NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            sensor_id VARCHAR NOT NULL,
            temperature DOUBLE NOT NULL,
            humidity DOUBLE NOT NULL,
            status VARCHAR NOT NULL,
            source_topic VARCHAR NOT NULL,
            source_partition INTEGER NOT NULL,
            source_offset BIGINT NOT NULL,
            stored_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _commit_message(consumer: KafkaConsumer, message: Any) -> None:
    """Commit only the Kafka record persisted by the sink."""

    topic_partition = TopicPartition(message.topic, message.partition)
    consumer.commit(
        offsets={topic_partition: OffsetAndMetadata(message.offset + 1, None)}
    )


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _decode(raw: bytes | None) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode("utf-8", errors="replace")


def _build_sink_dlq(raw: Any, error: Exception, topic: str, partition: int, offset: int) -> dict[str, Any]:
    return {
        "schema_version": "factory-sensor-sink-dlq.v1",
        "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "source": {"topic": topic, "partition": partition, "offset": offset},
        "raw_event": raw,
    }


def _publish_sink_dlq(
    producer: KafkaProducer,
    topic: str,
    raw: Any,
    error: Exception,
    message: Any,
) -> None:
    producer.send(
        topic,
        value=_build_sink_dlq(raw, error, message.topic, message.partition, message.offset),
    ).get(timeout=10)


def run() -> None:
    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("CLEAN_TOPIC", "factory.sensor.clean.v1")
    dlq_topic = os.getenv("SINK_DLQ_TOPIC", "factory.sensor.sink.dlq.v1")
    group_id = os.getenv("SINK_CONSUMER_GROUP", "sensor-duckdb-sink-v1")
    database_path = os.getenv("DUCKDB_PATH", "/data/sensor.duckdb")

    metrics = MetricsRegistry()
    metrics_server = start_metrics_server(metrics, metrics_port_from_env(9101))
    os.makedirs(os.path.dirname(database_path) or ".", exist_ok=True)
    connection: duckdb.DuckDBPyConnection | None = None
    consumer: KafkaConsumer | None = None
    producer: KafkaProducer | None = None

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
            "sink_started topic=%s dlq_topic=%s group_id=%s database=%s",
            topic,
            dlq_topic,
            group_id,
            database_path,
        )
        while not STOP_REQUESTED:
            records = consumer.poll(timeout_ms=1_000, max_records=50)
            metrics.set_gauge(
                "sensor_sink_heartbeat_unixtime",
                time.time(),
                help_text="Unix timestamp of the last completed consumer poll.",
            )
            for _tp, messages in records.items():
                for message in messages:
                    event = message.value
                    try:
                        validate_canonical_sensor_event(event)
                        source = event["source"]
                        inserted = connection.execute(
                            """
                            INSERT INTO sensor_events (
                                event_id, schema_version, event_time, ingested_at,
                                sensor_id, temperature, humidity, status,
                                source_topic, source_partition, source_offset
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                            """,
                            [
                                event["event_id"],
                                event["schema_version"],
                                event["event_time"],
                                event["ingested_at"],
                                event["sensor_id"],
                                event["temperature"],
                                event["humidity"],
                                event["status"],
                                source["topic"],
                                source["partition"],
                                source["offset"],
                            ],
                        ).fetchone()
                        connection.commit()
                    except (DataQualityError, TypeError, ValueError, KeyError) as error:
                        if producer is None:
                            raise RuntimeError("sink DLQ producer is not initialized") from error
                        try:
                            _publish_sink_dlq(producer, dlq_topic, event, error, message)
                        except KafkaError:
                            metrics.inc(
                                "sensor_sink_quarantine_failures_total",
                                help_text="Number of sink contract failures that could not be quarantined.",
                            )
                            LOGGER.exception(
                                "sink_dlq_publish_failed topic=%s partition=%s offset=%s",
                                message.topic,
                                message.partition,
                                message.offset,
                            )
                            raise
                        metrics.inc(
                            "sensor_sink_records_total",
                            labels={"result": "quarantined"},
                            help_text="Number of canonical events handled by sink outcome.",
                        )
                        _commit_message(consumer, message)
                        LOGGER.error(
                            "event_quarantined topic=%s partition=%s offset=%s error=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                            error,
                        )
                        continue
                    except Exception:
                        connection.rollback()
                        metrics.inc(
                            "sensor_sink_failures_total",
                            help_text="Number of records that failed before durable sink commit.",
                        )
                        LOGGER.exception(
                            "event_store_failed topic=%s partition=%s offset=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                        )
                        raise

                    result = "stored" if inserted else "duplicate"
                    metrics.inc(
                        "sensor_sink_records_total",
                        labels={"result": result},
                        help_text="Number of canonical events handled by sink outcome.",
                    )
                    _commit_message(consumer, message)
                    metrics.set_gauge(
                        "sensor_sink_last_success_unixtime",
                        time.time(),
                        help_text="Unix timestamp of the last successfully persisted record.",
                    )
                    LOGGER.info(
                        "event_%s event_id=%s sensor_id=%s kafka_offset=%s",
                        result,
                        event["event_id"],
                        event["sensor_id"],
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
        LOGGER.info("sink_stopped")


if __name__ == "__main__":
    run()
