"""At-least-once Kafka normalizer with a data-quality dead-letter topic."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata
from kafka.errors import KafkaError

from .contract import DLQ_SCHEMA_VERSION, DataQualityError, normalize_sensor_event
from .metrics import MetricsRegistry, metrics_port_from_env, start_metrics_server


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("sensor-processor")
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


def _build_dlq(raw: Any, error: Exception, topic: str, partition: int, offset: int) -> dict[str, Any]:
    return {
        "schema_version": DLQ_SCHEMA_VERSION,
        "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "source": {"topic": topic, "partition": partition, "offset": offset},
        "raw_event": raw,
    }


def _commit_message(consumer: KafkaConsumer, message: Any) -> None:
    """Commit only the record that has reached a terminal output."""

    topic_partition = TopicPartition(message.topic, message.partition)
    consumer.commit(
        offsets={topic_partition: OffsetAndMetadata(message.offset + 1, None)}
    )


def _publish(
    producer: KafkaProducer,
    topic: str,
    value: Any,
    metrics: MetricsRegistry,
    output: str,
    key: str | None = None,
) -> None:
    """Publish durably and classify broker failures separately from data quality."""

    try:
        producer.send(topic, key=key, value=value).get(timeout=10)
    except KafkaError:
        metrics.inc(
            "sensor_processor_publish_failures_total",
            labels={"output": output},
            help_text="Number of Kafka output publish failures.",
        )
        raise


def _update_consumer_lag(consumer: KafkaConsumer, metrics: MetricsRegistry) -> None:
    """Export best-effort per-partition lag without slowing every message."""

    try:
        assigned = consumer.assignment()
        if not assigned:
            return
        end_offsets = consumer.end_offsets(assigned)
        for topic_partition in assigned:
            position = consumer.position(topic_partition)
            if position is None:
                continue
            lag = max(0, end_offsets.get(topic_partition, position) - position)
            metrics.set_gauge(
                "sensor_processor_consumer_lag",
                float(lag),
                labels={
                    "topic": topic_partition.topic,
                    "partition": topic_partition.partition,
                },
                help_text="Current Kafka consumer lag by topic and partition.",
            )
    except Exception:
        metrics.inc(
            "sensor_processor_lag_errors_total",
            help_text="Number of failures while collecting Kafka consumer lag.",
        )
        LOGGER.warning("consumer_lag_collection_failed", exc_info=True)


def run() -> None:
    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    raw_topics = tuple(
        topic.strip()
        for topic in os.getenv(
            "RAW_TOPICS", "factory.sensor.raw.json.v1,factory.sensor.raw.text.v1"
        ).split(",")
        if topic.strip()
    )
    clean_topic = os.getenv("CLEAN_TOPIC", "factory.sensor.clean.v1")
    dlq_topic = os.getenv("DLQ_TOPIC", "factory.sensor.dlq.v1")
    group_id = os.getenv("CONSUMER_GROUP", "sensor-normalizer-v1")
    lag_interval_seconds = float(os.getenv("LAG_INTERVAL_SECONDS", "10"))
    if lag_interval_seconds <= 0:
        raise ValueError("LAG_INTERVAL_SECONDS must be greater than zero")

    metrics = MetricsRegistry()
    metrics_server = start_metrics_server(metrics, metrics_port_from_env(9100))

    consumer: KafkaConsumer | None = None
    producer: KafkaProducer | None = None
    next_lag_check = 0.0

    try:
        consumer = KafkaConsumer(
            *raw_topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=50,
            request_timeout_ms=30_000,
            session_timeout_ms=10_000,
        )
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=10,
            linger_ms=10,
            compression_type="gzip",
            key_serializer=lambda key: key.encode("utf-8") if key else None,
            value_serializer=_json_bytes,
        )

        LOGGER.info(
            "processor_started raw_topics=%s clean_topic=%s dlq_topic=%s group_id=%s",
            raw_topics,
            clean_topic,
            dlq_topic,
            group_id,
        )
        assert consumer is not None
        assert producer is not None
        while not STOP_REQUESTED:
            records = consumer.poll(timeout_ms=1_000, max_records=50)
            metrics.set_gauge(
                "sensor_processor_heartbeat_unixtime",
                time.time(),
                help_text="Unix timestamp of the last completed consumer poll.",
            )
            if time.monotonic() >= next_lag_check:
                _update_consumer_lag(consumer, metrics)
                next_lag_check = time.monotonic() + lag_interval_seconds
            for _tp, messages in records.items():
                for message in messages:
                    raw = _decode(message.value)
                    try:
                        try:
                            normalized = normalize_sensor_event(raw)
                        except (DataQualityError, TypeError, ValueError, KeyError) as error:
                            dlq_record = _build_dlq(
                                raw, error, message.topic, message.partition, message.offset
                            )
                            _publish(producer, dlq_topic, dlq_record, metrics, "dlq")
                            metrics.inc(
                                "sensor_processor_records_total",
                                labels={"result": "dlq"},
                                help_text="Number of raw records routed by outcome.",
                            )
                            _commit_message(consumer, message)
                            metrics.set_gauge(
                                "sensor_processor_last_success_unixtime",
                                time.time(),
                                help_text="Unix timestamp of the last successfully processed record.",
                            )
                            LOGGER.warning(
                                "event_quarantined topic=%s partition=%s offset=%s error=%s",
                                message.topic,
                                message.partition,
                                message.offset,
                                error,
                            )
                            continue

                        output = normalized.as_dict(
                            source_topic=message.topic,
                            source_partition=message.partition,
                            source_offset=message.offset,
                        )
                        _publish(
                            producer,
                            clean_topic,
                            output,
                            metrics,
                            "clean",
                            key=normalized.sensor_id,
                        )
                        metrics.inc(
                            "sensor_processor_records_total",
                            labels={"result": "clean"},
                            help_text="Number of raw records routed by outcome.",
                        )
                        _commit_message(consumer, message)
                        metrics.set_gauge(
                            "sensor_processor_last_success_unixtime",
                            time.time(),
                            help_text="Unix timestamp of the last successfully processed record.",
                        )
                        LOGGER.info(
                            "event_normalized event_id=%s sensor_id=%s source=%s:%s:%s",
                            normalized.event_id,
                            normalized.sensor_id,
                            message.topic,
                            message.partition,
                            message.offset,
                        )
                    except KafkaError:
                        LOGGER.exception("kafka_processing_failed; offset will be retried")
                        raise
    finally:
        if producer is not None:
            producer.flush(timeout=10)
            producer.close()
        if consumer is not None:
            consumer.close()
        if metrics_server is not None:
            metrics_server.shutdown()
            metrics_server.server_close()
        LOGGER.info("processor_stopped")


if __name__ == "__main__":
    run()
