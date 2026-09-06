from __future__ import annotations

import unittest
import sys
from pathlib import Path

from kafka.errors import KafkaError

APP_PATH = Path(__file__).parents[1] / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

from outbox_publisher import publish_batch  # noqa: E402


class FakeFuture:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def get(self, timeout: int) -> None:
        if self.error is not None:
            raise self.error


class FakeProducer:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[str, bytes, dict[str, object]]] = []

    def send(self, topic: str, *, key: bytes, value: dict[str, object]) -> FakeFuture:
        self.sent.append((topic, key, value))
        return FakeFuture(self.error)


class FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, object]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        return False

    def execute(self, statement: str, parameters=None) -> None:
        self.executed.append((" ".join(statement.split()), parameters))

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows


class FakeConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.cursor_instance = FakeCursor(rows)
        self.commit_count = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1


def _row() -> dict[str, object]:
    return {
        "event_id": "00000000-0000-4000-8000-000000000042",
        "aggregate_id": 42,
        "payload": {
            "event_id": "00000000-0000-4000-8000-000000000042",
            "event_type": "d2c.application.approved.v1",
        },
    }


class OutboxPublisherTest(unittest.TestCase):
    def test_publishes_event_and_marks_it_only_after_kafka_ack(self) -> None:
        connection = FakeConnection([_row()])
        producer = FakeProducer()

        published, failed = publish_batch(connection, producer, "d2c.events", 50)

        self.assertEqual((published, failed), (1, 0))
        self.assertEqual(connection.commit_count, 1)
        self.assertEqual(producer.sent[0][0], "d2c.events")
        self.assertEqual(producer.sent[0][1], b"42")
        self.assertTrue(any("SET published_at" in statement for statement, _ in connection.cursor_instance.executed))

    def test_failed_publish_is_counted_and_left_unpublished_for_retry(self) -> None:
        connection = FakeConnection([_row()])
        producer = FakeProducer(KafkaError("broker unavailable"))

        published, failed = publish_batch(connection, producer, "d2c.events", 50)

        self.assertEqual((published, failed), (0, 1))
        self.assertEqual(connection.commit_count, 1)
        updates = [
            (statement, parameters)
            for statement, parameters in connection.cursor_instance.executed
            if statement.startswith("UPDATE d2c_outbox_events")
        ]
        self.assertEqual(len(updates), 1)
        self.assertIn("publish_attempts = publish_attempts + 1", updates[0][0])
        self.assertEqual(updates[0][1][1], _row()["event_id"])


if __name__ == "__main__":
    unittest.main()
