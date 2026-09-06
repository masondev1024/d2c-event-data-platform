from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


APP_PATH = Path(__file__).parents[1] / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

import app as app_module  # noqa: E402


class FakeCursor:
    def __init__(self, fetchone_values=None):
        self.fetchone_values = list(fetchone_values or [])
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, statement, parameters=None):
        self.executed.append((" ".join(statement.split()), parameters))

    def fetchone(self):
        return self.fetchone_values.pop(0) if self.fetchone_values else None


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.close_count += 1


class D2CApprovalApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        app_module.app.config.update(TESTING=True)

    def setUp(self) -> None:
        self.client = app_module.app.test_client()

    def test_health_and_security_headers_do_not_require_database(self) -> None:
        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"status": "ok"})
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")

    def test_apply_persists_application_and_outbox_before_one_commit(self) -> None:
        cursor = FakeCursor([{"id": 42, "status": "APPROVED"}])
        connection = FakeConnection(cursor)

        with patch.object(app_module, "get_db_connection", return_value=connection):
            response = self.client.post(
                "/api/apply",
                json={"user_id": 7, "campaign_id": 1, "idempotency_key": "apply-0001"},
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.get_json()["application_id"], 42)
        self.assertEqual(connection.commit_count, 1)
        self.assertEqual(connection.rollback_count, 0)
        self.assertEqual(connection.close_count, 1)
        statements = [statement for statement, _ in cursor.executed]
        self.assertIn("INSERT INTO d2c_applications", statements[0])
        self.assertIn("INSERT INTO d2c_outbox_events", statements[1])
        event_payload = next(
            parameters[-1]
            for statement, parameters in cursor.executed
            if "INSERT INTO d2c_outbox_events" in statement
        )
        event = getattr(event_payload, "obj", event_payload)
        self.assertIsInstance(event, dict)
        self.assertEqual(event["event_type"], app_module.D2C_EVENT_TYPE)
        self.assertEqual(event["data"], {"application_id": 42, "campaign_id": 1, "user_id": 7})

    def test_duplicate_request_does_not_create_a_second_outbox_event(self) -> None:
        cursor = FakeCursor(
            [
                None,
                {
                    "id": 42,
                    "status": "APPROVED",
                    "user_id": 7,
                    "campaign_id": 1,
                    "idempotency_key": "apply-0001",
                },
            ]
        )
        connection = FakeConnection(cursor)

        with patch.object(app_module, "get_db_connection", return_value=connection):
            response = self.client.post(
                "/api/apply",
                json={"user_id": 7, "campaign_id": 1, "idempotency_key": "apply-0001"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"application_id": 42, "status": "duplicate"})
        self.assertEqual(connection.commit_count, 1)
        self.assertFalse(any("INSERT INTO d2c_outbox_events" in statement for statement, _ in cursor.executed))

    def test_reused_idempotency_key_for_another_application_is_rejected(self) -> None:
        cursor = FakeCursor(
            [
                None,
                {
                    "id": 42,
                    "status": "APPROVED",
                    "user_id": 7,
                    "campaign_id": 1,
                    "idempotency_key": "apply-0001",
                },
            ]
        )
        connection = FakeConnection(cursor)

        with patch.object(app_module, "get_db_connection", return_value=connection):
            response = self.client.post(
                "/api/apply",
                json={"user_id": 8, "campaign_id": 1, "idempotency_key": "apply-0001"},
            )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["status"], "error")
        self.assertEqual(connection.commit_count, 1)
        self.assertFalse(any("INSERT INTO d2c_outbox_events" in statement for statement, _ in cursor.executed))

    def test_outbox_failure_drill_rolls_back_application_and_event_together(self) -> None:
        cursor = FakeCursor([{"id": 42, "status": "APPROVED"}])
        connection = FakeConnection(cursor)
        drill_environment = {
            "D2C_ENV": "validation",
            "ALLOW_FAILURE_DRILL": "true",
            "D2C_OUTBOX_FAILURE_INJECTION": "before_outbox_insert",
        }

        with patch.dict(os.environ, drill_environment, clear=False), patch.object(
            app_module, "get_db_connection", return_value=connection
        ):
            response = self.client.post(
                "/api/apply",
                json={"user_id": 7, "campaign_id": 1, "idempotency_key": "apply-0002"},
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(connection.commit_count, 0)
        self.assertEqual(connection.rollback_count, 1)
        self.assertFalse(any("INSERT INTO d2c_outbox_events" in statement for statement, _ in cursor.executed))

    def test_invalid_request_is_rejected_before_database_access(self) -> None:
        with patch.object(app_module, "get_db_connection") as get_connection:
            response = self.client.post(
                "/api/apply",
                json={"user_id": 7, "campaign_id": 1, "idempotency_key": "short"},
            )

        self.assertEqual(response.status_code, 400)
        get_connection.assert_not_called()

    def test_parity_metric_reflects_database_truth(self) -> None:
        cursor = FakeCursor([{"missing_events": 2, "orphan_events": 1}])
        connection = FakeConnection(cursor)

        with patch.object(app_module, "get_db_connection", return_value=connection):
            response = self.client.get("/metrics")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("d2c_outbox_parity_gap 3.0", body)
        self.assertIn("d2c_outbox_parity_check_success 1.0", body)


if __name__ == "__main__":
    unittest.main()
