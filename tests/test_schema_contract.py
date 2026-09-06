from __future__ import annotations

import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from services.contract import normalize_sensor_event


class CanonicalSchemaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schema_path = Path(__file__).parents[1] / "schemas" / "factory-sensor.v1.schema.json"
        cls.validator = Draft202012Validator(
            json.loads(schema_path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )

    def test_normalized_event_conforms_to_published_schema(self) -> None:
        event = normalize_sensor_event(
            {
                "timestamp": "2026-09-06T12:00:00Z",
                "sensor_id": "AI-FACTORY-001",
                "temperature": 87.5,
                "humidity": 42.4,
                "status": "RUNNING",
            }
        ).as_dict("factory.sensor.raw.json.v1", 0, 12)

        self.assertEqual(list(self.validator.iter_errors(event)), [])

    def test_schema_rejects_contract_violation(self) -> None:
        event = normalize_sensor_event(
            {
                "timestamp": "2026-09-06T12:00:00Z",
                "sensor_id": "AI-FACTORY-001",
                "temperature": 87.5,
                "humidity": 42.4,
                "status": "RUNNING",
            }
        ).as_dict("factory.sensor.raw.json.v1", 0, 12)
        event["humidity"] = 101

        self.assertTrue(list(self.validator.iter_errors(event)))


if __name__ == "__main__":
    unittest.main()
