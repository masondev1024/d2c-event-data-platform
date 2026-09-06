from __future__ import annotations

import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from d2c_contract import build_application_approved_event


class D2CEventContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schema_path = (
            Path(__file__).parents[1]
            / "schemas"
            / "d2c.application.approved.v1.schema.json"
        )
        cls.validator = Draft202012Validator(
            json.loads(schema_path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )

    def test_approved_event_conforms_to_published_schema(self) -> None:
        event = build_application_approved_event(
            application_id=42,
            user_id=7,
            campaign_id=1,
        )

        self.assertEqual(list(self.validator.iter_errors(event)), [])

    def test_schema_rejects_an_event_with_an_unapproved_shape(self) -> None:
        event = build_application_approved_event(
            application_id=42,
            user_id=7,
            campaign_id=1,
        )
        event["data"]["campaign_id"] = 0

        self.assertTrue(list(self.validator.iter_errors(event)))


if __name__ == "__main__":
    unittest.main()
