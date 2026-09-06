from __future__ import annotations

import unittest

from d2c_contract import build_application_approved_event
from services.d2c_event_consumer import _validate_event


class D2CConsumerContractTest(unittest.TestCase):
    def test_consumer_accepts_the_shared_event_contract(self) -> None:
        event = build_application_approved_event(
            application_id=42,
            user_id=7,
            campaign_id=1,
        )

        _validate_event(event)

    def test_consumer_rejects_unknown_fields(self) -> None:
        event = build_application_approved_event(
            application_id=42,
            user_id=7,
            campaign_id=1,
        )
        event["unexpected"] = "breaks-contract"

        with self.assertRaises(ValueError):
            _validate_event(event)


if __name__ == "__main__":
    unittest.main()
