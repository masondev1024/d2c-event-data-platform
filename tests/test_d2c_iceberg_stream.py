from __future__ import annotations

import json
import unittest

from d2c_contract import build_application_approved_event
from lakehouse.d2c_iceberg_stream import (
    canonical_source_ranges,
    parse_d2c_kafka_payload,
    settings_from_environment,
)


class D2CIcebergStreamTest(unittest.TestCase):
    def test_parses_the_shared_d2c_contract_into_a_canonical_fact(self) -> None:
        event = build_application_approved_event(
            application_id=42,
            user_id=7,
            campaign_id=3,
        )

        parsed = parse_d2c_kafka_payload(
            '{"data":{"campaign_id":3,"application_id":42,"user_id":7},'
            f'"event_version":1,"event_type":"d2c.application.approved.v1",'
            f'"occurred_at":"{event["occurred_at"]}","event_id":"{event["event_id"]}"}}'
        )

        self.assertEqual(parsed["event_id"], event["event_id"])
        self.assertEqual(parsed["application_id"], 42)
        self.assertEqual(parsed["failure_code"], None)
        self.assertIn('"application_id":42', str(parsed["payload_json"]))

    def test_quarantines_contract_drift_without_throwing_the_stream(self) -> None:
        parsed = parse_d2c_kafka_payload(
            '{"event_id":"not-a-uuid","event_type":"d2c.application.approved.v1",'
            '"event_version":1,"occurred_at":"2026-09-23T00:00:00Z",'
            '"data":{"application_id":1,"user_id":2,"campaign_id":3}}'
        )

        self.assertEqual(parsed["failure_code"], "contract_violation")
        self.assertEqual(parsed["event_id"], None)

    def test_rejects_an_unknown_contract_field(self) -> None:
        event = build_application_approved_event(
            application_id=42,
            user_id=7,
            campaign_id=3,
        )
        event["unsupported"] = "schema-drift"

        parsed = parse_d2c_kafka_payload(json.dumps(event))

        self.assertEqual(parsed["failure_code"], "contract_violation")

    def test_source_range_fingerprint_is_stable_and_order_independent(self) -> None:
        ranges = [
            {
                "topic": "d2c.application.approved.v1",
                "partition": 2,
                "start_offset": 10,
                "end_offset": 11,
                "records": 2,
            },
            {
                "topic": "d2c.application.approved.v1",
                "partition": 0,
                "start_offset": 4,
                "end_offset": 4,
                "records": 1,
            },
        ]

        first_fingerprint, first_json = canonical_source_ranges(ranges)
        second_fingerprint, second_json = canonical_source_ranges(reversed(ranges))

        self.assertEqual(first_fingerprint, second_fingerprint)
        self.assertEqual(first_json, second_json)
        self.assertEqual(len(first_fingerprint), 64)

    def test_rejects_a_source_range_with_missing_offsets(self) -> None:
        with self.assertRaisesRegex(ValueError, "record count"):
            canonical_source_ranges(
                [
                    {
                        "topic": "d2c.application.approved.v1",
                        "partition": 0,
                        "start_offset": 4,
                        "end_offset": 5,
                        "records": 1,
                    }
                ]
            )

    def test_settings_fail_closed_for_another_topic_or_non_local_warehouse(self) -> None:
        with self.assertRaisesRegex(ValueError, "D2C_EVENT_TOPIC"):
            settings_from_environment({"D2C_EVENT_TOPIC": "some-other-topic"})
        with self.assertRaisesRegex(ValueError, "WAREHOUSE"):
            settings_from_environment({"D2C_LAKEHOUSE_WAREHOUSE": "s3://unreviewed-bucket"})

    def test_default_settings_use_a_bounded_replay_safe_local_profile(self) -> None:
        settings = settings_from_environment({})

        self.assertEqual(settings.trigger, "available_now")
        self.assertEqual(settings.starting_offsets, "earliest")
        self.assertEqual(settings.warehouse, "file:///warehouse")
        self.assertEqual(settings.approvals_table, "d2c_lakehouse.d2c.application_approvals")


if __name__ == "__main__":
    unittest.main()
