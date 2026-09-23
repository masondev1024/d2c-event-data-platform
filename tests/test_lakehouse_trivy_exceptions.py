from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from scripts.validate_lakehouse_trivy_exceptions import PolicyError, validate_policy


ROOT = Path(__file__).parents[1]
POLICY_PATH = ROOT / "security" / "lakehouse.trivyignore.yaml"
REFERENCE_DATE = date(2026, 9, 23)


class LakehouseTrivyExceptionPolicyTest(unittest.TestCase):
    def test_checked_in_policy_is_short_lived_and_path_scoped(self) -> None:
        entries = validate_policy(POLICY_PATH, today=REFERENCE_DATE)

        self.assertEqual(len(entries), 10)
        self.assertEqual(
            sum(len(entry["paths"]) for entry in entries),
            17,
        )
        self.assertTrue(
            all(entry["expired_at"] == date(2026, 10, 23) for entry in entries)
        )

    def test_policy_rejects_an_expired_or_unscoped_exception(self) -> None:
        policy = {
            "vulnerabilities": [
                {
                    "id": "CVE-2026-1234",
                    "paths": ["opt/spark/jars/example-1.0.0.jar"],
                    "expired_at": "2026-09-23",
                    "statement": "This sentence is deliberately long enough to describe a risk and a replacement plan.",
                },
                {
                    "id": "CVE-2026-5678",
                    "expired_at": "2026-10-01",
                    "statement": "This sentence is deliberately long enough to describe a risk and a replacement plan.",
                },
            ]
        }

        with TemporaryDirectory() as directory:
            policy_path = Path(directory) / "policy.yaml"
            policy_path.write_text(yaml.safe_dump(policy), encoding="utf-8")
            with self.assertRaisesRegex(PolicyError, "must be after today"):
                validate_policy(policy_path, today=REFERENCE_DATE)

            policy["vulnerabilities"].pop(0)
            policy_path.write_text(yaml.safe_dump(policy), encoding="utf-8")
            with self.assertRaisesRegex(PolicyError, "scope every finding"):
                validate_policy(policy_path, today=REFERENCE_DATE)

    def test_policy_rejects_wildcard_or_long_lived_paths(self) -> None:
        policy = {
            "vulnerabilities": [
                {
                    "id": "CVE-2026-1234",
                    "paths": ["opt/spark/jars/*.jar"],
                    "expired_at": "2026-12-01",
                    "statement": "This sentence is deliberately long enough to describe a risk and a replacement plan.",
                }
            ]
        }

        with TemporaryDirectory() as directory:
            policy_path = Path(directory) / "policy.yaml"
            policy_path.write_text(yaml.safe_dump(policy), encoding="utf-8")
            with self.assertRaisesRegex(PolicyError, "must not contain a wildcard"):
                validate_policy(policy_path, today=REFERENCE_DATE)

            policy["vulnerabilities"][0]["paths"] = ["opt/spark/jars/example-1.0.0.jar"]
            policy_path.write_text(yaml.safe_dump(policy), encoding="utf-8")
            with self.assertRaisesRegex(PolicyError, "must be within 45 days"):
                validate_policy(policy_path, today=REFERENCE_DATE)


if __name__ == "__main__":
    unittest.main()
