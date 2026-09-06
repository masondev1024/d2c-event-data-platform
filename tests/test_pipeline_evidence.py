from __future__ import annotations

import json
import unittest
from pathlib import Path

from scripts.validate_pipeline_evidence import validate_evidence


class PipelineEvidenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = Path(__file__).parents[1] / "examples" / "pipeline-evidence.ci.json"
        cls.valid_document = json.loads(path.read_text(encoding="utf-8"))

    def test_valid_evidence_passes_cross_stage_invariants(self) -> None:
        self.assertEqual(validate_evidence(self.valid_document), [])

    def test_count_mismatch_is_rejected(self) -> None:
        document = json.loads(json.dumps(self.valid_document))
        document["routing"]["dlq_records"] = 0

        errors = validate_evidence(document)

        self.assertTrue(any("input_records" in error for error in errors))

    def test_failed_quality_report_is_rejected(self) -> None:
        document = json.loads(json.dumps(self.valid_document))
        document["quality"] = {"passed": False, "failures": ["duplicate_rows"]}

        errors = validate_evidence(document)

        self.assertIn("quality.passed must be true", errors)
        self.assertIn("quality.failures must be empty", errors)


if __name__ == "__main__":
    unittest.main()
