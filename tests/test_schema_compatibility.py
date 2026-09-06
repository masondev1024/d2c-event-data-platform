from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from scripts.check_schema_compatibility import compatibility_errors, load_schema


class SchemaCompatibilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schema_dir = Path(__file__).parents[1] / "schemas"
        cls.baseline = load_schema(schema_dir / "factory-sensor.v1.schema.json")
        cls.compatible_candidate = load_schema(schema_dir / "factory-sensor.v1.next.schema.json")

    def test_optional_property_evolution_is_compatible(self) -> None:
        self.assertEqual(compatibility_errors(self.baseline, self.compatible_candidate), [])

    def test_new_required_property_is_rejected(self) -> None:
        candidate = copy.deepcopy(self.compatible_candidate)
        candidate["required"].append("quality")

        errors = compatibility_errors(self.baseline, candidate)

        self.assertTrue(any("required fields changed" in error for error in errors))

    def test_removed_property_is_rejected(self) -> None:
        candidate = copy.deepcopy(self.compatible_candidate)
        del candidate["properties"]["humidity"]

        errors = compatibility_errors(self.baseline, candidate)

        self.assertTrue(any("humidity" in error and "removed" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
