"""Check a conservative backward-compatible JSON Schema evolution.

The check intentionally favours a safe false negative over silently approving a
breaking data contract. It allows additive optional properties and widened
numeric/enum bounds, while preserving the required fields and constraints that
existing consumers rely on.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


class SchemaCompatibilityError(ValueError):
    """Raised when a candidate schema can break existing valid events."""


Schema = dict[str, Any]


def _types(schema: Schema) -> set[str] | None:
    value = schema.get("type")
    if value is None:
        return None
    if isinstance(value, str):
        return {value}
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return set(value)
    return {"<invalid>"}


def _path(parent: str, child: str) -> str:
    return f"{parent}.{child}" if parent else child


def _compare_constraints(
    baseline: Schema,
    candidate: Schema,
    location: str,
    errors: list[str],
) -> None:
    baseline_types = _types(baseline)
    candidate_types = _types(candidate)
    if baseline_types is not None:
        if candidate_types is None:
            errors.append(f"{location}: candidate removed the baseline type constraint")
        elif not baseline_types.issubset(candidate_types):
            errors.append(
                f"{location}: candidate types {sorted(candidate_types)} do not include "
                f"baseline types {sorted(baseline_types)}"
            )

    if "const" in baseline:
        if candidate.get("const") != baseline["const"]:
            errors.append(f"{location}: const value changed or was removed")

    if "enum" in baseline:
        baseline_values = set(baseline["enum"])
        candidate_values = set(candidate.get("enum", ()))
        if not baseline_values.issubset(candidate_values):
            errors.append(f"{location}: candidate enum removed existing values")

    for keyword in ("minimum", "exclusiveMinimum", "minLength", "minItems"):
        if keyword in baseline:
            if keyword not in candidate:
                errors.append(f"{location}: candidate removed {keyword}")
            elif candidate[keyword] > baseline[keyword]:
                errors.append(f"{location}: candidate tightened {keyword}")

    for keyword in ("maximum", "exclusiveMaximum", "maxLength", "maxItems"):
        if keyword in baseline:
            if keyword not in candidate:
                errors.append(f"{location}: candidate removed {keyword}")
            elif candidate[keyword] < baseline[keyword]:
                errors.append(f"{location}: candidate tightened {keyword}")

    if "pattern" in baseline and candidate.get("pattern") != baseline["pattern"]:
        errors.append(f"{location}: candidate changed or removed the baseline pattern")

    if "additionalProperties" in baseline:
        baseline_additional = baseline["additionalProperties"]
        candidate_additional = candidate.get("additionalProperties")
        if candidate_additional != baseline_additional:
            errors.append(f"{location}: additionalProperties policy changed")

    baseline_required = set(baseline.get("required", ()))
    candidate_required = set(candidate.get("required", ()))
    if baseline_required != candidate_required:
        added = sorted(candidate_required - baseline_required)
        removed = sorted(baseline_required - candidate_required)
        errors.append(
            f"{location}: required fields changed; added={added or '-'} removed={removed or '-'}"
        )

    baseline_properties = baseline.get("properties", {})
    candidate_properties = candidate.get("properties", {})
    if not isinstance(baseline_properties, dict) or not isinstance(candidate_properties, dict):
        errors.append(f"{location}: properties must be objects")
        return

    for name, baseline_property in baseline_properties.items():
        child_location = _path(location, name)
        if name not in candidate_properties:
            errors.append(f"{child_location}: baseline property was removed")
            continue
        if not isinstance(baseline_property, dict) or not isinstance(candidate_properties[name], dict):
            errors.append(f"{child_location}: property schema must be an object")
            continue
        _compare_constraints(baseline_property, candidate_properties[name], child_location, errors)

    baseline_items = baseline.get("items")
    candidate_items = candidate.get("items")
    if baseline_items is not None:
        if not isinstance(baseline_items, dict) or not isinstance(candidate_items, dict):
            errors.append(f"{location}: array items schema was removed or is invalid")
        else:
            _compare_constraints(baseline_items, candidate_items, f"{location}[]", errors)


def load_schema(path: str | Path) -> Schema:
    schema_path = Path(path)
    try:
        value = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaCompatibilityError(f"unable to read schema {schema_path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SchemaCompatibilityError(f"schema {schema_path} must be a JSON object")
    return value


def compatibility_errors(baseline: Schema, candidate: Schema) -> list[str]:
    """Return all compatibility violations between two schema documents."""

    errors: list[str] = []
    _compare_constraints(baseline, candidate, "$", errors)
    return errors


def check_compatibility(baseline_path: str | Path, candidate_path: str | Path) -> None:
    baseline = load_schema(baseline_path)
    candidate = load_schema(candidate_path)
    errors = compatibility_errors(baseline, candidate)
    if errors:
        details = "\n".join(f"- {error}" for error in errors)
        raise SchemaCompatibilityError(
            f"schema is not backward compatible:\n{details}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="Previously deployed schema JSON")
    parser.add_argument("--candidate", required=True, help="Proposed schema JSON")
    args = parser.parse_args()
    try:
        check_compatibility(args.baseline, args.candidate)
    except SchemaCompatibilityError as exc:
        parser.error(str(exc))
    print(f"schema compatible: baseline={args.baseline} candidate={args.candidate}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
