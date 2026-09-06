"""Validate cross-stage counts captured from one pipeline run.

Metrics such as consumer lag prove that workers are alive, but they do not prove
that the same records crossed each boundary. This gate checks the arithmetic
between routing, sink, and data-quality evidence before a run is called healthy.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping


class PipelineEvidenceError(ValueError):
    """Raised when a pipeline evidence document violates its invariants."""


def _count(document: Mapping[str, Any], path: str, errors: list[str]) -> int | None:
    value: Any = document
    for part in path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            errors.append(f"missing count: {path}")
            return None
        value = value[part]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        errors.append(f"count must be a non-negative integer: {path}")
        return None
    return value


def validate_evidence(document: Mapping[str, Any]) -> list[str]:
    """Return invariant violations for a captured pipeline evidence document."""

    errors: list[str] = []
    if document.get("schema_version") != "factory-sensor.v1":
        errors.append("schema_version must be factory-sensor.v1")

    routing_input = _count(document, "routing.input_records", errors)
    routing_clean = _count(document, "routing.clean_records", errors)
    routing_dlq = _count(document, "routing.dlq_records", errors)
    sink_received = _count(document, "sink.received_records", errors)
    sink_unique = _count(document, "sink.unique_rows", errors)
    sink_duplicates = _count(document, "sink.duplicate_rows", errors)
    sink_failures = _count(document, "sink.failed_records", errors)

    if routing_input is not None and routing_clean is not None and routing_dlq is not None:
        if routing_input != routing_clean + routing_dlq:
            errors.append("routing.input_records must equal clean_records + dlq_records")

    if routing_clean is not None and sink_received is not None and routing_clean != sink_received:
        errors.append("routing.clean_records must equal sink.received_records")

    if sink_received is not None and sink_unique is not None and sink_duplicates is not None and sink_failures is not None:
        if sink_received != sink_unique + sink_duplicates + sink_failures:
            errors.append(
                "sink.received_records must equal unique_rows + duplicate_rows + failed_records"
            )
        if sink_unique > sink_received:
            errors.append("sink.unique_rows cannot exceed sink.received_records")

    quality = document.get("quality")
    if not isinstance(quality, Mapping):
        errors.append("quality must be an object")
    else:
        if quality.get("passed") is not True:
            errors.append("quality.passed must be true")
        failures = quality.get("failures")
        if not isinstance(failures, list):
            errors.append("quality.failures must be a list")
        elif failures:
            errors.append("quality.failures must be empty")

    return errors


def load_evidence(path: str | Path) -> dict[str, Any]:
    evidence_path = Path(path)
    try:
        document = json.loads(evidence_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineEvidenceError(f"unable to read evidence {evidence_path}: {exc}") from exc
    if not isinstance(document, dict):
        raise PipelineEvidenceError("evidence document must be a JSON object")
    return document


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="JSON evidence document to validate")
    args = parser.parse_args()
    try:
        document = load_evidence(args.manifest)
    except PipelineEvidenceError as exc:
        parser.error(str(exc))
    errors = validate_evidence(document)
    if errors:
        parser.error("pipeline evidence failed:\n" + "\n".join(f"- {error}" for error in errors))
    print(
        "pipeline evidence passed: "
        f"input={document['routing']['input_records']} "
        f"clean={document['routing']['clean_records']} "
        f"dlq={document['routing']['dlq_records']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
