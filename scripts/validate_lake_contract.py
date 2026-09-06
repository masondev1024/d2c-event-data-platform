"""Run the canonical Parquet lake quality gate and emit JSON evidence."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Support both ``python -m scripts.validate_lake_contract`` and the shorter
# ``python scripts/validate_lake_contract.py`` form used in the runbook.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from services.quality import LakeQualityError, validate_lake_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, help="Parquet file or directory to inspect")
    parser.add_argument(
        "--expected-schema-version",
        default="factory-sensor.v1",
        help="Canonical schema version expected in every row",
    )
    parser.add_argument(
        "--max-event-age-seconds",
        type=float,
        default=None,
        help="Optional freshness limit measured against the current UTC time",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Allow an empty Parquet path; useful for an intentional no-data window",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path for the JSON quality evidence artifact",
    )
    args = parser.parse_args()

    try:
        report = validate_lake_path(
            args.path,
            expected_schema_version=args.expected_schema_version,
            allow_empty=args.allow_empty,
            max_event_age_seconds=args.max_event_age_seconds,
        )
    except LakeQualityError as exc:
        parser.error(str(exc))

    payload = report.as_json()
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
