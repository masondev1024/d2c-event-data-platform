"""Reject broad or stale Trivy suppression entries before an image scan runs."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import yaml


class PolicyError(ValueError):
    """Raised when a Trivy exception policy would weaken the release gate."""


MAX_EXCEPTION_LIFETIME = timedelta(days=45)
_ID_PATTERN = re.compile(
    r"(?:CVE-\d{4}-\d{4,}|GHSA-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4})$"
)
_ALLOWED_ENTRY_FIELDS = frozenset({"id", "paths", "expired_at", "statement"})
_FORBIDDEN_PATH_TOKENS = frozenset({"*", "?", "[", "]"})


def _require_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PolicyError(f"{label} must be a non-empty string")
    return value.strip()


def _expiration(value: object, label: str) -> date:
    if isinstance(value, datetime):
        raise PolicyError(f"{label} must be an ISO date without a time")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise PolicyError(f"{label} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise PolicyError(f"{label} must be an ISO date") from error


def _validate_path(value: object, label: str) -> str:
    path = _require_string(value, label)
    if any(token in path for token in _FORBIDDEN_PATH_TOKENS):
        raise PolicyError(f"{label} must not contain a wildcard")

    normalized = PurePosixPath(path)
    if (
        path.startswith("/")
        or ".." in normalized.parts
        or not path.startswith("opt/spark/jars/")
        or not path.endswith(".jar")
    ):
        raise PolicyError(f"{label} must be an exact nested Spark jar path")
    return path


def validate_policy(
    policy_path: Path, *, today: date | None = None
) -> tuple[Mapping[str, object], ...]:
    """Validate a narrowly scoped, short-lived Trivy YAML ignore policy."""

    reference_date = today or date.today()
    try:
        document: Any = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    except OSError as error:
        raise PolicyError(f"cannot read {policy_path}: {error}") from error
    except yaml.YAMLError as error:
        raise PolicyError(f"cannot parse {policy_path}: {error}") from error

    if not isinstance(document, Mapping):
        raise PolicyError("policy root must be a mapping")
    if set(document) != {"vulnerabilities"}:
        raise PolicyError("policy may contain only the vulnerabilities section")

    entries = document["vulnerabilities"]
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)) or not entries:
        raise PolicyError("vulnerabilities must be a non-empty list")

    seen: set[tuple[str, str]] = set()
    validated: list[Mapping[str, object]] = []
    for index, entry in enumerate(entries, start=1):
        label = f"vulnerabilities[{index}]"
        if not isinstance(entry, Mapping):
            raise PolicyError(f"{label} must be a mapping")
        unexpected = set(entry) - _ALLOWED_ENTRY_FIELDS
        if unexpected:
            raise PolicyError(f"{label} has unsupported fields: {sorted(unexpected)}")

        finding_id = _require_string(entry.get("id"), f"{label}.id")
        if not _ID_PATTERN.fullmatch(finding_id):
            raise PolicyError(f"{label}.id must be an exact CVE or GHSA identifier")

        raw_paths = entry.get("paths")
        if not isinstance(raw_paths, Sequence) or isinstance(raw_paths, (str, bytes)) or not raw_paths:
            raise PolicyError(f"{label}.paths must scope every finding to one or more jars")
        for path_index, raw_path in enumerate(raw_paths, start=1):
            path = _validate_path(raw_path, f"{label}.paths[{path_index}]")
            key = (finding_id, path)
            if key in seen:
                raise PolicyError(f"{label} repeats the {finding_id} exception for {path}")
            seen.add(key)

        expires_at = _expiration(entry.get("expired_at"), f"{label}.expired_at")
        if expires_at <= reference_date:
            raise PolicyError(f"{label}.expired_at must be after today")
        if expires_at > reference_date + MAX_EXCEPTION_LIFETIME:
            raise PolicyError(
                f"{label}.expired_at must be within {MAX_EXCEPTION_LIFETIME.days} days"
            )

        statement = _require_string(entry.get("statement"), f"{label}.statement")
        if len(statement) < 80:
            raise PolicyError(f"{label}.statement must explain the risk and replacement plan")
        validated.append(entry)

    return tuple(validated)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a bounded Trivy ignore policy for the lakehouse image."
    )
    parser.add_argument("--policy", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        entries = validate_policy(args.policy)
    except PolicyError as error:
        print(f"lakehouse Trivy exception policy rejected: {error}", file=sys.stderr)
        return 1

    earliest_expiry = min(_expiration(entry["expired_at"], "expired_at") for entry in entries)
    print(
        "lakehouse Trivy exception policy valid: "
        f"{len(entries)} findings, {sum(len(entry['paths']) for entry in entries)} exact paths, "
        f"earliest expiry {earliest_expiry.isoformat()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
