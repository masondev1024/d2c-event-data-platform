"""Fail closed when Bandit skips files, emits invalid JSON or reports findings.

Bandit 1.8.6 can exit zero after AST visitor exceptions. A zero process status
therefore proves success only with an error-free, populated analysis report.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


class ReportError(ValueError):
    pass


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ReportError("duplicate JSON object keys")
        value[key] = item
    return value


def reject_constant(_value):
    raise ReportError("non-finite JSON number")


def evaluate_report(document, bandit_exit_code) -> dict:
    result = {"schema_version": 1, "outcome": "infrastructure_fail", "bandit_exit_code": bandit_exit_code,
              "findings": None, "scan_errors": None, "analyzed_lines": None}
    try:
        if type(bandit_exit_code) is not int or bandit_exit_code not in (0, 1):
            raise ReportError("Bandit did not exit normally with status 0 or 1")
        if not isinstance(document, dict):
            raise ReportError("Bandit report root must be an object")
        errors, findings = document.get("errors"), document.get("results")
        if not isinstance(errors, list) or not isinstance(findings, list):
            raise ReportError("Bandit errors and results must both be lists")
        result.update(findings=len(findings), scan_errors=len(errors))
        if any(not isinstance(item, dict) for item in errors + findings):
            raise ReportError("Bandit errors and results must contain objects")
        metrics = document.get("metrics")
        totals = metrics.get("_totals") if isinstance(metrics, dict) else None
        lines = totals.get("loc") if isinstance(totals, dict) else None
        if type(lines) is not int or lines <= 0:
            raise ReportError("Bandit must report a positive integer analyzed-line count")
        result["analyzed_lines"] = lines
        if errors:
            raise ReportError("Bandit skipped or failed one or more source files")
        if bool(bandit_exit_code) != bool(findings):
            raise ReportError("Bandit exit status and filtered findings disagree")
        result["outcome"] = "security_fail" if findings else "pass"
        if findings:
            result["reason"] = "Bandit reported findings at the configured thresholds"
    except ReportError as error:
        result["reason"] = str(error)
    return result


def main(arguments=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--bandit-exit-code", type=int, required=True)
    options = parser.parse_args(arguments)
    try:
        document = json.loads(options.report.read_text(encoding="utf-8"), object_pairs_hook=unique_object,
                              parse_constant=reject_constant)
        result = evaluate_report(document, options.bandit_exit_code)
    except (OSError, ValueError, UnicodeError):
        result = {"schema_version": 1, "outcome": "infrastructure_fail", "bandit_exit_code": options.bandit_exit_code,
                  "reason": "Bandit report is unavailable or is not valid UTF-8 JSON"}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["outcome"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
