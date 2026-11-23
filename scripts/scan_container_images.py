"""Collect every requested image's evidence before enforcing the security gate.

Only completed, fresh Trivy reports for the inspected immutable image ID count
as results. A failed scan is recorded as an infrastructure failure, never zero
findings. The default contract requires all four D2C image roles.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.validate_lakehouse_trivy_exceptions import validate_policy

ROLES = ("worker", "api", "catalog", "lakehouse")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}$")


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, data: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


class ImageScanner:
    def __init__(self, options: argparse.Namespace):
        self.options = options
        self.output = options.output_dir.resolve()
        # Do not overwrite or accidentally validate evidence from an earlier run.
        self.output.mkdir(parents=True, exist_ok=False)
        self.trivy_config = self.output / "trivy-config.yaml"
        self.trivy_config.write_text("{}\n", encoding="utf-8")
        self.images = dict(options.image)
        self.layouts = dict(options.oci_layout)
        self.roles = options.required_role or list(ROLES)
        self.deadline = time.monotonic() + options.total_timeout
        self.summary: dict[str, Any] = {
            "schema_version": 1,
            "started_at": timestamp(),
            "required_roles": self.roles,
            "overall_timeout_seconds": options.total_timeout,
            "gate_criteria": {"severity": ["HIGH", "CRITICAL"],
                              "scanners": ["vuln", "secret", "misconfig"],
                              "ignore_unfixed": True},
            "scanner_config_sha256": hashlib.sha256(self.trivy_config.read_bytes()).hexdigest(),
            "images": {}, "failures": [],
        }

    def save(self) -> None:
        write_json(self.output / "summary.json", self.summary)

    def failure(self, stage: str, message: str, kind: str = "infrastructure_fail") -> None:
        self.summary["failures"].append({"stage": stage, "outcome": kind, "message": message})

    def ensure_budget(self) -> None:
        if time.monotonic() >= self.deadline:
            raise ValueError("overall verification deadline exceeded; command not executed")

    def command(self, label: str, command: list[str], *, guarded: bool = False,
                capture: bool = False) -> dict[str, Any]:
        log = self.output / (label + ".log")
        started = time.time()
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            message = "overall verification deadline exceeded; command not executed"
            log.write_text(message + "\n", encoding="utf-8")
            return {"started_at_unix": started, "finished_at_unix": time.time(), "exit_code": None,
                    "error": message, "log": log.name, "command": command}
        timeout = min(self.options.timeout, remaining)
        arguments = command
        if guarded and self.options.guard_script:
            arguments = [sys.executable, str(self.options.guard_script), "--kind", "scan",
                         "--minimum-free-gib", str(self.options.guard_minimum_free_gib),
                         "--reserve-free-gib", "6", "--timeout-seconds", str(timeout),
                         "--evidence-dir", str(self.output / "guard" / label)]
            cache_storage = self.options.cache_dir.resolve()
            while not cache_storage.exists():
                cache_storage = cache_storage.parent
            storage_paths = [self.output, cache_storage, *self.options.guard_storage_path]
            for storage in dict.fromkeys(storage_paths):
                arguments += ["--storage-path", str(storage)]
            arguments += ["--", *command]
        result: dict[str, Any] = {"started_at_unix": started, "command": arguments,
                                  "exit_code": None, "error": None, "log": log.name}
        try:
            completed = subprocess.run(arguments, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True,
                                       # Trivy environment settings otherwise
                                       # provide another route to skip paths,
                                       # statuses, or policies outside this gate.
                                       env={key: value for key, value in os.environ.items()
                                            if not key.startswith("TRIVY_")},
                                       # The guard owns its timeout and cleanup.
                                       # subprocess.run's timeout would SIGKILL the
                                       # guard before it could stop its own child.
                                       timeout=None if guarded and self.options.guard_script else timeout)
            result["exit_code"] = completed.returncode
            # Image inspect may contain application environment values. Its full
            # output is parsed in memory and is deliberately not written to logs.
            log.write_text(("" if capture else completed.stdout) + completed.stderr, encoding="utf-8")
            if capture:
                result["stdout"] = completed.stdout
            if guarded and self.options.guard_script:
                guard = json.loads((self.output / "guard" / label / "summary.json").read_text())
                result["guard"] = {key: guard.get(key) for key in ("outcome", "child_exit", "abort", "min_free_bytes")}
                if (guard.get("outcome") not in ("completed", "child_failed")
                        or guard.get("child_exit") != completed.returncode
                        or guard.get("abort") is not None):
                    result["error"] = "guard did not confirm a completed child command"
                child_log = self.output / "guard" / label / "command.log"
                if child_log.exists():
                    log.write_text(child_log.read_text() + completed.stdout + completed.stderr, encoding="utf-8")
                elif not result["error"]:
                    result["error"] = "guard child log is missing"
        except (OSError, ValueError, TypeError, subprocess.TimeoutExpired) as error:
            result["error"] = type(error).__name__ + ": " + str(error)
            with log.open("a", encoding="utf-8") as stream:
                stream.write(result["error"] + "\n")
        result["finished_at_unix"] = time.time()
        return result

    def trivy(self, *arguments: str) -> list[str]:
        return [self.options.trivy, "--config", str(self.trivy_config),
                "--cache-dir", str(self.options.cache_dir), *arguments]

    def prepare(self) -> None:
        policy = self.options.policy
        try:
            entries = validate_policy(policy)
            self.summary["policy"] = {"outcome": "pass", "exceptions": len(entries),
                                      "sha256": hashlib.sha256(policy.read_bytes()).hexdigest()}
            if entries:
                raise ValueError("the D2C remediation requires zero suppression entries")
        except Exception as error:
            self.summary["policy"] = {"outcome": "policy_fail", "error": str(error)}
            self.failure("policy", str(error), "policy_fail")
        # Use no suppression file even after a policy failure. This preserves raw
        # evidence and cannot apply an invalid or newly introduced exception.
        for label, flag in (("download-db", "--download-db-only"),
                            ("download-java-db", "--download-java-db-only")):
            result = self.command(label, self.trivy("image", flag), guarded=True)
            write_json(self.output / (label + ".status.json"), result)
            if result["exit_code"] != 0 or result["error"]:
                self.failure(label, "database download did not complete")
        databases = {}
        for relative in ("db/trivy.db", "java-db/trivy-java.db"):
            try:
                path = self.options.cache_dir / relative
                if path.stat().st_size == 0:
                    raise ValueError("empty database")
                digest = hashlib.sha256()
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                        self.ensure_budget()
                        digest.update(block)
                metadata = json.loads((path.parent / "metadata.json").read_text())
                if not metadata.get("Version") or not metadata.get("UpdatedAt"):
                    raise ValueError("database metadata lacks version or update time")
                databases[relative] = {"sha256": digest.hexdigest(), "metadata": metadata}
            except Exception as error:
                databases[relative] = {"error": str(error)}
                self.failure("database:" + relative, "actual database or metadata is unavailable")
        self.summary["databases"] = databases
        result = self.command("trivy-version", self.trivy("--version", "--format", "json"), capture=True)
        try:
            if result["exit_code"] != 0 or result["error"]:
                raise ValueError("Trivy version collection failed")
            version = json.loads(result.pop("stdout"))
            if not isinstance(version.get("Version"), str):
                raise ValueError("Trivy did not report its version")
            if self.options.expected_trivy_version and version["Version"] != self.options.expected_trivy_version:
                raise ValueError("Trivy version does not match the pinned workflow version")
            self.summary["tool"] = version
            write_json(self.output / "trivy-version.json", version)
        except Exception as error:
            self.failure("trivy-version", str(error))
        self.save()

    def inspect(self, role: str, reference: str) -> str:
        self.ensure_budget()
        if role in self.layouts:
            # This config-digest contract belongs to Trivy 0.76.1's OCI input
            # reader. It is not an assumption about Docker/containerd .Id.
            if self.options.expected_trivy_version != "0.76.1" or not self.options.platform:
                raise ValueError("OCI input requires pinned Trivy 0.76.1 and an explicit platform")
            from scripts.oci_release import prepare_scan_layout
            identity = prepare_scan_layout(Path(self.layouts[role]), self.output / (role + "-scan-view"),
                                           self.options.platform)
            image_id = identity["expected_trivy_image_id"]
            if not IMAGE_ID.fullmatch(image_id) or image_id != identity["config_digest"]:
                raise ValueError("OCI input did not identify the expected Trivy config digest")
            view = Path(identity["scan_input_path"])
            image = self.summary["images"][role]
            image.update({"image_id": image_id, "platform": identity["platform"],
                          "source_kind": "oci_layout", "oci_identity": identity,
                          "scan_view_sha256": {name: hashlib.sha256((view / name).read_bytes()).hexdigest()
                                               for name in ("index.json", "oci-layout")}})
            write_json(self.output / (role + "-image.json"), image)
            self.validate_source(role)
            return image_id
        result = self.command(role + "-inspect", [self.options.docker, "image", "inspect", reference], capture=True)
        if result["exit_code"] != 0 or result["error"]:
            raise ValueError("image inspection failed or image is missing")
        metadata = json.loads(result["stdout"])
        if not isinstance(metadata, list) or len(metadata) != 1:
            raise ValueError("image inspection must identify one image")
        item = metadata[0]
        image_id = item.get("Id", "")
        if not IMAGE_ID.fullmatch(image_id):
            raise ValueError("image inspection returned an invalid immutable ID")
        if self.options.platform:
            system, architecture = self.options.platform.split("/", 1)
            if item.get("Os") != system or item.get("Architecture") != architecture:
                raise ValueError("image platform does not match the requested platform")
        evidence = {key: item.get(key) for key in
                    ("Id", "RepoTags", "RepoDigests", "Os", "Architecture", "Created", "Size")}
        write_json(self.output / (role + "-image.json"), evidence)
        self.summary["images"][role].update({"image_id": image_id, "platform":
                                            item.get("Os", "") + "/" + item.get("Architecture", ""),
                                            "source_kind": "docker_image"})
        return image_id

    def validate_source(self, role: str) -> None:
        self.ensure_budget()
        image = self.summary["images"][role]
        if image.get("source_kind") != "oci_layout":
            return
        from scripts.oci_release import validate_layout
        expected = image["oci_identity"]
        current = validate_layout(Path(self.layouts[role]), self.options.platform)
        for key in ("root_digest", "runtime_leaf_digest", "config_digest", "metadata_sha256", "platform"):
            if current[key] != expected[key]:
                raise ValueError("original OCI layout changed during verification")
        view = Path(expected["scan_input_path"])
        if not (view / "blobs").samefile(Path(self.layouts[role]) / "blobs"):
            raise ValueError("OCI scan view no longer shares the verified source blobs")
        for name, digest in image["scan_view_sha256"].items():
            if hashlib.sha256((view / name).read_bytes()).hexdigest() != digest:
                raise ValueError("OCI scan view changed during verification")

    def report(self, role: str, image_id: str, mode: str) -> dict[str, Any]:
        label = role + "-" + mode
        path = self.output / (role + ".sbom.spdx.json" if mode == "sbom" else label + ".json")
        arguments = self.trivy("image", "--cache-backend", "memory",
                               "--parallel", "1", "--skip-db-update", "--skip-java-db-update", "--timeout", "15m",
                               "--ignorefile", os.devnull, "--ignore-policy", "", "--secret-config", "")
        if mode == "sbom":
            arguments += ["--format", "spdx-json", "--output", str(path)]
        else:
            arguments += ["--scanners", "vuln,secret,misconfig", "--severity", "HIGH,CRITICAL",
                          "--format", "json", "--output", str(path)]
            if mode == "gate":
                arguments += ["--ignore-unfixed", "--exit-code", "1"]
        image = self.summary["images"][role]
        if image.get("source_kind") == "oci_layout":
            arguments += ["--input", image["oci_identity"]["scan_input_path"]]
        else:
            arguments += ["--image-src", "docker", image_id]
        try:
            self.validate_source(role)
            result = self.command(label, arguments, guarded=True)
        except Exception as error:
            log = self.output / (label + ".log")
            log.write_text(str(error) + "\n")
            result = {"exit_code": None, "error": str(error), "log": log.name}
        result["image_id"] = image_id
        result["outcome"] = "infrastructure_fail"
        try:
            if result["error"] or result["exit_code"] not in ((0, 1) if mode == "gate" else (0,)):
                raise ValueError("scanner or guard did not complete normally")
            if re.search(r"(?m)(?:^|\s)FATAL(?:\s|$)", (self.output / result["log"]).read_text()):
                raise ValueError("scanner logged a fatal error")
            if path.stat().st_mtime < result["started_at_unix"] - 1:
                raise ValueError("report predates this scan")
            report_bytes = path.read_bytes()
            data = json.loads(report_bytes)
            if mode == "sbom":
                if not str(data.get("spdxVersion", "")).startswith("SPDX-") or not isinstance(data.get("packages"), list) or not data["packages"]:
                    raise ValueError("report is not a populated SPDX SBOM")
                result["packages"] = len(data["packages"])
            else:
                if data.get("Metadata", {}).get("ImageID") != image_id:
                    raise ValueError("report does not match the immutable scan target")
                created = datetime.fromisoformat(data["CreatedAt"].replace("Z", "+00:00"))
                if (created.tzinfo is None or created.timestamp() < result["started_at_unix"] - 2
                        or created.timestamp() > result["finished_at_unix"] + 5):
                    raise ValueError("report creation time is outside this scan")
                if not isinstance(data.get("Results"), list) or not data["Results"]:
                    raise ValueError("report contains no analyzed results")
                version = data.get("Trivy", {}).get("Version")
                if version and version != self.summary.get("tool", {}).get("Version"):
                    raise ValueError("report scanner version does not match the recorded tool")
                results = data["Results"]
                values = [value for group in results for value in group.get("Vulnerabilities", [])]
                result["counts"] = {severity: sum(v["Severity"] == severity for v in values)
                                    for severity in ("HIGH", "CRITICAL")}
                result["fix_available"] = sum(bool(v.get("FixedVersion")) for v in values)
                result["secrets"] = sum(len(group.get("Secrets", [])) for group in results)
                result["misconfigurations"] = sum(v.get("Status", "FAIL") == "FAIL"
                    for group in results for v in group.get("Misconfigurations", []))
                result["suppressed"] = sum(len(group.get("ExperimentalModifiedFindings", [])) for group in results)
                if result["suppressed"]:
                    raise ValueError("suppression records are forbidden by this zero-exception gate")
                findings = len(values) + result["secrets"] + result["misconfigurations"]
                if mode == "gate" and bool(result["exit_code"]) != bool(findings):
                    raise ValueError("gate exit code and actual findings disagree")
            self.validate_source(role)
            result["report"] = path.name
            result["report_sha256"] = hashlib.sha256(report_bytes).hexdigest()
            result["outcome"] = ("security_fail" if mode == "gate" and result["exit_code"] == 1
                                 else "pass" if mode == "gate" else "completed")
        except Exception as error:
            result["validation_error"] = str(error)
        if result["outcome"] not in ("pass", "completed"):
            self.failure(label, result.get("validation_error", "HIGH/CRITICAL gate findings"), result["outcome"])
        write_json(self.output / (label + ".status.json"), result)
        return result

    def run(self) -> int:
        self.prepare()
        # Inspect all roles before scanning so a tag moving later cannot change
        # the scan target. A missing image must not suppress the other images.
        for role in self.roles:
            reference = self.images.get(role) or self.layouts.get(role)
            self.summary["images"][role] = {"reference": reference}
            try:
                if not reference:
                    raise ValueError("required image was not supplied")
                self.inspect(role, reference)
            except Exception as error:
                self.failure(role + "-inspect", str(error))
                self.summary["images"][role]["inspection_error"] = str(error)
        for role in self.roles:
            image = self.summary["images"][role]
            for mode in ("raw", "gate", "sbom"):
                if image.get("image_id"):
                    image[mode] = self.report(role, image["image_id"], mode)
                else:
                    result = {"outcome": "infrastructure_fail", "image_id": None,
                              "error": "image unavailable; scan was not executed"}
                    image[mode] = result
                    write_json(self.output / (role + "-" + mode + ".status.json"), result)
                    (self.output / (role + "-" + mode + ".log")).write_text(result["error"] + "\n")
                self.save()
        failures = self.summary["failures"]
        self.summary["outcome"] = ("infrastructure_fail" if any(f["outcome"] == "infrastructure_fail" for f in failures)
                                   else "policy_fail" if any(f["outcome"] == "policy_fail" for f in failures)
                                   else "security_fail" if failures else "pass")
        self.summary["exit_code"] = int(bool(failures))
        self.summary["finished_at"] = timestamp()
        self.save()
        return self.summary["exit_code"]


def image_argument(value: str) -> tuple[str, str]:
    role, separator, reference = value.partition("=")
    if role not in ROLES or not separator or not reference or reference.startswith("-"):
        raise argparse.ArgumentTypeError("image must be ROLE=REFERENCE for a supported D2C role")
    return role, reference


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--image", type=image_argument, action="append", default=[])
    result.add_argument("--oci-layout", type=image_argument, action="append", default=[],
                        help="ROLE=PATH to a verified attested OCI directory; never infer Docker .Id from config digest")
    result.add_argument("--required-role", choices=ROLES, action="append")
    result.add_argument("--output-dir", type=Path, required=True)
    result.add_argument("--policy", type=Path, default=Path(__file__).resolve().parents[1] / "security/lakehouse.trivyignore.yaml")
    result.add_argument("--trivy", default="trivy")
    result.add_argument("--docker", default="docker")
    result.add_argument("--expected-trivy-version")
    result.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    result.add_argument("--cache-dir", type=Path, default=Path.home() / ".cache/trivy")
    result.add_argument("--timeout", type=int, default=1200)
    result.add_argument("--total-timeout", type=int, default=5400,
                        help="overall verification budget; remaining attempts become explicit infrastructure failures")
    result.add_argument("--guard-script", type=Path)
    result.add_argument("--guard-storage-path", type=Path, action="append", default=[])
    result.add_argument("--guard-minimum-free-gib", type=int, default=10)
    return result


def main(arguments: list[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    if options.timeout <= 0 or options.total_timeout <= 0:
        raise SystemExit("verification timeouts must be positive")
    sources = options.image + options.oci_layout
    if len(dict(sources)) != len(sources):
        raise SystemExit("each image role may be supplied only once")
    if options.required_role and len(set(options.required_role)) != len(options.required_role):
        raise SystemExit("required roles must be unique")
    if options.required_role and set(dict(sources)) - set(options.required_role):
        raise SystemExit("supplied images must belong to the explicitly required role set")
    return ImageScanner(options).run()


if __name__ == "__main__":
    raise SystemExit(main())
