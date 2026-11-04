"""Validate attested OCI graphs and publish the exact graph that passed Trivy.

OCI-input identity is deliberately separate from Docker daemon/containerd Id.
Trivy 0.76.1 archiveImage.ID calls image.ID, which returns ConfigName():
https://github.com/aquasecurity/trivy/blob/v0.76.1/pkg/fanal/image/image.go
The original OCI index and attestations are never rewritten for publication.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
CONFIG = "application/vnd.oci.image.config.v1+json"
STATEMENT = "application/vnd.in-toto+json"
SPDX = "https://spdx.dev/Document"
PROVENANCE = {"https://slsa.dev/provenance/v0.2", "https://slsa.dev/provenance/v1"}
LAYERS = {"application/vnd.oci.image.layer.v1.tar", "application/vnd.oci.image.layer.v1.tar+gzip"}
DIGEST = re.compile(r"sha256:[0-9a-f]{64}$")
ROLES = ("worker", "api", "catalog", "lakehouse")
IDENTITY_KEYS = ("root_digest", "runtime_leaf_digest", "config_digest", "metadata_sha256", "platform")
MAX_METADATA_BYTES = 64 * 1024 * 1024
LOCAL_COPY_TIMEOUT_SECONDS = 600
REGISTRY_COPY_TIMEOUT_SECONDS = 1200


class OCIError(ValueError):
    """An OCI source, scan result, or publication cannot be linked safely."""


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise OCIError("duplicate JSON object key")
        result[key] = value
    return result


def _json(data: bytes) -> dict:
    if len(data) > MAX_METADATA_BYTES:
        raise OCIError("OCI metadata exceeds the supported size")
    value = json.loads(data, object_pairs_hook=_object,
                       parse_constant=lambda value: (_ for _ in ()).throw(OCIError("non-finite JSON value")))
    if not isinstance(value, dict):
        raise OCIError("OCI metadata must be an object")
    return value


def _sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _regular(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise OCIError("OCI metadata/blob is missing or is a symlink: " + path.name)


def validate_layout(original: Path, platform: str) -> dict[str, Any]:
    """Hash a local OCI graph without extracting layers or executing image code.

    Only a single runtime platform plus its BuildKit attestations is supported;
    extra runtime platforms cannot be published without their own verification.
    """
    if platform not in ("linux/amd64", "linux/arm64"):
        raise OCIError("an explicit supported OCI platform is required")
    original = Path(original)
    if original.is_symlink() or not original.is_dir():
        raise OCIError("original OCI layout must be a real directory")
    root = original.resolve()
    if any(character in str(root) for character in (":", "@", "\n", "\r")):
        raise OCIError("OCI path contains an ambiguous transport separator")
    for directory in (root / "blobs", root / "blobs/sha256"):
        if directory.is_symlink() or not directory.is_dir():
            raise OCIError("OCI blobs must be stored inside the original directory")
    inventory: dict[str, str] = {}
    for name in ("oci-layout", "index.json"):
        _regular(root / name)
        content = (root / name).read_bytes()
        inventory[name] = _sha(content)
    if _json((root / "oci-layout").read_bytes()) != {"imageLayoutVersion": "1.0.0"}:
        raise OCIError("unsupported OCI layout version")
    outer = _json((root / "index.json").read_bytes())
    if outer.get("schemaVersion") != 2 or outer.get("mediaType", INDEX) != INDEX:
        raise OCIError("invalid OCI layout index")
    descriptors = outer.get("manifests")
    if not isinstance(descriptors, list) or len(descriptors) != 1:
        raise OCIError("release layout must select exactly one root descriptor")
    cache: dict[str, bytes | None] = {}

    def blob(descriptor: dict, *, metadata: bool = True) -> dict | None:
        if not isinstance(descriptor, dict) or not DIGEST.fullmatch(str(descriptor.get("digest", ""))):
            raise OCIError("invalid OCI descriptor digest")
        if type(descriptor.get("size")) is not int or descriptor["size"] < 0 or descriptor.get("urls"):
            raise OCIError("invalid or external OCI descriptor")
        digest = descriptor["digest"]
        path = root / "blobs/sha256" / digest[7:]
        _regular(path)
        if path.stat().st_size != descriptor["size"]:
            raise OCIError("OCI descriptor size mismatch")
        if digest not in cache:
            h = hashlib.sha256()
            chunks = []
            if metadata and descriptor["size"] > MAX_METADATA_BYTES:
                raise OCIError("OCI metadata exceeds the supported size")
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    h.update(chunk)
                    if metadata:
                        chunks.append(chunk)
            if "sha256:" + h.hexdigest() != digest:
                raise OCIError("OCI blob digest mismatch")
            inventory[str(path.relative_to(root))] = digest
            cache[digest] = b"".join(chunks) if metadata else None
        if metadata:
            data = cache[digest]
            if data is None:
                raise OCIError("OCI blob has conflicting metadata/layer uses")
            return _json(data)
        return None

    leaves: list[tuple[dict, dict]] = []
    visiting: set[str] = set()

    def walk(descriptor: dict, depth: int = 0) -> None:
        if depth > 8 or len(cache) > 10000:
            raise OCIError("OCI graph exceeds the supported bounds")
        document = blob(descriptor)
        digest = descriptor["digest"]
        if digest in visiting:
            raise OCIError("cyclic OCI graph")
        if document.get("schemaVersion") != 2 or document.get("mediaType") != descriptor.get("mediaType"):
            raise OCIError("OCI descriptor media type disagrees with its document")
        if descriptor.get("mediaType") == INDEX:
            children = document.get("manifests")
            if not isinstance(children, list) or not children:
                raise OCIError("empty OCI image index")
            visiting.add(digest)
            for child in children:
                walk(child, depth + 1)
            visiting.remove(digest)
        elif descriptor.get("mediaType") == MANIFEST:
            leaves.append((descriptor, document))
        else:
            raise OCIError("unsupported OCI root or child media type")

    root_descriptor = descriptors[0]
    if root_descriptor.get("mediaType") != INDEX:
        raise OCIError("an attested release must have an OCI image-index root")
    walk(root_descriptor)
    runtime = []
    attestations = []
    for descriptor, document in leaves:
        annotations = descriptor.get("annotations", {})
        if not isinstance(annotations, dict):
            raise OCIError("invalid OCI descriptor annotations")
        if (annotations.get("vnd.docker.reference.type") == "attestation-manifest"
                or document.get("artifactType") == "application/vnd.docker.attestation.manifest.v1+json"):
            attestations.append((descriptor, document))
        else:
            runtime.append((descriptor, document))
    if len(runtime) != 1:
        raise OCIError("exactly one verified runtime platform is required")
    descriptor, image = runtime[0]
    os_name, architecture = platform.split("/")
    if descriptor.get("platform", {}).get("os") != os_name or descriptor.get("platform", {}).get("architecture") != architecture:
        raise OCIError("OCI runtime descriptor platform mismatch")
    config_descriptor = image.get("config", {})
    if config_descriptor.get("mediaType") != CONFIG:
        raise OCIError("unsupported OCI runtime config")
    config = blob(config_descriptor)
    if config.get("os") != os_name or config.get("architecture") != architecture:
        raise OCIError("OCI runtime config platform mismatch")
    layers = image.get("layers")
    diff_ids = config.get("rootfs", {}).get("diff_ids")
    if (not isinstance(layers, list) or not isinstance(diff_ids, list) or len(layers) != len(diff_ids)
            or config.get("rootfs", {}).get("type") != "layers"
            or any(not DIGEST.fullmatch(str(value)) for value in diff_ids)):
        raise OCIError("invalid OCI runtime layers or diff IDs")
    for layer, diff_id in zip(layers, diff_ids):
        if layer.get("mediaType") not in LAYERS:
            raise OCIError("unsupported or foreign OCI runtime layer")
        blob(layer, metadata=False)
        # Bind the config's uncompressed content IDs to the manifest's actual
        # layer blobs, without extracting a filesystem or executing the image.
        layer_path = root / "blobs/sha256" / layer["digest"][7:]
        opener = gzip.open if layer["mediaType"].endswith("+gzip") else open
        h = hashlib.sha256()
        with opener(layer_path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(chunk)
        if "sha256:" + h.hexdigest() != diff_id:
            raise OCIError("runtime layer content does not match its config diff ID")
    predicate_types: set[str] = set()
    attestation_evidence = []
    leaf_digest = descriptor["digest"]
    for att_descriptor, attestation in attestations:
        link = att_descriptor.get("annotations", {}).get("vnd.docker.reference.digest")
        subject = attestation.get("subject", {}).get("digest")
        if (link or subject) != leaf_digest or (link and link != leaf_digest) or (subject and subject != leaf_digest):
            raise OCIError("attestation manifest does not refer to the runtime leaf")
        blob(attestation.get("config", {}))
        statements = attestation.get("layers")
        if not isinstance(statements, list) or not statements:
            raise OCIError("empty OCI attestation")
        for statement_descriptor in statements:
            if statement_descriptor.get("mediaType") != STATEMENT:
                raise OCIError("unsupported attestation payload")
            statement = blob(statement_descriptor)
            if statement.get("_type") not in ("https://in-toto.io/Statement/v0.1", "https://in-toto.io/Statement/v1"):
                raise OCIError("invalid in-toto statement")
            subjects = statement.get("subject")
            if (not isinstance(subjects, list) or not subjects
                    or any(value.get("digest") != {"sha256": leaf_digest[7:]} for value in subjects)):
                raise OCIError("attestation statement subject mismatch")
            predicate_type = statement.get("predicateType")
            if predicate_type not in PROVENANCE | {SPDX} or not isinstance(statement.get("predicate"), dict) or not statement["predicate"]:
                raise OCIError("missing or unsupported build attestation predicate")
            annotation_type = statement_descriptor.get("annotations", {}).get("in-toto.io/predicate-type")
            if annotation_type and annotation_type != predicate_type:
                raise OCIError("attestation predicate annotation mismatch")
            predicate_types.add(predicate_type)
        attestation_evidence.append({"manifest_digest": att_descriptor["digest"], "subject_digest": leaf_digest})
    if SPDX not in predicate_types or not predicate_types.intersection(PROVENANCE):
        raise OCIError("both SBOM and provenance attestations are required")
    return {
        "layout_path": str(root), "platform": platform,
        "root_digest": root_descriptor["digest"], "runtime_leaf_digest": leaf_digest,
        "root_descriptor": root_descriptor,
        "config_digest": config_descriptor["digest"],
        "expected_trivy_image_id": config_descriptor["digest"],
        "metadata_sha256": _sha(json.dumps(inventory, sort_keys=True).encode()),
        "metadata_blobs": inventory, "attestations": attestation_evidence,
        "predicate_types": sorted(predicate_types), "runtime_descriptor": descriptor,
        "rootfs_diff_ids": diff_ids,
    }


def prepare_scan_layout(original: Path, scan_view: Path, platform: str) -> dict[str, Any]:
    identity = validate_layout(original, platform)
    root = Path(identity["layout_path"])
    view = Path(scan_view).resolve()
    if view == root or root in view.parents or any(c in str(view) for c in (":", "@", "\n", "\r")):
        raise OCIError("scan view must be separate from the original layout")
    view.mkdir(parents=True, exist_ok=False)
    (view / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}\n')
    (view / "index.json").write_text(json.dumps({"schemaVersion": 2, "mediaType": INDEX,
        "manifests": [identity["runtime_descriptor"]]}, sort_keys=True) + "\n")
    (view / "blobs").symlink_to(root / "blobs", target_is_directory=True)
    return {**identity, "scan_input_path": str(view)}


def verify_release(summary_path: Path, layouts: dict[str, Path], platform: str) -> dict[str, Any]:
    summary = _json(summary_path.read_bytes())
    if (summary.get("schema_version") != 1 or summary.get("outcome") != "pass" or type(summary.get("exit_code")) is not int or summary.get("exit_code") != 0
            or summary.get("failures") != [] or set(summary.get("required_roles", [])) != set(ROLES)
            or set(summary.get("images", {})) != set(ROLES)):
        raise OCIError("all four release image gates must pass")
    if summary.get("policy", {}).get("outcome") != "pass" or summary["policy"].get("exceptions") != 0:
        raise OCIError("release requires the valid zero-exception policy")
    if summary.get("tool", {}).get("Version") != "0.76.1":
        raise OCIError("OCI release requires the reviewed Trivy version")
    if summary.get("gate_criteria") != {"severity": ["HIGH", "CRITICAL"], "scanners": ["vuln", "secret", "misconfig"], "ignore_unfixed": True}:
        raise OCIError("release gate criteria changed")
    if set(layouts) != set(ROLES):
        raise OCIError("every original OCI release layout is required")
    verified = {}
    for role in ROLES:
        image = summary["images"][role]
        current = validate_layout(layouts[role], platform)
        saved = image.get("oci_identity", {})
        if image.get("source_kind") != "oci_layout" or any(saved.get(key) != current[key] for key in IDENTITY_KEYS):
            raise OCIError("original OCI graph changed after scanning: " + role)
        if image.get("image_id") != current["expected_trivy_image_id"]:
            raise OCIError("scan identity is not the verified OCI runtime config: " + role)
        for mode in ("raw", "gate", "sbom"):
            status = image.get(mode, {})
            if status.get("outcome") != ("pass" if mode == "gate" else "completed") or type(status.get("exit_code")) is not int or status.get("exit_code") != 0 or status.get("image_id") != image["image_id"]:
                raise OCIError("incomplete image evidence: " + role + "/" + mode)
            name = role + ".sbom.spdx.json" if mode == "sbom" else role + "-" + mode + ".json"
            if status.get("report") != name:
                raise OCIError("unexpected release report path")
            report_path = summary_path.parent / name
            raw_report = report_path.read_bytes()
            if hashlib.sha256(raw_report).hexdigest() != status.get("report_sha256"):
                raise OCIError("report changed after scanning")
            started, finished = status.get("started_at_unix"), status.get("finished_at_unix")
            if type(started) not in (int, float) or type(finished) not in (int, float) or finished < started:
                raise OCIError("missing bounded scan timing evidence")
            if report_path.stat().st_mtime < started - 1:
                raise OCIError("report predates this scan")
            report = _json(raw_report)
            if mode == "sbom":
                if not str(report.get("spdxVersion", "")).startswith("SPDX-") or not report.get("packages"):
                    raise OCIError("missing populated SPDX evidence")
            elif report.get("Metadata", {}).get("ImageID") != current["expected_trivy_image_id"]:
                raise OCIError("report identity mismatch")
            else:
                created = datetime.fromisoformat(report.get("CreatedAt", "").replace("Z", "+00:00"))
                if created.tzinfo is None or not started - 2 <= created.timestamp() <= finished + 5:
                    raise OCIError("report creation time is outside the recorded scan")
                if not isinstance(report.get("Results"), list) or not report["Results"]:
                    raise OCIError("report contains no analyzed results")
            if mode == "gate":
                for group in report["Results"]:
                    if (group.get("Vulnerabilities") or group.get("Secrets") or group.get("ExperimentalModifiedFindings")
                            or any(v.get("Status", "FAIL") == "FAIL" for v in group.get("Misconfigurations", []))):
                        raise OCIError("release gate still contains findings")
        verified[role] = current
    return verified


def graph_identity(identity: dict) -> dict:
    """Compare the exported graph, allowing a transport's outer layout wrapper."""
    return {key: identity[key] for key in ("root_digest", "runtime_leaf_digest", "config_digest", "platform",
                                          "rootfs_diff_ids", "attestations", "predicate_types")} | {
        "blob_digests": {name: digest for name, digest in identity["metadata_blobs"].items()
                         if name.startswith("blobs/")}}


def _pinned_source(identity: dict, state: Path, role: str) -> Path:
    source = state / role
    source.mkdir(mode=0o700)
    (source / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}\n')
    (source / "index.json").write_text(json.dumps({"schemaVersion": 2, "mediaType": INDEX,
        "manifests": [identity["root_descriptor"]]}, sort_keys=True) + "\n")
    (source / "blobs").symlink_to(Path(identity["layout_path"]) / "blobs", target_is_directory=True)
    return source


def _skopeo_version(options: argparse.Namespace) -> str:
    version = subprocess.check_output([options.skopeo, "--version"], text=True, timeout=30).strip()
    if not re.fullmatch(r"skopeo version " + re.escape(options.expected_skopeo_version) + r"(?:\s.*)?", version):
        raise OCIError("Skopeo binary version does not match the reviewed package")
    return version


def _run_copy(options: argparse.Namespace, command: list[str], output: Path, role: str, timeout: int) -> None:
    if options.guard_script:
        prefix = [sys.executable, str(options.guard_script), "--kind", "scan", "--minimum-free-gib", "10",
                  "--reserve-free-gib", "6", "--timeout-seconds", str(timeout),
                  "--evidence-dir", str(output / (role + "-copy-guard"))]
        for storage in options.storage_path:
            prefix += ["--storage-path", str(storage)]
        command = prefix + ["--"] + command
    with (output / (role + "-copy.log")).open("w") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True,
                       timeout=None if options.guard_script else timeout)
    if options.guard_script:
        guard = _json((output / (role + "-copy-guard") / "summary.json").read_bytes())
        if guard.get("outcome") != "completed" or guard.get("child_exit") != 0 or guard.get("abort") is not None:
            raise OCIError("copy guard did not confirm a completed child")


def local_copy(options: argparse.Namespace) -> None:
    """Exercise the installed Skopeo against fresh local OCI destinations only."""
    verified = verify_release(options.scan_summary, dict(options.oci_layout), options.platform)
    output, state, destinations = (options.evidence_dir.resolve(), options.publish_state_dir.resolve(),
                                    options.local_copy_dir.resolve())
    if (output == state or output in state.parents or output == destinations or output in destinations.parents
            or state == destinations or state in destinations.parents or destinations in state.parents):
        raise OCIError("local blob/state directories must be separate from evidence")
    output.mkdir(parents=True, exist_ok=False)
    state.mkdir(parents=True, exist_ok=False, mode=0o700)
    destinations.mkdir(parents=True, exist_ok=False, mode=0o700)
    proof = {"schema_version": 1, "outcome": "pending", "skopeo_version": None,
             "scan_summary_sha256": hashlib.sha256(options.scan_summary.read_bytes()).hexdigest(),
             "sources": {role: graph_identity(identity) for role, identity in verified.items()}, "copies": {}}
    proof_path = output / "local-copy.json"
    try:
        proof["skopeo_version"] = _skopeo_version(options)
        for role in ROLES:
            current = validate_layout(Path(verified[role]["layout_path"]), options.platform)
            if any(current[key] != verified[role][key] for key in IDENTITY_KEYS):
                raise OCIError("original OCI graph changed before local copy")
            source = _pinned_source(current, state, role)
            destination = destinations / role
            if destination.exists():
                raise OCIError("local OCI destination must be new")
            digest_path = output / (role + "-copied.digest")
            command = [options.skopeo, "copy", "--all", "--preserve-digests", "--digestfile", str(digest_path),
                       "oci:" + str(source), "oci:" + str(destination)]
            _run_copy(options, command, output, role, LOCAL_COPY_TIMEOUT_SECONDS)
            if digest_path.read_text().strip() != current["root_digest"]:
                raise OCIError("local copy root digest differs from the original attested index")
            copied = validate_layout(destination, options.platform)
            if graph_identity(copied) != graph_identity(current):
                raise OCIError("local copy changed the verified OCI graph")
            proof["copies"][role] = {"destination_path": str(destination), "graph": graph_identity(copied)}
            proof_path.write_text(json.dumps(proof, indent=2) + "\n")
        proof["outcome"] = "local_copy_verified"
    except Exception as error:
        proof["outcome"] = "failed_local_copy"
        proof["error"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        proof_path.write_text(json.dumps(proof, indent=2) + "\n")


def verify_local_copy(options: argparse.Namespace, verified: dict) -> None:
    proof = _json(options.local_copy_proof.read_bytes())
    if (proof.get("schema_version") != 1 or proof.get("outcome") != "local_copy_verified"
            or set(proof.get("sources", {})) != set(ROLES) or set(proof.get("copies", {})) != set(ROLES)
            or proof.get("scan_summary_sha256") != hashlib.sha256(options.scan_summary.read_bytes()).hexdigest()
            or not re.fullmatch(r"skopeo version " + re.escape(options.expected_skopeo_version) + r"(?:\s.*)?", str(proof.get("skopeo_version", "")))):
        raise OCIError("successful local copy proof for this exact scan is required")
    for role in ROLES:
        destination = options.local_copy_dir.resolve() / role
        record = proof["copies"][role]
        if record.get("destination_path") != str(destination):
            raise OCIError("local copy proof points at an unexpected destination")
        expected = graph_identity(verified[role])
        if (proof["sources"][role] != expected or record.get("graph") != expected
                or graph_identity(validate_layout(destination, options.platform)) != expected):
            raise OCIError("local copy proof or copied OCI graph changed")


def publish(options: argparse.Namespace) -> None:
    """Copy original graphs only after every local gate/identity check passes."""
    owner = options.registry_owner.lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", owner) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", options.tag):
        raise OCIError("invalid release destination")
    verified = verify_release(options.scan_summary, dict(options.oci_layout), options.platform)
    verify_local_copy(options, verified)
    output = options.evidence_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    state = options.publish_state_dir.resolve()
    if state == output or output in state.parents:
        raise OCIError("publication blob views must be outside uploaded evidence")
    state.mkdir(parents=True, exist_ok=False, mode=0o700)
    result = {"skopeo_version": None, "images": {}, "copy_attempts": [], "outcome": "pending"}
    path = output / "publication.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    names = {"worker": "d2c-event-data-platform", "api": "d2c-event-data-platform-api", "catalog": "d2c-event-data-platform-catalog", "lakehouse": "d2c-event-data-platform-lakehouse"}
    try:
        version = _skopeo_version(options)
        result["skopeo_version"] = version
        for role in ROLES:
            identity = validate_layout(Path(verified[role]["layout_path"]), options.platform)
            if any(identity[key] != verified[role][key] for key in IDENTITY_KEYS):
                raise OCIError("OCI graph changed before publication")
            # Pin the verified root descriptor in a private view. A concurrent
            # rewrite of the original layout's mutable index.json must never
            # select another graph between preflight and Skopeo opening it.
            source = _pinned_source(identity, state, role)
            destination = f"ghcr.io/{owner}/{names[role]}:{options.tag}"
            digest_path = output / (role + "-published.digest")
            command = [options.skopeo, "copy", "--all", "--preserve-digests", "--digestfile", str(digest_path),
                       "--dest-authfile", str(options.authfile), "oci:" + str(source), "docker://" + destination]
            result["copy_attempts"].append({"role": role, "reference": destination,
                                            "expected_root_digest": identity["root_digest"]})
            path.write_text(json.dumps(result, indent=2) + "\n")
            _run_copy(options, command, output, role, REGISTRY_COPY_TIMEOUT_SECONDS)
            if digest_path.read_text().strip() != identity["root_digest"]:
                raise OCIError("published root digest differs from the verified attested index")
            result["images"][role] = {"reference": destination, **{k: identity[k] for k in IDENTITY_KEYS}}
            path.write_text(json.dumps(result, indent=2) + "\n")
        result["outcome"] = "published_verified_original_graphs"
    except Exception as error:
        result["outcome"] = "failed; earlier recorded roles may already be published"
        result["error"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        path.write_text(json.dumps(result, indent=2) + "\n")


def role_path(value: str) -> tuple[str, Path]:
    role, separator, source = value.partition("=")
    if role not in ROLES or not separator or not source:
        raise argparse.ArgumentTypeError("OCI layout must be ROLE=PATH")
    return role, Path(source)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("local-copy", "publish"), default="publish")
    parser.add_argument("--scan-summary", type=Path, required=True)
    parser.add_argument("--oci-layout", type=role_path, action="append", required=True)
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"), required=True)
    parser.add_argument("--registry-owner")
    parser.add_argument("--tag")
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--publish-state-dir", type=Path, required=True)
    parser.add_argument("--skopeo", default="skopeo")
    parser.add_argument("--expected-skopeo-version", required=True)
    parser.add_argument("--authfile", type=Path)
    parser.add_argument("--local-copy-dir", type=Path, required=True)
    parser.add_argument("--local-copy-proof", type=Path)
    parser.add_argument("--guard-script", type=Path)
    parser.add_argument("--storage-path", type=Path, action="append", default=[])
    options = parser.parse_args()
    if len(dict(options.oci_layout)) != len(options.oci_layout):
        parser.error("duplicate OCI layout roles")
    if options.mode == "local-copy":
        local_copy(options)
    else:
        if not all((options.registry_owner, options.tag, options.authfile, options.local_copy_proof)):
            parser.error("publish requires registry-owner, tag, authfile and local-copy-proof")
        publish(options)


if __name__ == "__main__":
    main()
