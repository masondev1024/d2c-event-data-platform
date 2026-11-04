"""Seal local Java artifacts and build them in bounded, isolated phases.

Seal/verify prove payload and recipe identity only. Source helpers own compilation
checks. Final image boot, both target platforms, regressions and scans are separate.

The low-budget build mode requires a new work/output tree outside the repository,
a native Linux platform, the actual host backing-storage path, and explicit
--discard-created-builder-cache. It never prunes images or old/shared caches.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from zipfile import BadZipFile, ZipFile

GIB = 2 ** 30
MINIMUM_FREE_BYTES = 16 * GIB
RESERVE_FREE_BYTES = 6 * GIB
LOCK_PATH = Path(tempfile.gettempdir()) / f"d2c-container-security-{os.getuid()}.lock"
ROLES = ("hadoop", "parquet", "iceberg", "spark", "dependencies")
TARGETS = {role: role + "-artifacts" for role in ROLES}
TARGETS["dependencies"] = "dependency-artifacts"
# Iceberg consumes the Parquet Maven tree. All five are Java artifacts compiled
# once on a native build platform, not five runtime image/platform validations.
BUILD_ORDER = ("hadoop", "parquet", "iceberg", "spark", "dependencies")
REQUIRED = {
    "hadoop": (
        "hadoop-client-api-3.5.0-d2c-local1.jar", "hadoop-client-runtime-3.5.0-d2c-local1.jar",
        "provenance.json", "hadoop-client-api-dependency-tree.json",
        "hadoop-client-runtime-dependency-tree.json", "maven-version.txt",
    ),
    "parquet": (
        "parquet-jackson-1.17.0-d2c-jackson2.21.7.jar",
        "parquet-jackson-1.17.1-d2c-jackson2.21.7.jar", "parquet-source-provenance.json",
        *(f"maven/org/apache/parquet/parquet-jackson/{version}-d2c-jackson2.21.7/"
          f"parquet-jackson-{version}-d2c-jackson2.21.7.{extension}"
          for version in ("1.17.0", "1.17.1") for extension in ("jar", "pom")),
    ),
    "iceberg": (
        "iceberg-spark-runtime-4.2_2.13-1.12.0-d2c-security1.jar",
        "iceberg-source-provenance.json", "security-runtime-artifacts.json",
        "iceberg-rebuild-verification.json", "security-guava-artifacts.json",
    ),
    "spark": (
        "spark-core_2.13-4.2.1-d2c-jetty12.1.14.jar", "spark-source-provenance.json",
        "spark-core-verification.json", "spark-core-build-dependencies.json",
        "spark-network-common_2.13-4.2.1-d2c-guava33.7.2-jre.jar",
        "spark-network-common-verification.json", "spark-network-common-build-dependencies.json",
        "LICENSE.spark-4.2.1", "NOTICE.spark-4.2.1",
    ),
    "dependencies": (
        "org.apache.spark_spark-sql-kafka-0-10_2.13-4.2.1.jar",
        "com.google.guava_guava-33.7.2-jre.jar", "com.google.guava_failureaccess-1.0.3.jar",
        "org.xerial.snappy_snappy-java-1.1.10.9.jar",
        "dependency-source-provenance.json",
    ),
}
PROVENANCE = {
    "hadoop": "provenance.json", "parquet": "parquet-source-provenance.json",
    "iceberg": "iceberg-source-provenance.json", "spark": "spark-source-provenance.json",
    "dependencies": "dependency-source-provenance.json",
}
BUDGET = {
    "admission_gib": 16, "reserve_gib": 6, "estimated_transient_gib": 7.7,
    "estimate_gib": {"base": 2.3, "compressed_inputs": 0.8, "module_dependencies": 3.0,
                     "source_and_outputs": 1.6},
    "admission_margin_gib": 2.3,
    "note": "Per-component estimate, not a measured peak. Guard samples measure actual host free space.",
}


class ArtifactError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ArtifactError(message)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def files_under(root: Path) -> dict[str, Path]:
    require(root.is_dir() and not root.is_symlink(), f"regular directory required: {root}")
    result = {}
    for directory, folders, files in os.walk(root, followlinks=False):
        for name in folders + files:
            path = Path(directory) / name
            require(not path.is_symlink(), f"symlink rejected: {path}")
            mode = path.stat().st_mode
            require(stat.S_ISDIR(mode) or stat.S_ISREG(mode), f"nonregular artifact/source rejected: {path}")
        for name in files:
            path = Path(directory) / name
            result[path.relative_to(root).as_posix()] = path
    return result


def recipe_hashes(source_root: Path) -> dict[str, str]:
    require(source_root.is_dir() and not source_root.is_symlink(), "source-root must be a regular directory")
    paths = ("lakehouse/Dockerfile", "scripts/lakehouse_artifacts.py", "scripts/container_security_guard.py")
    result = {}
    for relative in paths:
        path = source_root / relative
        require(path.is_file() and not path.is_symlink(), f"recipe source absent or symlinked: {relative}")
        parts = Path(relative).parts
        require(all(not source_root.joinpath(*parts[:index]).is_symlink() for index in range(1, len(parts))),
                f"symlinked recipe directory: {relative}")
        result[relative] = sha256(path)
    helpers = source_root / "container-build/lakehouse"
    for relative, path in files_under(helpers).items():
        # Match .dockerignore's generated Python caches. All other recipe files,
        # including README and future extensions, remain part of the identity.
        if "__pycache__" in Path(relative).parts or path.suffix == ".pyc":
            continue
        result["container-build/lakehouse/" + relative] = sha256(path)
    require(len(result) > len(paths), "no component build helpers found")
    return dict(sorted(result.items()))


def recipe_digest(hashes: dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ArtifactError(f"invalid/missing JSON evidence: {path}: {type(exc).__name__}") from exc


def check_jar(path: Path, *, require_class: bool) -> None:
    try:
        with ZipFile(path) as jar:
            require("META-INF/MANIFEST.MF" in jar.namelist(), f"JAR manifest missing: {path}")
            classes = [item for item in jar.infolist() if item.filename.endswith(".class")]
            if require_class:
                require(bool(classes), f"compiled implementation absent: {path}")
                with jar.open(classes[0]) as stream:
                    require(stream.read(4) == b"\xca\xfe\xba\xbe", f"invalid class header: {path}")
    except (BadZipFile, OSError) as exc:
        raise ArtifactError(f"invalid JAR: {path}") from exc


def validate_component(root: Path, role: str, source: dict[str, str] | None = None) -> dict:
    require(role in ROLES, f"unknown component role: {role}")
    files = files_under(root / role)
    for relative in REQUIRED[role] + ("build-status.json",):
        require(relative in files, f"required {role} output missing: {relative}")
    status = read_json(files["build-status.json"])
    require(isinstance(status, dict) and status.get("component") == role
            and type(status.get("command_exit")) is int and status["command_exit"] == 0,
            f"component has no successful command-exit evidence: {role}")
    bound_source = status.get("source_recipe_files_sha256")
    require(isinstance(bound_source, dict) and bool(bound_source)
            and status.get("source_recipe_sha256") == recipe_digest(bound_source),
            f"component has no bound execution recipe: {role}")
    if source is not None:
        require(bound_source == source, f"component was built from a different source recipe: {role}")
    require(status.get("native_source_compile_platform") in ("linux/amd64", "linux/arm64"),
            f"component native compile platform absent: {role}")
    provenance = read_json(files[PROVENANCE[role]])
    require(isinstance(provenance, (dict, list)) and bool(provenance), f"empty component provenance: {role}")
    jars = []
    for relative, path in files.items():
        if relative.endswith(".jar"):
            check_jar(path, require_class=relative in REQUIRED[role])
            jars.append(relative)
    require(bool(jars), f"component contains no JARs: {role}")
    if role == "dependencies":
        for relative in jars:
            require(not re.search(r"(?:^|[._-])(?:hadoop-client-(?:api|runtime)|spark-(?:core|network-common)_2\.13)-", relative),
                    f"upstream Hadoop/Spark implementation leaked into dependency bundle: {relative}")
            for prefix, expected in (("com.google.guava_guava-", "com.google.guava_guava-33.7.2-jre.jar"),
                                     ("com.google.guava_failureaccess-", "com.google.guava_failureaccess-1.0.3.jar"),
                                     ("org.xerial.snappy_snappy-java-", "org.xerial.snappy_snappy-java-1.1.10.9.jar")):
                require(not Path(relative).name.startswith(prefix) or relative == expected,
                        f"unexpected direct runtime dependency version: {relative}")
        require(isinstance(provenance, dict) and provenance.get("spark_version") == "4.2.1"
                and provenance.get("artifact_sha256") == {name: sha256(files[name]) for name in jars},
                "dependency resolver provenance does not match every actual copied JAR")
        required_requests = {"com.google.guava:guava:33.7.2-jre", "com.google.guava:failureaccess:1.0.3",
                             "org.xerial.snappy:snappy-java:1.1.10.9"}
        require(isinstance(provenance.get("requested_packages"), list)
                and all(isinstance(value, str) for value in provenance["requested_packages"])
                and required_requests <= set(provenance["requested_packages"]),
                "patched direct Guava/failureaccess/Snappy requests are absent from resolver provenance")
    if role == "parquet":
        for version in ("1.17.0", "1.17.1"):
            name = f"parquet-jackson-{version}-d2c-jackson2.21.7.jar"
            repository = f"maven/org/apache/parquet/parquet-jackson/{version}-d2c-jackson2.21.7/{name}"
            require(sha256(files[name]) == sha256(files[repository]), "Parquet exported Maven JAR differs from runtime artifact")
    produced = {name: sha256(path) for name, path in sorted(files.items()) if name != "build-status.json"}
    require(status.get("produced_files_sha256") == produced,
            f"component payload changed since its successful helper execution: {role}")
    return {"command_exit": 0, "provenance": role + "/" + PROVENANCE[role], "jars": sorted(jars),
            "source_recipe_sha256": status["source_recipe_sha256"],
            "native_source_compile_platform": status["native_source_compile_platform"]}


def payload_hashes(root: Path) -> dict[str, str]:
    files = files_under(root)
    for name in files:
        require(name == "manifest.json" or name.split("/", 1)[0] in ROLES,
                f"unexpected artifact top-level path: {name}")
    directories = {path.name for path in root.iterdir() if path.is_dir()}
    require(directories == set(ROLES), "artifact root must contain exactly the five component directories")
    return {name: sha256(path) for name, path in sorted(files.items()) if name != "manifest.json"}


def make_manifest(artifacts: Path, source_root: Path, *, execution: dict | None = None) -> dict:
    source = recipe_hashes(source_root)
    components = {role: validate_component(artifacts, role, source) for role in ROLES}
    native_platforms = {record["native_source_compile_platform"] for record in components.values()}
    require(len(native_platforms) == 1, "component artifacts were not compiled on one native platform")
    if execution is not None:
        require(execution.get("native_source_compile_platform") == next(iter(native_platforms)),
                "execution platform does not match component receipts")
    return {
        "schema_version": 1, "kind": "d2c-lakehouse-source-artifacts",
        "payload_sha256": payload_hashes(artifacts), "source_recipe_files_sha256": source,
        "source_recipe_sha256": recipe_digest(source), "components": components,
        "execution": execution or {"mode": "seal-existing-output", "native_source_compile_platform": next(iter(native_platforms))},
        "verification_scope": "Payload/recipe identity and successful component command receipts only; individual helpers own source-to-binary verification.",
        "runtime_platform_validation": {"linux/amd64": "NOT_RUN", "linux/arm64": "NOT_RUN"},
        "D2C_functional_regression": "NOT_RUN", "vulnerability_scan": "NOT_RUN",
    }


def atomic_json(path: Path, value) -> None:
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                     prefix=".d2c-json-", delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
    temporary.replace(path)


def component(args: argparse.Namespace) -> dict:
    """Build-stage wrapper: bind an actual successful helper run to its sources."""
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    require(bool(command), "component helper command required")
    require(not (args.directory / "build-status.json").exists(), "component status must be new")
    source = recipe_hashes(args.source_root)
    started = datetime.now(timezone.utc).isoformat()
    result = subprocess.run(command)
    require(result.returncode == 0, f"{args.component} helper exited {result.returncode}; no success receipt written")
    require(recipe_hashes(args.source_root) == source, "component recipe changed during helper execution")
    require(args.directory.is_dir() and not args.directory.is_symlink(), "component helper did not create its regular output directory")
    aliases = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}
    architecture = aliases.get(platform.machine().lower())
    require(platform.system() == "Linux" and architecture is not None, "native Linux component execution required")
    status = {"component": args.component, "command_exit": result.returncode,
              "method": "actual source helper invocation returned zero; source identity checked before and after",
              "source_recipe_files_sha256": source, "source_recipe_sha256": recipe_digest(source),
              "command_sha256": hashlib.sha256(json.dumps(command).encode()).hexdigest(),
              "produced_files_sha256": {name: sha256(path) for name, path in sorted(files_under(args.directory).items())},
              "native_source_compile_platform": "linux/" + architecture,
              "started_at_utc": started, "finished_at_utc": datetime.now(timezone.utc).isoformat()}
    atomic_json(args.directory / "build-status.json", status)
    return status


def seal(artifacts: Path, source_root: Path, *, execution: dict | None = None) -> dict:
    require(not (artifacts / "manifest.json").exists(), "artifact root is already sealed; verify it instead")
    manifest = make_manifest(artifacts, source_root, execution=execution)
    atomic_json(artifacts / "manifest.json", manifest)
    return manifest


def verify(artifacts: Path, source_root: Path) -> dict:
    manifest_path = artifacts / "manifest.json"
    require(manifest_path.is_file() and not manifest_path.is_symlink(), "regular artifact manifest required")
    manifest = read_json(manifest_path)
    require(isinstance(manifest, dict) and manifest.get("schema_version") == 1
            and manifest.get("kind") == "d2c-lakehouse-source-artifacts", "unsupported artifact manifest")
    actual = make_manifest(artifacts, source_root, execution=manifest.get("execution"))
    require(manifest == actual, "artifact payload, exact file list, recipe or declared verification scope changed")
    return manifest


def run_capture(command: list[str], *, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, text=True, env=env, timeout=30)


def checked_capture(command: list[str], *, env: dict | None = None) -> str:
    result = run_capture(command, env=env)
    require(result.returncode == 0, f"read-only engine command failed: {' '.join(command[:3])}")
    return result.stdout


def engine_snapshot() -> dict[str, list[str]]:
    commands = {
        "images": ["docker", "image", "ls", "--all", "--quiet", "--no-trunc"],
        "containers": ["docker", "container", "ls", "--all", "--quiet", "--no-trunc"],
        "volumes": ["docker", "volume", "ls", "--quiet"],
    }
    return {kind: sorted(set(checked_capture(command).split())) for kind, command in commands.items()}


def require_preserved(before: dict, after: dict) -> None:
    for kind in ("images", "containers", "volumes"):
        require(set(before[kind]) <= set(after[kind]), f"pre-existing {kind} disappeared; stop without further cleanup")


def cleanup_plan(before: dict, phase: dict, saved: dict, summary: dict, container: dict,
                 consumers: dict[str, set[str]], registration: dict) -> dict:
    require(phase.get("created_new_build_state") is True, "reused/uncertain build state cannot be discarded")
    require(phase.get("outcome") == "export_verified" and bool(phase.get("payload_sha256")),
            "component export has not been verified")
    require(summary.get("outcome") == "completed" and summary.get("exit") == 0
            and summary.get("child_exit") == 0 and summary.get("cleanup_errors") == [],
            "failed or uncertain guard outcome; builder cache retained")
    name, token = saved.get("name", ""), saved.get("token", "")
    require(re.fullmatch(r"[a-f0-9]{32}", token) is not None and name == "d2c-security-" + token[:24],
            "private builder identity is invalid")
    require(registration.get("Name") == name and registration.get("Driver") == "docker-container"
            and registration.get("Dynamic", False) is False,
            "private Buildx registration does not match the owned builder")
    nodes = registration.get("Nodes")
    require(isinstance(nodes, list) and len(nodes) == 1, "only a verified single-node builder can be discarded")
    node = nodes[0]
    require(node.get("Name") == name + "0"
            and node.get("DriverOpts", {}).get("env.D2C_SECURITY_OWNER") == token,
            "private builder node ownership does not match")
    require(node.get("Endpoint") in phase.get("engine_endpoint_candidates", []),
            "private builder node points at a different engine endpoint")
    cid = summary.get("builder", {}).get("container_id")
    require(cid and cid == container.get("Id") and cid not in before["containers"],
            "builder container identity is absent, mismatched or pre-existing")
    require(summary["builder"].get("name") == name
            and container.get("Name") == "/buildx_buildkit_" + name + "0", "builder name mismatch")
    require("D2C_SECURITY_OWNER=" + token in container.get("Config", {}).get("Env", []),
            "builder ownership marker mismatch")
    require(container.get("State", {}).get("Running") is False, "builder is not confirmed stopped")
    volumes = []
    for mount in container.get("Mounts", []):
        if mount.get("Type") != "volume":
            continue
        volume = mount.get("Name")
        require(isinstance(volume, str) and volume and volume not in before["volumes"],
                "pre-existing/unknown builder volume; cache retained")
        require(consumers.get(volume) == {cid}, "builder volume has another or uncertain consumer")
        volumes.append(volume)
    require(bool(volumes), "builder cache volume ownership is not established")
    return {"name": name, "container_id": cid, "volumes": sorted(volumes)}


def absent(result: subprocess.CompletedProcess, noun: str, target: str) -> bool:
    pattern = r"(?:Error(?: response from daemon)?:\s*)?No such " + noun + r":?\s+" + re.escape(target) + r"\s*"
    if noun == "volume":
        pattern = "(?:" + pattern + r"|(?:Error response from daemon:\s*)?get " + re.escape(target) + r": no such volume\s*)"
    # Docker inspect normally prints [] on stdout for a missing object. Other
    # mixed warnings/errors are uncertain, never a proof of deletion.
    return (result.returncode != 0 and result.stdout.strip() in ("", "[]")
            and re.fullmatch(pattern, result.stderr.strip(), re.I) is not None)


def discard_created_builder(before: dict, phase: dict, state_dir: Path, guard_dir: Path,
                            artifacts: Path) -> dict:
    # Do not hold this lock while invoking the child guard. Reacquire the same
    # lock only for proof -> exact removal -> post-state, preventing another
    # guard from reactivating a builder between inspection and removal.
    with LOCK_PATH.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ArtifactError("another guard owns the engine lock; cache retained") from exc
        return _discard_locked(before, phase, state_dir, guard_dir, artifacts)


def _discard_locked(before: dict, phase: dict, state_dir: Path, guard_dir: Path,
                    artifacts: Path) -> dict:
    require(engine_endpoints() == phase.get("engine_endpoint_candidates"),
            "Docker endpoint changed before cleanup; cache retained")
    role = phase.get("role")
    validate_component(artifacts, role)
    actual = {role + "/" + name: sha256(path) for name, path in files_under(artifacts / role).items()}
    require(actual == phase.get("payload_sha256"), "verified component changed before cleanup; cache retained")
    saved = read_json(state_dir / "builder.json")
    summary = read_json(guard_dir / "summary.json")
    cid = summary.get("builder", {}).get("container_id")
    require(isinstance(cid, str) and bool(cid), "guard did not identify a builder container")
    inspected = read_json_text(checked_capture(["docker", "container", "inspect", cid]))
    require(isinstance(inspected, list) and len(inspected) == 1, "ambiguous builder inspection")
    container = inspected[0]
    consumers = {}
    for mount in container.get("Mounts", []):
        if mount.get("Type") == "volume" and mount.get("Name"):
            consumers[mount["Name"]] = set(checked_capture([
                "docker", "container", "ls", "--all", "--quiet", "--no-trunc",
                "--filter", "volume=" + mount["Name"],
            ]).split())
    registration_file = state_dir / "buildx" / "instances" / saved["name"]
    require(registration_file.is_file() and not registration_file.is_symlink(),
            "private single-node Buildx registration file is absent/uncertain")
    plan = cleanup_plan(before, phase, saved, summary, container, consumers, read_json(registration_file))
    atomic_json(state_dir / "discard-plan.json", plan)
    env = os.environ.copy()
    env["BUILDX_CONFIG"] = str(state_dir / "buildx")
    result = run_capture(["docker", "buildx", "rm", plan["name"]], env=env)
    require(result.returncode == 0, "exact builder removal failed/uncertain; no retry or other deletion attempted")
    require(absent(run_capture(["docker", "container", "inspect", plan["container_id"]]), "container", plan["container_id"]),
            "builder removal outcome uncertain; container absence not established")
    for volume in plan["volumes"]:
        require(absent(run_capture(["docker", "volume", "inspect", volume]), "volume", volume),
                "builder cache volume still present/uncertain; no direct volume deletion attempted")
    after = engine_snapshot()
    require_preserved(before, after)
    require(plan["container_id"] not in after["containers"] and not set(plan["volumes"]) & set(after["volumes"]),
            "removed builder resources remain in the final engine snapshot")
    return {**plan, "exact_buildx_rm_exit": 0, "pre_existing_resources_preserved": True}


def read_json_text(content: str):
    try:
        return json.loads(content)
    except ValueError as exc:
        raise ArtifactError("invalid engine JSON response") from exc


def free_bytes(path: Path) -> int:
    value = os.statvfs(path)
    return value.f_bavail * value.f_frsize


def wait_for_space(storage: Path) -> list[dict]:
    samples = []
    deadline = time.monotonic() + 60
    while True:
        value = free_bytes(storage)
        samples.append({"time_unix": time.time(), "free_bytes": value})
        require(value >= RESERVE_FREE_BYTES, "host reserve reached after cleanup; stop")
        if value >= MINIMUM_FREE_BYTES or time.monotonic() >= deadline:
            return samples
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def guard_command(source_root: Path, role: str, output: Path, evidence: Path, state: Path,
                  storage: Path, native: str, assembled: Path) -> list[str]:
    command = [sys.executable, str(source_root / "scripts/container_security_guard.py"),
               "--kind", "build", "--minimum-free-gib", "16", "--reserve-free-gib", "6",
               "--timeout-seconds", "3600", "--storage-path", str(storage),
               "--evidence-dir", str(evidence), "--build-state-dir", str(state), "--",
               "docker", "build", "--platform", native, "--file", "lakehouse/Dockerfile",
               "--target", TARGETS[role], "--output", "type=local,dest=" + str(output)]
    if role == "iceberg":
        command += ["--build-context", "d2c_artifacts=" + str(assembled),
                    "--build-arg", "D2C_PARQUET_STAGE=d2c_artifacts"]
    return command + ["."]


def assert_native(native: str) -> None:
    aliases = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}
    actual = checked_capture(["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"])
    parts = actual.strip().split("/")
    require(len(parts) == 2 and parts[0] == "linux" and aliases.get(parts[1]) == native.split("/")[1],
            "requested source-build platform is not the Docker engine's native Linux platform")
    require(aliases.get(platform.machine().lower()) == native.split("/")[1],
            "requested source-build platform is not the host's native architecture")


def engine_endpoints() -> list[str]:
    context = checked_capture(["docker", "context", "show"]).strip()
    require(bool(context), "current Docker context is absent")
    host = checked_capture(["docker", "context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"]).strip()
    declared_host = os.environ.get("DOCKER_HOST")
    # DOCKER_CONTEXT overrides DOCKER_HOST. Reject any remote ambiguity rather
    # than accepting a local-looking environment variable for a remote context.
    require(host.startswith("unix://") and (not declared_host or declared_host.startswith("unix://")),
            "phased host-storage budget requires an unambiguous local Unix-socket Docker engine")
    effective = host if os.environ.get("DOCKER_CONTEXT") else (declared_host or host)
    return sorted({context, effective, host})


def validate_resume(state: dict, source: dict, artifacts: Path, native: str) -> None:
    require(state.get("source_recipe_files_sha256") == source and state.get("artifacts") == str(artifacts)
            and state.get("native_platform") == native, "resume recipe/output/platform changed")
    phases = state.get("phases", [])
    require(isinstance(phases, list) and len(phases) <= len(BUILD_ORDER), "invalid resume phase list")
    expected_entries = {phase.get("role") for phase in phases}
    if (artifacts / "manifest.json").exists():
        expected_entries.add("manifest.json")
    require(set(path.name for path in artifacts.iterdir()) == expected_entries,
            "resume artifact tree has unrecorded partial/extra outputs")
    for index, phase in enumerate(phases):
        require(phase.get("role") == BUILD_ORDER[index] and phase.get("outcome") == "assembled"
                and phase.get("discard", {}).get("pre_existing_resources_preserved") is True,
                "phase dispatch/failure/cleanup outcome is uncertain; inspect evidence, do not automatically retry")
        validate_component(artifacts, phase["role"], source)
        current = {phase["role"] + "/" + name: sha256(path)
                   for name, path in files_under(artifacts / phase["role"]).items()}
        require(current == phase.get("payload_sha256"), "completed phase payload changed")


def build(args: argparse.Namespace) -> dict:
    require(args.discard_created_builder_cache,
            "bounded phased build requires explicit --discard-created-builder-cache; no build/cleanup was started")
    source_root, artifacts, work = args.source_root.resolve(), args.artifacts.resolve(), args.work_dir.resolve()
    require(source_root not in artifacts.parents and source_root not in work.parents
            and artifacts != source_root and work != source_root and artifacts != work,
            "phased work/artifact directories must be distinct and outside the source build context")
    storage = args.storage_path.resolve()
    require(storage.exists(), "actual host backing storage path must already exist")
    source = recipe_hashes(source_root)
    assert_native(args.native_platform)
    endpoints = engine_endpoints()
    state_file = work / "state.json"
    if args.resume:
        state = read_json(state_file)
        validate_resume(state, source, artifacts, args.native_platform)
        require(state.get("engine_endpoint_candidates") == endpoints, "Docker endpoint changed since the recorded build")
        require_preserved(state["before"], engine_snapshot())
        if (artifacts / "manifest.json").exists():
            return verify(artifacts, source_root)
    else:
        require(not work.exists() and not artifacts.exists(), "build work/output directories must both be new")
        work.mkdir(parents=True)
        artifacts.mkdir(parents=True)
        before = engine_snapshot()
        state = {"schema_version": 1, "invocation": uuid.uuid4().hex, "started_at_unix": time.time(),
                 "source_recipe_files_sha256": source, "artifacts": str(artifacts),
                 "native_platform": args.native_platform, "engine_endpoint_candidates": endpoints,
                 "before": before, "budget": BUDGET, "phases": []}
        atomic_json(work / "engine-before.json", before)
        atomic_json(state_file, state)
    for role in BUILD_ORDER[len(state["phases"]):]:
        validate_resume(state, source, artifacts, args.native_platform)
        assert_native(args.native_platform)
        require(engine_endpoints() == endpoints, "Docker endpoint changed before the next component")
        require(recipe_hashes(source_root) == source, "source recipe changed during phased build")
        require(free_bytes(storage) >= MINIMUM_FREE_BYTES, "component admission requires 16 GiB free")
        phase_root = work / (str(len(state["phases"]) + 1) + "-" + role)
        require(not phase_root.exists(), "phase directory already exists; uncertain/reused execution is not retried")
        phase_root.mkdir()
        output, evidence, builder_state = phase_root / "export", phase_root / "guard", phase_root / "builder-state"
        phase = {"role": role, "outcome": "dispatching", "created_new_build_state": True,
                 "start_free_bytes": free_bytes(storage), "guard_evidence": str(evidence),
                 "builder_state": str(builder_state), "export": str(output)}
        phase["engine_endpoint_candidates"] = endpoints
        state["phases"].append(phase)
        atomic_json(state_file, state)
        command = guard_command(source_root, role, output, evidence, builder_state, storage,
                                args.native_platform, artifacts)
        try:
            with (phase_root / "guard-driver.log").open("x") as log:
                result = subprocess.run(command, cwd=source_root, stdout=log, stderr=subprocess.STDOUT)
            phase["guard_exit"] = result.returncode
            summary = read_json(evidence / "summary.json")
            phase["guard_outcome"] = summary.get("outcome")
            phase["minimum_free_bytes"] = summary.get("min_free_bytes")
            if isinstance(summary.get("min_free_bytes"), int):
                phase["observed_host_free_drop_bytes"] = max(0, phase["start_free_bytes"] - summary["min_free_bytes"])
            require(result.returncode == 0 and summary.get("outcome") == "completed"
                    and summary.get("exit") == 0 and summary.get("cleanup_errors") == [],
                    "guard failed or cleanup is uncertain; cache retained and no subsequent phase started")
            require(recipe_hashes(source_root) == source, "recipe changed while component compiled")
            require(set(path.name for path in output.iterdir()) == {role}, "scratch export must contain only its component directory")
            component_record = validate_component(output, role, source)
            require(component_record["native_source_compile_platform"] == args.native_platform,
                    "export was built on a different native platform")
            hashes = {role + "/" + name: sha256(path) for name, path in files_under(output / role).items()}
            # A same-filesystem rename assembles the already-verified payload
            # without a second physical JAR copy. Keep phase logs/receipts intact.
            require(not (artifacts / role).exists(), "assembled component destination already exists")
            require((output / role).stat().st_dev == artifacts.stat().st_dev,
                    "phased export and artifact root must share a filesystem for bounded assembly")
            (output / role).rename(artifacts / role)
            phase["payload_sha256"] = hashes
            phase["outcome"] = "export_verified"
            atomic_json(state_file, state)
            phase["cleanup_free_before_bytes"] = free_bytes(storage)
            phase["discard"] = discard_created_builder(state["before"], phase, builder_state, evidence, artifacts)
            phase["cleanup_space_samples"] = wait_for_space(storage)
            phase["cleanup_free_after_bytes"] = phase["cleanup_space_samples"][-1]["free_bytes"]
            phase["outcome"] = "assembled"
            atomic_json(state_file, state)
        except Exception as exc:
            phase["outcome"] = "blocked_or_uncertain"
            phase["error"] = type(exc).__name__ + ": " + str(exc)
            atomic_json(state_file, state)
            raise
    validate_resume(state, source, artifacts, args.native_platform)
    execution = {"mode": "guarded-phased-source-build", "native_source_compile_platform": args.native_platform,
                 "source_components_compiled_once_on_native_platform": True, "budget": BUDGET,
                 "phase_receipts": state["phases"], "runtime_validation_on_both_architectures_still_required": True}
    result = seal(artifacts, source_root, execution=execution)
    state["outcome"] = "sealed"
    state["manifest_sha256"] = sha256(artifacts / "manifest.json")
    atomic_json(state_file, state)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("seal", "verify", "build"):
        command = subparsers.add_parser(action)
        command.add_argument("--artifacts", type=Path, required=True)
        command.add_argument("--source-root", type=Path, required=True)
        if action == "build":
            command.add_argument("--work-dir", type=Path, required=True)
            command.add_argument("--storage-path", type=Path, required=True)
            command.add_argument("--native-platform", choices=("linux/arm64", "linux/amd64"), required=True)
            command.add_argument("--discard-created-builder-cache", action="store_true")
            command.add_argument("--resume", action="store_true")
    wrapper = subparsers.add_parser("component", help="build-stage-only wrapper for a source helper")
    wrapper.add_argument("--component", choices=ROLES, required=True)
    wrapper.add_argument("--directory", type=Path, required=True)
    wrapper.add_argument("--source-root", type=Path, required=True)
    wrapper.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        if args.action == "component":
            result = component(args)
            print(json.dumps({"action": "component", "component": args.component,
                              "command_exit": result["command_exit"], "source_recipe_sha256": result["source_recipe_sha256"]}))
            return 0
        result = build(args) if args.action == "build" else globals()[args.action](args.artifacts, args.source_root)
        print(json.dumps({"action": args.action, "result": "PASS", "source_recipe_sha256": result["source_recipe_sha256"],
                          "payload_files": len(result["payload_sha256"]),
                          "scope": result["verification_scope"]}, sort_keys=True))
        return 0
    except (ArtifactError, OSError, subprocess.SubprocessError, ValueError) as exc:
        print("Artifact operation blocked: " + str(exc), file=sys.stderr)
        return 70


if __name__ == "__main__":
    raise SystemExit(main())
