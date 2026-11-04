#!/usr/bin/env python3
"""Extract the pinned official Trivy CLI on fresh GitHub-hosted Linux jobs.

The extraction container is created stopped and never started. Only that
owner-matched new container and its proven new anonymous volumes are removed.
The official tool image and all preexisting images/containers/volumes remain.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory
import uuid

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.prepare_ci_storage import PreparationBlocked, validate_host


VERSION = "0.76.1"
IMAGE = "aquasec/trivy"
INDEX = "sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c"
# Official OCI index and platform metadata, independently verified in
# evidence/20261104/trivy-official-oci-manifest.txt. No GitHub release asset,
# remote installer, get.trivy.dev request, or scanner container is used.
PLATFORMS = {
    "x86_64": ("amd64", "sha256:2e6c00d711ab4ae41d8a30c2c1fd3bd3b62704c177bb120f95012e939edfb3df", 62),
    "aarch64": ("arm64", "sha256:fc223b6f033406fd0c5efe59b566295e90cff76ccd2af696bd0d66d53ad83a2d", 183),
}
DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")
CID = re.compile(r"[a-f0-9]{64}\Z")
GIB = 1024 ** 3


class InstallError(RuntimeError):
    """Infrastructure failure; never a successful installation."""


def command(arguments, timeout=30):
    return subprocess.run(arguments, text=True, capture_output=True, timeout=timeout, check=False)


def run_guard(arguments, driver_path=None):
    # The guard owns its deadline and cleanup. An outer subprocess.run timeout
    # would SIGKILL it and bypass that cleanup; forward cancellation instead.
    process, forwarded = None, False
    cancelled = []
    def cancel(signum, _frame):
        nonlocal forwarded
        if not cancelled:
            cancelled.append(signum)
        if process is not None and not forwarded:
            forwarded = True
            if process.poll() is None:
                process.send_signal(signum)
    previous = {number: signal.signal(number, cancel) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        with (driver_path.open("x") if driver_path else open(os.devnull, "w")) as driver:
            process = subprocess.Popen(arguments, stdout=driver, stderr=subprocess.STDOUT, start_new_session=True)
            if cancelled:
                cancel(cancelled[0], None)
            code = process.wait()
        return subprocess.CompletedProcess(arguments, 128 + cancelled[0] if cancelled else code)
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def inspect_container(name):
    result = command(["docker", "container", "inspect", name])
    if result.returncode:
        missing = r"(?:Error response from daemon: |Error: )No such (?:container|object): " + re.escape(name)
        if re.fullmatch(missing, result.stderr.strip()):
            return None
        raise InstallError("extraction container state cannot be established")
    try:
        values = json.loads(result.stdout)
        if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], dict):
            raise ValueError
        return values[0]
    except (ValueError, TypeError) as error:
        raise InstallError("extraction container inspection is invalid") from error


def owned_container(value, name, owner, image_id, container_id=None):
    if (value.get("Name") != "/" + name or value.get("Config", {}).get("Labels", {}).get("d2c.security.owner") != owner
            or value.get("Image") != image_id or not CID.fullmatch(str(value.get("Id", "")))
            or (container_id is not None and value["Id"] != container_id)):
        raise InstallError("extraction container ownership or immutable image identity does not match")


def new_volumes(value, before):
    volumes = []
    for mount in value.get("Mounts", []):
        name = mount.get("Name")
        if mount.get("Type") != "volume" or not isinstance(name, str) or not name or name in before:
            raise InstallError("extraction container has an unproven or preexisting mount")
        volumes.append(name)
    return sorted(volumes)


def cleanup(name, owner, image_id, container_id, before, expected_volumes, receipt):
    value = inspect_container(name)
    if value is None:
        raise InstallError("attempted container creation or owned volume cleanup remains uncertain")
    owned_container(value, name, owner, image_id, container_id)
    volumes = new_volumes(value, before)
    if expected_volumes is not None and volumes != expected_volumes:
        raise InstallError("extraction container mounts changed before cleanup")
    result = command(["docker", "container", "rm", "--force", "--volumes", value["Id"]], timeout=60)
    if result.returncode or inspect_container(name) is not None:
        raise InstallError("owned extraction container removal is unconfirmed")
    result = command(["docker", "volume", "ls", "--quiet"])
    after = set(result.stdout.splitlines())
    if result.returncode or set(volumes) & after or not before.issubset(after):
        raise InstallError("owned anonymous volume cleanup or prior volume preservation is unconfirmed")
    receipt["cleanup"] = {"outcome": "removed_owned_container_and_anonymous_volumes", "container_id": value["Id"],
                          "anonymous_volumes": volumes, "prior_volume_inventory_preserved": True, "errors": []}


def extract_native(args, receipt, image_id, binary):
    name, owner = "d2c-trivy-extract-" + uuid.uuid4().hex, uuid.uuid4().hex
    container_id, expected_volumes, attempted_create = None, None, False
    listing = command(["docker", "volume", "ls", "--quiet"])
    if listing.returncode:
        raise InstallError("preexisting volume inventory is unavailable")
    before = set(listing.stdout.splitlines())
    receipt["prior_volume_inventory"] = {"count": len(before), "sha256": hashlib.sha256("\n".join(sorted(before)).encode()).hexdigest()}
    if inspect_container(name) is not None:
        raise InstallError("new extraction container name is already occupied")
    try:
        if shutil.disk_usage(args.evidence_dir).free < 6 * GIB:
            raise InstallError("free storage fell below the 6 GiB reserve before extraction")
        attempted_create = True
        result = command(["docker", "container", "create", "--name", name, "--label", "d2c.security.owner=" + owner,
                          "--network", "none", "--read-only", "--platform", receipt["platform"],
                          "--entrypoint", "/usr/bin/true", image_id])
        if result.returncode or not CID.fullmatch(result.stdout.strip()):
            raise InstallError("stopped extraction container creation failed")
        container_id = result.stdout.strip()
        value = inspect_container(name)
        if value is None:
            raise InstallError("new stopped extraction container is missing")
        owned_container(value, name, owner, image_id, container_id)
        if value.get("State", {}).get("Running") is not False or value.get("State", {}).get("Status") != "created":
            raise InstallError("extraction container must be newly created and never running")
        expected_volumes = new_volumes(value, before)
        receipt["extraction_container_id"] = container_id
        result = command(["docker", "cp", container_id + ":/usr/local/bin/trivy", str(binary)], timeout=60)
        if result.returncode or binary.is_symlink() or not binary.is_file() or not 20 <= binary.stat().st_size <= 256 * 1024 * 1024:
            raise InstallError("official Trivy binary copy is invalid")
        if shutil.disk_usage(args.evidence_dir).free < 6 * GIB:
            raise InstallError("free storage fell below the 6 GiB reserve during extraction")
    finally:
        if attempted_create:
            deferred = []
            def defer(signum, _frame):
                deferred.append(signum)
                receipt.setdefault("interrupt_signals", []).append(signum)
            previous = {number: signal.signal(number, defer) for number in (signal.SIGINT, signal.SIGTERM)}
            try:
                cleanup(name, owner, image_id, container_id, before, expected_volumes, receipt)
            except (InstallError, OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
                receipt["cleanup"] = {"outcome": "uncertain", "errors": ["owned extraction cleanup could not be confirmed"]}
                raise InstallError("owned extraction cleanup could not be confirmed")
            finally:
                for number, handler in previous.items():
                    signal.signal(number, handler)
            if deferred:
                raise InstallError("installation interrupted during owned cleanup")


def install(args, receipt):
    workspace, temporary = validate_host(dict(os.environ))
    native = platform.machine()
    if native not in PLATFORMS:
        raise InstallError("only native Linux x86_64 and aarch64 runners are supported")
    arch, leaf, elf_machine = PLATFORMS[native]
    destination = args.install_dir.resolve()
    if (args.install_dir.absolute() != destination or args.evidence_dir.absolute() != args.evidence_dir.resolve()
            or not destination.is_relative_to(temporary) or not args.evidence_dir.resolve().is_relative_to(temporary)
            or "\n" in str(destination) or "\r" in str(destination) or destination.exists() or destination.is_symlink()):
        raise InstallError("installation and evidence paths must be fresh paths inside RUNNER_TEMP")
    if args.github_path is not None and (not args.github_path.is_file() or args.github_path.is_symlink()
                                        or not args.github_path.resolve().is_relative_to(temporary)):
        raise InstallError("GitHub PATH command file must be an existing regular file")
    reference = IMAGE + "@" + leaf
    receipt.update({"architecture": native, "platform": "linux/" + arch, "source_index": IMAGE + "@" + INDEX,
                    "source_leaf": reference, "expected_platform_manifest": leaf})
    storage = args.storage_path or [workspace, temporary, Path("/var/lib/docker")]
    pull = [sys.executable, str(args.guard_script), "--kind", "scan", "--minimum-free-gib", "10",
            "--reserve-free-gib", "6", "--timeout-seconds", "300", "--evidence-dir", str(args.evidence_dir / "pull-guard")]
    for path in storage:
        pull.extend(["--storage-path", str(path)])
    pull.extend(["--", "docker", "pull", "--platform", receipt["platform"], reference])
    result = run_guard(pull, args.evidence_dir / "pull-driver.log")
    try:
        guard = json.loads((args.evidence_dir / "pull-guard/summary.json").read_text())
    except (OSError, ValueError) as error:
        raise InstallError("guarded official tool pull evidence is missing or invalid") from error
    if (result.returncode or guard.get("outcome") != "completed" or guard.get("child_exit") != 0
            or guard.get("exit") != 0 or guard.get("abort") is not None or guard.get("cleanup_errors") != []):
        raise InstallError("guarded official tool pull did not complete safely")
    result = command(["docker", "image", "inspect", reference])
    try:
        images = json.loads(result.stdout)
        if result.returncode or not isinstance(images, list) or len(images) != 1:
            raise ValueError
        image = images[0]
        image_id = image.get("Id", "")
        if (not DIGEST.fullmatch(image_id) or image.get("Os") != "linux" or image.get("Architecture") != arch
                or not any(value.rsplit("@", 1)[-1] == leaf for value in image.get("RepoDigests", []))):
            raise ValueError
    except (ValueError, TypeError, AttributeError) as error:
        raise InstallError("official tool image identity, platform or pinned leaf digest does not match") from error
    receipt["actual_image_id"] = image_id
    receipt["image_identity_scope"] = "Docker immutable ID/native platform; RepoDigests retains the exact official pinned OCI leaf"
    with TemporaryDirectory(prefix="binary-", dir=args.evidence_dir) as staging:
        binary = Path(staging) / "trivy"
        extract_native(args, receipt, image_id, binary)
        with binary.open("rb") as stream:
            header = stream.read(20)
        if header[:6] != b"\x7fELF\x02\x01" or len(header) != 20 or int.from_bytes(header[18:20], "little") != elf_machine:
            raise InstallError("official tool binary ELF architecture does not match the runner")
        binary.chmod(0o755)
        result = command([str(binary), "--version", "--format", "json"])
        try:
            version = json.loads(result.stdout)
            if result.returncode or not isinstance(version, dict) or version.get("Version") != VERSION:
                raise ValueError
        except (ValueError, TypeError) as error:
            raise InstallError("official tool binary version does not match pinned 0.76.1") from error
        receipt["binary_sha256"] = sha256(binary)
        destination.mkdir(mode=0o700, parents=True, exist_ok=False)
        shutil.copyfile(binary, destination / "trivy")
        (destination / "trivy").chmod(0o755)
        if sha256(destination / "trivy") != receipt["binary_sha256"]:
            raise InstallError("installed binary checksum changed during copy")
        if args.github_path is not None:
            with args.github_path.open("a") as path_file:
                path_file.write(str(destination) + "\n")
    receipt.update({"outcome": "installed", "install_dir": str(destination), "version": VERSION})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install-dir", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--github-path", type=Path)
    parser.add_argument("--guard-script", type=Path, default=Path(__file__).with_name("container_security_guard.py"))
    parser.add_argument("--storage-path", type=Path, action="append")
    args = parser.parse_args(argv)
    receipt = {"schema_version": 1, "outcome": "failed", "version": VERSION,
               "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        _, temporary = validate_host(dict(os.environ))
        if (args.evidence_dir.absolute() != args.evidence_dir.resolve()
                or not args.evidence_dir.resolve().is_relative_to(temporary)):
            raise InstallError("installer evidence must be inside this job's RUNNER_TEMP")
        args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    except (OSError, InstallError, PreparationBlocked):
        print("Trivy installation failed: require a fresh evidence path on an ephemeral hosted Linux job", file=sys.stderr)
        return 70
    def interrupted(signum, _frame):
        first = not receipt.get("interrupt_signals")
        receipt.setdefault("interrupt_signals", []).append(signum)
        if first:
            raise InstallError("installation interrupted by signal " + str(signum))
    previous = {number: signal.signal(number, interrupted) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        install(args, receipt)
        result = 0
    except (InstallError, PreparationBlocked, OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as error:
        receipt["outcome"] = "failed"
        receipt["error"] = str(error) if isinstance(error, (InstallError, PreparationBlocked)) else type(error).__name__
        print("Trivy installation failed: " + receipt["error"], file=sys.stderr)
        result = 70
    finally:
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        receipt["exit"] = result
        (args.evidence_dir / "summary.json").write_text(json.dumps(receipt, indent=2) + "\n")
        for number, handler in previous.items():
            signal.signal(number, handler)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
