"""Report CI capacity; remove unused SDKs only on an explicitly opted-in hosted VM.

This is provisioning code for a newly allocated GitHub-hosted Linux job, not a
cleanup command for developer machines or self-hosted runners. Environment and
filesystem markers prevent accidental use; they are not remote attestation.
No cleanup is enabled by default. A future remote run using the cleanup flag
requires the user's separate approval.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import pwd
import stat
import subprocess
from typing import Any

GIB = 2**30
WORK_ROOT = Path("/home/runner/work")
RUNNER_HOME = Path("/home/runner")
# Deliberately fixed; callers cannot supply additional cleanup paths.
# Official image installation recipes document the first two roots:
# https://github.com/actions/runner-images/blob/main/images/ubuntu/scripts/build/install-android-sdk.sh
# https://github.com/actions/runner-images/blob/main/images/ubuntu/scripts/build/install-dotnetcore-sdk.sh
# /opt/ghc is a legacy candidate requested for this bounded provisioning plan;
# current images may omit it. Missing or symlinked SDK roots are skipped.
SDK_PATHS = (Path("/usr/local/lib/android"), Path("/usr/share/dotnet"), Path("/opt/ghc"))


class PreparationBlocked(RuntimeError):
    pass


def real_directory(path: Path) -> Path:
    if not path.is_absolute():
        raise PreparationBlocked("required directory must be absolute")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        mode = current.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise PreparationBlocked("required directory or ancestor is not a real directory")
    return current.resolve(strict=True)


def validate_host(environment: dict[str, str]) -> tuple[Path, Path]:
    expected = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_OS": "Linux"}
    if platform.system() != "Linux" or any(environment.get(key) != value for key, value in expected.items()):
        raise PreparationBlocked("only a new GitHub-hosted Linux job is supported")
    uid = os.geteuid()
    account = pwd.getpwuid(uid)
    if uid == 0 or uid != os.getuid() or account.pw_name != "runner" or Path(account.pw_dir) != RUNNER_HOME:
        raise PreparationBlocked("the standard unprivileged hosted runner account is required")
    workspace = real_directory(Path(environment.get("GITHUB_WORKSPACE", "")))
    temporary = real_directory(Path(environment.get("RUNNER_TEMP", "")))
    root = real_directory(WORK_ROOT)
    if (not workspace.is_relative_to(root) or workspace == root
            or not temporary.is_relative_to(root) or temporary == root
            or workspace.is_relative_to(temporary) or temporary.is_relative_to(workspace)
            or workspace.stat().st_uid != uid or temporary.stat().st_uid != uid):
        raise PreparationBlocked("workspace and temporary directory do not match a hosted job")
    return workspace, temporary


def available_bytes(path: Path) -> int:
    value = os.statvfs(path)
    return value.f_bavail * value.f_frsize


def mounted_paths() -> set[Path]:
    # A bind mount may have the same st_dev as its parent. Parse mountinfo too;
    # rm --one-file-system alone is insufficient for that case.
    mounts = set()
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if len(fields) < 6:
            raise PreparationBlocked("cannot establish mount topology")
        value = fields[4]
        for escaped, plain in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
            value = value.replace(escaped, plain)
        mounts.add(Path(value))
    return mounts


def sdk_fingerprint(path: Path, runner_uid: int) -> tuple:
    """Bind a fixed SDK root to trusted, non-writable parent directories."""
    if path not in SDK_PATHS:
        raise PreparationBlocked("cleanup target is outside the fixed SDK whitelist")
    paths = [Path(path.anchor)]
    for part in path.parts[1:]:
        paths.append(paths[-1] / part)
    identity = []
    for current in paths:
        value = current.lstat()
        owners = {0, runner_uid} if current == path else {0}
        if (not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode)
                or value.st_uid not in owners or stat.S_IMODE(value.st_mode) & 0o022):
            raise PreparationBlocked("SDK root or ancestor has unsafe type, ownership, or write permissions")
        identity.append((str(current), value.st_dev, value.st_ino, value.st_uid, stat.S_IMODE(value.st_mode)))
    return tuple(identity)


def eligible_sdk(path: Path, workspace: Path, temporary: Path) -> bool:
    if path not in SDK_PATHS:
        raise PreparationBlocked("cleanup target is outside the fixed SDK whitelist")
    try:
        sdk_fingerprint(path, os.geteuid())
    except FileNotFoundError:
        return False
    target = path
    if any(target == protected or target in protected.parents or protected in target.parents
           for protected in (workspace, temporary)):
        raise PreparationBlocked("SDK path intersects job data")
    if any(mount == target or mount.is_relative_to(target) for mount in mounted_paths()):
        raise PreparationBlocked("SDK path contains a mount point")
    return True


def delete_sdk(path: Path, *, allow: bool = False, expected_identity: tuple | None = None) -> int:
    if not allow:
        raise PreparationBlocked("SDK removal requires the explicit opt-in")
    if path not in SDK_PATHS:
        raise PreparationBlocked("cleanup target is outside the fixed SDK whitelist")
    workspace, temporary = validate_host(dict(os.environ))
    if not eligible_sdk(path, workspace, temporary):
        raise PreparationBlocked("SDK disappeared before its removal")
    if expected_identity is None or sdk_fingerprint(path, os.geteuid()) != expected_identity:
        raise PreparationBlocked("SDK identity changed before removal")
    # Exact SDK roots only, no glob, trailing slash, shell, Docker, or toolcache.
    result = subprocess.run(["sudo", "-n", "rm", "--recursive", "--force", "--one-file-system", "--", str(path)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=300)
    return result.returncode


def prepare(options: argparse.Namespace) -> int:
    output = options.evidence_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result: dict[str, Any] = {"schema_version": 1, "started_at": datetime.now(timezone.utc).isoformat(),
        "minimum_free_bytes": int(options.minimum_free_gib * GIB),
        "cleanup_explicitly_enabled": options.allow_ephemeral_sdk_cleanup,
        "allowed_sdk_paths": [str(path) for path in SDK_PATHS], "samples": [], "actions": [],
        "outcome": "blocked", "exit_code": 75,
        "scope": "new ephemeral hosted VM only; never developer or self-hosted data"}

    def save():
        (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")

    try:
        workspace, temporary = validate_host(dict(os.environ))
        if not output.is_relative_to(temporary):
            raise PreparationBlocked("storage evidence must be inside this job's temporary directory")
        paths = [workspace, temporary]
        for path in (Path("/var/lib/docker"), Path("/var/lib/containerd")):
            if path.exists():
                paths.append(real_directory(path))

        def ready() -> bool:
            free = {str(path): available_bytes(path) for path in paths}
            result["samples"].append({"free_bytes": free})
            save()
            return min(free.values()) >= result["minimum_free_bytes"]

        if ready():
            result.update(outcome="ready", exit_code=0)
        elif not options.allow_ephemeral_sdk_cleanup:
            result["reason"] = "insufficient measured capacity; no cleanup was authorized"
        else:
            for sdk in SDK_PATHS:
                # Revalidate every decision immediately before a removal.
                if validate_host(dict(os.environ)) != (workspace, temporary):
                    raise PreparationBlocked("hosted job identity changed")
                try:
                    eligible = eligible_sdk(sdk, workspace, temporary)
                except PreparationBlocked as error:
                    result["actions"].append({"path": str(sdk), "outcome": "skipped", "reason": str(error)})
                    save()
                    continue
                if not eligible:
                    result["actions"].append({"path": str(sdk), "outcome": "absent"})
                    save()
                    continue
                if ready():
                    result.update(outcome="ready", exit_code=0)
                    break
                identity = sdk_fingerprint(sdk, os.geteuid())
                action = {"path": str(sdk), "outcome": "attempting_exact_sdk_removal"}
                result["actions"].append(action)
                save()
                code = delete_sdk(sdk, allow=True, expected_identity=identity)
                action.update(outcome="removed" if code == 0 else "failed", exit_code=code)
                save()
                if code != 0:
                    raise PreparationBlocked("an exact SDK removal did not complete")
                if os.path.lexists(sdk):
                    action["outcome"] = "cleanup_uncertain"
                    raise PreparationBlocked("SDK removal returned without confirming the exact root is absent")
                if ready():
                    result.update(outcome="ready", exit_code=0)
                    break
            if result["exit_code"]:
                result["reason"] = "fixed SDK whitelist did not provide sufficient measured capacity"
    except (PreparationBlocked, OSError, ValueError, subprocess.TimeoutExpired) as error:
        result["reason"] = type(error).__name__ + ": " + str(error)
    finally:
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        save()
    print(json.dumps({key: result[key] for key in ("outcome", "exit_code", "cleanup_explicitly_enabled")}, sort_keys=True))
    return result["exit_code"]


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minimum-free-gib", type=float, default=32)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--allow-ephemeral-sdk-cleanup", action="store_true")
    options = parser.parse_args(arguments)
    if not math.isfinite(options.minimum_free_gib) or options.minimum_free_gib < 32:
        parser.error("at least 32 GiB of measured capacity is required")
    return prepare(options)


if __name__ == "__main__":
    raise SystemExit(main())
