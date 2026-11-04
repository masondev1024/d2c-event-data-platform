"""Serialize resource-heavy verification and stop only this invocation's work.

Builds use a private BuildKit container with one concurrent build vertex. Its
cache is retained: this command never prunes images, volumes or build caches.
Monitor every host filesystem that backs Docker's storage and build outputs.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid

GIB = 2**30
DEFAULTS = {"build": (20, 1800), "scan": (10, 1200), "test": (8, 1200)}
LOCK_PATH = Path(tempfile.gettempdir()) / f"d2c-container-security-{os.getuid()}.lock"


class GuardError(RuntimeError):
    def __init__(self, message: str, code: int = 70):
        super().__init__(message)
        self.code = code


class Interrupted(GuardError):
    pass


def available(path: Path) -> int:
    fs = os.statvfs(path)
    return fs.f_bavail * fs.f_frsize


def nearest_existing(path: Path) -> Path:
    while not path.exists():
        if path == path.parent:
            raise GuardError("cannot locate output filesystem")
        path = path.parent
    return path


def build_arguments(command: list[str], builder: str) -> tuple[list[str], list[Path]]:
    if command[:2] == ["docker", "build"]:
        args = command[2:]
    elif command[:3] == ["docker", "buildx", "build"]:
        args = command[3:]
    else:
        raise GuardError("build kind accepts only docker build or docker buildx build")
    outputs: list[Path] = []
    explicit_output = False
    for index, arg in enumerate(args):
        key = arg.split("=", 1)[0]
        if key in {"--push", "--builder"}:
            raise GuardError("registry push and caller-selected builders are not allowed")
        if key == "--cache-to":
            raise GuardError("cache export requires a separately scoped operation")
        if key in {"--output", "-o"} or arg.startswith("-o"):
            raw = arg[2:].lstrip("=") if arg.startswith("-o") and arg != "-o" else (arg.split("=", 1)[1] if "=" in arg else (args[index + 1] if index + 1 < len(args) else ""))
            fields = dict(item.split("=", 1) for item in raw.split(",") if "=" in item)
            if fields.get("type") not in {"local", "oci"} or not fields.get("dest"):
                raise GuardError("only explicit local or OCI directory exports are allowed")
            if fields.get("push", "false") != "false" or fields.get("type") == "oci" and fields.get("tar") != "false":
                raise GuardError("OCI export must be a local directory, never a registry push")
            destination = Path(fields["dest"]).resolve()
            if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
                raise GuardError("build output must be new or an empty directory")
            outputs.append(destination)
            explicit_output = True
    if explicit_output and "--load" in args:
        raise GuardError("do not combine load with a directory export")
    return ["docker", "buildx", "build", "--builder", builder, *([] if explicit_output or "--load" in args else ["--load"]), *args], outputs


class Guard:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.started = time.monotonic()
        self.deadline = self.started + args.timeout_seconds
        self.out = Path(args.evidence_dir).resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.paths = [Path(p).resolve() for p in args.storage_path] or [Path.cwd().resolve()]
        if any(not p.exists() for p in self.paths):
            raise GuardError("every monitored storage path must exist")
        self.state = {"kind": args.kind, "outcome": "starting", "child_exit": None, "abort": None,
                      "samples": [], "min_free_bytes": None, "reserve_bytes": int(args.reserve_free_gib * GIB),
                      "minimum_bytes": int(args.minimum_free_gib * GIB), "started_at_unix": time.time(),
                      "command_sha256": hashlib.sha256(json.dumps(args.command).encode()).hexdigest(),
                      "cleanup_errors": [], "steps": []}
        self.env = os.environ.copy()
        self.proc: subprocess.Popen | None = None
        self.status_file: Path | None = None
        self.builder_container: str | None = None
        self.builder_token: str | None = None
        self.container_dispatched = False
        self.process_groups: list[int] = []
        self.cleaning = False
        self.write()

    def write(self) -> None:
        (self.out / "summary.json").write_text(json.dumps(self.state, indent=2) + "\n")

    def sample(self, *, admission: bool = False) -> None:
        values = {str(p): available(nearest_existing(p)) for p in self.paths}
        lowest = min(values.values())
        self.state["min_free_bytes"] = min(self.state["min_free_bytes"] or lowest, lowest)
        self.state["samples"].append({"elapsed": round(time.monotonic() - self.started, 3), "free_bytes": values})
        minimum = self.state["minimum_bytes"] if admission else self.state["reserve_bytes"]
        if lowest < minimum:
            raise GuardError("storage admission failed" if admission else "storage reserve reached", 75 if admission else 70)
        if time.monotonic() >= self.deadline:
            raise GuardError("verification timeout", 124)

    def interrupt(self, number, _frame) -> None:
        if not self.cleaning:
            raise Interrupted(f"signal {number}", 128 + number)

    def run(self, label: str, command: list[str]) -> int:
        self.sample()
        step = {"label": label, "exit": None}
        self.state["steps"].append(step)
        # A supervisor remains alive after the actual command exits, anchoring
        # its session/group ID until every descendant is cancelled. Never reap
        # that anchor before signalling its own group.
        self.status_file = self.out / f".{label}-status.json"
        with tempfile.TemporaryDirectory(prefix="d2c-guard-command-") as private:
            payload = Path(private) / "command.json"
            payload.write_text(json.dumps(command))
            payload.chmod(0o600)
            log = (self.out / f"{label}.log").open("xb")
            self.proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--supervise", str(payload), str(self.status_file)], stdout=log, stderr=subprocess.STDOUT,
                                         env=self.env, start_new_session=True)
            self.process_groups.append(self.proc.pid)
            try:
                while True:
                    self.sample()
                    if self.status_file.exists():
                        status = json.loads(self.status_file.read_text())["exit"]
                        if not isinstance(status, int):
                            raise GuardError("supervisor returned an invalid command status")
                        step["exit"] = status
                        return status
                    if self.proc.poll() is not None:
                        raise GuardError("command supervisor exited without a status")
                    time.sleep(2)
            finally:
                self.stop_process()
                log.close()
                self.write()

    def stop_process(self) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
            # The anchor catches TERM; exec'd children retain their normal
            # handlers. Give an active command a bounded grace period first.
            end = time.monotonic() + (0.2 if self.status_file and self.status_file.exists() else 5)
            while time.monotonic() < end and self.proc.poll() is None:
                if self.status_file and self.status_file.exists():
                    break
                time.sleep(0.1)
            if self.proc.poll() is None:
                os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired) as exc:
            self.state["cleanup_errors"].append("own process group: " + type(exc).__name__)

    def check_process_groups(self) -> None:
        # A reaped leader no longer anchors a PID. Do not risk signalling a
        # possibly reused group ID: detect lingering members and fail closed.
        # Docker-specific cleanup has already stopped only owned containers.
        for group in self.process_groups:
            present = False
            for attempt in range(5):
                try:
                    os.killpg(group, 0)
                    present = True
                except ProcessLookupError:
                    present = False
                    break
                except PermissionError:
                    present = True
                    break
                if attempt < 4:
                    time.sleep(0.1)
            if present:
                self.state["cleanup_errors"].append(f"process group {group} remains after leader exit; no unanchored signal sent")

    def docker(self, *args: str, timeout: int = 15) -> subprocess.CompletedProcess:
        return subprocess.run(["docker", *args], env=self.env, capture_output=True, text=True, timeout=timeout)

    def inspect_owned(self, name: str, token: str, *, allow_absent: bool = True) -> dict | None:
        result = self.docker("container", "inspect", name)
        if result.returncode:
            if allow_absent and re.search(r"(?:^|\n)(?:error(?: response from daemon)?:?\s*)?no such container(?:\s|:)", result.stderr, re.IGNORECASE):
                return None
            raise GuardError("cannot establish owned container state")
        data = json.loads(result.stdout)[0]
        labels = data["Config"].get("Labels") or {}
        environment = data["Config"].get("Env") or []
        if labels.get("d2c.security.owner") != token and "D2C_SECURITY_OWNER=" + token not in environment:
            raise GuardError("container ownership mismatch; no stop attempted")
        return data

    def stop_owned(self, name: str, token: str) -> None:
        try:
            data = self.inspect_owned(name, token)
            if data is None or not data["State"]["Running"]:
                return
            iid = data["Id"]
            self.state.setdefault("stopped_owned_container_ids", []).append(iid)
            try:
                result = self.docker("stop", "--time", "10", iid, timeout=25)
                stopped = result.returncode == 0
            except subprocess.TimeoutExpired:
                stopped = False
            if not stopped:
                if self.docker("kill", iid).returncode:
                    raise GuardError("owned container stop and kill failed")
            final = self.inspect_owned(iid, token)
            if final is not None and final["State"]["Running"]:
                raise GuardError("owned container still running")
        except Exception as exc:
            self.state["cleanup_errors"].append("owned container: " + str(exc))

    def prepare_build(self) -> list[str]:
        folder = Path(self.args.build_state_dir).resolve()
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.env["BUILDX_CONFIG"] = str(folder / "buildx")
        state_file = folder / "builder.json"
        if state_file.exists():
            saved = json.loads(state_file.read_text())
            name, token = saved["name"], saved["token"]
            if not re.fullmatch(r"d2c-security-[a-f0-9]{24}", name) or not re.fullmatch(r"[a-f0-9]{32}", token):
                raise GuardError("invalid private builder state")
            if saved.get("configuration_sha256") != hashlib.sha256(b"[worker.oci]\n  max-parallelism = 1\n").hexdigest():
                raise GuardError("private builder configuration changed")
            if hashlib.sha256((folder / "buildkitd.toml").read_bytes()).hexdigest() != saved["configuration_sha256"]:
                raise GuardError("private builder configuration file changed")
            container = "buildx_buildkit_" + name + "0"
            data = self.inspect_owned(container, token, allow_absent=False)
            if data["State"]["Running"]:
                raise GuardError("private builder already running; possible overlapping work")
            self.builder_container, self.builder_token = container, token
        else:
            token = uuid.uuid4().hex
            name = "d2c-security-" + token[:24]
            self.builder_container, self.builder_token = "buildx_buildkit_" + name + "0", token
            config = folder / "buildkitd.toml"
            config.write_text("[worker.oci]\n  max-parallelism = 1\n")
            saved = {"name": name, "token": token, "configuration_sha256": hashlib.sha256(config.read_bytes()).hexdigest()}
            # Save before dispatch so an uncertain bootstrap can be identified.
            state_file.write_text(json.dumps(saved, indent=2) + "\n")
            if self.run("builder-create", ["docker", "buildx", "create", "--name", name,
                    "--driver", "docker-container", "--buildkitd-config", str(config),
                    "--driver-opt", "env.D2C_SECURITY_OWNER=" + token,
                    "--driver-opt", "memory=2500m", "--driver-opt", "cpu-period=100000",
                    "--driver-opt", "cpu-quota=200000", "--driver-opt", "restart-policy=no"]) != 0:
                raise GuardError("private builder creation failed")
        command, outputs = build_arguments(self.args.command, name)
        self.paths.extend(nearest_existing(p) for p in outputs)
        self.sample(admission=True)
        if self.run("builder-bootstrap", ["docker", "buildx", "inspect", "--bootstrap", name]) != 0:
            raise GuardError("private builder bootstrap failed")
        data = self.inspect_owned(self.builder_container, token, allow_absent=False)
        self.state["builder"] = {"name": name, "container_id": data["Id"], "max_parallelism": 1,
                                 "cache": "retained; no prune or volume deletion"}
        return command

    def execute(self) -> int:
        previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        for sig in previous:
            signal.signal(sig, self.interrupt)
        code = 70
        try:
            self.sample(admission=True)
            command = self.prepare_build() if self.args.kind == "build" else self.args.command
            if self.args.container_name:
                probe = self.docker("container", "inspect", self.args.container_name)
                absent = re.search(r"(?:^|\n)(?:error(?: response from daemon)?:?\s*)?no such container(?:\s|:)", probe.stderr, re.IGNORECASE)
                if probe.returncode == 0 or not absent:
                    raise GuardError("container name unavailable or engine state uncertain")
                self.container_dispatched = True
            code = self.run("command", command)
            self.state["child_exit"] = code
            self.state["outcome"] = "completed" if code == 0 else "child_failed"
            if code < 0:
                code = 70
        except GuardError as exc:
            self.state.update(abort=str(exc), outcome="blocked" if exc.code == 75 else "aborted")
            code = exc.code
        except Exception as exc:
            self.state.update(abort=type(exc).__name__ + ": " + str(exc), outcome="aborted")
            code = 70
        finally:
            self.cleaning = True
            self.stop_process()
            if self.args.container_name and self.container_dispatched:
                self.stop_owned(self.args.container_name, self.args.owner_token)
            if self.builder_container and self.builder_token:
                self.stop_owned(self.builder_container, self.builder_token)
            self.check_process_groups()
            if self.state["cleanup_errors"]:
                code = 70
                self.state["outcome"] = "cleanup_uncertain"
            self.state["finished_at_unix"] = time.time()
            self.state["exit"] = code
            self.write()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
        return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=tuple(DEFAULTS), default="scan")
    parser.add_argument("--minimum-free-gib", type=float)
    parser.add_argument("--reserve-free-gib", type=float, default=6)
    parser.add_argument("--timeout-seconds", type=float)
    parser.add_argument("--storage-path", action="append", default=[])
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--build-state-dir", default=str(Path(tempfile.gettempdir()) / f"d2c-security-build-{os.getuid()}"))
    parser.add_argument("--container-name")
    parser.add_argument("--owner-token")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    args.command = args.command[1:] if args.command[:1] == ["--"] else args.command
    args.minimum_free_gib = DEFAULTS[args.kind][0] if args.minimum_free_gib is None else args.minimum_free_gib
    args.timeout_seconds = DEFAULTS[args.kind][1] if args.timeout_seconds is None else args.timeout_seconds
    if not args.command or bool(args.container_name) != bool(args.owner_token):
        parser.error("command required; container-name and owner-token must be supplied together")
    numbers = (args.minimum_free_gib, args.reserve_free_gib, args.timeout_seconds)
    if not all(math.isfinite(v) and v > 0 for v in numbers) or args.minimum_free_gib <= args.reserve_free_gib:
        parser.error("positive finite limits and minimum > reserve are required")
    # The lock covers all stages, bootstrap, command, and bounded cleanup.
    with LOCK_PATH.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Another container-security operation is running.")
            return 75
        try:
            return Guard(args).execute()
        except (GuardError, OSError) as exc:
            print("Verification guard could not start: " + str(exc))
            return 70


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--supervise":
        # Internal only. This process must stay alive to prevent PGID reuse.
        signal.signal(signal.SIGTERM, lambda *_: None)
        signal.signal(signal.SIGINT, lambda *_: None)
        payload, status = map(Path, sys.argv[2:])
        child = subprocess.Popen(json.loads(payload.read_text()))
        result = child.wait()
        temporary = status.with_suffix(".tmp")
        temporary.write_text(json.dumps({"exit": result}))
        temporary.replace(status)
        while True:
            time.sleep(1)
    else:
        raise SystemExit(main())
