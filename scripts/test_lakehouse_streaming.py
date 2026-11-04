"""Run the real Kafka/checkpoint regression using only fresh, owned Docker state.

Images must already be present. This command never pulls, publishes, prunes or
mounts host data. Each Spark process runs under the container-security guard.
Its host output directory contains limited identity, phase and cleanup evidence.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid


IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
RESOURCE_ID = re.compile(r"[0-9a-f]{64}\Z")
OWNER_LABEL = "d2c.security.owner"
PHASES = ("initial", "resume", "crash", "recover")
PROOF_PREFIX = "D2C_STREAM_PROOF="
GIB = 2**30
BROKER_TMPFS_BYTES = {"/etc/kafka/secrets": 1_048_576, "/var/lib/kafka/data": 67_108_864,
                      "/mnt/shared/config": 4_194_304}
CANCEL_RESERVE_SECONDS = 90
CLEANUP_RESERVE_SECONDS = 60
FINALIZATION_RESERVE_SECONDS = CANCEL_RESERVE_SECONDS + CLEANUP_RESERVE_SECONDS
INIT_SOURCE = r'''
import base64, hashlib, json, os
from pathlib import Path
payload = json.load(__import__('sys').stdin)
source = base64.b64decode(payload['source_b64'], validate=True)
if hashlib.sha256(source).hexdigest() != payload['sha256']:
    raise RuntimeError('phase source checksum mismatch')
root = Path('/state')
if any(root.iterdir()):
    raise RuntimeError('new state volume is unexpectedly nonempty')
for path in (root, root/'warehouse', root/'checkpoint', root/'proofs'):
    path.mkdir(exist_ok=True)
    os.chown(path, 185, 0)
    path.chmod(0o750)
script = root/'lakehouse_kafka_roundtrip.py'
script.write_bytes(source)
os.chown(script, 185, 0)
script.chmod(0o640)
print('D2C_STREAM_INIT=' + json.dumps({'source_sha256': payload['sha256']}))
'''


class StreamingError(RuntimeError):
    pass


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def available(path):
    value = os.statvfs(path)
    return value.f_bavail * value.f_frsize


def absent(kind, result, target=None):
    # A missing socket or an arbitrary daemon error never establishes absence.
    ordinary = re.search(
        rf"(?:^|\n)(?:error(?: response from daemon)?:?\s*)?no such {kind}(?:\s|:)",
        result.stderr, re.IGNORECASE,
    )
    specific = False
    if target is not None:
        name = re.escape(target)
        prefix = r"(?:^|\n)(?:error(?: response from daemon)?:?\s*)?"
        if kind == "network":
            specific = re.search(prefix + rf"network {name} not found(?:\s|$)", result.stderr, re.IGNORECASE)
        elif kind == "volume":
            specific = re.search(prefix + rf"get {name}: no such volume(?:\s|$)", result.stderr, re.IGNORECASE)
    return result.returncode != 0 and bool(ordinary or specific)


class StreamingTest:
    def __init__(self, options):
        self.options = options
        self.output = options.output_dir.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.token = uuid.uuid4().hex
        self.prefix = "d2c-stream-" + self.token[:16]
        self.names = {"network": self.prefix + "-net", "volume": self.prefix + "-state",
                      "broker": self.prefix + "-broker", "init": self.prefix + "-init",
                      "broker_identity": self.prefix + "-broker-identity",
                      **{phase: self.prefix + "-" + phase for phase in PHASES}}
        self.started = time.monotonic()
        self.deadline = self.started + options.total_timeout_seconds
        self.storage = list(dict.fromkeys([self.output, *[path.resolve() for path in options.storage_path]]))
        self.containers = {}
        self.resources = {}
        self.attempted_containers = set()
        self.active_guard = None
        self.interrupted = None
        self.guard_uncertain = False
        self.cleaning = False
        self.cleanup_deadline = None
        self.broker_mount_profile = {}
        self.summary = {
            "schema_version": 1, "started_at": timestamp(), "outcome": "starting",
            "platform": options.platform, "owner_token": self.token,
            "budgets": {"phase_seconds": options.phase_timeout_seconds,
                        "overall_seconds": options.total_timeout_seconds,
                        "guard_cancel_reserve_seconds": CANCEL_RESERVE_SECONDS,
                        "owned_cleanup_reserve_seconds": CLEANUP_RESERVE_SECONDS,
                        "guard_minimum_free_gib": 8, "guard_reserve_free_gib": 6},
            "names": self.names, "resources": self.resources, "phases": {},
            "container_create_attempts": [], "container_ids": self.containers,
            "failures": [], "cleanup": {"errors": [], "removed": [], "preserved": []},
            "limits": "Real Kafka/checkpoint phases only; this is not a vulnerability scan or remote CI result",
        }

    def save(self):
        write_json(self.output / "summary.json", self.summary)

    def budget(self):
        if self.interrupted is not None:
            raise StreamingError(f"interrupted by signal {self.interrupted}")
        remaining = self.deadline - time.monotonic()
        if remaining <= FINALIZATION_RESERVE_SECONDS:
            raise StreamingError("overall execution deadline reached; 150 seconds reserved for guard cancellation and owned cleanup")
        return remaining - FINALIZATION_RESERVE_SECONDS

    def guard_wait(self, requested):
        remaining = self.deadline - time.monotonic() - CLEANUP_RESERVE_SECONDS
        if remaining <= 0:
            raise subprocess.TimeoutExpired("guard cancellation deadline", 0)
        return min(requested, remaining)

    def sample(self, *, admission=False):
        self.budget()
        samples = {str(path): available(path) for path in self.storage}
        if min(samples.values()) < (8 if admission else 6) * GIB:
            raise StreamingError("storage admission failed" if admission else "storage reserve reached")
        self.summary.setdefault("free_space_samples", []).append({"at": timestamp(), "free_bytes": samples})

    def docker(self, *arguments, timeout=20, cleanup=False):
        if cleanup and self.cleanup_deadline is not None:
            remaining = self.cleanup_deadline - time.monotonic()
            if remaining <= 0:
                raise StreamingError("owned cleanup deadline reached; remaining resources preserved")
            timeout = min(timeout, remaining)
        elif not cleanup:
            timeout = min(timeout, self.budget())
        return subprocess.run(["docker", *arguments], capture_output=True, text=True, timeout=timeout)

    def checked(self, *arguments, timeout=20, cleanup=False):
        result = self.docker(*arguments, timeout=timeout, cleanup=cleanup)
        if result.returncode:
            raise StreamingError(f"Docker {' '.join(arguments[:2])} failed; engine state not confirmed")
        return result.stdout

    def inspect(self, kind, target, *, cleanup=False):
        result = self.docker(kind, "inspect", target, cleanup=cleanup)
        if absent(kind, result, target):
            return None
        if result.returncode:
            raise StreamingError(f"cannot establish {kind} identity")
        data = json.loads(result.stdout)
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            raise StreamingError(f"ambiguous {kind} identity")
        return data[0]

    def snapshot(self, *, cleanup=False):
        commands = {
            "images": ("image", "ls", "--quiet", "--no-trunc"),
            "containers": ("container", "ls", "--all", "--quiet", "--no-trunc"),
            "volumes": ("volume", "ls", "--quiet"),
            "networks": ("network", "ls", "--quiet", "--no-trunc"),
        }
        snapshot = {key: sorted(set(self.checked(*command, cleanup=cleanup).split()))
                    for key, command in commands.items()}
        # Docker volumes have names rather than immutable object IDs. Record
        # their creation identity to detect a same-name replacement as well.
        snapshot["volume_identities"] = {}
        for name in snapshot["volumes"]:
            data = self.inspect("volume", name, cleanup=cleanup)
            if data is None:
                raise StreamingError("volume disappeared while capturing original inventory")
            snapshot["volume_identities"][name] = {key: data.get(key) for key in ("Name", "CreatedAt", "Driver", "Scope")}
        return snapshot

    def image(self, reference, role, *, immutable=False):
        if immutable and not IMAGE_ID.fullmatch(reference):
            raise StreamingError("--image must be a complete immutable sha256 image ID")
        data = self.inspect("image", reference)
        if data is None or not IMAGE_ID.fullmatch(data.get("Id", "")):
            raise StreamingError(f"{role} image is not present as one immutable local image")
        if immutable and data["Id"] != reference:
            raise StreamingError("final image inspection did not match the requested immutable ID")
        if data.get("Os", "") + "/" + data.get("Architecture", "") != self.options.platform:
            raise StreamingError(f"{role} image platform does not match the requested platform")
        config = data.get("Config") or {}
        declared = set(config.get("Volumes") or {})
        if role == "broker":
            if declared != set(BROKER_TMPFS_BYTES) or config.get("User") != "appuser":
                raise StreamingError("broker image user/declared volumes differ from the official 3.7.0 profile")
        elif declared:
            raise StreamingError(f"{role} image declares implicit anonymous volumes; no container was created")
        self.summary.setdefault("images", {})[role] = {
            key: data.get(key) for key in ("Id", "Os", "Architecture", "RepoDigests", "Created")
        }
        self.summary["images"][role]["declared_volumes"] = sorted(declared)
        return data["Id"]

    def require_fresh_names(self):
        for key, name in self.names.items():
            kind = "network" if key == "network" else "volume" if key == "volume" else "container"
            if self.inspect(kind, name) is not None:
                raise StreamingError(f"resource name collision: {name}")

    def capture_resource(self, kind, *, cleanup=False):
        data = self.inspect(kind, self.names[kind], cleanup=cleanup)
        if data is None or (data.get("Labels") or {}).get(OWNER_LABEL) != self.token:
            raise StreamingError(f"new {kind} ownership not confirmed")
        if kind == "network":
            identity = data.get("Id", "")
            if not RESOURCE_ID.fullmatch(identity) or identity in self.summary["before"]["networks"]:
                raise StreamingError("new network identity not confirmed")
            self.resources[kind] = {"id": identity, "name": data["Name"]}
        else:
            if data.get("Name") in self.summary["before"]["volumes"] or not data.get("CreatedAt"):
                raise StreamingError("new volume identity not confirmed")
            self.resources[kind] = {"name": data["Name"], "created_at": data["CreatedAt"], "driver": data["Driver"]}
        self.save()

    def create_resources(self):
        for kind, arguments in (
            ("network", ("network", "create", "--driver", "bridge", "--internal")),
            ("volume", ("volume", "create")),
        ):
            self.sample(admission=True)
            # Record dispatch before mutation so an uncertain CLI result can be
            # reconciled using this fresh name and unguessable owner token.
            self.summary.setdefault("resource_create_attempts", []).append(kind)
            self.save()
            result = self.docker(*arguments, "--label", OWNER_LABEL + "=" + self.token, self.names[kind])
            self.capture_resource(kind)
            if result.returncode:
                raise StreamingError(f"{kind} create did not report success")

    def guard_command(self, label, name, command, timeout):
        folder = self.output / "guard" / label
        arguments = [sys.executable, str(self.options.guard_script.resolve()), "--kind", "test",
                     "--minimum-free-gib", "8", "--reserve-free-gib", "6",
                     "--timeout-seconds", str(timeout), "--evidence-dir", str(folder),
                     "--container-name", name, "--owner-token", self.token]
        for path in self.storage:
            arguments += ["--storage-path", str(path)]
        return arguments + ["--", *command], folder

    def interrupt(self, number, _frame):
        self.interrupted = number
        if self.active_guard is not None and self.active_guard.poll() is None:
            # The guard must remain alive to cancel its own anchored process
            # group and stop its exact labelled phase container.
            try:
                self.active_guard.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass

    def guarded(self, label, name, command, *, timeout, stdin=None):
        self.sample(admission=True)
        timeout = min(timeout, self.budget())
        arguments, folder = self.guard_command(label, name, command, timeout)
        self.attempt_container(name)
        self.save()
        process = subprocess.Popen(arguments, stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        self.active_guard = process
        try:
            self.summary.setdefault("guard_invocations", {})[label] = {
                "pid": process.pid, "container_name": name, "timeout_seconds": timeout,
                "evidence_directory": str(folder.relative_to(self.output)),
            }
            self.save()
            try:
                stdout, stderr = process.communicate(input=stdin, timeout=self.guard_wait(timeout + 45))
            except subprocess.TimeoutExpired:
                process.send_signal(signal.SIGTERM)
                try:
                    stdout, stderr = process.communicate(timeout=self.guard_wait(45))
                except subprocess.TimeoutExpired as error:
                    self.guard_uncertain = True
                    raise StreamingError("guard cancellation uncertain; owned Docker state preserved") from error
            guard = json.loads((folder / "summary.json").read_text())
            log = folder / "command.log"
            if (process.returncode != 0 or guard.get("outcome") != "completed"
                    or guard.get("child_exit") != 0 or guard.get("abort") is not None
                    or guard.get("cleanup_errors") or not log.is_file()):
                raise StreamingError(f"{label} did not complete under its resource guard")
            result = log.read_text()
            (self.output / (label + ".log")).write_text(result + stdout + stderr)
            self.budget()
            return result
        except BaseException:
            if process.poll() is None and not self.guard_uncertain:
                process.send_signal(signal.SIGTERM)
                try:
                    process.communicate(timeout=self.guard_wait(45))
                except (OSError, subprocess.TimeoutExpired):
                    self.guard_uncertain = True
            raise
        finally:
            self.active_guard = None

    def attempt_container(self, name):
        self.attempted_containers.add(name)
        if name not in self.summary["container_create_attempts"]:
            self.summary["container_create_attempts"].append(name)
        self.save()

    def container_options(self, name, *, memory, cpus, network):
        return ["docker", "run", "--rm", "--pull", "never", "--name", name,
                "--platform", self.options.platform, "--label", OWNER_LABEL + "=" + self.token,
                "--memory", memory, "--memory-swap", memory, "--cpus", cpus,
                "--pids-limit", "192", "--network", network,
                "--log-driver", "json-file",
                "--log-opt", "max-size=5m", "--log-opt", "max-file=2"]

    def initialize(self, image_id):
        source = self.options.phase_script.read_bytes()
        payload = {"source_b64": base64.b64encode(source).decode(), "sha256": hashlib.sha256(source).hexdigest()}
        command = self.container_options(self.names["init"], memory="128m", cpus="1", network="none")
        command += ["--interactive", "--user", "0:0", "--mount", f"type=volume,src={self.names['volume']},dst=/state",
                    "--entrypoint", "python3", image_id, "-c", INIT_SOURCE]
        output = self.guarded("init", self.names["init"], command, timeout=60, stdin=json.dumps(payload))
        matches = [json.loads(line.removeprefix("D2C_STREAM_INIT=")) for line in output.splitlines()
                   if line.startswith("D2C_STREAM_INIT=")]
        if matches != [{"source_sha256": payload["sha256"]}]:
            raise StreamingError("fresh volume initialization proof is missing or ambiguous")
        self.summary["phase_source_sha256"] = payload["sha256"]

    def broker_tmpfs(self, uid, gid):
        return {path: f"rw,nosuid,nodev,noexec,size={size},mode=0700,uid={uid},gid={gid}"
                for path, size in BROKER_TMPFS_BYTES.items()}

    def add_broker_tmpfs(self, command, profile):
        for path, options in profile.items():
            command += ["--tmpfs", path + ":" + options]

    def verify_broker_identity(self, image_id):
        # Run as the image's configured appuser. No UID/GID is inferred from an
        # unpinned base image's adduser defaults, and every VOLUME is covered.
        command = self.container_options(self.names["broker_identity"], memory="128m", cpus="1", network="none")
        self.add_broker_tmpfs(command, self.broker_tmpfs(0, 0))
        command += ["--entrypoint", "/bin/sh", image_id, "-c",
                    "printf 'D2C_BROKER_IDENTITY=%s:%s\\n' \"$(id -u)\" \"$(id -g)\""]
        result = self.guarded("broker-identity", self.names["broker_identity"], command, timeout=60)
        identities = re.findall(r"^D2C_BROKER_IDENTITY=([0-9]+):([0-9]+)$", result, re.MULTILINE)
        if len(identities) != 1:
            raise StreamingError("broker runtime UID/GID proof is missing or ambiguous")
        uid, gid = map(int, identities[0])
        if uid <= 0 or gid <= 0:
            raise StreamingError("broker must use its non-root image identity")
        self.broker_mount_profile = self.broker_tmpfs(uid, gid)
        self.summary["broker_identity"] = {"uid": uid, "gid": gid,
            "source": "https://github.com/apache/kafka/blob/3.7.0/docker/jvm/Dockerfile",
            "source_blob": "df552ad019abd77650678c9de3f42cd7d7f3bd23",
            "temporary_mounts": self.broker_mount_profile}
        self.save()

    def verify_broker_mounts(self, data):
        mounts = data.get("Mounts")
        if not isinstance(mounts, list):
            raise StreamingError("broker mount inventory differs from its dedicated temporary profile")
        by_path = {mount.get("Destination"): mount for mount in mounts}
        # Legacy --tmpfs is represented by HostConfig.Tmpfs, not necessarily
        # Mounts (Moby container.GetMountPoints). The kernel is checked below.
        if len(by_path) != len(mounts) or not set(by_path) <= set(BROKER_TMPFS_BYTES):
            raise StreamingError("broker has an unexpected mount destination")
        for mount in mounts:
            if (mount.get("Type") != "tmpfs" or mount.get("Source", "") not in ("", "tmpfs")
                    or mount.get("Name") or mount.get("RW") is not True):
                raise StreamingError("broker mount is not a fresh writable tmpfs; no phase may run")
        if (data.get("HostConfig") or {}).get("Tmpfs") != self.broker_mount_profile:
            raise StreamingError("broker tmpfs ownership, privacy or size limits changed")

    def verify_broker_kernel_mounts(self, identity):
        script = """awk '$5=="/etc/kafka/secrets" || $5=="/var/lib/kafka/data" || $5=="/mnt/shared/config" {for(i=7;i<=NF;i++) if($i=="-"){print "D2C_BROKER_MOUNT=" $5 ":" $(i+1) ":" $(i+2); break}}' /proc/self/mountinfo
stat -c 'D2C_BROKER_MODE=%n:%u:%g:%a' /etc/kafka/secrets /var/lib/kafka/data /mnt/shared/config"""
        raw = self.checked("exec", identity, "/bin/sh", "-c", script, timeout=10)
        mounts, permissions = {}, {}
        for line in raw.splitlines():
            if line.startswith("D2C_BROKER_MOUNT="):
                path, filesystem, source = line.removeprefix("D2C_BROKER_MOUNT=").split(":")
                if path in mounts:
                    raise StreamingError("duplicate broker kernel mount identity")
                mounts[path] = {"filesystem": filesystem, "source": source}
            elif line.startswith("D2C_BROKER_MODE="):
                path, uid, gid, mode = line.removeprefix("D2C_BROKER_MODE=").split(":")
                if path in permissions:
                    raise StreamingError("duplicate broker directory identity")
                permissions[path] = {"uid": int(uid), "gid": int(gid), "mode": mode}
        expected = self.summary["broker_identity"]
        if set(mounts) != set(BROKER_TMPFS_BYTES) or set(permissions) != set(BROKER_TMPFS_BYTES):
            raise StreamingError("broker kernel temporary mounts are missing or unexpected")
        for path in BROKER_TMPFS_BYTES:
            if mounts[path] != {"filesystem": "tmpfs", "source": "tmpfs"} or permissions[path] != {
                    "uid": expected["uid"], "gid": expected["gid"], "mode": "700"}:
                raise StreamingError("broker kernel mount type/source or private ownership differs")
        self.summary["broker_kernel_mounts"] = {path: {**mounts[path], **permissions[path]} for path in mounts}
        self.save()

    def start_broker(self, image_id):
        self.sample(admission=True)
        if not self.broker_mount_profile:
            raise StreamingError("broker UID/GID verification must precede launch")
        command = self.container_options(self.names["broker"], memory="512m", cpus="1", network=self.names["network"])
        self.add_broker_tmpfs(command, self.broker_mount_profile)
        command += ["--detach", "--network-alias", "broker"]
        environment = {
            "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@broker:9093",
            "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://broker:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER", "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_NUM_PARTITIONS": "1", "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
            # The official 3.7.0 Dockerfile assigns this data directory to
            # appuser. The exact declared VOLUME is explicitly private tmpfs.
            # https://github.com/apache/kafka/blob/3.7.0/docker/jvm/Dockerfile
            "KAFKA_LOG_DIRS": "/var/lib/kafka/data/d2c-" + self.token, "KAFKA_HEAP_OPTS": "-Xms128m -Xmx256m",
            "CLUSTER_ID": base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="),
        }
        for key, value in environment.items():
            command += ["--env", key + "=" + value]
        command += [image_id]
        self.attempt_container(self.names["broker"])
        result = self.docker(*command[1:])
        data = self.owned_container(self.names["broker"])
        if data is None:
            raise StreamingError("broker launch identity was not retained")
        self.verify_broker_mounts(data)
        self.verify_broker_kernel_mounts(data["Id"])
        if result.returncode:
            raise StreamingError("broker launch did not report success")
        end = time.monotonic() + min(90, self.budget())
        while time.monotonic() < end:
            self.sample()
            data = self.owned_container(self.names["broker"])
            if data is None or not data["State"]["Running"]:
                raise StreamingError("fresh broker stopped before readiness")
            self.verify_broker_mounts(data)
            logs = self.docker("logs", "--tail", "80", data["Id"])
            if logs.returncode:
                raise StreamingError("cannot read the owned broker readiness log")
            if re.search(r"Kafka (?:Server|server) started", logs.stdout + logs.stderr):
                self.summary["broker_readiness"] = "own broker startup log; producer retries and single-partition checks run inside each phase"
                return
            time.sleep(2)
        raise StreamingError("fresh broker did not report readiness within 90 seconds")

    def phase(self, phase, image_id):
        name = self.names[phase]
        timeout = min(self.options.phase_timeout_seconds, self.budget())
        if timeout < 50:
            raise StreamingError("insufficient phase budget; no phase container dispatched")
        query_timeout = min(180, int(timeout - 40))
        command = self.container_options(name, memory="2000m", cpus="2", network=self.names["network"])
        command += ["--user", "185:0", "--mount", f"type=volume,src={self.names['volume']},dst=/state",
                    "--env", "D2C_SECURITY_IMAGE_ID=" + image_id, "--env", "D2C_SECURITY_OWNER=" + self.token,
                    "--entrypoint", "/opt/spark/bin/spark-submit", image_id,
                    "--master", "local[1]", "--driver-memory", "512m", "--conf", "spark.driver.bindAddress=0.0.0.0",
                    "/state/lakehouse_kafka_roundtrip.py", "--phase", phase,
                    "--bootstrap-server", "broker:9092", "--state-dir", "/state",
                    "--query-timeout-seconds", str(query_timeout)]
        output = self.guarded(phase, name, command, timeout=timeout)
        proofs = [json.loads(line[len(PROOF_PREFIX):]) for line in output.splitlines() if line.startswith(PROOF_PREFIX)]
        if (len(proofs) != 1 or proofs[0].get("phase") != phase or proofs[0].get("status") != "PASS"
                or proofs[0].get("image_id") != image_id or proofs[0].get("owner") != self.token):
            raise StreamingError(f"{phase} real-image proof is missing, ambiguous or mismatched")
        self.summary["phases"][phase] = proofs[0]
        write_json(self.output / (phase + ".json"), proofs[0])
        self.save()

    def owned_container(self, name, *, cleanup=False):
        data = self.inspect("container", name, cleanup=cleanup)
        if data is None:
            return None
        labels = (data.get("Config") or {}).get("Labels") or {}
        identity = data.get("Id", "")
        if (name not in self.attempted_containers or not RESOURCE_ID.fullmatch(identity)
                or identity in self.summary.get("before", {}).get("containers", [])
                or labels.get(OWNER_LABEL) != self.token or data.get("Name") != "/" + name):
            raise StreamingError(f"container ownership mismatch: {name}; no stop/remove permitted")
        prior = self.containers.setdefault(name, identity)
        if prior != identity:
            raise StreamingError(f"container identity changed: {name}")
        return data

    def cleanup_container(self, name):
        data = self.owned_container(name, cleanup=True)
        if data is None:
            return
        identity = data["Id"]
        if name == self.names["broker"]:
            logs = self.docker("logs", "--tail", "120", identity, cleanup=True)
            if logs.returncode == 0:
                (self.output / "broker.log").write_text(logs.stdout + logs.stderr)
        if data["State"]["Running"]:
            result = self.docker("stop", "--time", "10", identity, timeout=25, cleanup=True)
            data = self.owned_container(name, cleanup=True)
            if data is not None and data["State"]["Running"]:
                if result.returncode == 0:
                    raise StreamingError("owned container reports running after successful stop")
                self.checked("kill", identity, cleanup=True)
        data = self.owned_container(name, cleanup=True)
        if data is not None:
            if data["State"]["Running"]:
                raise StreamingError("owned container remains running")
            self.checked("container", "rm", identity, cleanup=True)
        if self.owned_container(name, cleanup=True) is not None:
            raise StreamingError("container removal not confirmed")
        self.summary["cleanup"]["removed"].append({"kind": "container", "id": identity})

    def cleanup_resource(self, kind):
        record = self.resources.get(kind)
        if record is None:
            if kind not in self.summary.get("resource_create_attempts", []):
                return
            # A timed-out create may have completed. Do not infer identity or
            # remove anything unless the original snapshot and label prove it.
            if self.inspect(kind, self.names[kind], cleanup=True) is None:
                return
            self.capture_resource(kind, cleanup=True)
            record = self.resources[kind]
        target = record["id"] if kind == "network" else record["name"]
        data = self.inspect(kind, target, cleanup=True)
        if data is None:
            return
        if (data.get("Labels") or {}).get(OWNER_LABEL) != self.token:
            raise StreamingError(f"{kind} ownership changed; resource preserved")
        if kind == "network":
            if data.get("Id") != record["id"] or data.get("Name") != record["name"] or data.get("Containers"):
                raise StreamingError("network identity or references changed; resource preserved")
        elif (data.get("Name") != record["name"] or data.get("CreatedAt") != record["created_at"]
              or data.get("Driver") != record["driver"]):
            raise StreamingError("volume identity changed; resource preserved")
        references = self.checked("container", "ls", "--all", "--quiet", "--filter", kind + "=" + target, cleanup=True)
        if references.strip():
            raise StreamingError(f"{kind} still referenced; resource preserved")
        self.checked(kind, "rm", target, cleanup=True)
        if self.inspect(kind, target, cleanup=True) is not None:
            raise StreamingError(f"{kind} removal not confirmed")
        self.summary["cleanup"]["removed"].append({"kind": kind, **record})

    def cleanup(self):
        self.cleaning = True
        self.cleanup_deadline = min(self.deadline, time.monotonic() + CLEANUP_RESERVE_SECONDS)
        if self.guard_uncertain:
            self.summary["cleanup"]["errors"].append("guard remains uncertain; all owned Docker resources preserved")
            return
        for name in sorted(self.attempted_containers):
            try:
                self.cleanup_container(name)
            except Exception as error:
                self.summary["cleanup"]["errors"].append(str(error))
                self.summary["cleanup"]["preserved"].append({"kind": "container", "name": name})
        for kind in ("volume", "network"):
            try:
                self.cleanup_resource(kind)
            except Exception as error:
                self.summary["cleanup"]["errors"].append(str(error))
                self.summary["cleanup"]["preserved"].append({"kind": kind, "name": self.names[kind]})

    def run(self):
        previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        for number in previous:
            signal.signal(number, self.interrupt)
        try:
            self.save()
            self.sample(admission=True)
            self.summary["before"] = self.snapshot()
            image_id = self.image(self.options.image, "lakehouse", immutable=True)
            broker_id = self.image(self.options.broker_image, "broker")
            self.require_fresh_names()
            self.create_resources()
            self.initialize(image_id)
            self.verify_broker_identity(broker_id)
            self.start_broker(broker_id)
            for phase in PHASES:
                self.phase(phase, image_id)
            self.summary["outcome"] = "phases_passed"
        except Exception as error:
            self.summary["failures"].append(type(error).__name__ + ": " + str(error))
            self.summary["outcome"] = "failed"
        finally:
            self.cleanup()
            if "before" in self.summary:
                try:
                    after = self.snapshot(cleanup=True)
                    self.summary["after"] = after
                    missing = {key: sorted(set(values) - set(after[key])) for key, values in self.summary["before"].items()
                               if key != "volume_identities"}
                    self.summary["original_resources_missing_afterward"] = missing
                    replaced = sorted(name for name, identity in self.summary["before"]["volume_identities"].items()
                                      if after["volume_identities"].get(name) != identity)
                    self.summary["original_volume_identities_changed_afterward"] = replaced
                    if any(missing.values()) or replaced:
                        self.summary["failures"].append("original Docker inventory changed during the test; cause is not inferred")
                except Exception as error:
                    self.summary["cleanup"]["errors"].append("final snapshot uncertain: " + str(error))
            success = (len(self.summary["phases"]) == len(PHASES) and not self.summary["failures"]
                       and not self.summary["cleanup"]["errors"])
            self.summary["outcome"] = "PASS" if success else "cleanup_uncertain" if self.summary["cleanup"]["errors"] else "FAIL"
            self.summary["finished_at"] = timestamp()
            self.summary["elapsed_seconds"] = round(time.monotonic() - self.started, 3)
            self.save()
            for number, handler in previous.items():
                signal.signal(number, handler)
        return 0 if self.summary["outcome"] == "PASS" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--broker-image", required=True)
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--guard-script", type=Path, required=True)
    parser.add_argument("--storage-path", type=Path, action="append", default=[])
    parser.add_argument("--phase-script", type=Path, default=Path(__file__).resolve().parents[1] / "tests/integration/lakehouse_kafka_roundtrip.py")
    parser.add_argument("--phase-timeout-seconds", type=float, default=300)
    parser.add_argument("--total-timeout-seconds", type=float, default=1200)
    options = parser.parse_args(argv)
    for value in (options.phase_timeout_seconds, options.total_timeout_seconds):
        if not math.isfinite(value) or value < 50 or value > 1200:
            parser.error("finite timeouts must be at least 50 and at most 1200 seconds")
    if not IMAGE_ID.fullmatch(options.image):
        parser.error("--image must be a full immutable sha256 image ID")
    if not options.guard_script.is_file() or not options.phase_script.is_file():
        parser.error("guard and phase source files must exist")
    if any(not path.exists() for path in options.storage_path):
        parser.error("all monitored storage paths must exist")
    try:
        return StreamingTest(options).run()
    except (OSError, StreamingError) as error:
        print("Streaming verification did not start: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
