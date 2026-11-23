"""Run an isolated API/libpq regression against preloaded immutable images.

No image is pulled or built here. A fresh PostgreSQL container uses bounded tmpfs;
credentials remain in memory and travel to the API probe through stdin.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.test_lakehouse_streaming import IMAGE_ID, OWNER_LABEL, StreamingError, StreamingTest, timestamp, write_json


PREFIX = "D2C_PYTHON_RUNTIME_PROOF="
PGDATA = "/var/lib/postgresql/data"
PG_TMPFS = "rw,nosuid,nodev,size=128m,mode=0700"

# The resource guard captures its command's output. This relay deliberately
# discards Docker stderr and emits only this fixed proof schema, never a raw
# child log, connection configuration, credential or unrecognized payload.
RELAY_SOURCE = r'''
import json, re, subprocess, sys
PREFIX = 'D2C_PYTHON_RUNTIME_PROOF='
def text(value, maximum=256):
    if not isinstance(value, str) or not 0 < len(value) <= maximum or any(ord(c) < 32 for c in value):
        raise ValueError('unexpected scalar')
    return value
def sanitize(raw):
    if raw.get('status') != 'PASS' or raw.get('role') != 'api' or raw.get('mode') != 'database':
        raise ValueError('database probe failed')
    native = raw['checks']['native_psycopg']
    database = raw['checks']['database_roundtrip']
    version = native['libpq_version']
    if native['implementation'] != 'c' or not isinstance(version, int) or isinstance(version, bool):
        raise ValueError('unexpected native implementation')
    if not (170010 <= version < 180000 or 180004 <= version < 190000):
        raise ValueError('unexpected libpq version')
    if native['libpq_dependency_tls_method'] is not True or native['binary_wheel_distribution_absent'] is not True:
        raise ValueError('native system library proof failed')
    expected = {'server_version_num':160014, 'committed_rows':1, 'rolled_back_rows':0,
                'unicode_jsonb_roundtrip':True, 'parameterized_statements':True, 'fixture_sslmode':'disable'}
    if any(database.get(key) != value or type(database.get(key)) is not type(value) for key,value in expected.items()):
        raise ValueError('database counts differ')
    imports = raw['checks']['imports']
    psycopg_version = text(imports.get('psycopg'), 64)
    c_version = text(imports.get('psycopg-c'), 64)
    version_pattern = r'v?(?:[0-9]+!)?[0-9]+(?:[.][0-9]+)*(?:(?:a|b|rc)[0-9]+)?(?:[.]post[0-9]+)?(?:[.]dev[0-9]+)?(?:[+][A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?'
    if psycopg_version != c_version or not re.fullmatch(version_pattern, psycopg_version, re.IGNORECASE):
        raise ValueError('same-version packaging proof failed')
    paths = {}
    for library in ('libpq', 'libssl', 'libcrypto'):
        values = native['system_library_mappings'][library]
        if not isinstance(values, list) or not 1 <= len(values) <= 16:
            raise ValueError('system mappings missing')
        values = [text(path, 2048) for path in values]
        if any(not path.startswith(('/usr/lib/', '/lib/')) or '.libs/' in path for path in values):
            raise ValueError('unexpected private library mapping')
        paths[library] = values
    if raw['uid'] != 10001 or raw['python_implementation'] != 'cpython' or raw['virtual_environment'] != '/opt/venv':
        raise ValueError('runtime identity mismatch')
    python = text(raw['python'])
    if not re.fullmatch(r'3\.12\.\d+[A-Za-z0-9.+-]*', python):
        raise ValueError('runtime ABI mismatch')
    tls = text(native['libpq_dependency_openssl_version'])
    if not tls.startswith('OpenSSL '):
        raise ValueError('system OpenSSL identity missing')
    return {'schema_version':1, 'status':'PASS', 'role':'api', 'mode':'database',
        'uid':10001, 'python':python, 'python_implementation':'cpython',
        'virtual_environment':'/opt/venv', 'os_id':text(raw['os_id']),
        'machine':text(raw['machine']), 'openssl':text(raw['openssl']),
        'checks':{'imports':{'psycopg':psycopg_version,'psycopg-c':c_version},
          'native_psycopg':{'implementation':'c','libpq_version':version,
            'system_library_mappings':paths,'libpq_dependency_openssl_version':tls,
            'libpq_dependency_tls_method':True,'binary_wheel_distribution_absent':True},
          'database_roundtrip':expected},
        'limits':'One isolated PostgreSQL transaction/rollback and native C/system-library proof with fixture SSL disabled; no actual TLS handshake, production database, Flask application transaction, outbox delivery or Kafka broker was tested'}
try:
    result = subprocess.run(sys.argv[1:], stdin=sys.stdin, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
    if result.returncode != 0 or len(result.stdout) > 65536:
        raise ValueError('container probe did not complete')
    lines = [line[len(PREFIX):] for line in result.stdout.splitlines() if line.startswith(PREFIX)]
    if len(lines) != 1:
        raise ValueError('one fixed proof required')
    proof = sanitize(json.loads(lines[0]))
except Exception:
    print(PREFIX + json.dumps({'schema_version':1,'status':'FAIL','role':'api','mode':'database','reason':'isolated database probe failed; child logs intentionally discarded'}), flush=True)
    raise SystemExit(1)
print(PREFIX + json.dumps(proof, sort_keys=True), flush=True)
'''


def validate_proof(proof):
    if proof.get("status") != "PASS" or proof.get("role") != "api" or proof.get("mode") != "database":
        raise StreamingError("isolated database proof did not pass")
    native = proof.get("checks", {}).get("native_psycopg", {})
    database = proof.get("checks", {}).get("database_roundtrip", {})
    version = native.get("libpq_version", 0)
    if (native.get("implementation") != "c" or type(version) is not int
            or not (170010 <= version < 180000 or 180004 <= version < 190000)):
        raise StreamingError("same-version C/system libpq proof is missing")
    expected = {"server_version_num": 160014, "committed_rows": 1,
                "rolled_back_rows": 0, "unicode_jsonb_roundtrip": True,
                "parameterized_statements": True, "fixture_sslmode": "disable"}
    if any(database.get(key) != value or type(database.get(key)) is not type(value) for key, value in expected.items()):
        raise StreamingError("database version, committed/rollback counts or Unicode JSONB proof differs")
    return proof


class ApiDatabaseTest(StreamingTest):
    def __init__(self, options):
        # Reuse the existing exact-ID/owner/snapshot/guard/cleanup machinery.
        options.phase_timeout_seconds = options.guard_timeout_seconds
        super().__init__(options)
        self.prefix = "d2c-api-db-" + self.token[:16]
        self.names = {"network": self.prefix + "-net", "database": self.prefix + "-postgres", "api": self.prefix + "-api"}
        self.summary["names"] = self.names
        self.summary.pop("phases", None)
        self.summary["test"] = "api_c_psycopg_isolated_database"
        self.summary["budgets"] = {"overall_seconds": options.total_timeout_seconds,
            "guard_seconds": options.guard_timeout_seconds, "probe_seconds": 90,
            "guard_cancel_reserve_seconds": 30, "owned_cleanup_reserve_seconds": 60,
            "guard_minimum_free_gib": 8, "guard_reserve_free_gib": 6}
        self.summary["limits"] = "Isolated driver and C ABI regression using disposable state; production data, API transactions, outbox delivery, Kafka and deployment are outside this test"
        self.password = secrets.token_urlsafe(32)

    def budget(self):
        if self.interrupted is not None:
            raise StreamingError("isolated database test interrupted")
        remaining = self.deadline - time.monotonic() - 90
        if remaining <= 0:
            raise StreamingError("execution deadline reached; cancellation and owned cleanup reserved")
        return remaining

    def fresh_names(self):
        for kind, name in (("network", self.names["network"]), ("container", self.names["database"]), ("container", self.names["api"])):
            if self.inspect(kind, name) is not None:
                raise StreamingError("new isolated resource name is unavailable")

    def create_network(self):
        self.sample(admission=True)
        self.summary["resource_create_attempts"] = ["network"]
        self.save()
        result = self.docker("network", "create", "--driver", "bridge", "--internal", "--label",
                             OWNER_LABEL + "=" + self.token, self.names["network"])
        self.capture_resource("network")
        if result.returncode:
            raise StreamingError("new isolated network creation was not confirmed")

    def database_image(self):
        data = self.inspect("image", self.options.database_image)
        if (data is None or data.get("Id") != self.options.database_image
                or data.get("Os", "") + "/" + data.get("Architecture", "") != self.options.platform
                or set((data.get("Config") or {}).get("Volumes") or {}) != {PGDATA}):
            raise StreamingError("database image must match its immutable ID, platform and the single official PG16 volume declaration")
        self.summary.setdefault("images", {})["database"] = {key: data.get(key) for key in ("Id", "Os", "Architecture", "Created")}
        return data["Id"]

    def database_command(self, image):
        return ["docker", "run", "--rm", "--pull", "never", "--detach", "--name", self.names["database"],
                "--platform", self.options.platform, "--label", OWNER_LABEL + "=" + self.token,
                "--network", self.names["network"], "--network-alias", "database",
                "--memory", "512m", "--memory-swap", "512m", "--cpus", "1", "--pids-limit", "128",
                "--log-driver", "none", "--tmpfs", PGDATA + ":" + PG_TMPFS,
                "--env", "POSTGRES_PASSWORD", "--env", "POSTGRES_USER=d2c_probe", "--env", "POSTGRES_DB=d2c_probe",
                image, "postgres", "-c", "shared_buffers=32MB", "-c", "max_connections=10",
                "-c", "max_wal_size=64MB", "-c", "min_wal_size=32MB"]

    def start_database(self, image):
        self.sample(admission=True)
        self.attempt_container(self.names["database"])
        environment = {**os.environ, "POSTGRES_PASSWORD": self.password}
        try:
            result = subprocess.run(self.database_command(image), env=environment, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, text=True, timeout=min(20, self.budget()))
        finally:
            environment.pop("POSTGRES_PASSWORD", None)
        data = self.owned_container(self.names["database"])
        if result.returncode or data is None or not data.get("State", {}).get("Running"):
            raise StreamingError("new bounded database service did not start")
        tmpfs = (data.get("HostConfig") or {}).get("Tmpfs") or {}
        mounts = data.get("Mounts") or []
        destinations = [item.get("Destination") for item in mounts]
        if (set(tmpfs) != {PGDATA} or tmpfs[PGDATA] != PG_TMPFS
                or len(destinations) != len(set(destinations))
                or any(item.get("Type") != "tmpfs" or item.get("Destination") != PGDATA
                       or item.get("Source", "") not in ("", "tmpfs")
                       or item.get("Name") not in (None, "") or item.get("RW") is not True
                       for item in mounts)):
            raise StreamingError("database mounts differ from the single bounded tmpfs contract")
        self.summary["database_storage"] = {"type": "tmpfs", "destination": PGDATA, "size_bytes": 128 * 2**20,
                                            "existing_volume_or_host_bind": False}

    def api_command(self, image, probe_timeout=90):
        source = self.options.probe_script.read_text()
        self.summary["probe_source_sha256"] = hashlib.sha256(source.encode()).hexdigest()
        return ["docker", "run", "--rm", "--pull", "never", "--interactive", "--name", self.names["api"],
                "--platform", self.options.platform, "--label", OWNER_LABEL + "=" + self.token,
                "--network", self.names["network"], "--memory", "384m", "--memory-swap", "384m",
                "--cpus", "1", "--pids-limit", "128", "--log-driver", "none",
                "--entrypoint", "python", image, "-c", source, "api", "--database-stdin", "--timeout-seconds", str(probe_timeout)]

    def probe_database(self, image):
        self.sample(admission=True)
        timeout = min(self.options.guard_timeout_seconds, self.budget())
        if timeout < 15:
            raise StreamingError("insufficient guarded budget for a bounded database probe")
        probe_timeout = min(90, int(timeout - 5))
        name = self.names["api"]
        command = [sys.executable, "-c", RELAY_SOURCE, *self.api_command(image, probe_timeout)]
        arguments, folder = self.guard_command("database", name, command, timeout)
        self.attempt_container(name)
        configuration = {"host": "database", "port": 5432, "dbname": "d2c_probe", "user": "d2c_probe", "password": self.password}
        # The new DB password is never installed in os.environ or the guard's
        # environment. Only the short-lived postgres launch receives its copy.
        environment = {key: value for key, value in os.environ.items() if key not in {"POSTGRES_PASSWORD", "PGPASSWORD", "DATABASE_URL"}}
        process = subprocess.Popen(arguments, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, text=True, start_new_session=True)
        self.active_guard = process
        try:
            try:
                stdout, _ = process.communicate(json.dumps(configuration), timeout=self.guard_wait(timeout + 15))
            except subprocess.TimeoutExpired:
                process.send_signal(signal.SIGTERM)
                try:
                    stdout, _ = process.communicate(timeout=self.guard_wait(15))
                except subprocess.TimeoutExpired as error:
                    self.guard_uncertain = True
                    raise StreamingError("guard cancellation uncertain; new isolated resources preserved") from error
            guard = json.loads((folder / "summary.json").read_text())
            if (process.returncode != 0 or guard.get("outcome") != "completed" or guard.get("child_exit") != 0
                    or guard.get("abort") is not None or guard.get("cleanup_errors")):
                raise StreamingError("isolated database probe did not complete under its resource guard")
            # command.log contains only the allowlisted relay output. Never
            # store guard stderr or a raw Docker/native exception string.
            lines = [line[len(PREFIX):] for line in (folder / "command.log").read_text().splitlines() if line.startswith(PREFIX)]
            if len(lines) != 1:
                raise StreamingError("one sanitized database proof is required")
            proof = validate_proof(json.loads(lines[0]))
            self.summary["proof"] = proof
            write_json(self.output / "database-proof.json", proof)
        except BaseException:
            if process.poll() is None and not self.guard_uncertain:
                process.send_signal(signal.SIGTERM)
                try:
                    process.communicate(timeout=self.guard_wait(15))
                except (OSError, subprocess.TimeoutExpired):
                    self.guard_uncertain = True
            raise
        finally:
            self.active_guard = None
            configuration.clear()

    def cleanup_container(self, name):
        # Reuse immutable identity and owner checks, but never request DB logs.
        data = self.owned_container(name, cleanup=True)
        if data is None:
            return
        identity = data["Id"]
        if data["State"]["Running"]:
            result = self.docker("stop", "--time", "10", identity, timeout=25, cleanup=True)
            data = self.owned_container(name, cleanup=True)
            if data is not None and data["State"]["Running"]:
                if result.returncode == 0:
                    raise StreamingError("owned database test container remained running after stop")
                self.checked("kill", identity, cleanup=True)
        data = self.owned_container(name, cleanup=True)
        if data is not None:
            if data["State"]["Running"]:
                raise StreamingError("owned database test container remains running")
            self.checked("container", "rm", identity, cleanup=True)
        if self.owned_container(name, cleanup=True) is not None:
            raise StreamingError("owned container removal not confirmed")
        self.summary["cleanup"]["removed"].append({"kind": "container", "id": identity})

    def run(self):
        previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        for number in previous:
            signal.signal(number, self.interrupt)
        try:
            self.save()
            self.sample(admission=True)
            self.summary["before"] = self.snapshot()
            api = self.image(self.options.api_image, "api", immutable=True)
            database = self.database_image()
            self.fresh_names()
            self.create_network()
            self.start_database(database)
            self.probe_database(api)
        except Exception as error:
            self.summary["failures"].append(type(error).__name__ + ": isolated database test did not complete")
        finally:
            self.cleanup()
            try:
                if "before" in self.summary:
                    after = self.snapshot(cleanup=True)
                    self.summary["after"] = after
                    before = self.summary["before"]
                    missing = {key: sorted(set(values) - set(after[key])) for key, values in before.items() if key != "volume_identities"}
                    replaced = sorted(name for name, identity in before["volume_identities"].items() if after["volume_identities"].get(name) != identity)
                    self.summary["original_resources_missing_afterward"] = missing
                    self.summary["original_volume_identities_changed_afterward"] = replaced
                    if any(missing.values()) or replaced:
                        self.summary["failures"].append("original Docker inventory changed; cause is not inferred")
            except Exception:
                self.summary["cleanup"]["errors"].append("final Docker inventory could not be confirmed")
            passed = "proof" in self.summary and not self.summary["failures"] and not self.summary["cleanup"]["errors"]
            self.summary["outcome"] = "PASS" if passed else "cleanup_uncertain" if self.summary["cleanup"]["errors"] else "FAIL"
            self.summary["finished_at"] = timestamp()
            self.password = None
            self.save()
            for number, handler in previous.items():
                signal.signal(number, handler)
        return 0 if self.summary["outcome"] == "PASS" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-image", required=True)
    parser.add_argument("--database-image", required=True)
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--guard-script", type=Path, required=True)
    parser.add_argument("--storage-path", type=Path, action="append", default=[])
    parser.add_argument("--probe-script", type=Path, default=Path(__file__).with_name("check_python_runtime.py"))
    parser.add_argument("--guard-timeout-seconds", type=float, default=120)
    parser.add_argument("--total-timeout-seconds", type=float, default=240)
    options = parser.parse_args(argv)
    if not all(IMAGE_ID.fullmatch(value) for value in (options.api_image, options.database_image)):
        parser.error("both image arguments must be complete immutable sha256 IDs; images are never pulled")
    if not options.guard_script.is_file() or not options.probe_script.is_file() or any(not path.exists() for path in options.storage_path):
        parser.error("probe, guard and monitored storage paths must exist")
    if (not math.isfinite(options.guard_timeout_seconds) or not 15 <= options.guard_timeout_seconds <= 120
            or not math.isfinite(options.total_timeout_seconds) or not 105 <= options.total_timeout_seconds <= 240):
        parser.error("guard timeout must be 15..120 seconds and overall timeout 105..240 seconds")
    try:
        return ApiDatabaseTest(options).run()
    except (OSError, StreamingError):
        print("Isolated database verification did not start; no raw configuration is emitted", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
