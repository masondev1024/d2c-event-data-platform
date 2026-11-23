"""Offline control-flow tests for the isolated API database orchestrator.

Mocks check immutable identity, private resources and sanitized evidence.
Native image/database success is never inferred from these tests.
"""
import argparse
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("api_database_orchestrator", Path(__file__).parents[1] / "scripts/test_api_database.py")
host = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(host)
API = "sha256:" + "a" * 64
DATABASE = "sha256:" + "b" * 64
FIXTURE_PASSWORD = "only-a-mock-password"


def proof(version="3.2.10"):
    return {"schema_version": 1, "status": "PASS", "role": "api", "mode": "database",
            "uid": 10001, "python": "3.12.16", "python_implementation": "cpython",
            "virtual_environment": "/opt/venv", "os_id": "wolfi", "machine": "aarch64", "openssl": "OpenSSL 3.6.0",
            "checks": {"imports": {"psycopg": version, "psycopg-c": version},
                "native_psycopg": {"implementation": "c", "libpq_version": 170010,
                    "system_library_mappings": {"libpq": ["/usr/lib/libpq.so.5"], "libssl": ["/usr/lib/libssl.so.3"], "libcrypto": ["/usr/lib/libcrypto.so.3"]},
                    "libpq_dependency_openssl_version": "OpenSSL 3.6.0", "libpq_dependency_tls_method": True,
                    "binary_wheel_distribution_absent": True},
                "database_roundtrip": {"server_version_num": 160014, "committed_rows": 1,
                                       "rolled_back_rows": 0, "unicode_jsonb_roundtrip": True,
                                       "parameterized_statements": True, "fixture_sslmode": "disable"}}}


class DatabaseHarnessTest(unittest.TestCase):
    def setUp(self):
        self.scratch = TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.source = self.root / "probe.py"
        self.source.write_text("# mock runtime probe source\n")
        self.block_run = patch.object(host.subprocess, "run", side_effect=AssertionError("unmocked process execution is forbidden"))
        self.block_popen = patch.object(host.subprocess, "Popen", side_effect=AssertionError("unmocked guard execution is forbidden"))
        self.block_run.start(); self.block_popen.start()
        self.addCleanup(self.block_run.stop); self.addCleanup(self.block_popen.stop)

    def options(self):
        return argparse.Namespace(output_dir=self.root / "evidence", api_image=API, database_image=DATABASE,
            platform="linux/arm64", storage_path=[], guard_timeout_seconds=120, total_timeout_seconds=240,
            guard_script=Path(__file__).parents[1] / "scripts/container_security_guard.py", probe_script=self.source)

    def instance(self):
        with patch.object(host.secrets, "token_urlsafe", return_value=FIXTURE_PASSWORD):
            return host.ApiDatabaseTest(self.options())

    def test_password_is_absent_from_arguments_and_summary(self):
        instance = self.instance()
        database = instance.database_command(DATABASE)
        api = instance.api_command(API)
        self.assertNotIn(FIXTURE_PASSWORD, json.dumps(database))
        self.assertNotIn(FIXTURE_PASSWORD, json.dumps(api))
        self.assertNotIn(FIXTURE_PASSWORD, json.dumps(instance.summary))
        self.assertIn("POSTGRES_PASSWORD", database)
        self.assertNotIn("POSTGRES_PASSWORD=" + FIXTURE_PASSWORD, database)

    def test_database_uses_only_the_bounded_official_pgdata_tmpfs(self):
        instance = self.instance(); command = instance.database_command(DATABASE)
        self.assertEqual(command[command.index("--tmpfs") + 1], host.PGDATA + ":" + host.PG_TMPFS)
        self.assertEqual(command[command.index("--memory") + 1], "512m")
        self.assertEqual(command[command.index("--cpus") + 1], "1")
        self.assertEqual(command[command.index("--log-driver") + 1], "none")
        self.assertIn("--pull", command); self.assertIn("never", command)
        self.assertFalse(any(value in command for value in ("--publish", "-v", "--volume", "--mount", "--user")))

    def test_api_uses_image_default_user_source_c_and_stdin_credentials_mode(self):
        instance = self.instance(); command = instance.api_command(API, 60)
        self.assertEqual(command[command.index("--memory") + 1], "384m")
        self.assertNotIn("--user", command)
        self.assertIn("--interactive", command)
        self.assertIn("--database-stdin", command)
        self.assertEqual(command[command.index("--timeout-seconds") + 1], "60")
        self.assertEqual(command[command.index("-c") + 1], self.source.read_text())
        self.assertNotIn("--mount", command)

    def test_unexpected_image_volume_declarations_are_rejected_before_launch(self):
        instance = self.instance()
        metadata = {"Id": DATABASE, "Os": "linux", "Architecture": "arm64", "Config": {"Volumes": {"/unreviewed": {}}}}
        with patch.object(instance, "inspect", return_value=metadata):
            with self.assertRaisesRegex(host.StreamingError, "single official PG16 volume"):
                instance.database_image()

    def test_secret_is_passed_only_to_postgres_launch_environment(self):
        instance = self.instance(); captured = {}
        data = {"Id": "c" * 64, "State": {"Running": True}, "HostConfig": {"Tmpfs": {host.PGDATA: host.PG_TMPFS}},
                "Mounts": [{"Type": "tmpfs", "Destination": host.PGDATA, "Source": "tmpfs", "RW": True}]}
        def launch(command, **kwargs):
            captured.update(command=command, environment=dict(kwargs["env"]), stderr=kwargs["stderr"])
            return subprocess.CompletedProcess(command, 0, "c" * 64, None)
        with patch.object(instance, "sample"), patch.object(instance, "owned_container", return_value=data), \
             patch.object(host.subprocess, "run", side_effect=launch):
            instance.start_database(DATABASE)
        self.assertEqual(captured["environment"]["POSTGRES_PASSWORD"], FIXTURE_PASSWORD)
        self.assertEqual(captured["stderr"], subprocess.DEVNULL)
        self.assertNotIn(FIXTURE_PASSWORD, json.dumps(captured["command"]))
        self.assertNotIn(FIXTURE_PASSWORD, json.dumps(instance.summary))

    def test_cleanup_uses_only_owned_id_and_never_requests_database_logs(self):
        instance = self.instance(); name = instance.names["database"]
        identity = "c" * 64
        data = {"Id": identity, "State": {"Running": True}}
        with patch.object(instance, "owned_container", side_effect=[data, None, None, None]), \
             patch.object(instance, "docker", return_value=subprocess.CompletedProcess([], 0, "", "")) as docker:
            instance.cleanup_container(name)
        docker.assert_called_once_with("stop", "--time", "10", identity, timeout=25, cleanup=True)
        self.assertFalse(any("logs" in call.args for call in docker.call_args_list))

    def test_native_binary_backend_or_wrong_database_counts_fail(self):
        wrong = proof(); wrong["checks"]["native_psycopg"]["implementation"] = "binary"
        with self.assertRaises(host.StreamingError):
            host.validate_proof(wrong)
        wrong = proof(); wrong["checks"]["database_roundtrip"]["rolled_back_rows"] = 1
        with self.assertRaises(host.StreamingError):
            host.validate_proof(wrong)
        wrong = proof(); wrong["checks"]["database_roundtrip"]["server_version_num"] = 160013
        with self.assertRaises(host.StreamingError):
            host.validate_proof(wrong)

    def test_relay_emits_only_allowlisted_proof_and_discards_child_stderr(self):
        raw = proof(); raw["password"] = FIXTURE_PASSWORD
        raw["checks"]["database_roundtrip"]["connection_string"] = FIXTURE_PASSWORD
        child = subprocess.CompletedProcess([], 0, host.PREFIX + json.dumps(raw), None)
        output = io.StringIO()
        with patch.object(host.sys, "argv", ["relay", "docker", "run"]), \
             patch.object(host.subprocess, "run", return_value=child) as run, redirect_stdout(output):
            exec(host.RELAY_SOURCE, {})
        self.assertNotIn(FIXTURE_PASSWORD, output.getvalue())
        sanitized = json.loads(output.getvalue().strip()[len(host.PREFIX):])
        self.assertEqual(sanitized["checks"]["database_roundtrip"]["server_version_num"], 160014)
        self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)

    def relay(self, raw):
        child = subprocess.CompletedProcess([], 0, host.PREFIX + json.dumps(raw), None)
        output = io.StringIO()
        with patch.object(host.sys, "argv", ["relay", "docker", "run"]), \
             patch.object(host.subprocess, "run", return_value=child), redirect_stdout(output):
            exec(host.RELAY_SOURCE, {})
        return json.loads(output.getvalue().strip()[len(host.PREFIX):])

    def test_relay_preserves_matching_actual_package_versions(self):
        for version in ("3.2.11", "3.3.0rc1", "3.2.11+local.1"):
            with self.subTest(version=version):
                sanitized = self.relay(proof(version))
                self.assertEqual(sanitized["checks"]["imports"], {"psycopg": version, "psycopg-c": version})

    def test_relay_rejects_mismatched_package_versions(self):
        raw = proof("3.2.11")
        raw["checks"]["imports"]["psycopg-c"] = "3.2.10"
        with self.assertRaises(SystemExit) as failure:
            self.relay(raw)
        self.assertEqual(failure.exception.code, 1)

    def test_relay_rejects_missing_or_malformed_package_versions(self):
        for version in (None, "", "not-a-version", "3..2.10", "3.2.10\nsecret", "3.2.10;command"):
            with self.subTest(version=version), self.assertRaises(SystemExit) as failure:
                self.relay(proof(version))
            self.assertEqual(failure.exception.code, 1)

    def test_elapsed_budget_prevents_probe_guard_spawn(self):
        instance = self.instance()
        with patch.object(instance, "sample"), patch.object(instance, "budget", return_value=14), \
             patch.object(host.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(host.StreamingError, "insufficient guarded budget"):
                instance.probe_database(API)
        popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
