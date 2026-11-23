"""Mocked CI wiring checks; never execute apt, pip, Docker or a database."""
from __future__ import annotations

import json
import copy
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import yaml


ROOT = Path(__file__).resolve().parents[1]


def workflow(name):
    return yaml.load((ROOT / ".github/workflows" / (name + ".yml")).read_text(), Loader=yaml.BaseLoader)


class APINativeHostProvisioningTest(unittest.TestCase):
    def provisioning(self):
        for name, job, step in (("ci", "python", "Install dependencies"),
                                ("security", "dependency-and-code-scan", "Install runtime and audit tools")):
            yield name, next(value["run"] for value in workflow(name)["jobs"][job]["steps"] if value.get("name") == step)

    def invoke(self, script, *, host_exit=0, update_exit=0, install_exit=0):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for executable in ("python3", "python", "sudo"):
                target = root / executable
                target.write_text(f"#!{sys.executable}\n" +
                    "import json, os, pathlib, sys\n" +
                    "name = pathlib.Path(sys.argv[0]).name\n" +
                    "with open(os.environ['TEST_CALLS'], 'a') as output:\n" +
                    "    output.write(json.dumps([name] + sys.argv[1:]) + '\\n')\n" +
                    "value = 'TEST_HOST_EXIT' if name == 'python3' else 'TEST_UPDATE_EXIT' if name == 'sudo' and 'update' in sys.argv else 'TEST_INSTALL_EXIT' if name == 'sudo' else 'TEST_OK'\n" +
                    "raise SystemExit(int(os.environ[value]))\n")
                target.chmod(0o700)
            record = root / "calls.jsonl"
            environment = {**os.environ, "PATH": str(root) + os.pathsep + os.environ.get("PATH", ""),
                           "TEST_CALLS": str(record), "TEST_HOST_EXIT": str(host_exit),
                           "TEST_UPDATE_EXIT": str(update_exit), "TEST_INSTALL_EXIT": str(install_exit), "TEST_OK": "0"}
            result = subprocess.run(["bash", "-e", "-c", script], env=environment, cwd=ROOT,
                                    capture_output=True, text=True, timeout=10)
            return result.returncode, [json.loads(line) for line in record.read_text().splitlines()]

    def test_host_is_validated_before_ordinary_ephemeral_packages_or_c_extension_build(self):
        for name, script in self.provisioning():
            with self.subTest(workflow=name):
                code, calls = self.invoke(script)
                self.assertEqual(code, 0)
                self.assertEqual(calls[0][0:2], ["python3", "-c"])
                self.assertIn("validate_host(dict(os.environ))", calls[0][2])
                self.assertEqual(calls[1], ["sudo", "-n", "apt-get", "update"])
                self.assertEqual(calls[2], ["sudo", "-n", "apt-get", "install", "--yes", "--no-install-recommends", "build-essential", "libpq-dev"])
                c_install = next(call for call in calls if "app/requirements.txt" in call)
                self.assertIn("--no-binary=psycopg-c", c_install)
                self.assertLess(calls.index(calls[2]), calls.index(c_install))
                self.assertTrue(any(call[0:2] == ["python", "-c"] and 'pq.__impl__ == "c"' in call[2] for call in calls))
                self.assertEqual(workflow(name)["permissions"], {"contents": "read"})

    def test_local_or_self_hosted_refusal_prevents_any_sudo_or_pip(self):
        for name, script in self.provisioning():
            with self.subTest(workflow=name):
                code, calls = self.invoke(script, host_exit=75)
                self.assertEqual(code, 75)
                self.assertEqual(len(calls), 1)

    def test_package_update_or_install_failure_does_not_fall_through_to_pip(self):
        for name, script in self.provisioning():
            for failure in ("update", "install"):
                with self.subTest(workflow=name, failure=failure):
                    code, calls = self.invoke(script, update_exit=1 if failure == "update" else 0,
                                              install_exit=1 if failure == "install" else 0)
                    self.assertEqual(code, 1)
                    self.assertFalse(any(call[0] == "python" for call in calls))


def condition(value, *, role="api", storage="success", build="success", cancelled=False, fixture="success"):
    source = value.strip().removeprefix("${{").removesuffix("}}")
    source = source.replace("!cancelled()", repr(not cancelled)).replace("matrix.role", repr(role)).replace("&&", "and")
    source = re.sub(r"steps\.([a-z_-]+)\.outcome",
                    lambda match: repr({"storage": storage, "build": build, "api-database-image": fixture}.get(match[1], "failure")), source)
    return bool(eval("(" + source + ")", {"__builtins__": {}}, {}))


class APIDatabaseWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.job = workflow("security")["jobs"]["image-scan"]
        self.fixture = next(step for step in self.job["steps"] if step.get("id") == "api-database-image")
        self.roundtrip = next(step for step in self.job["steps"] if step.get("id") == "api-database-runtime")

    def test_fixture_is_api_only_and_still_runs_after_a_failed_security_scan(self):
        self.assertTrue(condition(self.fixture["if"]))
        for role in ("worker", "catalog"):
            self.assertFalse(condition(self.fixture["if"], role=role))
        for result in ("failure", "skipped", "cancelled"):
            self.assertFalse(condition(self.fixture["if"], build=result))
            self.assertFalse(condition(self.fixture["if"], storage=result))
        self.assertFalse(condition(self.fixture["if"], cancelled=True))
        self.assertNotIn("steps.scan", self.fixture["if"])
        self.assertNotIn("steps.python-runtime", self.fixture["if"])
        for required in ('reference="postgres:16.14-alpine"', "--kind scan --minimum-free-gib 10 --reserve-free-gib 6",
                         "--timeout-seconds 300", 'docker pull --platform "linux/$ARCH" "$reference"'):
            self.assertIn(required, self.fixture["run"])

    def test_fixture_uses_actual_native_iid_and_keeps_only_public_image_metadata(self):
        code = re.search(r"python3 - <<'PYTHON'\n(.*?)\nPYTHON(?:\n|$)", self.fixture["run"], re.DOTALL)[1]
        for corrupt in (None, "id", "arch", "repository", "missing_digest", "multiple"):
            with self.subTest(corrupt=corrupt), TemporaryDirectory() as temporary:
                root = Path(temporary)
                output = root / "d2c-api-database-amd64"
                output.mkdir()
                image = {"Id": "sha256:" + "1" * 64, "Os": "linux", "Architecture": "amd64",
                         "RepoDigests": ["postgres@sha256:" + "2" * 64],
                         "Config": {"Env": ["SECRET=must-not-persist"]}}
                if corrupt == "id": image["Id"] = "mutable-tag"
                if corrupt == "arch": image["Architecture"] = "arm64"
                if corrupt == "repository": image["RepoDigests"] = ["untrusted/postgres@sha256:" + "2" * 64]
                if corrupt == "missing_digest": image["RepoDigests"] = []
                inspected = [image, image] if corrupt == "multiple" else [image]
                with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ARCH": "amd64"}), \
                        patch("subprocess.check_output", return_value=json.dumps(inspected)):
                    if corrupt:
                        with self.assertRaises(SystemExit): exec(compile(code, "fixture-identity", "exec"), {})
                        self.assertFalse((output / "fixture.iid").exists())
                    else:
                        exec(compile(code, "fixture-identity", "exec"), {})
                        self.assertEqual((output / "fixture.iid").read_text(), image["Id"] + "\n")
                        metadata = (output / "fixture-image.json").read_text()
                        self.assertNotIn("SECRET", metadata)
                        self.assertEqual(json.loads(metadata)["expected_server_version_num"], 160014)

    def test_api_database_evidence_is_uploaded_even_if_testing_fails(self):
        upload = next(step for step in self.job["steps"] if step.get("name") == "Preserve every attempted image and build result")
        self.assertEqual(upload["if"], "always()")
        self.assertIn("${{ runner.temp }}/d2c-api-database-${{ matrix.arch }}", upload["with"]["path"])
        self.assertEqual(upload["with"]["retention-days"], "14")

    def test_real_roundtrip_uses_both_immutable_ids_and_api_only_gating(self):
        self.assertTrue(condition(self.roundtrip["if"]))
        for role in ("worker", "catalog"):
            self.assertFalse(condition(self.roundtrip["if"], role=role))
        for result in ("failure", "skipped", "cancelled"):
            self.assertFalse(condition(self.roundtrip["if"], build=result))
            self.assertFalse(condition(self.roundtrip["if"], storage=result))
            self.assertFalse(condition(self.roundtrip["if"], fixture=result))
        self.assertNotIn("steps.scan", self.roundtrip["if"])
        self.assertNotIn("continue-on-error", self.roundtrip)
        for required in ('image="$(cat "$RUNNER_TEMP/api.iid")"',
                         'database="$(cat "$RUNNER_TEMP/d2c-api-database-$ARCH/fixture.iid")"',
                         "python3 scripts/test_api_database.py", '--api-image "$image" --database-image "$database"',
                         '--platform "linux/$ARCH"', '--output-dir "$RUNNER_TEMP/d2c-api-database-$ARCH/roundtrip"',
                         "--guard-script scripts/container_security_guard.py",
                         '--storage-path "$GITHUB_WORKSPACE" --storage-path /var/lib/docker'):
            self.assertIn(required, self.roundtrip["run"])

    def test_roundtrip_proof_rejects_stale_ids_unfixed_libpq_or_incomplete_real_sql(self):
        code = re.search(r"python3 - <<'PYTHON'\n(.*?)\nPYTHON(?:\n|$)", self.roundtrip["run"], re.DOTALL)[1]
        current_pin = next(line.split("==", 1)[1] for line in (ROOT / "app/requirements.txt").read_text().splitlines()
                           if line.startswith("psycopg[c]=="))
        parts = current_pin.split(".")
        updated_pin = ".".join(parts[:-1] + [str(int(parts[-1]) + 1)])
        ids = {"api": "sha256:" + "1" * 64, "database": "sha256:" + "2" * 64}
        valid = {"outcome": "PASS", "failures": [], "cleanup": {"errors": []}, "platform": "linux/amd64",
                 "images": {key: {"Id": value} for key, value in ids.items()},
                 "proof": {"status": "PASS", "role": "api", "mode": "database", "checks": {
                     "imports": {"psycopg": current_pin, "psycopg-c": current_pin},
                     "native_psycopg": {"implementation": "c", "libpq_version": 180004,
                                        "binary_wheel_distribution_absent": True, "libpq_dependency_tls_method": True,
                                        "libpq_dependency_openssl_version": "OpenSSL fixture",
                                        "system_library_mappings": {key: ["/usr/lib/" + key + ".so"] for key in ("libpq", "libssl", "libcrypto")}},
                     "database_roundtrip": {"server_version_num": 160014, "committed_rows": 1,
                                            "rolled_back_rows": 0, "unicode_jsonb_roundtrip": True,
                                            "parameterized_statements": True, "fixture_sslmode": "disable"}}}}
        for corrupt in (None, "updated_pin", "missing_pin", "duplicate_pin", "range_pin", "binary_pin", "commented_pin",
                        "api_id", "database_id", "libpq", "library_family", "cleanup", "rollback", "wrong_server",
                        "unicode", "binary", "imports", "mode", "missing_maps", "private_library", "missing_tls", "parameterized"):
            with self.subTest(corrupt=corrupt), TemporaryDirectory() as temporary:
                root = Path(temporary)
                directory = root / "d2c-api-database-amd64"
                (directory / "roundtrip").mkdir(parents=True)
                (root / "api.iid").write_text(ids["api"])
                (directory / "fixture.iid").write_text(ids["database"])
                (root / "app").mkdir()
                requirement = "psycopg[c]==" + current_pin + "\n"
                value = copy.deepcopy(valid)
                native = value["proof"]["checks"]["native_psycopg"]
                database = value["proof"]["checks"]["database_roundtrip"]
                if corrupt == "api_id": value["images"]["api"]["Id"] = "sha256:" + "3" * 64
                if corrupt == "database_id": value["images"]["database"]["Id"] = "sha256:" + "3" * 64
                if corrupt == "libpq": native["libpq_version"] = 170005
                if corrupt == "library_family": native["libpq_version"] = 170010
                if corrupt == "cleanup": value["cleanup"]["errors"] = ["unconfirmed"]
                if corrupt == "rollback": database["rolled_back_rows"] = False
                if corrupt == "wrong_server": database["server_version_num"] = 170014
                if corrupt == "unicode": database["unicode_jsonb_roundtrip"] = False
                if corrupt == "binary": native["binary_wheel_distribution_absent"] = False
                if corrupt == "imports": value["proof"]["checks"]["imports"]["psycopg-c"] = updated_pin
                if corrupt == "updated_pin":
                    requirement = "psycopg[c]==" + updated_pin + "\n"
                    value["proof"]["checks"]["imports"] = {"psycopg": updated_pin, "psycopg-c": updated_pin}
                if corrupt == "missing_pin": requirement = "Flask==3.1.3\n"
                if corrupt == "duplicate_pin": requirement *= 2
                if corrupt == "range_pin": requirement = "psycopg[c]>=" + current_pin + "\n"
                if corrupt == "binary_pin": requirement = "psycopg[binary]==" + current_pin + "\n"
                if corrupt == "commented_pin": requirement = requirement.strip() + " # inline comment\n"
                if corrupt == "mode": value["proof"]["mode"] = "startup"
                if corrupt == "missing_maps": native["system_library_mappings"] = {}
                if corrupt == "private_library": native["system_library_mappings"]["libpq"] = ["/opt/venv/private.libs/libpq.so"]
                if corrupt == "missing_tls": native["libpq_dependency_openssl_version"] = ""
                if corrupt == "parameterized": database["parameterized_statements"] = False
                (root / "app/requirements.txt").write_text(requirement)
                (directory / "roundtrip/summary.json").write_text(json.dumps(value))
                with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "GITHUB_WORKSPACE": str(root), "ARCH": "amd64"}):
                    if corrupt not in (None, "updated_pin"):
                        with self.assertRaises(SystemExit): exec(compile(code, "api-database-proof", "exec"), {})
                    else:
                        exec(compile(code, "api-database-proof", "exec"), {})


if __name__ == "__main__":
    unittest.main()
