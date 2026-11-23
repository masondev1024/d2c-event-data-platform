"""Offline checks of the final-image Snappy proof and fail-closed CI consumer."""
from __future__ import annotations

import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from zipfile import ZipFile

import yaml


ROOT = Path(__file__).resolve().parents[1]
JAR = "/opt/spark/jars/org.xerial.snappy_snappy-java-1.1.10.9.jar"
PAYLOAD = "dependencies/org.xerial.snappy_snappy-java-1.1.10.9.jar"
PREFIX = "D2C_SNAPPY_RUNTIME_PROOF="


def expression(value, **outcomes):
    source = value.strip().removeprefix("${{").removesuffix("}}")
    source = source.replace("!cancelled()", "True").replace("&&", "and")
    source = re.sub(r"steps\.([a-z_-]+)\.outcome", lambda match: repr(outcomes.get(match[1], "success")), source)
    return bool(eval("(" + source + ")", {"__builtins__": {}}, {}))


class SnappyRuntimeWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workflow = yaml.load((ROOT / ".github/workflows/security.yml").read_text(), Loader=yaml.BaseLoader)
        cls.job = workflow["jobs"]["lakehouse-scan"]
        cls.steps = {step["id"]: step for step in cls.job["steps"] if "id" in step}

    def test_native_check_uses_actual_final_iid_and_runs_independently_of_scan(self):
        step = self.steps["snappy-native"]
        sequence = self.job["steps"]
        self.assertLess(sequence.index(self.steps["spark-protobuf"]), sequence.index(step))
        self.assertLess(sequence.index(step), sequence.index(self.steps["broker"]))
        self.assertNotIn("continue-on-error", step)
        self.assertTrue(expression(step["if"], scan="failure", **{"spark-protobuf": "failure"}))
        for outcome in ("failure", "skipped", "cancelled"):
            self.assertFalse(expression(step["if"], storage=outcome))
            self.assertFalse(expression(step["if"], build=outcome))
        for name in ("broker", "streaming"):
            self.assertTrue(expression(self.steps[name]["if"], **{"snappy-native": "failure"}))
        for fragment in ('actualIID="$(cat "$RUNNER_TEMP/lakehouse.iid")"',
                         '[[ "$actualIID" =~ ^sha256:[0-9a-f]{64}$ ]]',
                         'name="d2c-snappy-native-$ARCH-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"',
                         "uuid.uuid4().hex", '--container-name "$name" --owner-token "$owner"',
                         "--kind test --minimum-free-gib 8 --reserve-free-gib 6 --timeout-seconds 180",
                         'docker run --rm --pull never --name "$name" --label "d2c.security.owner=$owner"',
                         '--platform "linux/$ARCH" --network none --memory 1g --cpus 2 --read-only',
                         "--tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m",
                         '--tmpfs /tmp/d2c-snappy-native/lib:rw,exec,nosuid,nodev,size=16m,mode=1777 --entrypoint /bin/sh "$actualIID"',
                         'javac -J-Xmx256m --release 17 -cp "/opt/spark/jars/*"',
                         'java -Xmx256m -cp "/tmp/d2c-snappy-classes:/opt/spark/jars/*"',
                         "VerifySnappyRuntime 21 " + JAR + ' "$1" "$2"',
                         "container-build/lakehouse/native/VerifySnappyRuntime.java",
                         "' d2c-snappy-native \"$ARCH\" \"$actualIID\""):
            self.assertIn(fragment, step["run"])
        for forbidden in ("--volume", "--publish", "--privileged", "curl ", "wget "):
            self.assertNotIn(forbidden, step["run"])
        parsed = subprocess.run(["bash", "-n"], input=step["run"], text=True, capture_output=True)
        self.assertEqual(parsed.returncode, 0, parsed.stderr)
        upload = next(item for item in sequence if "container-security/" in item.get("with", {}).get("path", ""))
        self.assertEqual(upload["if"], "always()")
        self.assertIn("d2c-lakehouse-runtime-${{ matrix.arch }}", upload["with"]["path"])

    def test_native_exec_permission_is_limited_to_the_small_separate_mount(self):
        def require_confined_tmpfs(run):
            mounts = dict(item.split(":", 1) for item in re.findall(r"--tmpfs\s+(\S+)", run))
            self.assertEqual(mounts, {
                "/tmp": "rw,noexec,nosuid,nodev,size=128m",
                "/tmp/d2c-snappy-native/lib": "rw,exec,nosuid,nodev,size=16m,mode=1777"})
            self.assertIn("mkdir -m 700 /tmp/d2c-snappy-classes", run)
            self.assertNotIn("mkdir -m 700 /tmp/d2c-snappy-native", run)
        run = self.steps["snappy-native"]["run"]
        require_confined_tmpfs(run)
        for before, after in (("rw,exec,nosuid,nodev,size=16m", "rw,nosuid,nodev,size=16m"),
                              ("rw,exec,nosuid,nodev,size=16m", "rw,noexec,nosuid,nodev,size=16m"),
                              ("rw,noexec,nosuid,nodev,size=128m", "rw,exec,nosuid,nodev,size=128m"),
                              ("size=16m,mode=1777", "size=128m,mode=1777")):
            with self.subTest(replacement=after), self.assertRaises(AssertionError):
                require_confined_tmpfs(run.replace(before, after))

    def test_consumer_requires_sealed_source_native_mapping_roundtrip_and_clean_guard(self):
        run = self.steps["snappy-native"]["run"]
        code = compile(re.search(r"python3 - <<'PYTHON'\n(.*?)\nPYTHON(?:\n|$)", run, re.DOTALL)[1],
                       "snappy-runtime-consumer", "exec")
        mutations = (None, "old-version", "java17", "expected17", "schema-bool", "extra-key", "missing-key",
                     "wrong-arch", "wrong-os", "native-false", "native-backend", "native-config", "native-version-empty",
                     "native-resource", "native-path", "native-hash", "resource-hash", "jar-sha", "jar-origin",
                     "class-origin", "class-missing", "metadata-source", "status", "missing-case", "count-bool",
                     "lengths", "input-size", "input-utf8", "runtime-empty", "vendor-empty", "wrong-iid", "mutable-iid",
                     "manifest-hash", "manifest-schema", "receipt-fail", "recipe-mismatch", "child-exit",
                     "boolean-exit", "abort", "missing-abort", "cleanup", "missing-marker", "duplicate", "malformed")
        for arch in ("amd64", "arm64"):
            resource = "org/xerial/snappy/native/Linux/" + {"amd64": "x86_64", "arm64": "aarch64"}[arch] + "/libsnappyjava.so"
            native = b"small native library fixture, never executed: " + arch.encode()
            native_sha = hashlib.sha256(native).hexdigest()
            buffer = io.BytesIO()
            with ZipFile(buffer, "w") as fixture:
                fixture.writestr(resource, native)
            source = buffer.getvalue()
            source_sha = hashlib.sha256(source).hexdigest()
            iid = "sha256:" + "1" * 64
            valid = {"schema_version": 1, "status": "passed", "actual_java_major": 21, "expected_java_major": 21,
                     "java_runtime_version": "21.0.13+fixture", "java_vendor": "fixture", "os_name": "Linux",
                     "actual_arch": arch, "expected_arch": arch, "image_id": iid,
                     "expected_jar": JAR, "actual_jar": JAR, "jar_sha256": source_sha,
                     "java_artifact_version": "1.1.10.9", "java_artifact_version_source": "META-INF/MANIFEST.MF:Implementation-Version",
                     "class_code_sources": {"org.xerial.snappy.Snappy": JAR, "org.xerial.snappy.SnappyNative": JAR},
                     "native_backend": "JNI", "native_loaded": True, "native_library_version": "1.1.10.8",
                     "native_configuration": {"use_systemlib": "false", "disable_bundled_libs": "false",
                         "purejava": "false", "lib_path": "", "lib_name": "", "tempdir": "/tmp/d2c-snappy-native/lib/owned"},
                     "native_library_resource": resource,
                     "native_library_path": "/tmp/d2c-snappy-native/lib/owned/snappy-1.1.10.9-fixture-libsnappyjava.so",
                     "native_library_sha256": native_sha, "native_resource_sha256": native_sha,
                     "cases": ["UTF8", "BINARY"], "roundtrips_verified": 2, "input_bytes": [53, 4096],
                     "uncompressed_lengths_verified": True}
            for mutation in mutations:
                with self.subTest(arch=arch, mutation=mutation), TemporaryDirectory() as scratch:
                    root = Path(scratch)
                    evidence = root / ("d2c-lakehouse-runtime-" + arch) / "snappy-native"
                    evidence.mkdir(parents=True)
                    jar = root / "d2c-artifacts" / PAYLOAD
                    jar.parent.mkdir(parents=True)
                    jar.write_bytes(source)
                    manifest = {"schema_version": 1, "kind": "d2c-lakehouse-source-artifacts",
                                "payload_sha256": {PAYLOAD: source_sha}, "source_recipe_sha256": "a" * 64}
                    receipt = {"action": "verify", "result": "PASS", "source_recipe_sha256": "a" * 64}
                    guard = {"outcome": "completed", "child_exit": 0, "exit": 0, "abort": None, "cleanup_errors": []}
                    proof = copy.deepcopy(valid)
                    field_mutations = {"old-version": ("java_artifact_version", "1.1.10.8"),
                        "java17": ("actual_java_major", 17), "expected17": ("expected_java_major", 17),
                        "schema-bool": ("schema_version", True), "wrong-arch": ("actual_arch", "other"),
                        "wrong-os": ("os_name", "Darwin"), "native-false": ("native_loaded", False),
                        "native-backend": ("native_backend", "pure-java"), "native-version-empty": ("native_library_version", ""),
                        "native-resource": ("native_library_resource", "different.so"),
                        "native-path": ("native_library_path", "/usr/lib/libsnappyjava.so"),
                        "native-hash": ("native_library_sha256", "0" * 64), "resource-hash": ("native_resource_sha256", "0" * 64),
                        "jar-sha": ("jar_sha256", "0" * 64), "jar-origin": ("actual_jar", "/old/snappy.jar"),
                        "metadata-source": ("java_artifact_version_source", "org/xerial/snappy/VERSION"),
                        "status": ("status", "PASS"), "missing-case": ("cases", ["UTF8"]),
                        "count-bool": ("roundtrips_verified", True), "lengths": ("uncompressed_lengths_verified", False),
                        "input-size": ("input_bytes", [53, 2 ** 30]), "input-utf8": ("input_bytes", [48, 4096]),
                        "runtime-empty": ("java_runtime_version", ""),
                        "vendor-empty": ("java_vendor", ""), "wrong-iid": ("image_id", "sha256:" + "2" * 64)}
                    if mutation in field_mutations:
                        key, value = field_mutations[mutation]
                        proof[key] = value
                    if mutation == "extra-key": proof["unexpected"] = True
                    if mutation == "missing-key": proof.pop("native_library_sha256")
                    if mutation == "class-origin": proof["class_code_sources"]["org.xerial.snappy.Snappy"] = "/old/snappy.jar"
                    if mutation == "class-missing": proof["class_code_sources"].pop("org.xerial.snappy.SnappyNative")
                    if mutation == "native-config": proof["native_configuration"]["use_systemlib"] = "true"
                    if mutation == "manifest-hash": manifest["payload_sha256"][PAYLOAD] = "0" * 64
                    if mutation == "manifest-schema": manifest["schema_version"] = True
                    if mutation == "receipt-fail": receipt["result"] = "FAIL"
                    if mutation == "recipe-mismatch": receipt["source_recipe_sha256"] = "b" * 64
                    if mutation == "child-exit": guard["child_exit"] = 1
                    if mutation == "boolean-exit": guard["exit"] = False
                    if mutation == "abort": guard["abort"] = "reserve reached"
                    if mutation == "missing-abort": guard.pop("abort")
                    if mutation == "cleanup": guard["cleanup_errors"] = ["uncertain"]
                    marker = PREFIX + json.dumps(proof) + "\n"
                    log = "bootstrap log\n" + marker
                    if mutation == "missing-marker": log = "prefix:" + marker
                    if mutation == "duplicate": log = marker + marker
                    if mutation == "malformed": log = PREFIX + "{incomplete\n"
                    (evidence / "summary.json").write_text(json.dumps(guard))
                    (evidence / "command.log").write_text(log)
                    (root / "lakehouse.iid").write_text("tag:latest" if mutation == "mutable-iid" else iid)
                    (root / "d2c-artifacts/manifest.json").write_text(json.dumps(manifest))
                    (root / ("d2c-lakehouse-artifact-verification-" + arch + ".json")).write_text(json.dumps(receipt))
                    with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ARCH": arch}):
                        if mutation:
                            with self.assertRaises((SystemExit, ValueError)): exec(code, {})
                            self.assertFalse((evidence / "runtime-proof.json").exists())
                        else:
                            exec(code, {})
                            actual = json.loads((evidence / "runtime-proof.json").read_text())
                            self.assertEqual(actual["image_id"], iid)
                            self.assertEqual(actual["platform"], "linux/" + arch)
                            self.assertEqual(actual["verified_source_jar_sha256"], source_sha)
                            self.assertEqual(actual["verified_source_native_sha256"], native_sha)
                            self.assertEqual(actual["remote_operations"], "NONE")

    def test_probe_has_bounded_inputs_metadata_and_actual_native_mapping_checks(self):
        source = (ROOT / "container-build/lakehouse/native/VerifySnappyRuntime.java").read_text()
        for required in ("Snappy.compress(input)", "Snappy.uncompress(compressed)", "new byte[4096]",
                         "Arrays.equals(input, restored)", "Snappy.uncompressedLength(compressed)",
                         "Snappy.getNativeLibraryVersion()", 'Path.of("/proc/self/maps")',
                         '"org/xerial/snappy/native/Linux/"', "nativeLibrarySha.equals(nativeResourceSha)",
                         '"META-INF/maven/org.xerial.snappy/snappy-java/pom.properties"', '"Implementation-Version"',
                         '"Bundle-Version"', "getProtectionDomain().getCodeSource()", "toRealPath()",
                         'PosixFilePermissions.fromString("rwx------")',
                         '"org.xerial.snappy.use.systemlib"', '"org.xerial.snappy.lib.path"',
                         '"org.xerial.snappy.disable.bundled.libs"', '"org.xerial.snappy.purejava"'):
            self.assertIn(required, source)
        for forbidden in ("setAccessible", "java.net.http", "Runtime.getRuntime().exec", "Snappy.rawUncompress", "Integer.MAX_VALUE"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
