"""Behavioral checks for admission, bounded cancellation and ownership safety."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("container_guard", Path(__file__).resolve().parents[1] / "scripts/container_security_guard.py")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)

    def args(self, **overrides):
        values = dict(kind="test", evidence_dir=str(self.root / "evidence"), storage_path=[str(self.root)],
                      reserve_free_gib=6, minimum_free_gib=8, timeout_seconds=20,
                      command=[sys.executable, "-c", "print('isolated guard test')"],
                      container_name=None, owner_token=None, iceberg_source_build=False, build_state_dir=str(self.root / "builder"))
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_iceberg_memory_probe_reads_container_ancestor_and_private_namespace_root(self):
        mount = self.root / "cgroup"
        cid = "f" * 64
        scope = mount / "system.slice" / ("docker-" + cid + ".scope")
        scope.mkdir(parents=True)
        membership = self.root / "membership"
        for directory, relative, mode in ((scope, "/system.slice/docker-" + cid + ".scope/init", "host"),
                                           (mount, "/init", "private")):
            for name, value in {"memory.max": "4096", "memory.current": "1800", "memory.peak": "2100",
                                "memory.swap.current": "0", "memory.stat": "anon 1200\nfile 400\n",
                                "memory.events": "oom 2\noom_kill 1\n"}.items():
                (directory / name).write_text(value)
            membership.write_text("0::" + relative + "\n")
            cmd = ["sh", str(guard.MEMORY_PROBE), str(mount), str(membership), cid, "4096", mode]
            actual = subprocess.check_output(cmd, text=True)
            self.assertEqual(dict(row.split() for row in actual.splitlines()), {
                "current_bytes": "1800", "anon_bytes": "1200", "file_bytes": "400",
                "peak_bytes": "2100", "swap_bytes": "0", "oom": "2", "oom_kill": "1"})
        # A matching numeric limit alone cannot identify a foreign host cgroup.
        for limit, mode in [("4096", "host"), ("9999", "private")]:
            self.assertNotEqual(subprocess.run(cmd[:-2] + [limit, mode], capture_output=True).returncode, 0)

    def test_ci_rejects_missing_builder_memory_evidence(self):
        instance = guard.Guard(self.args())
        instance.env["GITHUB_ACTIONS"] = "true"
        instance.state["builder"] = {}
        instance.memory_probe = ["exec", "owned-cid"]
        with patch.object(guard, "available", return_value=50 * guard.GIB), \
                patch.object(instance, "docker", return_value=subprocess.CompletedProcess([], 0, "oom 0", "")):
            with self.assertRaisesRegex(guard.GuardError, "requires builder memory"):
                instance.sample()
        self.assertIn("incomplete", instance.state["builder"]["memory_probe_error"])

    def test_builder_profiles_enforce_the_configured_limit(self):
        for iceberg, mib in ((False, 2500), (True, 4096)):
            with self.subTest(iceberg=iceberg):
                folder = self.root / str(mib)
                instance = guard.Guard(self.args(kind="build", iceberg_source_build=iceberg,
                    evidence_dir=str(folder / "evidence"), build_state_dir=str(folder / "builder"),
                    command=["docker", "build", "."]))
                data = {"Id": "a" * 64, "HostConfig": {"Memory": mib * 2**20, "CgroupnsMode": "private"}, "State": {"Pid": 1, "Running": False}}
                with patch.object(instance, "sample"), patch.object(instance, "run", return_value=0) as run, \
                        patch.object(instance, "inspect_owned", return_value=data):
                    instance.prepare_build()
                    self.assertIn(f"memory={mib}m", run.call_args_list[0].args[1])
                    self.assertEqual(instance.state["builder"]["memory_limit_bytes"], mib * 2**20)
                    data["HostConfig"]["Memory"] = 0
                    with self.assertRaisesRegex(guard.GuardError, "memory limit"):
                        instance.prepare_build()

    def test_admission_never_starts_child(self):
        instance = guard.Guard(self.args())
        with patch.object(guard, "available", return_value=7 * guard.GIB), patch.object(guard.subprocess, "Popen") as spawn:
            self.assertEqual(instance.execute(), 75)
        spawn.assert_not_called()
        self.assertEqual(instance.state["outcome"], "blocked")
        self.assertIsNone(instance.state["child_exit"])

    def test_normal_security_exit_is_preserved(self):
        instance = guard.Guard(self.args(command=[sys.executable, "-c", "raise SystemExit(1)"]))
        with patch.object(guard, "available", return_value=50 * guard.GIB):
            self.assertEqual(instance.execute(), 1)
        self.assertEqual(instance.state["outcome"], "child_failed")
        self.assertEqual(instance.state["child_exit"], 1)
        self.assertIsNone(instance.state["abort"])

    def test_timeout_reaps_only_its_own_process_group(self):
        instance = guard.Guard(self.args(timeout_seconds=0.05, command=[sys.executable, "-c", "import time; time.sleep(60)"]))
        with patch.object(guard, "available", return_value=50 * guard.GIB):
            self.assertEqual(instance.execute(), 124)
        self.assertEqual(instance.state["outcome"], "aborted")
        self.assertIsNotNone(instance.proc.poll())
        self.assertEqual(instance.state["cleanup_errors"], [])

    def test_reserve_breach_aborts_and_reaps_child(self):
        instance = guard.Guard(self.args(command=[sys.executable, "-c", "import time; time.sleep(60)"]))
        space = iter([50 * guard.GIB, 50 * guard.GIB, 50 * guard.GIB, 5 * guard.GIB])
        with patch.object(guard, "available", side_effect=lambda _: next(space)):
            self.assertEqual(instance.execute(), 70)
        self.assertEqual(instance.state["abort"], "storage reserve reached")
        self.assertIsNotNone(instance.proc.poll())

    def test_inspection_failure_never_stops_a_foreign_container(self):
        instance = guard.Guard(self.args())
        state = {"Id": "foreign", "Config": {"Labels": {}, "Env": []}, "State": {"Running": True}}
        with patch.object(instance, "docker", return_value=subprocess.CompletedProcess([], 0, json.dumps([state]), "")) as docker:
            instance.stop_owned("example", "our-token")
        self.assertEqual(docker.call_count, 1)
        self.assertIn("ownership mismatch", instance.state["cleanup_errors"][0])

    def test_container_admission_failure_does_not_stop_preexisting_work(self):
        instance = guard.Guard(self.args(container_name="preexisting", owner_token="token"))
        with patch.object(guard, "available", return_value=1 * guard.GIB), patch.object(instance, "stop_owned") as stop:
            self.assertEqual(instance.execute(), 75)
        stop.assert_not_called()

    def test_busy_private_builder_is_not_stopped(self):
        args = self.args(kind="build", command=["docker", "build", "."])
        folder = Path(args.build_state_dir)
        folder.mkdir()
        configuration = b"[worker.oci]\n  max-parallelism = 1\n"
        (folder / "builder.json").write_text(json.dumps({"name": "d2c-security-" + "a" * 24, "token": "b" * 32,
            "configuration_sha256": hashlib.sha256(configuration).hexdigest()}))
        (folder / "buildkitd.toml").write_bytes(configuration)
        instance = guard.Guard(args)
        with patch.object(guard, "available", return_value=50 * guard.GIB), patch.object(instance, "inspect_owned", return_value={"State": {"Running": True}}), patch.object(instance, "stop_owned") as stop:
            self.assertEqual(instance.execute(), 70)
        stop.assert_not_called()
        self.assertIn("already running", instance.state["abort"])

    def test_cleanup_uncertainty_overrides_success(self):
        instance = guard.Guard(self.args(container_name="mine", owner_token="token"))
        def uncertain(*_): instance.state["cleanup_errors"].append("engine disconnected")
        with patch.object(guard, "available", return_value=50 * guard.GIB), patch.object(instance, "docker", return_value=subprocess.CompletedProcess([], 1, "", "Error response from daemon: No such container: mine")), patch.object(instance, "run", return_value=0), patch.object(instance, "stop_owned", side_effect=uncertain):
            self.assertEqual(instance.execute(), 70)
        self.assertEqual(instance.state["outcome"], "cleanup_uncertain")

    def test_missing_socket_is_not_container_absence(self):
        instance = guard.Guard(self.args())
        failure = subprocess.CompletedProcess([], 1, "", "dial unix /docker.sock: connect: no such file or directory")
        with patch.object(instance, "docker", return_value=failure):
            with self.assertRaises(guard.GuardError): instance.inspect_owned("mine", "token")

    def test_daemon_confirmed_missing_container_is_absent(self):
        instance = guard.Guard(self.args())
        missing = subprocess.CompletedProcess([], 1, "", "Error response from daemon: No such container: mine")
        with patch.object(instance, "docker", return_value=missing):
            self.assertIsNone(instance.inspect_owned("mine", "token"))

    def test_fresh_evidence_required(self):
        guard.Guard(self.args())
        with self.assertRaises(FileExistsError): guard.Guard(self.args())

    def test_missing_monitor_path_fails_before_spawn(self):
        with self.assertRaises(guard.GuardError): guard.Guard(self.args(storage_path=[str(self.root / "missing")]))

    def test_local_build_uses_private_builder_and_load(self):
        command, outputs = guard.build_arguments(["docker", "build", "--tag", "probe", "."], "private")
        self.assertEqual(command, ["docker", "buildx", "build", "--builder", "private", "--load", "--tag", "probe", "."])
        self.assertEqual(outputs, [])

    def test_oci_attestations_preserved_without_load(self):
        target = self.root / "layout"
        command, outputs = guard.build_arguments(["docker", "buildx", "build", "--output", f"type=oci,dest={target},tar=false,compression=gzip", "--provenance=mode=min", "--sbom=true", "."], "private")
        self.assertNotIn("--load", command)
        self.assertIn("--provenance=mode=min", command)
        self.assertIn("--sbom=true", command)
        self.assertEqual(outputs, [target.resolve()])

    def test_term_ignoring_descendant_is_killed_even_after_command_exits(self):
        source = "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)']); print(p.pid,flush=True); time.sleep(.1)"
        instance = guard.Guard(self.args(command=[sys.executable, "-c", source]))
        child = None
        try:
            with patch.object(guard, "available", return_value=50 * guard.GIB):
                code = instance.execute()
            child = int((instance.out / "command.log").read_text().strip())
            self.assertEqual(code, 0)
            self.assertEqual(instance.state["outcome"], "completed")
            self.assertEqual(instance.state["cleanup_errors"], [])
            with self.assertRaises(ProcessLookupError): os.kill(child, 0)
        finally:
            if child is not None:
                try: os.kill(child, signal.SIGTERM)
                except ProcessLookupError: pass

    def test_remote_or_ambiguous_build_outputs_rejected(self):
        for flags in (["--push"], ["--builder=foreign"], ["--cache-to=type=registry,ref=remote"],
                      ["-otype=registry"], ["--output=type=image,push=true"], ["--output", "type=oci,dest=foo"],
                      ["--load", "--output", f"type=oci,dest={self.root / 'new'},tar=false"]):
            with self.subTest(flags=flags), self.assertRaises(guard.GuardError):
                guard.build_arguments(["docker", "build", *flags, "."], "private")

    def test_existing_output_is_never_overwritten(self):
        target = self.root / "layout"
        target.mkdir()
        (target / "proof").write_text("preserve")
        with self.assertRaises(guard.GuardError):
            guard.build_arguments(["docker", "build", "-o", f"type=oci,dest={target},tar=false", "."], "private")
        self.assertEqual((target / "proof").read_text(), "preserve")

    def test_global_lock_blocks_second_invocation_without_spawning(self):
        path = self.root / "global.lock"
        with path.open("a") as held, patch.object(guard, "LOCK_PATH", path), patch.object(guard, "Guard") as spawn:
            guard.fcntl.flock(held, guard.fcntl.LOCK_EX | guard.fcntl.LOCK_NB)
            code = guard.main(["--evidence-dir", str(self.root / "evidence"), "--", "true"])
        self.assertEqual(code, 75)
        spawn.assert_not_called()

    def test_non_finite_limits_rejected(self):
        with self.assertRaises(SystemExit) as caught:
            guard.main(["--evidence-dir", str(self.root / "evidence"), "--minimum-free-gib", "nan", "--", "true"])
        self.assertEqual(caught.exception.code, 2)


if __name__ == "__main__": unittest.main()
