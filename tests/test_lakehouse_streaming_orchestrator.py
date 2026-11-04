"""Offline control-plane checks; no Docker daemon or Spark is invoked."""
import argparse
import base64
import importlib.util
import json
from pathlib import Path
import signal
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location("streaming_orchestrator", Path(__file__).parents[1] / "scripts/test_lakehouse_streaming.py")
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)
FINAL = "sha256:" + "a" * 64
BROKER = "sha256:" + "b" * 64
FOREIGN = "e" * 64


class Engine:
    """A tiny ownership model, not a simulation of Kafka or Spark behavior."""
    def __init__(self):
        self.calls = []
        self.containers = {FOREIGN: {"Id": FOREIGN, "Name": "/user-work", "Config": {"Labels": {}}, "State": {"Running": True}}}
        self.volumes = {"user-data": {"Name": "user-data", "CreatedAt": "old", "Driver": "local", "Labels": {}}}
        self.networks = {"d" * 64: {"Id": "d" * 64, "Name": "user-net", "Labels": {}, "Containers": {}}}

    def run(self, *arguments, **_kwargs):
        self.calls.append(arguments)
        args = list(arguments)
        def done(value="", code=0, stderr=""):
            return subprocess.CompletedProcess(args, code, value if isinstance(value, str) else json.dumps(value), stderr)
        if args[:2] == ["image", "inspect"]:
            identity = FINAL if args[2] == FINAL else BROKER
            return done([{"Id": identity, "Os": "linux", "Architecture": "arm64", "Config": {"Volumes": {}, "Env": ["SECRET=must-not-appear"]}}])
        if args[:2] == ["image", "ls"]:
            return done(FINAL + "\n" + BROKER + "\n")
        if args[:2] == ["container", "ls"]:
            if "--filter" in args:
                kind, target = args[-1].split("=", 1)
                refs = [key for key, item in self.containers.items() if item.get(kind) == target]
                return done("\n".join(refs))
            return done("\n".join(self.containers))
        if args[:2] == ["volume", "ls"]:
            return done("\n".join(self.volumes))
        if args[:2] == ["network", "ls"]:
            return done("\n".join(self.networks))
        if len(args) > 1 and args[1] == "inspect":
            kind, target = args[0], args[2]
            data = {"container": self.containers, "volume": self.volumes, "network": self.networks}[kind]
            values = [item for key, item in data.items() if key == target or item.get("Name", "").lstrip("/") == target]
            return done(values) if values else done(code=1, stderr="Error response from daemon: No such " + kind + ": " + target)
        if args[:2] == ["network", "create"]:
            label = args[args.index("--label") + 1].split("=", 1)[1]
            identity = "1" * 64
            self.networks[identity] = {"Id": identity, "Name": args[-1], "Labels": {module.OWNER_LABEL: label}, "Containers": {}}
            return done(identity)
        if args[:2] == ["volume", "create"]:
            label = args[args.index("--label") + 1].split("=", 1)[1]
            self.volumes[args[-1]] = {"Name": args[-1], "CreatedAt": "new", "Driver": "local", "Labels": {module.OWNER_LABEL: label}}
            return done(args[-1])
        if args[0] == "run":
            name = args[args.index("--name") + 1]
            label = args[args.index("--label") + 1].split("=", 1)[1]
            identity = "2" * 64
            network = args[args.index("--network") + 1]
            self.containers[identity] = {"Id": identity, "Name": "/" + name, "Config": {"Labels": {module.OWNER_LABEL: label}}, "State": {"Running": True}, "network": network}
            next(item for item in self.networks.values() if item["Name"] == network)["Containers"][identity] = {}
            return done(identity)
        if args[0] == "logs":
            return done("Kafka Server started\n")
        if args[0] == "stop":
            identity = args[-1]
            self.containers.pop(identity)
            for network in self.networks.values():
                network["Containers"].pop(identity, None)
            return done(identity)
        if args[:2] == ["volume", "rm"]:
            self.volumes.pop(args[2]); return done(args[2])
        if args[:2] == ["network", "rm"]:
            self.networks.pop(args[2]); return done(args[2])
        raise AssertionError("Unexpected offline Docker command " + repr(args))


class Harness(module.StreamingTest):
    def __init__(self, options, engine):
        super().__init__(options)
        self.engine = engine
        self.guard_calls = []

    def docker(self, *args, **kwargs):
        return self.engine.run(*args, **kwargs)

    def guarded(self, label, name, command, *, timeout, stdin=None):
        self.attempt_container(name)
        self.guard_calls.append((self.guard_command(label, name, command, timeout)[0], command))
        if label == "init":
            payload = json.loads(stdin)
            return "D2C_STREAM_INIT=" + json.dumps({"source_sha256": payload["sha256"]})
        return module.PROOF_PREFIX + json.dumps({"phase": label, "status": "PASS", "image_id": FINAL, "owner": self.token})


class OrchestratorTest(unittest.TestCase):
    def setUp(self):
        self.scratch = TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.source = self.root / "phase.py"
        self.source.write_text("# offline source fixture\n")

    def options(self):
        return argparse.Namespace(output_dir=self.root / "evidence", image=FINAL,
            broker_image="apache/kafka:3.7.0", platform="linux/arm64", storage_path=[],
            phase_timeout_seconds=300, total_timeout_seconds=1200,
            guard_script=Path(__file__).parents[1] / "scripts/container_security_guard.py", phase_script=self.source)

    def instance(self):
        return module.StreamingTest(self.options())

    def test_full_offline_control_flow_preserves_original_resources(self):
        engine = Engine()
        instance = Harness(self.options(), engine)
        with patch.object(module, "available", return_value=50 * module.GIB):
            self.assertEqual(instance.run(), 0)
        self.assertEqual(instance.summary["outcome"], "PASS")
        self.assertEqual(set(instance.summary["phases"]), set(module.PHASES))
        self.assertEqual(set(engine.containers), {FOREIGN})
        self.assertEqual(set(engine.volumes), {"user-data"})
        self.assertEqual(set(engine.networks), {"d" * 64})
        self.assertNotIn("SECRET", json.dumps(instance.summary))
        self.assertEqual(len(instance.guard_calls), 5)
        self.assertFalse(any(call[:2] in (("image", "rm"), ("system", "prune")) for call in engine.calls))

    def test_phase_runs_are_independent_immutable_and_guarded(self):
        instance = Harness(self.options(), Engine())
        with patch.object(module, "available", return_value=50 * module.GIB):
            self.assertEqual(instance.run(), 0)
        names = []
        for guarded, command in instance.guard_calls[1:]:
            self.assertEqual(guarded[guarded.index("--kind") + 1], "test")
            self.assertEqual(guarded[guarded.index("--minimum-free-gib") + 1], "8")
            self.assertEqual(guarded[guarded.index("--reserve-free-gib") + 1], "6")
            self.assertEqual(guarded[guarded.index("--owner-token") + 1], instance.token)
            name = command[command.index("--name") + 1]; names.append(name)
            self.assertEqual(guarded[guarded.index("--container-name") + 1], name)
            self.assertIn(FINAL, command)
            self.assertEqual(command[command.index("--user") + 1], "185:0")
            self.assertEqual(command[command.index("--memory") + 1], "2000m")
            self.assertEqual(command[command.index("--cpus") + 1], "2")
            self.assertIn("type=volume,src=" + instance.names["volume"] + ",dst=/state", command)
            self.assertNotIn("type=bind", " ".join(command))
            self.assertNotIn("--publish", command)
            self.assertIn("--pull", command); self.assertIn("never", command)
        self.assertEqual(len(set(names)), 4)

    def test_init_copies_source_by_stdin_only_and_chowns_fresh_paths(self):
        instance = self.instance()
        captured = {}
        def guarded(label, name, command, **kwargs):
            captured.update(command=command, payload=json.loads(kwargs["stdin"]))
            return "D2C_STREAM_INIT=" + json.dumps({"source_sha256": captured["payload"]["sha256"]})
        with patch.object(instance, "guarded", side_effect=guarded):
            instance.initialize(FINAL)
        self.assertEqual(base64.b64decode(captured["payload"]["source_b64"]), self.source.read_bytes())
        self.assertIn("--interactive", captured["command"])
        self.assertIn("0:0", captured["command"])
        self.assertIn("os.chown(path, 185, 0)", module.INIT_SOURCE)
        self.assertIn("unexpectedly nonempty", module.INIT_SOURCE)

    def test_broker_support_service_has_bounded_resources_and_no_java_readiness_cli(self):
        engine = Engine(); instance = Harness(self.options(), engine)
        with patch.object(module, "available", return_value=50 * module.GIB):
            instance.run()
        command = next(call for call in engine.calls if call[0] == "run")
        self.assertEqual(command[command.index("--memory") + 1], "512m")
        self.assertEqual(command[command.index("--cpus") + 1], "1")
        self.assertIn("KAFKA_HEAP_OPTS=-Xms128m -Xmx256m", command)
        self.assertTrue(any(value.startswith("KAFKA_LOG_DIRS=/tmp/") for value in command))
        self.assertNotIn("--publish", command)
        self.assertFalse(any(call[0] == "exec" for call in engine.calls))

    def test_storage_admission_prevents_all_docker_calls(self):
        instance = self.instance()
        with patch.object(module, "available", return_value=7 * module.GIB), patch.object(instance, "docker") as docker:
            self.assertEqual(instance.run(), 1)
        docker.assert_not_called()

    def test_foreign_container_is_never_stopped_or_removed(self):
        instance = self.instance(); name = instance.names["initial"]
        instance.attempt_container(name)
        foreign = {"Id": "c" * 64, "Name": "/" + name, "Config": {"Labels": {}}, "State": {"Running": True}}
        with patch.object(instance, "inspect", return_value=foreign), patch.object(instance, "docker") as docker:
            with self.assertRaisesRegex(module.StreamingError, "ownership mismatch"):
                instance.cleanup_container(name)
        docker.assert_not_called()

    def test_referenced_volume_is_preserved(self):
        instance = self.instance()
        record = {"name": instance.names["volume"], "created_at": "now", "driver": "local"}
        instance.resources["volume"] = record
        data = {"Name": record["name"], "CreatedAt": "now", "Driver": "local", "Labels": {module.OWNER_LABEL: instance.token}}
        with patch.object(instance, "inspect", return_value=data), patch.object(instance, "checked", return_value=FOREIGN) as command:
            with self.assertRaisesRegex(module.StreamingError, "still referenced"):
                instance.cleanup_resource("volume")
        self.assertEqual(command.call_count, 1)
        self.assertEqual(command.call_args.args[:2], ("container", "ls"))

    def test_replaced_volume_identity_is_preserved(self):
        instance = self.instance()
        instance.resources["volume"] = {"name": instance.names["volume"], "created_at": "old", "driver": "local"}
        data = {"Name": instance.names["volume"], "CreatedAt": "replacement", "Driver": "local", "Labels": {module.OWNER_LABEL: instance.token}}
        with patch.object(instance, "inspect", return_value=data), patch.object(instance, "checked") as command:
            with self.assertRaisesRegex(module.StreamingError, "identity changed"):
                instance.cleanup_resource("volume")
        command.assert_not_called()

    def test_uncertain_guard_preserves_resources_without_global_kill(self):
        instance = self.instance()
        process = Mock()
        process.pid = 12345
        process.poll.return_value = None
        process.communicate.side_effect = subprocess.TimeoutExpired("guard", 300)
        with patch.object(module, "available", return_value=50 * module.GIB), patch.object(module.subprocess, "Popen", return_value=process):
            with self.assertRaisesRegex(module.StreamingError, "cancellation uncertain"):
                instance.guarded("initial", instance.names["initial"], ["docker", "run"], timeout=300)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        process.kill.assert_not_called()
        with patch.object(instance, "docker") as docker:
            instance.cleanup()
        docker.assert_not_called()
        self.assertTrue(instance.summary["cleanup"]["errors"])

    def test_only_confirmed_daemon_absence_counts(self):
        def result(text): return subprocess.CompletedProcess([], 1, "", text)
        self.assertTrue(module.absent("network", result("Error response from daemon: network mine not found"), "mine"))
        self.assertTrue(module.absent("volume", result("Error response from daemon: get mine: no such volume"), "mine"))
        self.assertFalse(module.absent("network", result("dial unix /docker.sock: no such file or directory"), "mine"))
        self.assertFalse(module.absent("network", result("Error response from daemon: network other not found"), "mine"))

    def test_duplicate_or_wrong_phase_proof_fails(self):
        instance = self.instance()
        proof = module.PROOF_PREFIX + json.dumps({"phase": "initial", "status": "PASS", "image_id": FINAL, "owner": instance.token})
        with patch.object(instance, "guarded", return_value=proof + "\n" + proof):
            with self.assertRaisesRegex(module.StreamingError, "ambiguous"):
                instance.phase("initial", FINAL)

    def test_insufficient_query_budget_never_dispatches_a_phase(self):
        instance = self.instance()
        with patch.object(instance, "budget", return_value=49), patch.object(instance, "guarded") as guarded:
            with self.assertRaisesRegex(module.StreamingError, "insufficient phase budget"):
                instance.phase("initial", FINAL)
        guarded.assert_not_called()

    def test_overall_deadline_reserves_guard_cancel_and_cleanup_before_new_dispatch(self):
        instance = self.instance(); instance.deadline = 1000
        with patch.object(module.time, "monotonic", return_value=800):
            self.assertEqual(instance.budget(), 50)
        with patch.object(module.time, "monotonic", return_value=851), patch.object(instance, "guarded") as guarded:
            with self.assertRaisesRegex(module.StreamingError, "150 seconds reserved"):
                instance.phase("initial", FINAL)
        guarded.assert_not_called()

    def test_guard_wait_and_cleanup_never_extend_overall_deadline(self):
        instance = self.instance(); instance.deadline = 1000
        with patch.object(module.time, "monotonic", return_value=850):
            self.assertEqual(instance.guard_wait(100), 90)
        with patch.object(module.time, "monotonic", return_value=941):
            with self.assertRaises(subprocess.TimeoutExpired):
                instance.guard_wait(45)
        with patch.object(module.time, "monotonic", return_value=995):
            instance.cleanup()
        self.assertEqual(instance.cleanup_deadline, 1000)
        with patch.object(module.time, "monotonic", return_value=1000), patch.object(module.subprocess, "run") as run:
            with self.assertRaisesRegex(module.StreamingError, "cleanup deadline"):
                instance.docker("network", "inspect", "owned", cleanup=True)
        run.assert_not_called()

    def test_existing_output_directory_is_preserved(self):
        options = self.options(); options.output_dir.mkdir(); sentinel = options.output_dir / "preserve"
        sentinel.write_text("original")
        with self.assertRaises(FileExistsError):
            module.StreamingTest(options)
        self.assertEqual(sentinel.read_text(), "original")


if __name__ == "__main__":
    unittest.main()
