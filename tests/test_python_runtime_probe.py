"""Offline checks for probe dispatch, HTTP deadlines and owned-child cleanup.

These mocks establish control flow only. They never claim that an image's
native libraries, actual HTTP server, database or Kafka connection passed.
"""
from contextlib import ExitStack, contextmanager, redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import signal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch


SPEC = importlib.util.spec_from_file_location("python_runtime_probe", Path(__file__).parents[1] / "scripts/check_python_runtime.py")
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class Child:
    """Only this fake Popen child can be terminated by the test."""
    def __init__(self, *, running=True, ignore_terminate=False):
        self.running = running
        self.ignore_terminate = ignore_terminate
        self.terminated = 0
        self.killed = 0
        self.waits = []

    def poll(self):
        return None if self.running else 0

    def terminate(self):
        self.terminated += 1
        if not self.ignore_terminate:
            self.running = False

    def kill(self):
        self.killed += 1
        self.running = False

    def wait(self, *, timeout):
        self.waits.append(timeout)
        if self.running:
            raise probe.subprocess.TimeoutExpired("owned fake child", timeout)
        return 0


def response(payload, *, status=200):
    result = Mock()
    result.__enter__ = Mock(return_value=result)
    result.__exit__ = Mock(return_value=False)
    result.status = status
    result.read.return_value = json.dumps(payload).encode()
    return result


@contextmanager
def mock_http(*, return_value=None, side_effect=None):
    request = Mock(return_value=return_value, side_effect=side_effect)
    with patch.object(probe.urllib.request, "build_opener", return_value=SimpleNamespace(open=request)):
        yield request


class RuntimeDispatchTest(unittest.TestCase):
    def test_each_role_dispatches_only_its_own_checks_and_restores_alarm(self):
        for role in probe.ROLES:
            with self.subTest(role=role), ExitStack() as stack:
                identity = {"role": role, "uid": 10001, "python": "3.12.16", "openssl": "fixture only"}
                stack.enter_context(patch.object(probe, "runtime_identity", return_value=identity))
                stack.enter_context(patch.object(probe.time, "monotonic", return_value=100))
                install = stack.enter_context(patch.object(probe.signal, "signal", return_value="previous-handler"))
                timer = stack.enter_context(patch.object(probe.signal, "setitimer"))
                workers = {name: stack.enter_context(patch.object(probe, name, return_value={"mock": name}))
                           for name in ("worker_probe", "api_probe", "catalog_probe")}
                result = probe.run(role)
                selected = role + "_probe"
                workers[selected].assert_called_once_with(*(() if role == "worker" else (145,)))
                for name in workers.keys() - {selected}:
                    workers[name].assert_not_called()
                self.assertEqual(result["status"], "PASS")
                self.assertIn("PostgreSQL", result["limits"])
                self.assertIn("Kafka broker", result["limits"])
                self.assertEqual(timer.call_args_list, [call(signal.ITIMER_REAL, 45), call(signal.ITIMER_REAL, 0)])
                self.assertEqual(install.call_args_list[-1], call(signal.SIGALRM, "previous-handler"))

    def test_failed_checks_still_restore_alarm(self):
        with patch.object(probe, "runtime_identity", return_value={"role": "worker"}), \
             patch.object(probe, "worker_probe", side_effect=probe.ProbeError("fixed check failed")), \
             patch.object(probe.signal, "signal", return_value="prior") as handler, \
             patch.object(probe.signal, "setitimer") as timer:
            with self.assertRaisesRegex(probe.ProbeError, "fixed check failed"):
                probe.run("worker")
        self.assertEqual(timer.call_args_list[-1], call(signal.ITIMER_REAL, 0))
        self.assertEqual(handler.call_args_list[-1], call(signal.SIGALRM, "prior"))

    def test_identity_requires_actual_uid_cpython_venv_and_wolfi(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(probe.sys, "implementation", SimpleNamespace(name="cpython")))
            stack.enter_context(patch.object(probe.sys, "version_info", (3, 12, 16)))
            stack.enter_context(patch.object(probe.sys, "prefix", "/opt/venv"))
            stack.enter_context(patch.object(probe.os, "getuid", return_value=10001))
            effective = stack.enter_context(patch.object(probe.os, "geteuid", return_value=10001))
            stack.enter_context(patch.object(probe.platform, "python_version", return_value="3.12.16"))
            release = stack.enter_context(patch.object(probe.Path, "read_text", return_value='ID="wolfi"\n'))
            packages = stack.enter_context(patch.object(probe.subprocess, "run", return_value=SimpleNamespace(stdout="python-3.12-3.12.16-r3\n")))
            identity = probe.runtime_identity("worker")
            self.assertEqual(identity["uid"], 10001)
            self.assertEqual(identity["os_id"], "wolfi")
            self.assertEqual(identity["apk_versions"], ["python-3.12-3.12.16-r3"])
            release.return_value = 'ID="chainguard"\nPRETTY_NAME="Chainguard"\n'
            identity = probe.runtime_identity("worker")
            self.assertEqual(identity["os_id"], "chainguard")
            self.assertEqual(identity["os_release"], release.return_value)
            self.assertEqual(packages.call_args.args[0], ["apk", "info", "-v"])
            effective.return_value = 0
            with self.assertRaisesRegex(probe.ProbeError, "UID 10001"):
                probe.runtime_identity("worker")

    def test_main_prints_one_fixed_prefix_and_redacts_arbitrary_failure_text(self):
        for failure, expected in ((None, 0), (RuntimeError("DATABASE_URL=private-value"), 1)):
            with self.subTest(failure=failure), patch.object(probe, "run", return_value={"status": "PASS", "role": "api"}, side_effect=failure):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(probe.main(["api"]), expected)
                lines = output.getvalue().splitlines()
                self.assertEqual(len(lines), 1)
                self.assertTrue(lines[0].startswith(probe.PREFIX))
                result = json.loads(lines[0][len(probe.PREFIX):])
                self.assertEqual(result["status"], "PASS" if failure is None else "FAIL")
                self.assertNotIn("private-value", lines[0])


class ServerLifecycleTest(unittest.TestCase):
    def setUp(self):
        blocker = patch.object(probe.socket, "socket", side_effect=AssertionError("offline fixtures must never open real sockets"))
        blocker.start()
        self.addCleanup(blocker.stop)

    def test_healthy_local_server_is_validated_and_its_only_child_reaped(self):
        child = Child()
        with patch.object(probe.subprocess, "Popen", return_value=child) as start, \
             mock_http(return_value=response({"status": "ok"})) as request, \
             patch.object(probe.time, "monotonic", return_value=10):
            result = probe.server_probe(["normal", "server"], {"http://127.0.0.1:8765/healthz": lambda item: item == {"status": "ok"}}, 30,
                                        environment={"SAFE": "fixture"})
        self.assertTrue(result["owned_child_reaped"])
        self.assertEqual(child.terminated, 1)
        self.assertEqual(child.killed, 0)
        start.assert_called_once_with(["normal", "server"], env={"SAFE": "fixture"}, stdout=probe.subprocess.DEVNULL, stderr=probe.subprocess.DEVNULL)
        request.assert_called_once_with("http://127.0.0.1:8765/healthz", timeout=1)

    def test_http_startup_timeout_is_bounded_and_child_cleanup_always_runs(self):
        child = Child(); clock = [0.0]
        with patch.object(probe.subprocess, "Popen", return_value=child), \
             mock_http(side_effect=TimeoutError("mock timeout")), \
             patch.object(probe.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(probe.time, "sleep", side_effect=lambda interval: clock.__setitem__(0, clock[0] + max(interval, 0.01))):
            with self.assertRaisesRegex(probe.ProbeError, "startup deadline"):
                probe.server_probe(["normal", "server"], {"http://127.0.0.1:8765/healthz": lambda _: True}, 6)
        self.assertLess(clock[0], 2)
        self.assertEqual(child.terminated, 1)
        self.assertTrue(child.waits)

    def test_expired_startup_budget_never_starts_a_child(self):
        with patch.object(probe.time, "monotonic", return_value=26), \
             patch.object(probe.subprocess, "Popen") as start, mock_http() as request:
            with self.assertRaisesRegex(probe.ProbeError, "deadline"):
                probe.server_probe(["normal"], {"http://127.0.0.1:8765/healthz": lambda _: True}, 30)
        start.assert_not_called()
        request.assert_not_called()

    def test_failed_payload_cannot_be_reported_as_healthy(self):
        child = Child()
        with patch.object(probe.subprocess, "Popen", return_value=child), \
             mock_http(return_value=response({"status": "wrong"})), \
             patch.object(probe.time, "monotonic", return_value=10):
            with self.assertRaisesRegex(probe.ProbeError, "payload failed"):
                probe.server_probe(["normal"], {"http://127.0.0.1:8765/healthz": lambda item: item == {"status": "ok"}}, 30)
        self.assertEqual(child.terminated, 1)

    def test_denied_loopback_socket_is_not_retried(self):
        child = Child()
        denied = probe.urllib.error.URLError(PermissionError(1, "Operation not permitted"))
        with patch.object(probe.subprocess, "Popen", return_value=child), \
             mock_http(side_effect=denied) as request, \
             patch.object(probe.time, "monotonic", return_value=10), \
             patch.object(probe.time, "sleep") as sleep:
            with self.assertRaisesRegex(probe.ProbeError, "access denied; no retry"):
                probe.server_probe(["normal"], {"http://127.0.0.1:8765/healthz": lambda _: True}, 30)
        request.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(child.terminated, 1)

    def test_exited_child_never_receives_a_termination_signal(self):
        child = Child(running=False)
        with patch.object(probe.subprocess, "Popen", return_value=child), mock_http() as request, \
             patch.object(probe.time, "monotonic", return_value=10):
            with self.assertRaisesRegex(probe.ProbeError, "exited before"):
                probe.server_probe(["normal"], {"http://127.0.0.1:8765/healthz": lambda _: True}, 30)
        request.assert_not_called()
        self.assertEqual(child.terminated, 0)
        self.assertEqual(child.killed, 0)

    def test_termination_timeout_kills_and_waits_only_for_owned_child(self):
        child = Child(ignore_terminate=True)
        with patch.object(probe.subprocess, "Popen", return_value=child), \
             mock_http(return_value=response({"status": "ok"})), \
             patch.object(probe.time, "monotonic", return_value=10):
            result = probe.server_probe(["normal"], {"http://127.0.0.1:8765/healthz": lambda _: True}, 30)
        self.assertTrue(result["owned_child_reaped"])
        self.assertEqual((child.terminated, child.killed), (1, 1))
        self.assertEqual(len(child.waits), 2)

    def test_external_http_endpoint_is_never_contacted(self):
        child = Child()
        with patch.object(probe.subprocess, "Popen", return_value=child), mock_http() as request, \
             patch.object(probe.time, "monotonic", return_value=10):
            with self.assertRaisesRegex(probe.ProbeError, "loopback"):
                probe.server_probe(["normal"], {"http://external.invalid:8765/healthz": lambda _: True}, 30)
        request.assert_not_called()
        self.assertEqual(child.terminated, 1)


if __name__ == "__main__":
    unittest.main()
