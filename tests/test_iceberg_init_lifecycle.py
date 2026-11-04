"""Offline task-path checks using Gradle 8.14.5's task-selection event order.

The fixture models project evaluation before task lookup and, with configuration
on demand, projectsEvaluated after lookup. It does not execute Groovy or Gradle;
the actual source build and input verification still have to pass in CI.
"""

from pathlib import Path
import re
import unittest


HELPERS = Path(__file__).resolve().parents[1] / "container-build/lakehouse/iceberg"
RUNTIME = ":iceberg-spark:iceberg-spark-runtime-4.2_2.13"
PROOF_TASK = "d2cRecordRuntimeInputs"


class TaskSelectionFixture:
    def __init__(self, hook, runtime_path):
        self.hook = hook
        self.runtime_path = runtime_path
        self.tasks = {path: {"shadowJar"} for path in (":", RUNTIME, ":iceberg-api")}
        self.events = []

    def evaluate(self, path):
        self.events.append(("evaluate", path))
        if self.hook == "afterProject" and path == self.runtime_path:
            self.tasks[path].add(PROOF_TASK)

    def projects_evaluated(self):
        self.events.append(("projectsEvaluated", None))
        if self.hook == "projectsEvaluated":
            self.tasks[self.runtime_path].add(PROOF_TASK)

    def select(self, path, *, configure_on_demand):
        if configure_on_demand:
            self.evaluate(path)
        else:
            for project in self.tasks:
                self.evaluate(project)
            self.projects_evaluated()
        self.events.append(("select", path))
        if PROOF_TASK not in self.tasks[path]:
            raise LookupError("requested proof task is absent at task selection")
        if configure_on_demand:
            self.projects_evaluated()


class IcebergInitLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.script = (HELPERS / "spark42-source.init.gradle").read_text()

    def registration(self):
        hook = re.findall(r"^gradle\.(\w+)\s*\{", self.script, re.MULTILINE)
        self.assertEqual(hook, ["afterProject"])
        target = re.findall(r"if \(component\.path == '([^']+)'\)", self.script)
        self.assertEqual(target, [RUNTIME])
        self.assertIn("def runtime = component", self.script)
        self.assertIn("runtime.tasks.register('d2cRecordRuntimeInputs')", self.script)
        return hook[0], target[0]

    def test_original_global_hook_is_too_late_only_with_configuration_on_demand(self):
        late = TaskSelectionFixture("projectsEvaluated", RUNTIME)
        with self.assertRaisesRegex(LookupError, "absent at task selection"):
            late.select(RUNTIME, configure_on_demand=True)
        self.assertNotIn(("projectsEvaluated", None), late.events)
        eager = TaskSelectionFixture("projectsEvaluated", RUNTIME)
        eager.select(RUNTIME, configure_on_demand=False)

    def test_current_project_hook_registers_before_lookup_in_both_modes(self):
        for enabled in (True, False):
            with self.subTest(configure_on_demand=enabled):
                fixture = TaskSelectionFixture(*self.registration())
                fixture.select(RUNTIME, configure_on_demand=enabled)
                self.assertLess(fixture.events.index(("evaluate", RUNTIME)),
                                fixture.events.index(("select", RUNTIME)))

    def test_proof_task_is_bound_to_the_requested_runtime_project_only(self):
        fixture = TaskSelectionFixture(*self.registration())
        fixture.evaluate(":iceberg-api")
        self.assertNotIn(PROOF_TASK, fixture.tasks[":iceberg-api"])
        command = (HELPERS / "rebuild-iceberg-spark42.sh").read_text()
        selected = re.findall(r"(:[\w:._-]+):d2cRecordRuntimeInputs", command)
        self.assertEqual(selected, [fixture.runtime_path])
        self.assertNotIn("gradle.rootProject.allprojects", self.script)

    def test_local_repository_uses_the_evaluated_project_uri_method(self):
        self.assertIn("url = component.uri('/rebuilt-repository')", self.script)
        self.assertNotIn("url = uri(", self.script)

    def test_record_task_still_depends_on_the_real_jar_and_hashes_actual_inputs(self):
        for required in (
            "dependsOn runtime.tasks.named('shadowJar')",
            "runtime.configurations.runtimeClasspath.resolvedConfiguration.resolvedArtifacts",
            "artifact.moduleVersion.id", "artifact.file.absolutePath",
            "MessageDigest.getInstance('SHA-256').digest(artifact.file.bytes)",
            "artifact.id.componentIdentifier instanceof org.gradle.api.artifacts.component.ProjectComponentIdentifier",
            "new File('/artifacts')", "security-runtime-artifacts.json", "runtime-dependencies.txt",
            "archiveVersion.set('1.12.0-d2c-security1')",
        ):
            self.assertIn(required, self.script)
        command = (HELPERS / "rebuild-iceberg-spark42.sh").read_text()
        self.assertLess(command.index(":d2cRecordRuntimeInputs"),
                        command.index('verify-source-runtime.py'))
        self.assertLess(command.index('verify-source-runtime.py'), command.index('cp "$d2c_iceberg_jar"'))


if __name__ == "__main__":
    unittest.main()
