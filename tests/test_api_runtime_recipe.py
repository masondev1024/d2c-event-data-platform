"""Offline contracts for the API's source-built Psycopg/system-libpq recipe."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]


def stages():
    text = (ROOT / "app/Dockerfile").read_text()
    matches = list(re.finditer(r"^FROM (\S+) AS (\S+)\s*$", text, re.M))
    return {
        match.group(2): (match.group(1), text[match.end():matches[index + 1].start() if index + 1 < len(matches) else None])
        for index, match in enumerate(matches)
    }


class ApiRuntimeRecipeTests(unittest.TestCase):
    def test_psycopg_has_one_numeric_version_pin_with_only_c_extra(self):
        requirements = (ROOT / "app/requirements.txt").read_text().splitlines()
        psycopg = [line.strip() for line in requirements if line.strip().lower().startswith("psycopg")]
        self.assertEqual(len(psycopg), 1)
        self.assertRegex(psycopg[0], r"\Apsycopg\[c\]==[0-9]+\.[0-9]+\.[0-9]+\Z")
        self.assertNotIn("binary", psycopg[0].lower())

    def test_runtime_uses_signed_fixed_family_without_build_dependencies(self):
        recipe = stages()
        self.assertIn("'libpq-18>=18.4'", recipe["runtime-base"][1])
        self.assertIn("'python-3.12>=3.12.16-r3'", recipe["runtime-base"][1])
        self.assertEqual(recipe["runtime"][0], "runtime-base")
        for stage in ("runtime-base", "runtime"):
            for package in ("build-base", "python-3.12-dev", "postgresql-18-dev", "py3.12-pip"):
                self.assertNotIn(package, recipe[stage][1])
        self.assertNotIn("--allow-untrusted", (ROOT / "app/Dockerfile").read_text())

    def test_c_extension_is_compiled_only_in_builder_and_only_venv_is_copied(self):
        recipe = stages()
        builder = recipe["dependencies"][1]
        for package in ("build-base", "'python-3.12-dev>=3.12.16-r3'", "'postgresql-18-dev>=18.4'"):
            self.assertIn(package, builder)
        self.assertIn("--no-binary=psycopg-c", builder)
        self.assertIn("-m venv --without-pip /opt/venv", builder)
        self.assertIn("--requirement requirements.txt", builder)
        copies = re.findall(r"^COPY --from=dependencies (.+)$", recipe["runtime"][1], re.M)
        self.assertEqual(copies, ["/opt/venv /opt/venv"])

    def test_both_stages_check_real_c_implementation_and_fixed_runtime_libpq(self):
        recipe = stages()
        for stage in ("dependencies", "runtime"):
            with self.subTest(stage=stage):
                self.assertIn('pq.__impl__ == "c"', recipe[stage][1])
                self.assertIn("180004 <= pq.version() < 190000", recipe[stage][1])
                self.assertIn("sys.exit(0 if", recipe[stage][1])


if __name__ == "__main__":
    unittest.main()
