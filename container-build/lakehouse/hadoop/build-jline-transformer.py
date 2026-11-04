"""Build a Maven Shade extension from local Java sources, before Hadoop assembly."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from patch_pom import TRANSFORMER_GAV

SHADE_VERSION = "3.6.1"
INSTALL_VERSION = "2.5.1"  # Already used by the original Hadoop 3.5.0 lifecycle.
PACKAGE = "d2c.shade.hadoop."
CLASS = "JlineProviderResourceTransformer"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(maven: str, work: Path, output: Path) -> None:
    source = Path(__file__).resolve().parent / "jline-transformer"
    production = source / f"src/main/java/{PACKAGE.replace('.', '/')}{CLASS}.java"
    contract = source / f"src/test/java/{PACKAGE.replace('.', '/')}{CLASS}SelfTest.java"
    pom = source / "pom.xml"
    work.mkdir(parents=True, exist_ok=False)
    classes, tests = work / "classes", work / "test-classes"
    classes.mkdir()
    tests.mkdir()
    artifact = work / "hadoop-jline-resource-transformer-1.0.0.jar"
    api = Path(f"/root/.m2/repository/org/apache/maven/plugins/maven-shade-plugin/{SHADE_VERSION}/maven-shade-plugin-{SHADE_VERSION}.jar")
    common = [maven, "--batch-mode", "--no-transfer-progress", "--file", str(pom)]
    commands = [
        ["java", "--version"], ["javac", "--version"],
        common + ["org.apache.maven.plugins:maven-dependency-plugin:3.8.1:get",
                  f"-Dartifact=org.apache.maven.plugins:maven-shade-plugin:{SHADE_VERSION}", "-Dtransitive=false"],
        ["javac", "--release", "17", "-cp", str(api), "-d", str(classes), str(production)],
        ["javac", "--release", "17", "-cp", f"{classes}:{api}", "-d", str(tests), str(contract)],
        ["java", "-cp", f"{tests}:{classes}:{api}", PACKAGE + CLASS + "SelfTest"],
        ["jar", "--create", "--file", str(artifact), "--date=2026-11-04T00:00:00Z", "-C", str(classes), "."],
        common + [f"org.apache.maven.plugins:maven-install-plugin:{INSTALL_VERSION}:install-file",
                  f"-Dfile={artifact}", f"-DpomFile={pom}", "-DgeneratePom=false"],
    ]
    log = output / "jline-transformer-build.log"
    with log.open("xb") as stream:
        for command in commands:
            stream.write((json.dumps({"command": command}) + "\n").encode())
            stream.flush()
            result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, check=False, cwd=work)
            if result.returncode:
                stream.flush()
                sys.stderr.write(log.read_text(errors="replace"))
                raise RuntimeError(f"JLine source-time adapter command failed with exit {result.returncode}")
    group, name, version = TRANSFORMER_GAV
    installed = Path("/root/.m2/repository") / group.replace(".", "/") / name / version / artifact.name
    if sha256(installed) != sha256(artifact):
        raise ValueError("installed build-only transformer differs from the compiled JAR")
    evidence = {"method": "authored source-time extension of the official Maven Shade ResourceTransformer API; no finished Hadoop JAR modification",
                "coordinate": ":".join(TRANSFORMER_GAV), "compiler_release": 17,
                "shade_api_version": SHADE_VERSION, "shade_api_jar_sha256": sha256(api),
                "shade_api_source_url": "https://github.com/apache/maven-shade-plugin/tree/maven-shade-plugin-3.6.1/src/main/java/org/apache/maven/plugins/shade/resource",
                "install_plugin_version": INSTALL_VERSION, "artifact_sha256": sha256(artifact),
                "installed_artifact_sha256": sha256(installed), "build_log_sha256": sha256(log),
                "source_sha256": {str(path.relative_to(source)): sha256(path) for path in (production, contract, pom)},
                "self_test": "PASS", "command_exit": 0, "runtime_dependency": False,
                "limits": "Compilation and transformer contracts only; final-image TerminalProvider.load(\"dumb\") remains separately required."}
    (output / "jline-transformer-build.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("jline_source_time_transformer_compile_and_contract=PASS;runtime_loader=NOT_RUN")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maven", required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.maven, args.work, args.output)
