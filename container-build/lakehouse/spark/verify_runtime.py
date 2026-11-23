"""Read-only Spark core structure and class-file ABI checks; no JAR rewriting."""

from __future__ import annotations

import argparse
from hashlib import sha256
import io
import json
from pathlib import Path
import struct
import zipfile


SPARK_PROPERTIES = "META-INF/maven/org.apache.spark/spark-core_2.13/pom.properties"
REQUIRED_JETTY_MODULES = {
    "jetty-http", "jetty-security", "jetty-server", "jetty-client",
    "jetty-ee10-servlet", "jetty-session", "jetty-compression-server",
}
REQUIRED_CLASSES = {
    "org/apache/spark/SparkContext.class",
    "org/apache/spark/SparkConf.class",
    "org/sparkproject/jetty/http/HttpParser.class",
    "org/sparkproject/jetty/server/Server.class",
    "org/sparkproject/jetty/ee10/servlet/ServletContextHandler.class",
}


def properties(content: bytes) -> dict[str, str]:
    result = {}
    for raw in content.decode("utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith(("#", "!")) and "=" in line:
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def verify_spark_identity(jar: zipfile.ZipFile, version: str) -> None:
    try:
        identity = properties(jar.read(SPARK_PROPERTIES))
    except KeyError as error:
        raise ValueError("Spark Maven identity metadata is missing") from error
    expected = {"groupId": "org.apache.spark", "artifactId": "spark-core_2.13", "version": version}
    if any(identity.get(key) != value for key, value in expected.items()):
        raise ValueError(f"Expected Spark core 2.13/{version}, found {identity}")


class ClassReader:
    """Read the structural parts of JVMS 4 class files without loading code."""

    def __init__(self, content: bytes):
        self.stream = io.BytesIO(content)
        self.pool = [None]

    def read(self, size: int) -> bytes:
        value = self.stream.read(size)
        if len(value) != size:
            raise ValueError("Truncated class file")
        return value

    def u1(self) -> int:
        return self.read(1)[0]

    def u2(self) -> int:
        return struct.unpack(">H", self.read(2))[0]

    def u4(self) -> int:
        return struct.unpack(">I", self.read(4))[0]

    def utf8(self, index: int) -> str:
        item = self.pool[index]
        if item is None or item[0] != 1:
            raise ValueError("Expected a class-file UTF8 constant")
        # Names/descriptors/signatures are UTF8; retain uncommon modified-UTF8
        # bytes reproducibly rather than guessing a different Unicode value.
        return item[1].decode("utf-8", errors="backslashreplace")

    def class_name(self, index: int) -> str | None:
        if index == 0:
            return None
        item = self.pool[index]
        if item is None or item[0] != 7:
            raise ValueError("Expected a class constant")
        return self.utf8(item[1])

    def constant(self, index: int):
        item = self.pool[index]
        if item[0] == 8:
            return [8, self.utf8(item[1])]
        return [item[0], item[1].hex()]

    def read_pool(self) -> None:
        count = self.u2()
        while len(self.pool) < count:
            tag = self.u1()
            if tag == 1:
                item = (tag, self.read(self.u2()))
            elif tag in (3, 4):
                item = (tag, self.read(4))
            elif tag in (5, 6):
                item = (tag, self.read(8))
            elif tag in (7, 8, 16, 19, 20):
                item = (tag, self.u2())
            elif tag in (9, 10, 11, 12, 17, 18):
                item = (tag, self.u2(), self.u2())
            elif tag == 15:
                item = (tag, self.u1(), self.u2())
            else:
                raise ValueError(f"Unsupported constant pool tag {tag}")
            self.pool.append(item)
            if tag in (5, 6):
                self.pool.append(None)

    def attributes(self) -> dict:
        result = {}
        for _ in range(self.u2()):
            name = self.utf8(self.u2())
            raw = self.read(self.u4())
            if name in ("Signature", "ConstantValue"):
                if len(raw) != 2:
                    raise ValueError(f"Invalid {name} attribute")
                index = struct.unpack(">H", raw)[0]
                result[name] = self.utf8(index) if name == "Signature" else self.constant(index)
            elif name == "Exceptions":
                if len(raw) < 2:
                    raise ValueError("Invalid Exceptions attribute")
                count = struct.unpack(">H", raw[:2])[0]
                if len(raw) != 2 + 2 * count:
                    raise ValueError("Invalid Exceptions attribute size")
                result[name] = [self.class_name(struct.unpack_from(">H", raw, 2 + 2 * i)[0]) for i in range(count)]
        return result

    def members(self) -> list:
        result = []
        for _ in range(self.u2()):
            access = self.u2()
            name = self.utf8(self.u2())
            descriptor = self.utf8(self.u2())
            attributes = self.attributes()
            if access & 0x0005:  # ACC_PUBLIC or ACC_PROTECTED
                result.append([name, descriptor, access, attributes])
        return sorted(result, key=lambda member: (member[0], member[1]))


def structural_class_api(content: bytes) -> dict:
    reader = ClassReader(content)
    if reader.u4() != 0xCAFEBABE:
        raise ValueError("Invalid class-file magic")
    minor, major = reader.u2(), reader.u2()
    reader.read_pool()
    access = reader.u2()
    name = reader.class_name(reader.u2())
    superclass = reader.class_name(reader.u2())
    interfaces = [reader.class_name(reader.u2()) for _ in range(reader.u2())]
    fields = reader.members()
    methods = reader.members()
    attributes = reader.attributes()
    if reader.stream.read(1):
        raise ValueError("Trailing bytes after class file")
    return {
        "name": name, "class_version": [major, minor], "access": access,
        "superclass": superclass, "interfaces": interfaces,
        "fields": fields, "methods": methods, "attributes": attributes,
    }


def verify_runtime(rebuilt: Path, baseline: Path, jetty_version: str = "12.1.14", spark_version: str = "4.2.1") -> dict:
    with zipfile.ZipFile(rebuilt) as new, zipfile.ZipFile(baseline) as old:
        verify_spark_identity(new, spark_version)
        verify_spark_identity(old, spark_version)
        names = set(new.namelist())
        if len(names) != len(new.namelist()):
            raise ValueError("Duplicate JAR entries")
        if REQUIRED_CLASSES - names:
            raise ValueError(f"Missing implementation classes: {sorted(REQUIRED_CLASSES - names)}")
        for name in sorted(REQUIRED_CLASSES):
            if structural_class_api(new.read(name))["name"] != name[:-6]:
                raise ValueError(f"Implementation class identity mismatch: {name}")
        if any(name.startswith("org/eclipse/jetty/") and name.endswith(".class") for name in names):
            raise ValueError("Unrelocated Jetty class in rebuilt Spark core")
        for document in ("META-INF/LICENSE", "META-INF/NOTICE"):
            if document not in names or not new.read(document).strip():
                raise ValueError(f"Missing upstream license document: {document}")
        jetty = {}
        for name in sorted(names):
            if name.startswith("META-INF/maven/org.eclipse.jetty") and name.endswith("/pom.properties"):
                identity = properties(new.read(name))
                if identity.get("version") != jetty_version:
                    raise ValueError(f"Jetty version mismatch: {name}: {identity.get('version')}")
                jetty[identity.get("artifactId", "")] = identity["version"]
        if REQUIRED_JETTY_MODULES - jetty.keys():
            raise ValueError(f"Jetty Maven metadata missing: {sorted(REQUIRED_JETTY_MODULES - jetty.keys())}")
        select = lambda entries: {name for name in entries if name.startswith("org/apache/spark/") and name.endswith(".class")}
        old_classes, new_classes = select(old.namelist()), select(names)
        if old_classes != new_classes:
            raise ValueError(f"Spark class set changed: removed={sorted(old_classes-new_classes)[:10]}, added={sorted(new_classes-old_classes)[:10]}")
        for name in sorted(old_classes):
            if structural_class_api(old.read(name)) != structural_class_api(new.read(name)):
                raise ValueError(f"Spark structural ABI changed: {name}")
    return {
        "spark_version": spark_version, "scala_binary_version": "2.13",
        "jetty_modules": jetty, "spark_classes_compared": len(old_classes),
        "rebuilt_sha256": sha256(rebuilt.read_bytes()).hexdigest(),
        "baseline_sha256": sha256(baseline.read_bytes()).hexdigest(),
        "structural_abi_matches_baseline": True,
        "license_and_notice_present": True,
        "limits": "Class-file structure is compared, not behavior, runtime linkage, annotations, or ScalaSignature payloads. Regression/startup tests and image scanning are separate requirements.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = verify_runtime(args.jar, args.baseline)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
