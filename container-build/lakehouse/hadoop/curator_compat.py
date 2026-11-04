"""Read-only compatibility checks for Curator's official unshaded classifiers.

The 5.2.0 build attaches the full pre-Shade JAR as ``osgi``. These checks compare
that artifact with the standard artifact; they do not construct or edit a JAR.
Actual runtime linkage and D2C regression tests remain separate requirements.
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path
import re
import struct
from zipfile import ZipFile
import xml.etree.ElementTree as ET


VERSION = "5.2.0"
MODULES = ("curator-client", "curator-framework", "curator-recipes")
OWN_PREFIX = "org/apache/curator/"
SHADED_PREFIX = OWN_PREFIX + "shaded/"
OLD_GUAVA = (b"org/apache/curator/shaded/com/google", b"org.apache.curator.shaded.com.google")
PUBLIC_OR_PROTECTED = 0x0005


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_final_curator_guava_namespace(name: str, content: bytes, *,
                                         reviewed_metadata: dict[str, str] | None = None) -> bool:
    """Reject retained old Guava references; report exact reviewed source POMs.

    The optional map is an exact JAR-entry-path -> official source SHA-256 map.
    An unchanged Curator source POM may document its former Shade configuration;
    that is build evidence rather than executable code or an active descriptor.
    Every class byte, other resource and resource path remains strict. The
    caller records a True return as a reviewed metadata mention, never silently
    removes or rewrites it. The default permits no metadata mentions.
    """
    path = name.encode("utf-8")
    require(not any(value in path for value in OLD_GUAVA),
            f"old Curator shaded Guava namespace retained in resource path: {name}")
    if not any(value in content for value in OLD_GUAVA):
        return False
    expected = (reviewed_metadata or {}).get(name)
    match = re.fullmatch(r"META-INF/maven/(org\.apache\.curator)/([A-Za-z0-9_.-]+)/pom\.xml", name)
    if match is not None and match.group(2) in (*MODULES, "apache-curator") \
            and isinstance(expected, str) and re.fullmatch(r"[a-f0-9]{64}", expected) \
            and hashlib.sha256(content).hexdigest() == expected:
        xml = ET.fromstring(content)
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        group = xml.findtext("m:groupId", namespaces=ns) or xml.findtext("m:parent/m:groupId", namespaces=ns)
        version = xml.findtext("m:version", namespaces=ns) or xml.findtext("m:parent/m:version", namespaces=ns)
        require((group, xml.findtext("m:artifactId", namespaces=ns), version)
                == (match.group(1), match.group(2), VERSION),
                f"reviewed Curator source POM context mismatch: {name}")
        return True
    raise ValueError(f"old Curator shaded Guava namespace retained: {name}")


class ClassReader:
    """Read JVMS structural ABI attributes without loading or executing code."""

    def __init__(self, content: bytes):
        self.stream = io.BytesIO(content)
        self.pool = [None]

    def read(self, size: int) -> bytes:
        value = self.stream.read(size)
        require(len(value) == size, "truncated Curator class file")
        return value

    def u1(self) -> int:
        return self.read(1)[0]

    def u2(self) -> int:
        return struct.unpack(">H", self.read(2))[0]

    def u4(self) -> int:
        return struct.unpack(">I", self.read(4))[0]

    def item(self, index: int, tags: tuple[int, ...]):
        require(0 < index < len(self.pool), "invalid Curator constant-pool index")
        value = self.pool[index]
        require(value is not None and value[0] in tags, "incorrect Curator constant-pool type")
        return value

    def utf8(self, index: int) -> str:
        # surrogateescape round-trips every original byte. Printable backslash
        # replacement would alias MUTF8 bytes with a distinct literal string.
        # This preserves comparison identity without claiming MUTF8 rendering.
        return self.item(index, (1,))[1].decode("utf-8", errors="surrogateescape")

    def class_name(self, index: int) -> str | None:
        return self.utf8(self.item(index, (7,))[1]) if index else None

    def constant(self, index: int):
        value = self.item(index, (3, 4, 5, 6, 8))
        # String constants use JVM modified UTF-8. Preserve their original
        # bytes: decoding errors as printable escapes can alias a different
        # valid Java string containing those literal backslashes.
        return ([8, self.item(value[1], (1,))[1].hex()] if value[0] == 8
                else [value[0], value[1].hex()])

    def read_pool(self) -> None:
        count = self.u2()
        require(count > 0, "empty Curator class constant pool")
        while len(self.pool) < count:
            tag = self.u1()
            if tag == 1:
                value = (tag, self.read(self.u2()))
            elif tag in (3, 4):
                value = (tag, self.read(4))
            elif tag in (5, 6):
                value = (tag, self.read(8))
            elif tag in (7, 8, 16, 19, 20):
                value = (tag, self.u2())
            elif tag in (9, 10, 11, 12, 17, 18):
                value = (tag, self.u2(), self.u2())
            elif tag == 15:
                value = (tag, self.u1(), self.u2())
            else:
                raise ValueError(f"unsupported Curator class constant-pool tag: {tag}")
            self.pool.append(value)
            if tag in (5, 6):
                self.pool.append(None)
        require(len(self.pool) == count, "invalid wide Curator constant-pool slot")

    def attributes(self) -> dict:
        result = {}
        for _ in range(self.u2()):
            name = self.utf8(self.u2())
            raw = self.read(self.u4())
            if name in ("Signature", "ConstantValue", "Exceptions", "InnerClasses"):
                require(name not in result, f"duplicate Curator structural attribute: {name}")
            if name in ("Signature", "ConstantValue"):
                require(len(raw) == 2, f"invalid Curator {name} attribute")
                index = struct.unpack(">H", raw)[0]
                result[name] = self.utf8(index) if name == "Signature" else self.constant(index)
            elif name == "Exceptions":
                require(len(raw) >= 2, "invalid Curator Exceptions attribute")
                count = struct.unpack_from(">H", raw)[0]
                require(len(raw) == 2 + 2 * count, "invalid Curator Exceptions attribute size")
                result[name] = [self.class_name(struct.unpack_from(">H", raw, 2 + 2 * i)[0])
                               for i in range(count)]
            elif name == "InnerClasses":
                require(len(raw) >= 2, "invalid Curator InnerClasses attribute")
                count = struct.unpack_from(">H", raw)[0]
                require(len(raw) == 2 + 8 * count, "invalid Curator InnerClasses attribute size")
                entries = []
                for i in range(count):
                    inner, outer, simple, access = struct.unpack_from(">HHHH", raw, 2 + 8 * i)
                    entries.append({"name": self.class_name(inner), "outer": self.class_name(outer),
                                    "simple_name": self.utf8(simple) if simple else None, "access": access})
                result[name] = entries
        return result

    def members(self) -> list:
        result = []
        for _ in range(self.u2()):
            access = self.u2()
            name = self.utf8(self.u2())
            descriptor = self.utf8(self.u2())
            attributes = self.attributes()
            require("InnerClasses" not in attributes, "InnerClasses attribute on Curator member")
            if access & PUBLIC_OR_PROTECTED:
                result.append([name, descriptor, access, attributes])
        return sorted(result, key=lambda value: (value[0], value[1]))


def structural_class_api(content: bytes) -> dict:
    reader = ClassReader(content)
    require(reader.u4() == 0xCAFEBABE, "invalid Curator class-file magic")
    minor, major = reader.u2(), reader.u2()
    reader.read_pool()
    access = reader.u2()
    name = reader.class_name(reader.u2())
    superclass = reader.class_name(reader.u2())
    interfaces = [reader.class_name(reader.u2()) for _ in range(reader.u2())]
    fields, methods, attributes = reader.members(), reader.members(), reader.attributes()
    require(not reader.stream.read(1), "trailing bytes after Curator class file")
    own_entries = [entry for entry in attributes.pop("InnerClasses", []) if entry["name"] == name]
    require(len(own_entries) <= 1, f"duplicate Curator nested-class identity: {name}")
    nested = own_entries[0] if own_entries else None
    # For nested types, actual Java visibility is in InnerClasses rather than
    # only access_flags. In particular this includes protected nested classes.
    visible_access = nested["access"] if nested is not None else access
    return {"name": name, "class_version": [major, minor], "access": access,
            "superclass": superclass, "interfaces": interfaces, "fields": fields,
            "methods": methods, "attributes": attributes, "nested_identity": nested,
            "public_or_protected": bool(visible_access & PUBLIC_OR_PROTECTED)}


def _properties(content: bytes) -> dict[str, str]:
    result = {}
    for line in content.decode("utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith(("#", "!")):
            continue
        require("=" in line, "unexpected Curator Maven properties format")
        key, value = (value.strip() for value in line.split("=", 1))
        require(key not in result, f"duplicate Curator Maven property: {key}")
        result[key] = value
    return result


def _unique_entries(jar: ZipFile) -> set[str]:
    names = jar.namelist()
    require(len(names) == len(set(names)), "duplicate Curator JAR entry")
    require(all(not name.startswith("/") and ".." not in name.split("/") for name in names),
            "unsafe Curator JAR entry path")
    return set(names)


def _metadata(jar: ZipFile, names: set[str], module: str) -> dict:
    root = f"META-INF/maven/org.apache.curator/{module}/"
    paths = (root + "pom.properties", root + "pom.xml")
    require(all(path in names for path in paths), f"Curator Maven identity metadata missing: {module}")
    expected = {"groupId": "org.apache.curator", "artifactId": module, "version": VERSION}
    actual = _properties(jar.read(paths[0]))
    require(all(actual.get(key) == value for key, value in expected.items()),
            f"incorrect Curator Maven identity: {module}")
    xml = ET.fromstring(jar.read(paths[1]))
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    group = xml.findtext("m:groupId", namespaces=ns) or xml.findtext("m:parent/m:groupId", namespaces=ns)
    version = xml.findtext("m:version", namespaces=ns) or xml.findtext("m:parent/m:version", namespaces=ns)
    require((group, xml.findtext("m:artifactId", namespaces=ns), version)
            == ("org.apache.curator", module, VERSION), f"incorrect Curator embedded POM GAV: {module}")
    return {path: hashlib.sha256(jar.read(path)).hexdigest() for path in paths}


def verify_curator_module(osgi_path: Path, baseline_path: Path, module: str, *,
                          reviewed_metadata: dict[str, str] | None = None) -> dict:
    """Compare one official full unshaded module against its standard artifact."""
    require(module in MODULES, f"unsupported Curator module: {module}")
    osgi_path, baseline_path = Path(osgi_path), Path(baseline_path)
    with ZipFile(osgi_path) as selected, ZipFile(baseline_path) as baseline:
        names, old_names = _unique_entries(selected), _unique_entries(baseline)
        identity, old_identity = _metadata(selected, names, module), _metadata(baseline, old_names, module)
        require(identity == old_identity, f"Curator project Maven metadata changed: {module}")
        documents = {}
        for name in ("META-INF/LICENSE", "META-INF/NOTICE"):
            require(name in names and name in old_names, f"Curator upstream document missing: {name}")
            content = selected.read(name)
            require(bool(content.strip()) and content == baseline.read(name),
                    f"Curator upstream document changed/empty: {name}")
            documents[name] = hashlib.sha256(content).hexdigest()
        metadata_mentions = {}
        for name in sorted(names):
            content = b"" if name.endswith("/") else selected.read(name)
            if validate_final_curator_guava_namespace(name, content, reviewed_metadata=reviewed_metadata):
                metadata_mentions[name] = hashlib.sha256(content).hexdigest()
            require(not (name.endswith(".class") and re.search(r"(?:^|/)com/google/(?:common|thirdparty)/", name)),
                    f"Guava implementation embedded in selected Curator OSGi artifact: {name}")
        own = lambda entries: {name for name in entries if name.startswith(OWN_PREFIX)
                               and not name.startswith(SHADED_PREFIX) and name.endswith(".class")}
        old_classes, classes = own(old_names), own(names)
        require(bool(classes), f"no Curator implementation classes: {module}")
        require(classes == old_classes,
                f"Curator own class set changed: removed={sorted(old_classes - classes)}, added={sorted(classes - old_classes)}")
        selected_apis, baseline_apis = {}, {}
        for name in sorted(classes):
            actual, original = structural_class_api(selected.read(name)), structural_class_api(baseline.read(name))
            require(actual["name"] == original["name"] == name[:-6], f"Curator class identity mismatch: {name}")
            require(actual["public_or_protected"] == original["public_or_protected"],
                    f"Curator type visibility changed: {name}")
            selected_apis[name[:-6]], baseline_apis[name[:-6]] = actual, original
        public_types = {name for name, api in baseline_apis.items() if api["public_or_protected"]}
        # A package-private base can contribute inherited public/protected API
        # to a public subclass. Compare those own ancestors as well, rather than
        # dismissing their members as implementation details based on visibility.
        exposed = set(public_types)
        pending = list(exposed)
        while pending:
            api = baseline_apis[pending.pop()]
            for ancestor in [api["superclass"], *api["interfaces"]]:
                if ancestor in baseline_apis and ancestor not in exposed:
                    exposed.add(ancestor)
                    pending.append(ancestor)
        internal_changes = []
        for binary_name in sorted(baseline_apis):
            actual, original = selected_apis[binary_name], baseline_apis[binary_name]
            if binary_name in exposed:
                require(actual == original, f"Curator public/protected structural ABI changed: {binary_name}.class")
            elif actual != original:
                internal_changes.append(binary_name + ".class")
        return {"module": module, "version": VERSION, "selected_classifier": "osgi",
                "osgi_sha256": hashlib.sha256(osgi_path.read_bytes()).hexdigest(),
                "standard_sha256": hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
                "own_classes_compared": len(classes), "public_protected_types_compared": len(public_types),
                "exposed_internal_ancestor_types_compared": sorted(exposed - public_types),
                "own_class_set_matches": True, "public_protected_structural_abi_matches": True,
                "internal_structural_changes": internal_changes,
                "old_shaded_guava_executable_namespace_absent": True,
                "reviewed_unchanged_source_metadata_mentions": metadata_mentions,
                "embedded_guava_implementation_absent": True,
                "maven_metadata_sha256": identity, "unchanged_upstream_document_sha256": documents,
                "limits": "Public/protected type headers, nested identity, member descriptors, generic signatures, constants and exceptions are compared. Internal type differences are reported. Executable behavior, annotation payloads and runtime linkage require separate final-image regression tests."}
