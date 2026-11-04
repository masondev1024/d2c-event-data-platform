"""Read-only compatibility checks for Curator's official unshaded classifiers.

The 5.2.0 build attaches the full pre-Shade JAR as ``osgi``. These checks compare
that artifact with the standard artifact; they do not construct or edit a JAR.
Actual runtime linkage and D2C regression tests remain separate requirements.
"""

from __future__ import annotations

import hashlib
import io
import json
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
REBUILT_GROUP = "io.github.masondev1024.d2c.curator"
REBUILT_VERSION = "5.2.0-d2c-guava33.7.2-jre"
FIXED_GUAVA_VERSION = "33.7.2-jre"
FIXED_FAILUREACCESS_VERSION = "1.0.3"
FIXED_GUAVA_PREFIX = "org/apache/curator/shaded/com/google/"


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


def _gav_metadata(jar: ZipFile, names: set[str], group: str, artifact: str, version: str) -> dict:
    root = f"META-INF/maven/{group}/{artifact}/"
    paths = (root + "pom.properties", root + "pom.xml")
    require(all(path in names for path in paths), f"required dependency Maven metadata missing: {group}:{artifact}")
    expected = {"groupId": group, "artifactId": artifact, "version": version}
    actual = _properties(jar.read(paths[0]))
    require(all(actual.get(key) == value for key, value in expected.items()),
            f"incorrect dependency Maven properties: {group}:{artifact}:{version}")
    xml = ET.fromstring(jar.read(paths[1]))
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    xml_group = xml.findtext("m:groupId", namespaces=ns) or xml.findtext("m:parent/m:groupId", namespaces=ns)
    xml_version = xml.findtext("m:version", namespaces=ns) or xml.findtext("m:parent/m:version", namespaces=ns)
    require((xml_group, xml.findtext("m:artifactId", namespaces=ns), xml_version) == (group, artifact, version),
            f"incorrect dependency embedded POM GAV: {group}:{artifact}:{version}")
    return {path: hashlib.sha256(jar.read(path)).hexdigest() for path in paths}


def _own_classes(names: set[str]) -> set[str]:
    return {name for name in names if name.startswith(OWN_PREFIX)
            and not name.startswith(SHADED_PREFIX) and name.endswith(".class")}


def _compare_rebuilt_curator_api(rebuilt: ZipFile, rebuilt_names: set[str], baseline: ZipFile,
                                 baseline_names: set[str]) -> dict:
    classes, old_classes = _own_classes(rebuilt_names), _own_classes(baseline_names)
    require(bool(classes) and classes == old_classes,
            f"Curator own class set changed: removed={sorted(old_classes - classes)}, added={sorted(classes - old_classes)}")
    new_apis, old_apis = {}, {}
    for name in sorted(classes):
        actual, original = structural_class_api(rebuilt.read(name)), structural_class_api(baseline.read(name))
        require(actual["name"] == original["name"] == name[:-6], f"Curator class identity mismatch: {name}")
        require(actual["public_or_protected"] == original["public_or_protected"],
                f"Curator type visibility changed: {name}")
        new_apis[name[:-6]], old_apis[name[:-6]] = actual, original
    public_types = {name for name, api in old_apis.items() if api["public_or_protected"]}
    exposed, pending = set(public_types), list(public_types)
    while pending:
        api = old_apis[pending.pop()]
        for ancestor in [api["superclass"], *api["interfaces"]]:
            if ancestor in old_apis and ancestor not in exposed:
                exposed.add(ancestor)
                pending.append(ancestor)
    internal_changes = []
    for name, original in sorted(old_apis.items()):
        if name in exposed:
            require(new_apis[name] == original, f"Curator public/protected structural ABI changed: {name}.class")
        elif new_apis[name] != original:
            internal_changes.append(name + ".class")
    return {"own_classes_compared": len(classes), "public_protected_types_compared": len(public_types),
            "exposed_internal_ancestor_types_compared": sorted(exposed - public_types),
            "own_class_set_matches": True, "public_protected_structural_abi_matches": True,
            "internal_structural_changes": internal_changes}


def _class_binary_name(name: str) -> str:
    return re.sub(r"^META-INF/versions/[1-9][0-9]*/", "", name).removesuffix(".class")


def _canonical_json_sha256(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def _manifest_content(jar: ZipFile, names: set[str], label: str) -> bytes:
    require("META-INF/MANIFEST.MF" in names, f"original {label} manifest missing")
    content = jar.read("META-INF/MANIFEST.MF")
    require(bool(content.strip()), f"original {label} manifest empty")
    return content


def _require_multi_release_manifest(jar: ZipFile, names: set[str], implementations: set[str]) -> bool:
    """Versioned implementation entries need the JVM's manifest opt-in."""
    if not any(name.startswith("META-INF/versions/") for name in implementations):
        return False
    require("META-INF/MANIFEST.MF" in names, "multi-release Guava implementation requires a manifest")
    unfolded = []
    for line in jar.read("META-INF/MANIFEST.MF").decode("utf-8").splitlines():
        if not line:
            break  # Only the main section controls Multi-Release.
        if line.startswith(" "):
            require(bool(unfolded), "invalid Guava manifest continuation")
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)
    attributes = {}
    for line in unfolded:
        require(": " in line, "invalid Guava main manifest attribute")
        name, value = line.split(": ", 1)
        key = name.lower()
        require(key not in attributes, f"duplicate Guava main manifest attribute: {name}")
        attributes[key] = value
    require(attributes.get("multi-release", "").lower() == "true",
            "versioned Guava implementation requires main manifest Multi-Release: true")
    return True


def _fixed_library_inputs(path: Path, artifact: str, version: str) -> dict:
    """Inventory complete fixed implementation inputs, separately reporting JPMS."""
    with ZipFile(path) as jar:
        names = _unique_entries(jar)
        metadata = _gav_metadata(jar, names, "com.google.guava", artifact, version)
        implementation, module_descriptors, documents = {}, [], {}
        module_descriptor_hashes = {}
        for name in sorted(names):
            if name.endswith(".class"):
                if re.fullmatch(r"(?:META-INF/versions/[1-9][0-9]*/)?module-info\.class", name):
                    module_descriptors.append(name)
                    module_descriptor_hashes[name] = hashlib.sha256(jar.read(name)).hexdigest()
                    continue
                require(re.fullmatch(r"(?:META-INF/versions/[1-9][0-9]*/)?com/google/(?:common|thirdparty)/(?:[\w$]+/)*(?:[\w$]+|package-info)\.class", name) is not None,
                        f"unexpected fixed Guava implementation package: {artifact}: {name}")
                require(structural_class_api(jar.read(name))["name"] == _class_binary_name(name),
                        f"fixed Guava implementation identity mismatch: {artifact}: {name}")
                mapped = name.replace("com/google/", FIXED_GUAVA_PREFIX, 1)
                implementation[mapped] = name
            if not name.endswith("/") and re.search(r"(?:^|/)(?:LICENSE|NOTICE)(?:[._-][^/]*)?$", name, re.IGNORECASE):
                content = jar.read(name)
                require(bool(content.strip()), f"empty fixed library license/notice: {artifact}: {name}")
                documents[name] = hashlib.sha256(content).hexdigest()
        require(bool(implementation), f"fixed library contains no implementation: {artifact}")
        if artifact == "guava":
            require(any("LICENSE" in Path(name).name.upper() for name in documents),
                    "fixed Guava upstream license missing")
        multi_release = _require_multi_release_manifest(jar, names, set(implementation.values()))
        original_manifest = _manifest_content(jar, names, artifact)
        return {"artifact": artifact, "version": version, "jar_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "implementation": implementation, "metadata_sha256": metadata, "documents_sha256": documents,
                "jpms_module_descriptors": module_descriptors, "multi_release_implementation_enabled": multi_release,
                "jpms_module_descriptor_sha256": module_descriptor_hashes,
                "original_manifest_sha256": hashlib.sha256(original_manifest).hexdigest()}


def _validate_fixed_guava_references(name: str, content: bytes, available: set[str], *, class_file: bool) -> list[str]:
    """Require original Curator relocation and closure for Guava implementation references."""
    values, symbolic = [], set()
    if class_file:
        reader = ClassReader(content)
        require(reader.u4() == 0xCAFEBABE, f"invalid rebuilt class header: {name}")
        reader.u2(), reader.u2()
        reader.read_pool()
        values = [reader.utf8(i) for i, value in enumerate(reader.pool) if value is not None and value[0] == 1]
        for i, value in enumerate(reader.pool):
            if value is not None and value[0] == 7:
                target = reader.class_name(i)
                symbolic.update(re.findall(r"L([A-Za-z_$][\w$/]*);", target) if target.startswith("[") else [target])
        for value in values:
            symbolic.update(re.findall(r"L([A-Za-z_$][\w$/]*)[;<]", value))
    else:
        values = [content.decode("utf-8", errors="surrogateescape")]
        for line in values[0].splitlines():
            provider = line.split("#", 1)[0].strip()
            if provider:
                symbolic.add(provider.replace(".", "/"))
    references = set()
    for value in values:
        remaining = value.replace(FIXED_GUAVA_PREFIX, "").replace(FIXED_GUAVA_PREFIX.replace("/", "."), "")
        require(not any(token in remaining for token in ("com/google/common", "com/google/thirdparty",
                                                        "com.google.common", "com.google.thirdparty")),
                f"unexpected unrelocated Guava namespace: {name}")
        for separator in ("/", "."):
            escaped = re.escape(separator)
            pattern = (rf"(?:[A-Za-z_$][\w$]*{escaped})*com{escaped}google{escaped}"
                       rf"(?:common|thirdparty){escaped}[A-Za-z_$][\w$]*(?:{escaped}[A-Za-z_$][\w$]*)*")
            for target in re.findall(pattern, value):
                if target.startswith(("Lorg" + separator, "Lcom" + separator)):
                    target = target[1:]
                target = target.replace(separator, "/")
                require(target.startswith(FIXED_GUAVA_PREFIX), f"unexpected relocated Guava namespace: {name}: {target}")
    for target in symbolic:
        if "/com/google/common/" in target or "/com/google/thirdparty/" in target \
                or target.startswith(("com/google/common/", "com/google/thirdparty/")):
            require(target.startswith(FIXED_GUAVA_PREFIX), f"unexpected relocated Guava namespace: {name}: {target}")
            require(target in available, f"dangling fixed Guava implementation reference: {name}: {target}")
            references.add(target)
    return sorted(references)


def verify_curator_shaded_module(rebuilt_path: Path, osgi_path: Path, baseline_path: Path, module: str,
                                 fixed_guava_path: Path, failureaccess_path: Path, reference_path: Path, *,
                                 reviewed_metadata: dict[str, str] | None = None) -> dict:
    """Verify a normal source-time reassembly that preserves Curator's public ABI.

    The reference is another normal Maven Shade assembly of only the two fixed
    implementation inputs. Every embedded implementation class must be byte for
    byte identical to that reference, not merely have updated Maven metadata.
    The source-time build commands and exact POM/input binding are recorded by
    the caller; no compiled artifact is rewritten by this verifier.
    """
    require(module in MODULES, f"unsupported Curator module: {module}")
    rebuilt_path, osgi_path, baseline_path = Path(rebuilt_path), Path(osgi_path), Path(baseline_path)
    fixed_guava_path, failureaccess_path, reference_path = Path(fixed_guava_path), Path(failureaccess_path), Path(reference_path)
    inputs = [_fixed_library_inputs(fixed_guava_path, "guava", FIXED_GUAVA_VERSION),
              _fixed_library_inputs(failureaccess_path, "failureaccess", FIXED_FAILUREACCESS_VERSION)]
    mapped_classes = set()
    for library in inputs:
        require(not mapped_classes.intersection(library["implementation"]), "fixed Guava input implementation collision")
        mapped_classes.update(library["implementation"])
    available = {_class_binary_name(name) for name in mapped_classes}
    with ZipFile(reference_path) as reference, ZipFile(rebuilt_path) as rebuilt, \
            ZipFile(osgi_path) as osgi, ZipFile(baseline_path) as baseline:
        ref_names, names = _unique_entries(reference), _unique_entries(rebuilt)
        osgi_names, baseline_names = _unique_entries(osgi), _unique_entries(baseline)
        descriptor = lambda name: re.fullmatch(r"(?:META-INF/versions/[1-9][0-9]*/)?module-info\.class", name) is not None
        ref_descriptors = {name for name in ref_names if descriptor(name)}
        expected_descriptors = {name for library in inputs for name in library["jpms_module_descriptors"]
                                if name != "module-info.class"}
        require(ref_descriptors == expected_descriptors,
                "normal Shade retained versioned JPMS descriptor inventory changed")
        descriptor_hashes = {name: hashlib.sha256(reference.read(name)).hexdigest() for name in ref_descriptors}
        ref_classes = {name for name in ref_names if name.endswith(".class") and not descriptor(name)}
        require(ref_classes == mapped_classes,
                f"fixed Guava reference class inventory changed: missing={sorted(mapped_classes-ref_classes)}, extra={sorted(ref_classes-mapped_classes)}")
        reference_multi_release = _require_multi_release_manifest(reference, ref_names, mapped_classes)
        for library in inputs:
            for entry, digest in library["metadata_sha256"].items():
                require(entry in ref_names and hashlib.sha256(reference.read(entry)).hexdigest() == digest,
                        f"fixed Guava reference metadata changed/missing: {entry}")
            target = f"META-INF/d2c-upstream/{library['artifact']}/META-INF/MANIFEST.MF.original"
            require(target in ref_names and hashlib.sha256(reference.read(target)).hexdigest() == library["original_manifest_sha256"],
                    f"fixed Guava reference original manifest changed/missing: {target}")
        for entry in sorted(ref_classes):
            require(structural_class_api(reference.read(entry))["name"] == _class_binary_name(entry),
                    f"fixed Guava reference class identity mismatch: {entry}")
            _validate_fixed_guava_references(entry, reference.read(entry), available, class_file=True)
        for library in inputs:
            for entry, digest in library["jpms_module_descriptor_sha256"].items():
                backup = f"META-INF/d2c-upstream/{library['artifact']}/{entry}.original"
                require(backup in ref_names and hashlib.sha256(reference.read(backup)).hexdigest() == digest,
                        f"original fixed Guava JPMS descriptor not preserved: {backup}")
        identity = _metadata(rebuilt, names, module)
        require(identity == _metadata(osgi, osgi_names, module) == _metadata(baseline, baseline_names, module),
                f"original Curator project Maven metadata changed: {module}")
        custom_identity = _gav_metadata(rebuilt, names, REBUILT_GROUP, module, REBUILT_VERSION)
        require(_own_classes(osgi_names) == _own_classes(baseline_names), "OSGi input Curator own class inventory differs")
        osgi_metadata_mentions = {}
        for entry in sorted(osgi_names):
            content = b"" if entry.endswith("/") else osgi.read(entry)
            if validate_final_curator_guava_namespace(entry, content, reviewed_metadata=reviewed_metadata):
                osgi_metadata_mentions[entry] = hashlib.sha256(content).hexdigest()
            require(not (entry.endswith(".class") and "com/google/" in entry),
                    f"Guava implementation unexpectedly embedded in OSGi input: {entry}")
        documents, preserved_curator_documents = {}, {}
        for entry in ("META-INF/LICENSE", "META-INF/NOTICE"):
            require(entry in names and entry in osgi_names and entry in baseline_names,
                    f"original Curator document missing: {entry}")
            content = rebuilt.read(entry)
            require(bool(content.strip()) and content == osgi.read(entry) == baseline.read(entry),
                    f"original Curator document changed/empty: {entry}")
            documents[entry] = hashlib.sha256(content).hexdigest()
            target = f"META-INF/d2c-licenses/{module}/{entry}.original"
            require(target in names and rebuilt.read(target) == content,
                    f"qualified original Curator document changed/missing: {target}")
            preserved_curator_documents[target] = documents[entry]
        preserved_manifests = {}
        for label, source, source_names in (("osgi", osgi, osgi_names), ("standard", baseline, baseline_names)):
            original = _manifest_content(source, source_names, f"Curator {label}")
            target = f"META-INF/d2c-upstream/{module}/{label}/META-INF/MANIFEST.MF.original"
            require(target in names and rebuilt.read(target) == original,
                    f"original Curator {label} manifest changed/missing: {target}")
            preserved_manifests[target] = hashlib.sha256(original).hexdigest()
        embedded = mapped_classes if module == "curator-client" else set()
        actual_descriptors = {name for name in names if descriptor(name)}
        require(actual_descriptors == (ref_descriptors if module == "curator-client" else set()),
                "compatible Curator retained JPMS descriptor inventory changed")
        for entry in actual_descriptors:
            require(rebuilt.read(entry) == reference.read(entry),
                    f"compatible Curator JPMS descriptor differs from normal fixed-only reference: {entry}")
        classes = {entry for entry in names if entry.endswith(".class") and not descriptor(entry)}
        own = _own_classes(names)
        require(classes == own | embedded,
                f"unexpected rebuilt Curator implementation inventory: missing={sorted((own|embedded)-classes)}, extra={sorted(classes-(own|embedded))}")
        require({entry for entry in classes if entry.startswith(SHADED_PREFIX)} == embedded,
                "rebuilt Curator embedded Guava inventory differs from complete fixed inputs")
        rebuilt_multi_release = _require_multi_release_manifest(rebuilt, names, embedded)
        for entry in sorted(embedded):
            require(rebuilt.read(entry) == reference.read(entry), f"embedded fixed Guava bytecode differs from reference: {entry}")
        preserved_documents = {}
        if module == "curator-client":
            for library in inputs:
                for entry, digest in library["jpms_module_descriptor_sha256"].items():
                    backup = f"META-INF/d2c-upstream/{library['artifact']}/{entry}.original"
                    require(backup in names and hashlib.sha256(rebuilt.read(backup)).hexdigest() == digest,
                            f"client original fixed Guava JPMS descriptor lost/changed: {backup}")
        for library in inputs:
            for entry, digest in library["metadata_sha256"].items():
                if module == "curator-client":
                    require(entry in names and hashlib.sha256(rebuilt.read(entry)).hexdigest() == digest,
                            f"embedded fixed Guava metadata changed/missing: {entry}")
                else:
                    require(entry not in names, f"unexpected Guava metadata in non-client Curator module: {entry}")
            for entry, digest in library["documents_sha256"].items():
                target = f"META-INF/d2c-licenses/{library['artifact']}/{entry}.original"
                if module == "curator-client" or target in names:
                    require(target in names and hashlib.sha256(rebuilt.read(target)).hexdigest() == digest,
                            f"fixed Guava original license/notice changed/missing: {target}")
                    preserved_documents[target] = digest
            if module == "curator-client":
                target = f"META-INF/d2c-upstream/{library['artifact']}/META-INF/MANIFEST.MF.original"
                require(target in names and hashlib.sha256(rebuilt.read(target)).hexdigest() == library["original_manifest_sha256"],
                        f"fixed Guava original manifest changed/missing: {target}")
                preserved_manifests[target] = library["original_manifest_sha256"]
        refs = {}
        for entry in sorted(names):
            if entry.endswith(".class") and not descriptor(entry) or entry.startswith("META-INF/services/") and not entry.endswith("/"):
                targets = _validate_fixed_guava_references(entry, rebuilt.read(entry), available,
                                                           class_file=entry.endswith(".class"))
                if targets:
                    refs[entry] = targets
        api = _compare_rebuilt_curator_api(rebuilt, names, baseline, baseline_names)
        input_summaries = []
        for library in inputs:
            summary = {key: value for key, value in library.items() if key != "implementation"}
            summary["implementation_class_count"] = len(library["implementation"])
            summary["implementation_inventory_canonical_json_sha256"] = _canonical_json_sha256(library["implementation"])
            input_summaries.append(summary)
        ref_summary = {"entries_with_refs": len(refs),
                       "classes_with_refs": sum(name.endswith(".class") for name in refs),
                       "reference_count": sum(len(values) for values in refs.values()),
                       "distinct_target_class_count": len({value for values in refs.values() for value in values}),
                       "canonical_json_sha256": _canonical_json_sha256(refs)}
        return {"module": module, "original_version": VERSION,
                "coordinate": f"{REBUILT_GROUP}:{module}:{REBUILT_VERSION}",
                "method": "full official OSGi module assembled by Maven Shade with the original Curator com.google relocation and fixed complete Guava inputs",
                "rebuilt_sha256": hashlib.sha256(rebuilt_path.read_bytes()).hexdigest(),
                "osgi_input_sha256": hashlib.sha256(osgi_path.read_bytes()).hexdigest(),
                "standard_baseline_sha256": hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
                "fixed_guava_reference_sha256": hashlib.sha256(reference_path.read_bytes()).hexdigest(),
                "osgi_reviewed_unchanged_source_metadata_mentions": osgi_metadata_mentions,
                **api, "original_curator_metadata_sha256": identity, "custom_metadata_sha256": custom_identity,
                "unchanged_curator_document_sha256": documents,
                "preserved_curator_document_sha256": preserved_curator_documents,
                "preserved_fixed_library_document_sha256": preserved_documents,
                "preserved_original_manifest_sha256": preserved_manifests,
                "fixed_library_inputs": input_summaries, "fixed_reference_classes_compared": len(mapped_classes),
                "versioned_fixed_implementation_entries": sorted(name for name in mapped_classes if name.startswith("META-INF/versions/")),
                "reference_multi_release_implementation_enabled": reference_multi_release,
                "rebuilt_multi_release_implementation_enabled": rebuilt_multi_release,
                "embedded_fixed_implementation_classes_compared_byte_for_byte": len(embedded),
                "fixed_guava_symbolic_reference_closure": ref_summary,
                "inventory_digest_format": "SHA-256 of UTF-8 JSON with sort_keys=True, separators=(',', ':'), ensure_ascii=True; every implementation and symbolic reference is checked before output is summarized",
                "normal_shade_retained_versioned_jpms_descriptor_sha256": descriptor_hashes if module == "curator-client" else {},
                "jpms_note": "No new JPMS deletion filter is applied. Normal Shade omits root module-info.class and retains versioned descriptors; client/reference descriptor bytes must match and every raw input descriptor is preserved under a qualified .original path. This classpath assembly claims no JPMS/OSGi module identity. Every implementation class remains mandatory.",
                "limits": "Exact reference-class byte equality and public/protected ABI are checked. Source-time Maven input/POM/command provenance, final-image classloading, functional regressions and vulnerability scans are independent requirements."}
