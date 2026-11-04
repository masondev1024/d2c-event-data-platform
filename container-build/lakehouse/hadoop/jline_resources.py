"""Exact JLine 3.30.17 resource contracts; no JAR mutation happens here."""

import hashlib
import re
from zipfile import ZipFile

VERSION = "3.30.17"
SOURCE_ROOT = f"https://raw.githubusercontent.com/jline/jline3/jline-{VERSION}/"
SHADED_PREFIX = "org.apache.hadoop.shaded."
MODERN_PREFIX = "META-INF/jline/providers/"
LEGACY_PREFIX = "META-INF/services/org/jline/terminal/provider/"
PROVIDERS = {
    MODERN_PREFIX + "dumb": ("org.jline.terminal.impl.DumbTerminalProvider", "48252719e3f40a0eb6197ce9ad989c179103b807e91b17f31868c0835ce08056"),
    MODERN_PREFIX + "exec": ("org.jline.terminal.impl.exec.ExecTerminalProvider", "cc3e8b83e01f2550d8e9369b3763ab42b88122667cfa3f85998d252871e18b86"),
    MODERN_PREFIX + "jni": ("org.jline.terminal.impl.jni.JniTerminalProvider", "7ec5c51d27102bdc4a083a76de447eb2c519424f4d8caf72e2ab9bbeebd52757"),
    MODERN_PREFIX + "ffm": ("org.jline.terminal.impl.ffm.FfmTerminalProvider", "ada38b6c74ee716d369c9f462f719d1872425b09809859d7531da53c1b6d991c"),
    LEGACY_PREFIX + "jna": ("org.jline.terminal.impl.jna.JnaTerminalProvider", "69e2f18eb11c285b5784ac86f479f7b6757f4914a02a9303e40931d85ebe2580"),
    LEGACY_PREFIX + "jansi": ("org.jline.terminal.impl.jansi.JansiTerminalProvider", "558d30e74e88e0b4e196d3aa0860c60eeaf1e764ed63d3e18e3dd6089d0642fd"),
}
LOADER_SOURCE = "terminal/src/main/java/org/jline/terminal/spi/TerminalProvider.java"
LOADER_SHA256 = "03406ee3117cdf6d2aedb3b3462ff9b02998aa4283a11570f7b3f3115e6f2416"
BINARY_NAME = re.compile(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+", re.ASCII)


def require(value: bool, message: str) -> None:
    if not value:
        raise ValueError(message)


def resource_kind(path: str) -> str:
    if path.startswith((MODERN_PREFIX, LEGACY_PREFIX)):
        require(path in PROVIDERS, f"unreviewed JLine provider resource path: {path}")
        return "jline-active" if path.startswith(MODERN_PREFIX) else "jline-legacy-properties"
    if path.startswith("META-INF/services/"):
        require(BINARY_NAME.fullmatch(path.removeprefix("META-INF/services/")) is not None,
                f"invalid Java SPI resource path: {path}")
        return "java-spi"
    return "other"


def provider_token(path: str, content: bytes, *, shaded: bool) -> str:
    kind = resource_kind(path)
    require(kind in ("jline-active", "jline-legacy-properties"), "not a typed JLine descriptor")
    lines = [line.strip() for line in content.decode("utf-8").splitlines()
             if line.strip() and not line.lstrip().startswith(("#", "!"))]
    require(len(lines) == 1, f"expected exactly one JLine provider value: {path}")
    value = lines[0]
    if kind == "jline-legacy-properties":
        match = re.fullmatch(r"class[ \t]*=[ \t]*(\S+)", value)
        require(match is not None, f"malformed JLine legacy class property: {path}")
        value = match.group(1)
    expected = (SHADED_PREFIX if shaded else "") + PROVIDERS[path][0]
    require(value == expected, f"incorrect JLine provider class or relocation: {path}: {value}")
    return value


def expected_resource(path: str, original: bytes) -> bytes:
    token = provider_token(path, original, shaded=False).encode()
    require(original.count(token) == 1, f"ambiguous JLine class token: {path}")
    return original.replace(token, SHADED_PREFIX.encode() + token, 1)


def validate_jline_resources(original: ZipFile, generated: ZipFile, classes: set[str]) -> dict:
    records = {}
    for archive, label in ((original, "official dependency"), (generated, "generated runtime")):
        names = [name for name in archive.namelist()
                 if not name.endswith("/") and name.startswith((MODERN_PREFIX, LEGACY_PREFIX))]
        require(set(names) == set(PROVIDERS) and len(names) == len(PROVIDERS),
                f"missing, duplicate or unknown JLine provider descriptors in {label}")
    for path, (original_class, digest) in PROVIDERS.items():
        source = original.read(path)
        actual = generated.read(path)
        require(hashlib.sha256(source).hexdigest() == digest,
                f"official JLine resource differs from pinned source: {path}")
        require(actual == expected_resource(path, source),
                f"JLine resource differs from exact source-time class relocation: {path}")
        relocated = provider_token(path, actual, shaded=True)
        baseline_present = original_class.replace(".", "/") + ".class" in original.namelist()
        present = relocated.replace(".", "/") in classes
        active = path.startswith(MODERN_PREFIX)
        require((baseline_present and present) or (not active and not baseline_present and not present),
                f"JLine provider class missing or differs from official baseline: {path}")
        records[path] = {"format": resource_kind(path), "source_sha256": digest,
                         "generated_sha256": hashlib.sha256(actual).hexdigest(),
                         "provider": relocated, "class_present": present,
                         "official_class_present": baseline_present,
                         "loader_status": "active load(String) resource" if active else "legacy descriptor not read by the pinned TerminalProvider.load(String)"}
    return {"version": VERSION, "loader_source_url": SOURCE_ROOT + LOADER_SOURCE,
            "loader_source_sha256": LOADER_SHA256, "provider_resources": records,
            "runtime_requirement": "Final-image Java21 TerminalProvider.load(\"dumb\") must instantiate the shaded DumbTerminalProvider from the rebuilt runtime JAR; optional native/FFM backends are not exercised."}
