"""In-container ABI and startup proof; feed this source to python - ROLE.

No database, broker, host mount, port publication or external service is used.
Only a directly owned local server subprocess is started and stopped.
"""
from __future__ import annotations

import argparse
import errno
from datetime import datetime, timezone
import importlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import signal
import socket
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request


PREFIX = "D2C_PYTHON_RUNTIME_PROOF="
ROLES = ("worker", "api", "catalog")


class ProbeError(RuntimeError):
    pass


def require(value, reason):
    if not value:
        raise ProbeError(reason)


def runtime_identity(role):
    require(role in ROLES, "unsupported runtime role")
    require(sys.implementation.name == "cpython" and sys.version_info[:2] == (3, 12), "CPython 3.12 is required")
    require(os.getuid() == 10001 and os.geteuid() == 10001, "runtime must execute as UID 10001")
    require(sys.prefix == "/opt/venv", "runtime must use the installed /opt/venv interpreter")
    release_text = Path("/etc/os-release").read_text()
    release = dict(line.split("=", 1) for line in release_text.splitlines() if "=" in line)
    os_id = release.get("ID", "").strip('"\'')
    require(os_id in {"wolfi", "chainguard"}, "the intended Wolfi/Chainguard runtime identity is required")
    packages = subprocess.run(["apk", "info", "-v"], check=True, capture_output=True, text=True, timeout=3)
    require(bool(packages.stdout.strip()), "installed APK version metadata is missing")
    return {"role": role, "uid": os.getuid(), "python": platform.python_version(),
            "python_implementation": sys.implementation.name, "executable": sys.executable,
            "virtual_environment": sys.prefix, "os_id": os_id, "os_release": release_text,
            "apk_versions": packages.stdout.splitlines(), "machine": platform.machine(),
            "openssl": ssl.OPENSSL_VERSION}


def import_versions(modules):
    for name in modules.values():
        importlib.import_module(name)
    return {distribution: metadata.version(distribution) for distribution in modules}


def duckdb_probe():
    duckdb = importlib.import_module("duckdb")
    connection = duckdb.connect(":memory:", config={"threads": 1, "memory_limit": "64MB"})
    try:
        value, version = connection.execute("SELECT 6 * 7, version()").fetchone()
        require(value == 42 and isinstance(version, str), "native DuckDB SQL result differs")
        return {"query_result": value, "engine_version": version, "database": ":memory:"}
    finally:
        connection.close()


def worker_probe():
    versions = import_versions({"kafka-python": "kafka", "jsonschema": "jsonschema", "pytz": "pytz", "duckdb": "duckdb"})
    processor = importlib.import_module("services.processor")
    contract = importlib.import_module("services.contract")
    require(processor.normalize_sensor_event is contract.normalize_sensor_event, "processor is not using the production normalizer")
    raw = {"timestamp": "2026-08-23T12:00:00Z", "sensor_id": "AI-FACTORY-001",
           "temperature": 87.5, "humidity": 42.4, "status": "RUNNING"}
    fixed = datetime(2026, 8, 23, 12, 1, tzinfo=timezone.utc)
    first = processor.normalize_sensor_event(raw, ingested_at=fixed)
    second = processor.normalize_sensor_event(raw, ingested_at=fixed)
    require(first == second and first.event_time == raw["timestamp"] and first.status == "RUNNING",
            "production normalization is not deterministic")
    contract.validate_canonical_sensor_event(first.as_dict("factory.sensor.raw.json.v1", 0, 0))
    try:
        processor.normalize_sensor_event({**raw, "temperature": 999}, ingested_at=fixed)
    except contract.DataQualityError:
        invalid_rejected = True
    else:
        invalid_rejected = False
    require(invalid_rejected, "production normalizer accepted an invalid temperature")
    return {"imports": versions, "native_duckdb": duckdb_probe(),
            "production_normalizer": {"deterministic_valid_event": True, "invalid_contract_rejected": True}}


def local_port():
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return candidate.getsockname()[1]


def stop_child(child, deadline):
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=max(0.01, min(3, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=max(0.01, min(2, deadline - time.monotonic())))
    require(child.poll() is not None, "owned server child cleanup was not confirmed")


def server_probe(command, endpoints, deadline, environment=None):
    require(deadline - time.monotonic() > 5, "normal server startup deadline exceeded")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    child = subprocess.Popen(command, env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    result = {"process_started": True, "endpoints": {}, "owned_child_reaped": False}
    try:
        for url, validate_payload in endpoints.items():
            require(url.startswith("http://127.0.0.1:"), "only an in-container loopback endpoint is allowed")
            while True:
                require(child.poll() is None, "normal server process exited before its health proof")
                remaining = deadline - time.monotonic() - 5
                require(remaining > 0, "normal server startup deadline exceeded")
                try:
                    with opener.open(url, timeout=min(1, remaining)) as response:
                        content = response.read(65_537)
                        require(response.status == 200 and len(content) <= 65_536, "unexpected local endpoint response")
                        payload = json.loads(content)
                        require(validate_payload(payload), "normal endpoint payload failed validation")
                    result["endpoints"][url.rsplit("/", 1)[-1]] = {"http_status": 200, "payload_validated": True}
                    break
                except urllib.error.HTTPError:
                    raise ProbeError("normal local endpoint returned an HTTP error") from None
                except (urllib.error.URLError, ConnectionError, TimeoutError) as error:
                    reason = getattr(error, "reason", error)
                    if isinstance(reason, OSError) and reason.errno in {errno.EPERM, errno.EACCES}:
                        raise ProbeError("loopback socket access denied; no retry") from None
                    time.sleep(min(0.1, max(0, deadline - time.monotonic() - 5)))
        require(child.poll() is None, "normal server process stopped during validation")
    finally:
        stop_child(child, deadline)
        result["owned_child_reaped"] = True
    return result


def api_probe(deadline):
    versions = import_versions({"psycopg": "psycopg", "psycopg-binary": "psycopg_binary",
                                "Flask": "flask", "gunicorn": "gunicorn", "kafka-python": "kafka"})
    psycopg = importlib.import_module("psycopg")
    require(psycopg.pq.__impl__ == "binary" and psycopg.pq.version() > 0, "the native psycopg binary/libpq implementation is required")
    os.environ.update({"D2C_ENV": "production", "TRUSTED_HOSTS": "localhost,127.0.0.1"})
    application = importlib.import_module("app")
    importlib.import_module("outbox_publisher")
    with application.app.test_client() as client:
        response = client.get("/healthz", base_url="http://localhost")
        require(response.status_code == 200 and response.get_json() == {"status": "ok"}, "production Flask healthz failed")
    port = local_port()
    command = [sys.executable, "-m", "gunicorn", "--bind", f"127.0.0.1:{port}",
               "--workers", "1", "--threads", "4", "--worker-tmp-dir", "/dev/shm", "--timeout", "30",
               "--graceful-timeout", "3", "--access-logfile", "/dev/null", "--error-logfile", "/dev/null", "app:app"]
    server = server_probe(command, {f"http://127.0.0.1:{port}/healthz": lambda payload: payload == {"status": "ok"}},
                          deadline, environment=os.environ.copy())
    return {"imports": versions, "native_psycopg": {"implementation": psycopg.pq.__impl__, "libpq_version": psycopg.pq.version()},
            "flask_test_client_healthz": True, "normal_gunicorn": server}


def catalog_probe(deadline):
    versions = import_versions({"kafka-python": "kafka", "jsonschema": "jsonschema", "pytz": "pytz", "duckdb": "duckdb"})
    portal = importlib.import_module("services.catalog_portal")
    products = portal.CatalogRepository().load()
    ids = sorted(product["id"] for product in products)
    require(bool(ids) and len(ids) == len(set(ids)), "actual catalog repository returned empty or duplicate products")
    port = local_port()
    environment = {**os.environ, "CATALOG_BIND_HOST": "127.0.0.1", "CATALOG_PORT": str(port)}
    base = f"http://127.0.0.1:{port}"
    endpoints = {base + "/healthz": lambda payload: payload == {"status": "ok", "data_products": len(ids)},
                 base + "/api/v1/data-products": lambda payload: sorted(product["id"] for product in payload["data_products"]) == ids}
    server = server_probe([sys.executable, "-m", "services.catalog_portal"], endpoints, deadline, environment)
    return {"imports": versions, "native_duckdb": duckdb_probe(), "validated_catalog_product_ids": ids, "normal_catalog_server": server}


def run(role, timeout_seconds=45):
    require(role in ROLES and 10 <= timeout_seconds <= 90, "role or probe timeout is invalid")
    started = time.monotonic()
    deadline = started + timeout_seconds
    def expired(_signal, _frame):
        raise ProbeError("in-container runtime probe deadline exceeded")
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        identity = runtime_identity(role)
        checks = worker_probe() if role == "worker" else api_probe(deadline) if role == "api" else catalog_probe(deadline)
        require(time.monotonic() <= deadline, "in-container runtime probe deadline exceeded")
        return {"schema_version": 1, "status": "PASS", **identity, "checks": checks,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "limits": {
                    "worker": "Worker imports, native DuckDB in-memory SQL and deterministic contract only; no Kafka broker/processor loop, PostgreSQL, persistent sink or end-to-end event delivery was tested",
                    "api": "API native psycopg/libpq/imports and Flask/Gunicorn loopback health startup only; no PostgreSQL connection/transaction, outbox delivery or Kafka broker was tested",
                    "catalog": "Catalog native DuckDB/imports, repository validation and loopback server endpoints only; no Kafka broker/worker loop, PostgreSQL, lakehouse pipeline or remote service was tested",
                }[role]}
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=ROLES)
    parser.add_argument("--timeout-seconds", type=int, default=45)
    args = parser.parse_args(argv)
    try:
        proof = run(args.role, args.timeout_seconds)
    except Exception as error:
        # Report fixed probe assertions or an exception type, never arbitrary
        # application/DB/child logs which might contain configuration secrets.
        reason = str(error) if isinstance(error, ProbeError) else type(error).__name__
        proof = {"schema_version": 1, "status": "FAIL", "role": args.role,
                 "exception_type": type(error).__name__, "reason": reason}
        print(PREFIX + json.dumps(proof, sort_keys=True), flush=True)
        return 1
    print(PREFIX + json.dumps(proof, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
