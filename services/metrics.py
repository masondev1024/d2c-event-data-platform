"""Small dependency-free Prometheus exporter for the streaming workers.

The PoC intentionally avoids pulling a metrics client into the worker image.  The
registry is thread-safe, exposes only explicitly registered series, and renders
the Prometheus text format over a local HTTP endpoint.  The workers keep label
values bounded to Kafka topic/partition and a small result enum.
"""

from __future__ import annotations

import logging
import math
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Mapping


LOGGER = logging.getLogger(__name__)
_NAME_PATTERN = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_name(name: str, pattern: re.Pattern[str]) -> str:
    if not pattern.fullmatch(name):
        raise ValueError(f"invalid Prometheus name: {name!r}")
    return name


def _escape_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


class MetricsRegistry:
    """Thread-safe counters and gauges with deterministic Prometheus output."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._definitions: dict[str, tuple[str, str]] = {}
        self._values: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}

    def _register(
        self,
        name: str,
        metric_type: str,
        help_text: str,
        labels: Mapping[str, object],
    ) -> tuple[str, tuple[tuple[str, str], ...]]:
        _validate_name(name, _NAME_PATTERN)
        if metric_type not in {"counter", "gauge"}:
            raise ValueError(f"unsupported metric type: {metric_type}")
        normalized_labels = tuple(
            sorted(
                (_validate_name(str(key), _LABEL_PATTERN), str(value))
                for key, value in labels.items()
            )
        )
        with self._lock:
            previous = self._definitions.get(name)
            definition = (metric_type, help_text)
            if previous is not None and previous != definition:
                raise ValueError(f"metric {name!r} was registered with a different type/help")
            self._definitions[name] = definition
        return name, normalized_labels

    def inc(
        self,
        name: str,
        labels: Mapping[str, object] | None = None,
        value: float = 1.0,
        help_text: str = "",
    ) -> None:
        """Increase a counter by a non-negative finite value."""

        if not math.isfinite(value) or value < 0:
            raise ValueError("counter increment must be finite and non-negative")
        key = self._register(name, "counter", help_text, labels or {})
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Mapping[str, object] | None = None,
        help_text: str = "",
    ) -> None:
        """Set a gauge to a finite value."""

        if not math.isfinite(value):
            raise ValueError("gauge value must be finite")
        key = self._register(name, "gauge", help_text, labels or {})
        with self._lock:
            self._values[key] = value

    def render(self) -> str:
        """Render a consistent Prometheus exposition payload."""

        with self._lock:
            definitions = dict(self._definitions)
            values = dict(self._values)

        lines: list[str] = []
        for name in sorted(definitions):
            metric_type, help_text = definitions[name]
            lines.append(f"# HELP {name} {help_text or name}")
            lines.append(f"# TYPE {name} {metric_type}")
            samples = sorted(
                (labels, value)
                for (metric_name, labels), value in values.items()
                if metric_name == name
            )
            for labels, value in samples:
                label_text = ""
                if labels:
                    label_text = "{" + ",".join(
                        f'{key}="{_escape_label(label_value)}"' for key, label_value in labels
                    ) + "}"
                lines.append(f"{name}{label_text} {value:g}")
        return "\n".join(lines) + ("\n" if lines else "")


def start_metrics_server(registry: MetricsRegistry, port: int) -> ThreadingHTTPServer | None:
    """Start a daemon metrics server; port 0 disables metrics for local callers."""

    if port == 0:
        return None
    if not 1 <= port <= 65535:
        raise ValueError("metrics port must be 0 or between 1 and 65535")

    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib HTTP handler API
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            body = registry.render().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    # Bind locally by default; container deployments opt in explicitly so a
    # metrics endpoint is reachable only on the intended network interface.
    bind_host = os.getenv("METRICS_BIND_HOST", "127.0.0.1")
    server = ThreadingHTTPServer((bind_host, port), MetricsHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        name=f"metrics-server-{port}",
        daemon=True,
    )
    thread.start()
    LOGGER.info("metrics_server_started address=%s:%s", bind_host, port)
    return server


def metrics_port_from_env(default: int) -> int:
    """Parse a worker's METRICS_PORT setting and fail fast on bad configuration."""

    raw_port = os.getenv("METRICS_PORT", str(default))
    try:
        return int(raw_port)
    except ValueError as exc:
        raise ValueError("METRICS_PORT must be an integer") from exc
