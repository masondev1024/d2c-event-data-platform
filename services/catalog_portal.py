"""Read-only local portal for the validated data-product catalog.

The portal deliberately serves only checked-in, validated metadata.  It is a
small discovery surface for a portfolio lab, not a substitute for an enterprise
metadata platform; the catalog contract keeps its records ready for a later Glue
or OpenMetadata integration without claiming that planned assets already exist.
"""

from __future__ import annotations

import html
import json
import logging
import os
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from .catalog import CatalogError, default_catalog_dir, load_catalog, repository_root


LOGGER = logging.getLogger(__name__)
PRODUCT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,62}$")


class CatalogRepository:
    """Reload catalog metadata for each request so invalid changes fail closed."""

    def __init__(self, catalog_dir: str | Path | None = None, root: str | Path | None = None) -> None:
        self._catalog_dir = Path(catalog_dir) if catalog_dir is not None else default_catalog_dir()
        self._root = Path(root) if root is not None else repository_root()

    def load(self) -> tuple[dict[str, Any], ...]:
        return load_catalog(catalog_dir=self._catalog_dir, repository=self._root)


def _summary(product: dict[str, Any]) -> dict[str, Any]:
    owner = product["owner"]
    return {
        "id": product["id"],
        "name": product["name"],
        "description": product["description"],
        "domain": product["domain"],
        "maturity": product["maturity"],
        "owner_team": owner["team"],
        "classification": product["classification"]["level"],
        "asset_states": {
            "implemented": sum(asset["state"] == "implemented" for asset in product["assets"]),
            "planned": sum(asset["state"] == "planned" for asset in product["assets"]),
        },
    }


def _portal_html(products: tuple[dict[str, Any], ...]) -> str:
    """Render manifest values as escaped static HTML without client-side code."""

    cards: list[str] = []
    for product in products:
        asset_rows = "".join(
            "<tr>"
            f"<td><code>{html.escape(str(asset['id']))}</code></td>"
            f"<td>{html.escape(str(asset['name']))}</td>"
            f"<td>{html.escape(str(asset['kind']))}</td>"
            f"<td>{html.escape(str(asset['state']))}</td>"
            f"<td>{html.escape(', '.join(str(key) for key in asset['primary_key']))}</td>"
            f"<td>{html.escape(', '.join(str(upstream) for upstream in asset['upstreams']) or 'source-system')}</td>"
            "</tr>"
            for asset in product["assets"]
        )
        service_level_rows = "".join(
            "<li>"
            f"<code>{html.escape(str(level['name']))}</code>: "
            f"{html.escape(str(level['objective']))} "
            f"<em>({html.escape(str(level['state']))})</em>"
            "</li>"
            for level in product["service_levels"]
        )
        classification_rows = "".join(
            "<li>"
            f"<code>{html.escape(str(field['asset_id']))}.{html.escape(str(field['path']))}</code>: "
            f"{html.escape(str(field['classification']))} — {html.escape(str(field['handling']))}"
            "</li>"
            for field in product["classification"]["fields"]
        )
        quality_rule_rows = "".join(
            "<li>"
            f"<code>{html.escape(str(rule['asset_id']))}.{html.escape(str(rule['name']))}</code>: "
            f"{html.escape(str(rule['rule']))} "
            f"<em>({html.escape(str(rule['state']))})</em>"
            "</li>"
            for rule in product["quality_rules"]
        )
        cards.append(
            "<article>"
            f"<h2>{html.escape(str(product['name']))}</h2>"
            f"<p>{html.escape(str(product['description']))}</p>"
            "<dl>"
            f"<dt>Product ID</dt><dd><code>{html.escape(str(product['id']))}</code></dd>"
            f"<dt>Domain</dt><dd>{html.escape(str(product['domain']))}</dd>"
            f"<dt>Maturity</dt><dd>{html.escape(str(product['maturity']))}</dd>"
            f"<dt>Owner</dt><dd>{html.escape(str(product['owner']['team']))}</dd>"
            f"<dt>Classification</dt><dd>{html.escape(str(product['classification']['level']))}</dd>"
            "</dl>"
            "<h3>Assets</h3>"
            "<table><thead><tr><th>Asset ID</th><th>Name</th><th>Kind</th><th>State</th><th>Primary key</th><th>Upstreams</th>"
            f"</tr></thead><tbody>{asset_rows}</tbody></table>"
            "<h3>Field classification</h3>"
            f"<ul>{classification_rows}</ul>"
            "<h3>Quality rules</h3>"
            f"<ul>{quality_rule_rows}</ul>"
            "<h3>Service levels</h3>"
            f"<ul>{service_level_rows}</ul>"
            "</article>"
        )
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Data Product Catalog</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
    body { max-width: 1000px; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }
    article { border: 1px solid #8886; border-radius: .5rem; padding: 1rem 1.25rem; margin: 1rem 0; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #8886; padding: .45rem; text-align: left; vertical-align: top; }
    dt { font-weight: 700; } dd { margin: 0 0 .5rem; }
    code { overflow-wrap: anywhere; }
  </style>
</head>
<body>
  <header>
    <h1>Data Product Catalog</h1>
    <p>Validated contract, ownership, quality, lineage, and implementation-state metadata.</p>
    <p>Machine API: <code>/api/v1/data-products</code></p>
  </header>
  <main>""" + "".join(cards) + """</main>
</body>
</html>
"""


class CatalogPortalServer(ThreadingHTTPServer):
    """HTTP server that carries a catalog repository without global mutable state."""

    def __init__(self, server_address: tuple[str, int], catalog: CatalogRepository) -> None:
        super().__init__(server_address, CatalogPortalHandler)
        self.catalog = catalog


class CatalogPortalHandler(BaseHTTPRequestHandler):
    """Serve a compact read-only catalog API and an HTML discovery view."""

    server: CatalogPortalServer

    def _send(self, status: HTTPStatus, content_type: str, body: bytes) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        )
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        self._send(
            status,
            "application/json; charset=utf-8",
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )

    def _not_found(self) -> None:
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_GET(self) -> None:  # noqa: N802 - required stdlib handler name.
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            self._not_found()
            return
        path = unquote(parsed.path)
        try:
            products = self.server.catalog.load()
        except CatalogError:
            LOGGER.exception("catalog_portal_catalog_validation_failed")
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "catalog unavailable"})
            return

        if path == "/healthz":
            self._send_json(
                HTTPStatus.OK,
                {"status": "ok", "data_products": len(products)},
            )
            return
        if path == "/":
            self._send(
                HTTPStatus.OK,
                "text/html; charset=utf-8",
                _portal_html(products).encode("utf-8"),
            )
            return
        if path == "/api/v1/data-products":
            self._send_json(HTTPStatus.OK, {"data_products": [_summary(product) for product in products]})
            return

        prefix = "/api/v1/data-products/"
        if path.startswith(prefix):
            product_id = path.removeprefix(prefix)
            if not PRODUCT_ID_PATTERN.fullmatch(product_id):
                self._not_found()
                return
            product = next((item for item in products if item["id"] == product_id), None)
            if product is None:
                self._not_found()
                return
            self._send_json(HTTPStatus.OK, {"data_product": product})
            return
        self._not_found()

    def log_message(self, format: str, *args: object) -> None:
        LOGGER.info("catalog_portal_request " + format, *args)


def create_server(host: str, port: int, catalog: CatalogRepository) -> CatalogPortalServer:
    """Create, but do not start, the local catalog portal server."""

    return CatalogPortalServer((host, port), catalog)


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    host = os.getenv("CATALOG_BIND_HOST", "127.0.0.1")
    try:
        port = int(os.getenv("CATALOG_PORT", "8082"))
    except ValueError as exc:
        raise SystemExit("CATALOG_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise SystemExit("CATALOG_PORT must be between 1 and 65535")

    catalog = CatalogRepository(
        catalog_dir=os.getenv("CATALOG_DIR", str(default_catalog_dir())),
        root=os.getenv("CATALOG_REPOSITORY_ROOT", str(repository_root())),
    )
    try:
        catalog.load()
    except CatalogError as exc:
        raise SystemExit(f"catalog portal refused to start: {exc}") from exc
    server = create_server(host, port, catalog)
    LOGGER.info("catalog_portal_started host=%s port=%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("catalog_portal_stopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
