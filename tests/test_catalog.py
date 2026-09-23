from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from tempfile import TemporaryDirectory

from services.catalog import CatalogError, load_catalog
from services.catalog_portal import CatalogRepository, create_server


ROOT = Path(__file__).parents[1]
CATALOG_DIR = ROOT / "catalog" / "data-products"
MANIFEST_PATH = CATALOG_DIR / "d2c-application-approvals.v1.json"


class CatalogValidationTest(unittest.TestCase):
    def _manifest(self) -> dict[str, object]:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    def test_checked_in_product_has_valid_contract_evidence_and_lineage(self) -> None:
        products = load_catalog(catalog_dir=CATALOG_DIR, repository=ROOT)

        self.assertEqual([product["id"] for product in products], ["d2c-application-approvals"])
        event = products[0]["assets"][0]
        self.assertEqual(event["event_type"], "d2c.application.approved.v1")
        self.assertEqual(event["event_version"], 1)
        self.assertEqual(products[0]["assets"][1]["upstreams"], [event["id"]])

    def test_catalog_rejects_an_implementation_reference_outside_repository(self) -> None:
        document = self._manifest()
        assets = document["assets"]
        assert isinstance(assets, list)
        assets[0]["implementation_references"][0] = "../../private-note.md"

        with TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "product.json"
            manifest_path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(CatalogError, "escapes repository root"):
                load_catalog(catalog_dir=directory, repository=ROOT)

    def test_catalog_rejects_topic_contract_drift(self) -> None:
        document = self._manifest()
        assets = document["assets"]
        assert isinstance(assets, list)
        assets[0]["event_type"] = "d2c.application.approved.v2"

        with TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "product.json"
            manifest_path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(CatalogError, "does not match contract const"):
                load_catalog(catalog_dir=directory, repository=ROOT)

    def test_catalog_rejects_an_implemented_asset_that_depends_on_planned_work(self) -> None:
        document = self._manifest()
        assets = document["assets"]
        assert isinstance(assets, list)
        planned_asset = next(
            asset
            for asset in assets
            if asset["id"] == "d2c-application-events-iceberg"
        )
        planned_asset["state"] = "planned"
        planned_asset["future_work"] = "Restore the completed implementation before publishing this data product."
        assets[1]["upstreams"] = [planned_asset["id"]]

        with TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "product.json"
            manifest_path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(CatalogError, "cannot depend on planned asset"):
                load_catalog(catalog_dir=directory, repository=ROOT)


class CatalogPortalTest(unittest.TestCase):
    def setUp(self) -> None:
        repository = CatalogRepository(catalog_dir=CATALOG_DIR, root=ROOT)
        self.server = create_server("127.0.0.1", 0, repository)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address[:2]
        self.base_url = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def _request(self, path: str) -> tuple[int, dict[str, object], dict[str, str]]:
        with urllib.request.urlopen(f"{self.base_url}{path}", timeout=5) as response:
            return (
                response.status,
                json.loads(response.read().decode("utf-8")),
                dict(response.headers.items()),
            )

    def test_api_lists_and_resolves_the_validated_product(self) -> None:
        status, index, headers = self._request("/api/v1/data-products")

        self.assertEqual(status, 200)
        self.assertEqual(index["data_products"][0]["id"], "d2c-application-approvals")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["Cache-Control"], "no-store")

        status, detail, _ = self._request("/api/v1/data-products/d2c-application-approvals")
        self.assertEqual(status, 200)
        self.assertEqual(detail["data_product"]["assets"][0]["event_version"], 1)

    def test_health_and_unknown_asset_responses(self) -> None:
        status, health, _ = self._request("/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health, {"data_products": 1, "status": "ok"})

        with self.assertRaises(urllib.error.HTTPError) as caught:
            self._request("/api/v1/data-products/not-real")
        self.assertEqual(caught.exception.code, 404)
        caught.exception.close()

    def test_invalid_catalog_fails_closed_at_the_http_boundary(self) -> None:
        document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        assets = document["assets"]
        assert isinstance(assets, list)
        assets[0]["event_type"] = "d2c.application.approved.v2"

        with TemporaryDirectory() as directory:
            Path(directory, "product.json").write_text(json.dumps(document), encoding="utf-8")
            invalid_server = create_server(
                "127.0.0.1",
                0,
                CatalogRepository(catalog_dir=directory, root=ROOT),
            )
            invalid_thread = threading.Thread(target=invalid_server.serve_forever, daemon=True)
            invalid_thread.start()
            host, port = invalid_server.server_address[:2]
            try:
                with self.assertLogs("services.catalog_portal", level="ERROR") as logs:
                    with self.assertRaises(urllib.error.HTTPError) as caught:
                        urllib.request.urlopen(f"http://{host}:{port}/healthz", timeout=5)
                self.assertEqual(caught.exception.code, 503)
                self.assertEqual(
                    json.loads(caught.exception.read().decode("utf-8")),
                    {"error": "catalog unavailable"},
                )
                caught.exception.close()
                self.assertIn("catalog_portal_catalog_validation_failed", logs.output[0])
            finally:
                invalid_server.shutdown()
                invalid_server.server_close()
                invalid_thread.join(timeout=5)

    def test_html_view_is_escaped_and_contains_catalog_content(self) -> None:
        with urllib.request.urlopen(f"{self.base_url}/", timeout=5) as response:
            body = response.read().decode("utf-8")

        self.assertIn("Data Product Catalog", body)
        self.assertIn("D2C application approvals", body)
        self.assertIn("d2c-application-events-iceberg", body)
        self.assertIn("data.user_id", body)
        self.assertIn("event-id-uniqueness", body)


if __name__ == "__main__":
    unittest.main()
