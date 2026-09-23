"""Validate the data-product catalog before it is exposed or released."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Support the runbook/Makefile form: ``python scripts/validate_catalog.py``.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from services.catalog import CatalogError, default_catalog_dir, load_catalog, repository_root


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog-dir",
        default=str(default_catalog_dir()),
        help="Directory containing data-product JSON manifests.",
    )
    parser.add_argument(
        "--repository-root",
        default=str(repository_root()),
        help="Repository root used to verify implementation and contract references.",
    )
    args = parser.parse_args()
    try:
        products = load_catalog(
            catalog_dir=Path(args.catalog_dir),
            repository=Path(args.repository_root),
        )
    except CatalogError as exc:
        parser.error(str(exc))
    implemented_assets = sum(
        asset["state"] == "implemented"
        for product in products
        for asset in product["assets"]
    )
    print(
        "catalog valid: "
        f"products={len(products)} implemented_assets={implemented_assets} "
        f"ids={','.join(product['id'] for product in products)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
