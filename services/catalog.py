"""Contract-driven data-product catalog validation and loading.

The catalog is intentionally code-validated instead of being a free-form document.
An asset marked ``implemented`` must point to local implementation evidence, and an
event asset must agree with the published JSON Schema that its Kafka consumers use.
This prevents a portal from silently overstating what the platform actually runs.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


class CatalogError(ValueError):
    """Raised when a data-product catalog cannot be safely published."""


def repository_root() -> Path:
    """Return the repository root when this module is run from source or a container."""

    return Path(__file__).resolve().parents[1]


def default_catalog_dir() -> Path:
    """Return the checked-in data-product manifest directory."""

    return repository_root() / "catalog" / "data-products"


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"unable to load {label} at {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalogError(f"{label} at {path} must be a JSON object")
    return value


def _repository_file(root: Path, relative_path: str, label: str) -> Path:
    candidate = Path(relative_path)
    if candidate.is_absolute():
        raise CatalogError(f"{label} must be repository-relative: {relative_path}")
    resolved_root = root.resolve()
    resolved = (resolved_root / candidate).resolve()
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise CatalogError(f"{label} escapes repository root: {relative_path}") from exc
    if not resolved.is_file():
        raise CatalogError(f"{label} does not exist: {relative_path}")
    return resolved


def _format_schema_errors(errors: Sequence[Any]) -> list[str]:
    formatted: list[str] = []
    for error in sorted(errors, key=lambda item: list(item.absolute_path)):
        location = ".".join(str(part) for part in error.absolute_path) or "$"
        formatted.append(f"{location}: {error.message}")
    return formatted


def _event_schema_value(schema: Mapping[str, Any], field: str) -> Any:
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return None
    field_schema = properties.get(field)
    if not isinstance(field_schema, Mapping):
        return None
    return field_schema.get("const")


def _schema_has_path(schema: Mapping[str, Any], path: str) -> bool:
    node: Mapping[str, Any] = schema
    for part in path.split("."):
        properties = node.get("properties")
        if not isinstance(properties, Mapping):
            return False
        child = properties.get(part)
        if not isinstance(child, Mapping):
            return False
        node = child
    return True


def _find_cycles(asset_upstreams: Mapping[str, Sequence[str]]) -> list[str]:
    """Return a readable lineage cycle list, if the directed graph has one."""

    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(asset_id: str) -> list[str] | None:
        if asset_id in visiting:
            start = stack.index(asset_id)
            return [*stack[start:], asset_id]
        if asset_id in visited:
            return None
        visiting.add(asset_id)
        stack.append(asset_id)
        for upstream in asset_upstreams[asset_id]:
            cycle = visit(upstream)
            if cycle:
                return cycle
        stack.pop()
        visiting.remove(asset_id)
        visited.add(asset_id)
        return None

    for asset_id in asset_upstreams:
        cycle = visit(asset_id)
        if cycle:
            return cycle
    return []


def _validate_product(
    product: Mapping[str, Any],
    manifest_path: Path,
    root: Path,
    validator: Draft202012Validator,
) -> list[str]:
    """Return every cross-field and repository-evidence violation for one product."""

    errors = _format_schema_errors(list(validator.iter_errors(product)))
    if errors:
        return errors

    owner = product["owner"]
    assert isinstance(owner, Mapping)
    try:
        _repository_file(root, str(owner["runbook"]), "owner.runbook")
    except CatalogError as exc:
        errors.append(str(exc))

    raw_assets = product["assets"]
    assert isinstance(raw_assets, list)
    assets = [asset for asset in raw_assets if isinstance(asset, Mapping)]
    asset_by_id: dict[str, Mapping[str, Any]] = {}
    for asset in assets:
        asset_id = str(asset["id"])
        if asset_id in asset_by_id:
            errors.append(f"assets: duplicate asset id {asset_id}")
            continue
        asset_by_id[asset_id] = asset

    if not any(asset.get("state") == "implemented" for asset in assets):
        errors.append("assets: at least one asset must be implemented")

    contracts: dict[str, Mapping[str, Any]] = {}
    asset_upstreams: dict[str, Sequence[str]] = {}
    for asset_id, asset in asset_by_id.items():
        upstreams = asset.get("upstreams")
        assert isinstance(upstreams, list)
        asset_upstreams[asset_id] = [str(upstream) for upstream in upstreams]
        for upstream in upstreams:
            upstream_id = str(upstream)
            if upstream_id == asset_id:
                errors.append(f"assets.{asset_id}: an asset cannot be its own upstream")
            elif upstream_id not in asset_by_id:
                errors.append(f"assets.{asset_id}: unknown upstream asset {upstream_id}")

        state = asset["state"]
        if state == "implemented":
            references = asset.get("implementation_references")
            assert isinstance(references, list)
            for reference in references:
                try:
                    _repository_file(
                        root,
                        str(reference),
                        f"assets.{asset_id}.implementation_references",
                    )
                except CatalogError as exc:
                    errors.append(str(exc))

        if asset["kind"] == "event":
            try:
                contract_path = _repository_file(
                    root,
                    str(asset["contract_path"]),
                    f"assets.{asset_id}.contract_path",
                )
                contract = _load_json(contract_path, f"event contract for {asset_id}")
                contracts[asset_id] = contract
                expected_type = _event_schema_value(contract, "event_type")
                expected_version = _event_schema_value(contract, "event_version")
                if expected_type != asset["event_type"]:
                    errors.append(
                        f"assets.{asset_id}: event_type {asset['event_type']!r} does not match "
                        f"contract const {expected_type!r}"
                    )
                if expected_version != asset["event_version"]:
                    errors.append(
                        f"assets.{asset_id}: event_version {asset['event_version']!r} does not match "
                        f"contract const {expected_version!r}"
                    )
                required = contract.get("required")
                expected_required = {"event_id", "event_type", "event_version", "occurred_at", "data"}
                if not isinstance(required, list) or not expected_required.issubset(required):
                    errors.append(
                        f"assets.{asset_id}: contract must require {sorted(expected_required)}"
                    )
            except CatalogError as exc:
                errors.append(str(exc))

    for asset_id, upstreams in asset_upstreams.items():
        for upstream_id in upstreams:
            upstream = asset_by_id.get(upstream_id)
            current = asset_by_id[asset_id]
            if upstream and current["state"] == "implemented" and upstream["state"] != "implemented":
                errors.append(
                    f"assets.{asset_id}: implemented asset cannot depend on planned asset {upstream_id}"
                )
    if not errors and len(asset_upstreams) == len(asset_by_id):
        cycle = _find_cycles(asset_upstreams)
        if cycle:
            errors.append(f"assets: lineage cycle detected: {' -> '.join(cycle)}")

    classification = product["classification"]
    assert isinstance(classification, Mapping)
    classification_fields = classification["fields"]
    assert isinstance(classification_fields, list)
    for field in classification_fields:
        assert isinstance(field, Mapping)
        asset_id = str(field["asset_id"])
        if asset_id not in asset_by_id:
            errors.append(f"classification.fields: unknown asset {asset_id}")
            continue
        contract = contracts.get(asset_id)
        if contract is None:
            errors.append(f"classification.fields: asset {asset_id} has no event contract")
            continue
        path = str(field["path"])
        if not _schema_has_path(contract, path):
            errors.append(f"classification.fields: {asset_id}.{path} does not exist in its contract")

    quality_rules = product["quality_rules"]
    assert isinstance(quality_rules, list)
    for rule in quality_rules:
        assert isinstance(rule, Mapping)
        asset_id = str(rule["asset_id"])
        if asset_id not in asset_by_id:
            errors.append(f"quality_rules: unknown asset {asset_id}")
        elif rule["state"] == "implemented" and asset_by_id[asset_id]["state"] != "implemented":
            errors.append(
                f"quality_rules: implemented rule {rule['name']} points to planned asset {asset_id}"
            )

    return errors


def load_catalog(
    catalog_dir: str | Path | None = None,
    repository: str | Path | None = None,
) -> tuple[dict[str, Any], ...]:
    """Load every checked-in data product after schema and evidence validation."""

    root = Path(repository).resolve() if repository is not None else repository_root()
    catalog_path = Path(catalog_dir).resolve() if catalog_dir is not None else default_catalog_dir()
    schema_path = root / "catalog" / "schemas" / "data-product.v1.schema.json"
    schema = _load_json(schema_path, "catalog schema")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    if not catalog_path.is_dir():
        raise CatalogError(f"catalog directory does not exist: {catalog_path}")
    manifest_paths = tuple(sorted(catalog_path.glob("*.json")))
    if not manifest_paths:
        raise CatalogError(f"catalog directory has no JSON data products: {catalog_path}")

    products: list[dict[str, Any]] = []
    product_ids: set[str] = set()
    failures: list[str] = []
    for manifest_path in manifest_paths:
        try:
            product = _load_json(manifest_path, "data-product manifest")
            errors = _validate_product(product, manifest_path, root, validator)
        except CatalogError as exc:
            failures.append(f"{manifest_path.name}: {exc}")
            continue
        if errors:
            failures.extend(f"{manifest_path.name}: {error}" for error in errors)
            continue
        product_id = str(product["id"])
        if product_id in product_ids:
            failures.append(f"{manifest_path.name}: duplicate product id {product_id}")
            continue
        product_ids.add(product_id)
        products.append(product)

    if failures:
        raise CatalogError("catalog validation failed:\n" + "\n".join(f"- {failure}" for failure in failures))
    return tuple(sorted(products, key=lambda product: str(product["id"])))
