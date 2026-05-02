"""Load and validate sc-analytics data product contracts.

The YAML files follow the open Data Contract Specification shape and keep
sc-analytics-specific executable checks under ``x-sc-analytics``. This module
validates both producer-facing Gold product contracts and consumer-facing
published dashboard asset contracts.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DASHBOARD_CONTRACT_DIR = REPO_ROOT / "pipeline" / "contracts" / "dashboard_assets"
DEFAULT_GOLD_CONTRACT_DIR = REPO_ROOT / "pipeline" / "contracts" / "gold"
DEFAULT_CATALOG_PATH = REPO_ROOT / "pipeline" / "contracts" / "data_products.yml"


class ContractValidationError(RuntimeError):
    """Raised when data does not satisfy a data product contract."""


@dataclass(frozen=True)
class FieldContract:
    name: str
    type: str
    required: bool
    nullable: bool
    allow_empty: bool


@dataclass(frozen=True)
class DataProductContract:
    contract_id: str
    version: str
    name: str
    product_kind: str
    table_or_dataset: str
    source_table: str
    published_path: str
    model_name: str
    fields: dict[str, FieldContract]
    unique_key: tuple[str, ...]
    rules: tuple[dict[str, Any], ...]
    path: Path

    @property
    def dataset(self) -> str:
        return self.table_or_dataset

    @property
    def table(self) -> str:
        return self.table_or_dataset


@dataclass(frozen=True)
class ProductCatalogEntry:
    product_id: str
    gold_table: str
    dashboard_dataset: str
    owner: str
    lifecycle: str
    exposure: str
    gold_contract_path: str
    dashboard_contract_path: str
    downstream_consumers: tuple[str, ...]
    refresh_expectation: str


@dataclass(frozen=True)
class ProductCatalog:
    contract_set_version: str
    entries: dict[str, ProductCatalogEntry]


DashboardAssetContract = DataProductContract
GoldProductContract = DataProductContract


def load_product_catalog(catalog_path: Path = DEFAULT_CATALOG_PATH) -> ProductCatalog:
    if not catalog_path.exists():
        return ProductCatalog(contract_set_version="", entries={})

    with catalog_path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    version = raw.get("contract_set_version")
    if not isinstance(version, str) or not version:
        raise ContractValidationError(f"{catalog_path}: missing contract_set_version.")

    entries_raw = raw.get("products") or []
    if not isinstance(entries_raw, list):
        raise ContractValidationError(f"{catalog_path}: products must be a list.")

    entries: dict[str, ProductCatalogEntry] = {}
    for index, item in enumerate(entries_raw):
        if not isinstance(item, dict):
            raise ContractValidationError(f"{catalog_path}: products[{index}] must be an object.")
        product_id = _required_string(item, "id", catalog_path)
        if product_id in entries:
            raise ContractValidationError(f'{catalog_path}: duplicate product id "{product_id}".')
        entries[product_id] = ProductCatalogEntry(
            product_id=product_id,
            gold_table=str(item.get("gold_table") or ""),
            dashboard_dataset=str(item.get("dashboard_dataset") or ""),
            owner=str(item.get("owner") or ""),
            lifecycle=str(item.get("lifecycle") or "active"),
            exposure=str(item.get("exposure") or ""),
            gold_contract_path=str(item.get("gold_contract") or ""),
            dashboard_contract_path=str(item.get("dashboard_contract") or ""),
            downstream_consumers=tuple(
                str(value) for value in item.get("downstream_consumers") or ()
            ),
            refresh_expectation=str(item.get("refresh_expectation") or ""),
        )
    return ProductCatalog(contract_set_version=version, entries=entries)


def load_dashboard_asset_contracts(
    contract_dir: Path = DEFAULT_DASHBOARD_CONTRACT_DIR,
) -> dict[str, DashboardAssetContract]:
    """Load dashboard asset contracts keyed by dataset name."""

    return {
        contract.dataset: contract
        for contract in _load_contracts_from_dir(contract_dir, product_kind="dashboard_asset")
    }


def load_gold_product_contracts(
    contract_dir: Path = DEFAULT_GOLD_CONTRACT_DIR,
) -> dict[str, GoldProductContract]:
    """Load Gold product contracts keyed by fully qualified Gold table name."""

    return {
        _normalize_table_name(contract.table): contract
        for contract in _load_contracts_from_dir(contract_dir, product_kind="gold_product")
    }


def validate_dashboard_asset_rows(
    dataset_name: str,
    rows: list[dict[str, Any]],
    contracts: dict[str, DashboardAssetContract] | None = None,
) -> None:
    """Validate published dashboard rows when a contract exists for the dataset."""

    contracts = contracts if contracts is not None else load_dashboard_asset_contracts()
    contract = contracts.get(dataset_name)
    if contract is None:
        return
    validate_rows(contract, rows)


def validate_gold_product_rows(
    table_name: str,
    rows: list[dict[str, Any]],
    contracts: dict[str, GoldProductContract] | None = None,
) -> None:
    """Validate Gold table rows when a contract exists for the table."""

    contracts = contracts if contracts is not None else load_gold_product_contracts()
    contract = contracts.get(_normalize_table_name(table_name))
    if contract is None:
        return
    validate_rows(contract, rows)


def validate_gold_product_projection_rows(
    table_name: str,
    rows: list[dict[str, Any]],
    contracts: dict[str, GoldProductContract] | None = None,
) -> None:
    """Validate exported projection rows against the matching Gold contract.

    Dashboard assets intentionally publish narrow JSON projections rather than
    ``SELECT *`` from each Gold table. Gold contracts are table contracts, so a
    projection can only validate selected fields and rules whose dependencies
    are present in the exported rows.
    """

    contracts = contracts if contracts is not None else load_gold_product_contracts()
    contract = contracts.get(_normalize_table_name(table_name))
    if contract is None:
        return
    validate_projection_rows(contract, rows)


def validate_rows(contract: DataProductContract, rows: list[dict[str, Any]]) -> None:
    _validate_row_shapes(contract, rows)
    _validate_unique_key(contract, rows)
    _validate_rules(contract, rows)


def validate_projection_rows(contract: DataProductContract, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    projected_fields = set().union(*(set(row) for row in rows))
    fields = {name: field for name, field in contract.fields.items() if name in projected_fields}
    if not fields:
        return

    unique_key = contract.unique_key if set(contract.unique_key).issubset(projected_fields) else ()
    rules = tuple(rule for rule in contract.rules if _rule_fields(rule).issubset(projected_fields))
    projection_contract = DataProductContract(
        contract_id=contract.contract_id,
        version=contract.version,
        name=contract.name,
        product_kind=contract.product_kind,
        table_or_dataset=contract.table_or_dataset,
        source_table=contract.source_table,
        published_path=contract.published_path,
        model_name=contract.model_name,
        fields=fields,
        unique_key=unique_key,
        rules=rules,
        path=contract.path,
    )
    validate_rows(projection_contract, rows)


def _load_contracts_from_dir(contract_dir: Path, *, product_kind: str) -> list[DataProductContract]:
    if not contract_dir.exists():
        return []

    contracts: list[DataProductContract] = []
    seen: set[str] = set()
    for path in sorted(contract_dir.glob("*.yml")):
        with path.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        contract = _parse_contract(path, raw, product_kind=product_kind)
        key = contract.table_or_dataset if product_kind == "dashboard_asset" else contract.table
        if key in seen:
            raise ContractValidationError(f'{path}: duplicate contract target "{key}".')
        seen.add(key)
        contracts.append(contract)
    return contracts


def _parse_contract(path: Path, raw: dict[str, Any], *, product_kind: str) -> DataProductContract:
    if not raw.get("dataContractSpecification"):
        raise ContractValidationError(f"{path}: missing dataContractSpecification.")

    contract_id = raw.get("id")
    if not isinstance(contract_id, str) or not contract_id:
        raise ContractValidationError(f"{path}: missing id.")

    info = raw.get("info") or {}
    version = info.get("version")
    if not isinstance(version, str) or not version:
        raise ContractValidationError(f"{path}: missing info.version.")

    extension = raw.get("x-sc-analytics") or {}
    model_name = ""
    source_table = ""
    published_path = ""
    rules: tuple[dict[str, Any], ...]
    if product_kind == "dashboard_asset":
        asset = extension.get("dashboardAsset") or {}
        target = asset.get("dataset")
        if not isinstance(target, str) or not target:
            raise ContractValidationError(f"{path}: missing x-sc-analytics.dashboardAsset.dataset.")
        model_name = target
        source_table = str(asset.get("sourceTable") or "")
        published_path = str(asset.get("publishedPath") or f"{target}.json")
        rules = tuple(asset.get("rules") or ())
    elif product_kind == "gold_product":
        product = extension.get("goldProduct") or {}
        target = product.get("table")
        if not isinstance(target, str) or not target:
            raise ContractValidationError(f"{path}: missing x-sc-analytics.goldProduct.table.")
        model_name = str(product.get("model") or _unqualified_table_name(target))
        source_table = target
        rules = tuple(product.get("rules") or ())
    else:
        raise ContractValidationError(f"{path}: unknown product kind {product_kind}.")

    models = raw.get("models") or {}
    model = models.get(model_name)
    if not isinstance(model, dict):
        raise ContractValidationError(f'{path}: missing model "{model_name}".')

    fields_raw = model.get("fields") or {}
    if not isinstance(fields_raw, dict) or not fields_raw:
        raise ContractValidationError(f'{path}: model "{model_name}" has no fields.')

    fields = {
        name: _parse_field_contract(name, spec)
        for name, spec in fields_raw.items()
        if isinstance(spec, dict)
    }

    unique_key_raw = model.get("primaryKey") or [
        name for name, spec in fields_raw.items() if isinstance(spec, dict) and spec.get("primary")
    ]
    unique_key = tuple(str(value) for value in unique_key_raw)
    missing_key_fields = sorted(set(unique_key) - set(fields))
    if missing_key_fields:
        raise ContractValidationError(
            f'{path}: primary key references undeclared fields: {", ".join(missing_key_fields)}'
        )

    return DataProductContract(
        contract_id=contract_id,
        version=version,
        name=str(info.get("title") or model_name),
        product_kind=product_kind,
        table_or_dataset=target,
        source_table=source_table,
        published_path=published_path,
        model_name=model_name,
        fields=fields,
        unique_key=unique_key,
        rules=rules,
        path=path,
    )


def _parse_field_contract(name: str, spec: dict[str, Any]) -> FieldContract:
    required = bool(spec.get("required", False))
    nullable = bool(spec.get("nullable", not required))
    allow_empty = bool(spec.get("allowEmpty", False))
    return FieldContract(
        name=name,
        type=str(spec.get("type", "string")),
        required=required,
        nullable=nullable,
        allow_empty=allow_empty,
    )


def _required_string(raw: dict[str, Any], key: str, path: Path) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise ContractValidationError(f"{path}: missing {key}.")
    return value


def _validate_row_shapes(contract: DataProductContract, rows: list[dict[str, Any]]) -> None:
    expected_fields = set(contract.fields)
    for index, row in enumerate(rows):
        missing = sorted(expected_fields - set(row))
        if missing:
            raise ContractValidationError(
                f'Dataset "{contract.name}" row {index} is missing fields: {", ".join(missing)}'
            )

        for field in contract.fields.values():
            value = row.get(field.name)
            if value is None:
                if not field.nullable:
                    raise ContractValidationError(
                        f'Dataset "{contract.name}" row {index} field "{field.name}" is null.'
                    )
                continue
            if value == "":
                if not field.allow_empty:
                    raise ContractValidationError(
                        f'Dataset "{contract.name}" row {index} field "{field.name}" is empty.'
                    )
                continue
            if not _matches_type(value, field.type):
                raise ContractValidationError(
                    f'Dataset "{contract.name}" row {index} field "{field.name}" '
                    f"expected {field.type}, got {type(value).__name__}."
                )


def _validate_unique_key(contract: DataProductContract, rows: list[dict[str, Any]]) -> None:
    if not contract.unique_key:
        return
    seen: set[tuple[Any, ...]] = set()
    for index, row in enumerate(rows):
        key = tuple(row.get(field) for field in contract.unique_key)
        invalid_parts = [
            field
            for field, value in zip(contract.unique_key, key, strict=True)
            if value is None or (value == "" and not contract.fields[field].allow_empty)
        ]
        if invalid_parts:
            raise ContractValidationError(
                f'Dataset "{contract.name}" row {index} has invalid unique key '
                f"{contract.unique_key}: {invalid_parts}."
            )
        if key in seen:
            raise ContractValidationError(
                f'Dataset "{contract.name}" has duplicate unique key {contract.unique_key}: {key}'
            )
        seen.add(key)


def _validate_rules(contract: DataProductContract, rows: list[dict[str, Any]]) -> None:
    for rule in contract.rules:
        name = str(rule.get("name") or "unnamed_rule")
        if "field" in rule and "enum" in rule:
            _validate_enum_rule(contract, rows, name, str(rule["field"]), set(rule["enum"]))
        if "expression" in rule:
            expression = str(rule["expression"])
            for index, row in enumerate(rows):
                context = {field: _coerce_value(row.get(field)) for field in contract.fields}
                if not _safe_eval_bool(expression, context):
                    raise ContractValidationError(
                        f'Dataset "{contract.name}" row {index} failed rule "{name}": '
                        f"{expression} (contract: {contract.path})"
                    )


def _rule_fields(rule: dict[str, Any]) -> set[str]:
    fields: set[str] = set()
    if "field" in rule:
        fields.add(str(rule["field"]))
    if "expression" in rule:
        expression = str(rule["expression"])
        tree = ast.parse(expression, mode="eval")
        fields.update(
            node.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Name) and node.id not in {"max", "min", "abs"}
        )
    return fields


def _validate_enum_rule(
    contract: DataProductContract,
    rows: list[dict[str, Any]],
    rule_name: str,
    field: str,
    allowed: set[Any],
) -> None:
    if field not in contract.fields:
        raise ContractValidationError(
            f'{contract.path}: rule "{rule_name}" references undeclared field "{field}".'
        )
    field_contract = contract.fields[field]
    for index, row in enumerate(rows):
        value = row.get(field)
        if value is None:
            continue
        if value == "" and field_contract.allow_empty:
            continue
        if value not in allowed:
            raise ContractValidationError(
                f'Dataset "{contract.name}" row {index} failed rule "{rule_name}": '
                f'"{field}"={value!r} not in {sorted(allowed)!r}.'
            )


def _normalize_table_name(table_name: str) -> str:
    return table_name.replace("`", "").lower()


def _unqualified_table_name(table_name: str) -> str:
    return table_name.replace("`", "").split(".")[-1]


def _matches_type(value: Any, expected_type: str) -> bool:
    normalized = expected_type.lower()
    if normalized in {"string", "text"}:
        return isinstance(value, str)
    if normalized in {"integer", "long", "int"}:
        return _as_int(value) is not None
    if normalized in {"number", "float", "double", "decimal"}:
        return _as_number(value) is not None
    if normalized in {"boolean", "bool"}:
        return isinstance(value, bool) or str(value).lower() in {"true", "false"}
    if normalized in {"date", "timestamp", "datetime"}:
        return isinstance(value, str) and bool(value.strip())
    if normalized == "array":
        return isinstance(value, list)
    return True


def _coerce_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    number = _as_number(value)
    if number is not None:
        return int(number) if number == int(number) else number
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    return value


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            number = Decimal(value)
        except Exception:
            return None
        if number == number.to_integral_value():
            return int(number)
    return None


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float | Decimal):
        return float(value)
    if isinstance(value, str):
        try:
            return float(Decimal(value))
        except Exception:
            return None
    return None


_ALLOWED_AST_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.Compare,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Is,
    ast.IsNot,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.Call,
)


def _safe_eval_bool(expression: str, context: dict[str, Any]) -> bool:
    tree = ast.parse(expression, mode="eval")
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_AST_NODES):
            raise ContractValidationError(f"Unsupported contract expression: {expression}")
        if isinstance(node, ast.Call) and (
            not isinstance(node.func, ast.Name) or node.func.id not in {"max", "min", "abs"}
        ):
            raise ContractValidationError(f"Unsupported contract function: {expression}")
        if (
            isinstance(node, ast.Name)
            and node.id not in context
            and node.id
            not in {
                "max",
                "min",
                "abs",
            }
        ):
            raise ContractValidationError(f"Unknown contract field in expression: {node.id}")

    compiled = compile(tree, "<data-product-contract>", "eval")
    return bool(eval(compiled, {"__builtins__": {}, "max": max, "min": min, "abs": abs}, context))
