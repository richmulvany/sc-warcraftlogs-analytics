"""Load and validate dashboard asset data contracts.

The contract files follow the open Data Contract Specification shape and keep
sc-analytics-specific executable checks under ``x-sc-analytics``.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONTRACT_DIR = REPO_ROOT / "pipeline" / "contracts" / "dashboard_assets"


class ContractValidationError(RuntimeError):
    """Raised when a dashboard asset does not satisfy its contract."""


@dataclass(frozen=True)
class FieldContract:
    name: str
    type: str
    required: bool


@dataclass(frozen=True)
class DashboardAssetContract:
    dataset: str
    source_table: str
    published_path: str
    fields: dict[str, FieldContract]
    unique_key: tuple[str, ...]
    rules: tuple[dict[str, Any], ...]
    path: Path


def load_dashboard_asset_contracts(
    contract_dir: Path = DEFAULT_CONTRACT_DIR,
) -> dict[str, DashboardAssetContract]:
    """Load dashboard asset contracts keyed by dataset name."""

    if not contract_dir.exists():
        return {}

    contracts: dict[str, DashboardAssetContract] = {}
    for path in sorted(contract_dir.glob("*.yml")):
        with path.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        contract = _parse_contract(path, raw)
        if contract.dataset in contracts:
            raise ContractValidationError(
                f'Duplicate dashboard asset contract for "{contract.dataset}".'
            )
        contracts[contract.dataset] = contract
    return contracts


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

    _validate_row_shapes(contract, rows)
    _validate_unique_key(contract, rows)
    _validate_rules(contract, rows)


def _parse_contract(path: Path, raw: dict[str, Any]) -> DashboardAssetContract:
    if not raw.get("dataContractSpecification"):
        raise ContractValidationError(f"{path}: missing dataContractSpecification.")

    extension = raw.get("x-sc-analytics") or {}
    asset = extension.get("dashboardAsset") or {}
    dataset = asset.get("dataset")
    if not isinstance(dataset, str) or not dataset:
        raise ContractValidationError(f"{path}: missing x-sc-analytics.dashboardAsset.dataset.")

    models = raw.get("models") or {}
    model = models.get(dataset)
    if not isinstance(model, dict):
        raise ContractValidationError(f'{path}: missing model "{dataset}".')

    fields_raw = model.get("fields") or {}
    if not isinstance(fields_raw, dict) or not fields_raw:
        raise ContractValidationError(f'{path}: model "{dataset}" has no fields.')

    fields = {
        name: FieldContract(
            name=name,
            type=str(spec.get("type", "string")),
            required=bool(spec.get("required", False)),
        )
        for name, spec in fields_raw.items()
        if isinstance(spec, dict)
    }

    unique_key_raw = model.get("primaryKey") or [
        name for name, spec in fields_raw.items() if isinstance(spec, dict) and spec.get("primary")
    ]
    unique_key = tuple(str(value) for value in unique_key_raw)

    source_table = asset.get("sourceTable")
    published_path = asset.get("publishedPath")
    return DashboardAssetContract(
        dataset=dataset,
        source_table=str(source_table or ""),
        published_path=str(published_path or f"{dataset}.json"),
        fields=fields,
        unique_key=unique_key,
        rules=tuple(asset.get("rules") or ()),
        path=path,
    )


def _validate_row_shapes(contract: DashboardAssetContract, rows: list[dict[str, Any]]) -> None:
    expected_fields = set(contract.fields)
    for index, row in enumerate(rows):
        missing = sorted(expected_fields - set(row))
        if missing:
            raise ContractValidationError(
                f'Dataset "{contract.dataset}" row {index} is missing fields: {", ".join(missing)}'
            )

        for field in contract.fields.values():
            value = row.get(field.name)
            if _is_null(value):
                if field.required:
                    raise ContractValidationError(
                        f'Dataset "{contract.dataset}" row {index} field "{field.name}" is null.'
                    )
                continue
            if not _matches_type(value, field.type):
                raise ContractValidationError(
                    f'Dataset "{contract.dataset}" row {index} field "{field.name}" '
                    f"expected {field.type}, got {type(value).__name__}."
                )


def _validate_unique_key(contract: DashboardAssetContract, rows: list[dict[str, Any]]) -> None:
    if not contract.unique_key:
        return
    seen: set[tuple[Any, ...]] = set()
    for index, row in enumerate(rows):
        key = tuple(row.get(field) for field in contract.unique_key)
        if any(_is_null(value) for value in key):
            raise ContractValidationError(
                f'Dataset "{contract.dataset}" row {index} has null unique key {contract.unique_key}.'
            )
        if key in seen:
            raise ContractValidationError(
                f'Dataset "{contract.dataset}" has duplicate unique key {contract.unique_key}: {key}'
            )
        seen.add(key)


def _validate_rules(contract: DashboardAssetContract, rows: list[dict[str, Any]]) -> None:
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
                        f'Dataset "{contract.dataset}" row {index} failed rule "{name}": '
                        f"{expression}"
                    )


def _validate_enum_rule(
    contract: DashboardAssetContract,
    rows: list[dict[str, Any]],
    rule_name: str,
    field: str,
    allowed: set[Any],
) -> None:
    for index, row in enumerate(rows):
        value = row.get(field)
        if _is_null(value):
            continue
        if value not in allowed:
            raise ContractValidationError(
                f'Dataset "{contract.dataset}" row {index} failed rule "{rule_name}": '
                f'"{field}"={value!r} not in {sorted(allowed)!r}.'
            )


def _is_null(value: Any) -> bool:
    return value is None or value == ""


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
    if _is_null(value):
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

    compiled = compile(tree, "<dashboard-asset-contract>", "eval")
    return bool(eval(compiled, {"__builtins__": {}, "max": max, "min": min, "abs": abs}, context))
