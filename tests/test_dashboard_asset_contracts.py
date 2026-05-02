from pathlib import Path

import pytest

from scripts.dashboard_asset_contracts import (
    ContractValidationError,
    DataProductContract,
    FieldContract,
    load_dashboard_asset_contracts,
    load_gold_product_contracts,
    load_product_catalog,
    validate_projection_rows,
    validate_rows,
)


def _contract(
    fields: dict[str, FieldContract], unique_key: tuple[str, ...] = ()
) -> DataProductContract:
    return DataProductContract(
        contract_id="test.contract",
        version="0.1.0",
        name="test_dataset",
        product_kind="dashboard_asset",
        table_or_dataset="test_dataset",
        source_table="03_gold.sc_analytics.test_dataset",
        published_path="test_dataset.json",
        model_name="test_dataset",
        fields=fields,
        unique_key=unique_key,
        rules=(),
        path=Path("contract.yml"),
    )


def _contract_with_rules(
    fields: dict[str, FieldContract],
    rules: tuple[dict[str, object], ...],
) -> DataProductContract:
    contract = _contract(fields)
    return DataProductContract(
        contract_id=contract.contract_id,
        version=contract.version,
        name=contract.name,
        product_kind=contract.product_kind,
        table_or_dataset=contract.table_or_dataset,
        source_table=contract.source_table,
        published_path=contract.published_path,
        model_name=contract.model_name,
        fields=contract.fields,
        unique_key=contract.unique_key,
        rules=rules,
        path=contract.path,
    )


def test_missing_field_fails_even_when_nullable() -> None:
    contract = _contract(
        {
            "id": FieldContract("id", "string", required=True, nullable=False, allow_empty=False),
            "note": FieldContract(
                "note", "string", required=False, nullable=True, allow_empty=True
            ),
        }
    )

    with pytest.raises(ContractValidationError, match="missing fields: note"):
        validate_rows(contract, [{"id": "player-1"}])


def test_null_and_empty_are_distinct() -> None:
    contract = _contract(
        {
            "id": FieldContract("id", "string", required=True, nullable=False, allow_empty=False),
            "label": FieldContract(
                "label", "string", required=True, nullable=False, allow_empty=True
            ),
        }
    )

    validate_rows(contract, [{"id": "player-1", "label": ""}])

    with pytest.raises(ContractValidationError, match='field "label" is null'):
        validate_rows(contract, [{"id": "player-1", "label": None}])


def test_empty_string_rejected_unless_allowed() -> None:
    contract = _contract(
        {
            "id": FieldContract("id", "string", required=True, nullable=False, allow_empty=False),
            "label": FieldContract(
                "label", "string", required=True, nullable=False, allow_empty=False
            ),
        }
    )

    with pytest.raises(ContractValidationError, match='field "label" is empty'):
        validate_rows(contract, [{"id": "player-1", "label": ""}])


def test_duplicate_unique_key_fails() -> None:
    contract = _contract(
        {"id": FieldContract("id", "string", required=True, nullable=False, allow_empty=False)},
        unique_key=("id",),
    )

    with pytest.raises(ContractValidationError, match="duplicate unique key"):
        validate_rows(contract, [{"id": "player-1"}, {"id": "player-1"}])


def test_enum_rule_fails_for_unknown_value() -> None:
    contract = _contract_with_rules(
        {
            "status": FieldContract(
                "status", "string", required=True, nullable=False, allow_empty=False
            )
        },
        ({"name": "valid_status", "field": "status", "enum": ["active", "inactive"]},),
    )

    with pytest.raises(ContractValidationError, match="not in"):
        validate_rows(contract, [{"status": "unknown"}])


def test_numeric_expression_rule_fails_out_of_range() -> None:
    contract = _contract_with_rules(
        {
            "score": FieldContract(
                "score", "number", required=True, nullable=False, allow_empty=False
            )
        },
        ({"name": "score_range", "expression": "score >= 0 and score <= 100"},),
    )

    with pytest.raises(ContractValidationError, match="score_range"):
        validate_rows(contract, [{"score": 101}])


def test_unique_key_rejects_empty_string_by_default() -> None:
    contract = _contract(
        {"id": FieldContract("id", "string", required=True, nullable=False, allow_empty=False)},
        unique_key=("id",),
    )

    with pytest.raises(ContractValidationError, match='field "id" is empty'):
        validate_rows(contract, [{"id": ""}])


def test_contract_metadata_loads() -> None:
    catalog = load_product_catalog()
    dashboard_contracts = load_dashboard_asset_contracts()
    gold_contracts = load_gold_product_contracts()

    assert catalog.contract_set_version
    assert dashboard_contracts["preparation_readiness"].version
    assert "03_gold.sc_analytics.gold_preparation_readiness" in gold_contracts
    assert catalog.entries["raid_summary"].exposure == "dashboard"


def test_projection_validation_allows_unselected_gold_fields() -> None:
    contract = _contract_with_rules(
        {
            "id": FieldContract("id", "string", required=True, nullable=False, allow_empty=False),
            "score": FieldContract(
                "score", "number", required=True, nullable=False, allow_empty=False
            ),
            "source_only": FieldContract(
                "source_only", "string", required=True, nullable=False, allow_empty=False
            ),
        },
        (
            {"name": "score_range", "expression": "score >= 0 and score <= 100"},
            {"name": "source_rule", "expression": "source_only != ''"},
        ),
    )

    validate_projection_rows(contract, [{"id": "row-1", "score": 90}])


def test_projection_validation_keeps_applicable_rules() -> None:
    contract = _contract_with_rules(
        {
            "id": FieldContract("id", "string", required=True, nullable=False, allow_empty=False),
            "score": FieldContract(
                "score", "number", required=True, nullable=False, allow_empty=False
            ),
            "source_only": FieldContract(
                "source_only", "string", required=True, nullable=False, allow_empty=False
            ),
        },
        ({"name": "score_range", "expression": "score >= 0 and score <= 100"},),
    )

    with pytest.raises(ContractValidationError, match="score_range"):
        validate_projection_rows(contract, [{"id": "row-1", "score": 101}])
