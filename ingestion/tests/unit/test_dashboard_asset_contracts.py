from __future__ import annotations

import pytest

from scripts.dashboard_asset_contracts import (
    ContractValidationError,
    load_dashboard_asset_contracts,
    validate_dashboard_asset_rows,
)
from scripts.publish_dashboard_assets import QUERY_EXPORTS


def test_load_dashboard_asset_contracts_includes_initial_high_risk_assets() -> None:
    contracts = load_dashboard_asset_contracts()

    assert set(contracts) >= {
        "boss_progression",
        "best_kills",
        "wipe_cooldown_utilization",
    }
    assert contracts["boss_progression"].unique_key == (
        "encounter_id",
        "boss_name",
        "zone_id",
        "zone_name",
        "difficulty",
    )


def test_dashboard_asset_contracts_target_published_datasets() -> None:
    contracts = load_dashboard_asset_contracts()

    for dataset, contract in contracts.items():
        assert dataset in QUERY_EXPORTS
        source_table, _query = QUERY_EXPORTS[dataset]
        assert contract.source_table == source_table
        assert contract.published_path == f"{dataset}.json"


def test_validate_dashboard_asset_rows_accepts_numeric_strings_from_sql_api() -> None:
    rows = [
        {
            "encounter_id": "3009",
            "boss_name": "Example Boss",
            "zone_id": "42",
            "zone_name": "Example Raid",
            "difficulty": "5",
            "difficulty_label": "Mythic",
            "total_pulls": "7",
            "total_kills": "1",
            "total_wipes": "6",
            "best_kill_seconds": "301",
            "avg_pull_duration_seconds": "245.5",
            "is_killed": "true",
            "first_kill_date": "2026-04-25",
            "last_attempt_date": "2026-04-26",
            "wipe_to_kill_ratio": "6.0",
        }
    ]

    validate_dashboard_asset_rows("boss_progression", rows)


def test_validate_dashboard_asset_rows_allows_same_encounter_difficulty_across_zones() -> None:
    base_row = {
        "encounter_id": 2824,
        "boss_name": "Example Boss",
        "zone_id": 100,
        "zone_name": "Example Raid",
        "difficulty": 4,
        "difficulty_label": "Heroic",
        "total_pulls": 7,
        "total_kills": 1,
        "total_wipes": 6,
        "best_kill_seconds": 301,
        "avg_pull_duration_seconds": 245.5,
        "is_killed": True,
        "first_kill_date": "2026-04-25",
        "last_attempt_date": "2026-04-26",
        "wipe_to_kill_ratio": 6.0,
    }
    other_zone_row = {
        **base_row,
        "boss_name": "Example Boss Revisited",
        "zone_id": 101,
        "zone_name": "Example Raid Revisited",
    }

    validate_dashboard_asset_rows("boss_progression", [base_row, other_zone_row])


def test_validate_dashboard_asset_rows_rejects_missing_required_field() -> None:
    rows = [
        {
            "encounter_id": 3009,
            "boss_name": "Example Boss",
            "zone_id": 42,
            "zone_name": "Example Raid",
            "difficulty": 5,
            "difficulty_label": "Mythic",
            "total_pulls": 7,
            "total_kills": 1,
            "total_wipes": 6,
            "best_kill_seconds": 301,
            "avg_pull_duration_seconds": 245.5,
            "is_killed": True,
            "first_kill_date": "2026-04-25",
            "last_attempt_date": "2026-04-26",
        }
    ]

    with pytest.raises(ContractValidationError, match="missing fields: wipe_to_kill_ratio"):
        validate_dashboard_asset_rows("boss_progression", rows)


def test_validate_dashboard_asset_rows_rejects_duplicate_unique_key() -> None:
    row = {
        "encounter_id": 3009,
        "boss_name": "Example Boss",
        "zone_name": "Example Raid",
        "difficulty": 5,
        "difficulty_label": "Mythic",
        "best_kill_seconds": 301,
        "avg_kill_seconds": 350,
        "total_kills": 2,
        "first_kill_date": "2026-04-25",
        "latest_kill_date": "2026-04-26",
        "best_kill_mm_ss": "5m 01s",
    }

    with pytest.raises(ContractValidationError, match="duplicate unique key"):
        validate_dashboard_asset_rows("best_kills", [row, dict(row)])


def test_validate_dashboard_asset_rows_accepts_scored_capacity_with_over_capacity_events() -> None:
    rows = [
        {
            "boss_name": "Example Boss",
            "zone_name": "Example Raid",
            "difficulty_label": "Heroic",
            "cooldown_category": "personal",
            "player_name": "Playerone",
            "player_class": "Mage",
            "ability_id": 110959,
            "ability_name": "Greater Invisibility",
            "possible_casts": 3,
            "observed_casts": 5,
            "actual_casts": 3,
            "over_capacity_casts": 2,
            "missed_casts": 0,
        }
    ]

    validate_dashboard_asset_rows("wipe_cooldown_utilization", rows)


def test_validate_dashboard_asset_rows_rejects_scored_actual_over_capacity() -> None:
    rows = [
        {
            "boss_name": "Example Boss",
            "zone_name": "Example Raid",
            "difficulty_label": "Heroic",
            "cooldown_category": "personal",
            "player_name": "Playerone",
            "player_class": "Mage",
            "ability_id": 110959,
            "ability_name": "Greater Invisibility",
            "possible_casts": 3,
            "observed_casts": 4,
            "actual_casts": 4,
            "over_capacity_casts": 0,
            "missed_casts": 0,
        }
    ]

    with pytest.raises(ContractValidationError, match="scored_actual_not_over_capacity"):
        validate_dashboard_asset_rows("wipe_cooldown_utilization", rows)


def test_validate_dashboard_asset_rows_rejects_over_capacity_formula_mismatch() -> None:
    rows = [
        {
            "boss_name": "Example Boss",
            "zone_name": "Example Raid",
            "difficulty_label": "Heroic",
            "cooldown_category": "personal",
            "player_name": "Playerone",
            "player_class": "Mage",
            "ability_id": 110959,
            "ability_name": "Greater Invisibility",
            "possible_casts": 3,
            "observed_casts": 5,
            "actual_casts": 3,
            "over_capacity_casts": 1,
            "missed_casts": 0,
        }
    ]

    with pytest.raises(ContractValidationError, match="over_capacity_formula"):
        validate_dashboard_asset_rows("wipe_cooldown_utilization", rows)
