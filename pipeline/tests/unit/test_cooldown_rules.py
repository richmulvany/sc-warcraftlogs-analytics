from __future__ import annotations

import re

from pipeline.gold._cooldown_rules import (
    COOLDOWN_RULE_RECORDS,
    HEALTH_POTION_ABILITY_IDS,
    HEALTHSTONE_ABILITY_IDS,
    PERSONAL_DEFENSIVE_RULE_RECORDS,
    defensive_cooldown_rules_sql,
    utility_ability_name_sql,
)


def _sql_row_count(sql: str) -> int:
    return sql.count("(")


def test_defensive_rules_sql_emits_expected_shape_for_every_rule() -> None:
    sql = defensive_cooldown_rules_sql()

    assert _sql_row_count(sql) == len(PERSONAL_DEFENSIVE_RULE_RECORDS)

    for rule in PERSONAL_DEFENSIVE_RULE_RECORDS:
        expected_row = (
            f"('{rule.player_class}', {rule.ability_id}, '{rule.ability_name}', "
            f"{rule.cooldown_seconds}, {rule.active_seconds}, "
            f"'{rule.allowed_spec_ids_sql}', '{rule.required_talent_spell_ids_sql}', "
            f"'{rule.capacity_model}', {rule.max_charges}, {rule.capacity_score_eligible_sql})"
        )
        assert expected_row in sql


def test_utility_ability_name_sql_emits_one_when_per_recovery_ability_without_duplicates() -> None:
    sql = utility_ability_name_sql()
    when_ability_ids = [int(match) for match in re.findall(r"WHEN (\d+) THEN", sql)]

    assert len(when_ability_ids) == len(set(when_ability_ids))

    for ability_id in HEALTHSTONE_ABILITY_IDS + HEALTH_POTION_ABILITY_IDS:
        assert when_ability_ids.count(ability_id) == 1


def test_rule_data_order_is_stable_across_python_and_sql_exports() -> None:
    sql = defensive_cooldown_rules_sql()
    ordered_ability_ids = [int(match) for match in re.findall(r"\('[^']+', (\d+),", sql)]

    assert (
        tuple(
            rule for rule in COOLDOWN_RULE_RECORDS if rule.category in {"personal", "personal_spec"}
        )
        == PERSONAL_DEFENSIVE_RULE_RECORDS
    )
    assert ordered_ability_ids == [rule.ability_id for rule in PERSONAL_DEFENSIVE_RULE_RECORDS]


def test_capacity_model_marks_form_state_cooldowns_unscored() -> None:
    metamorphosis = next(rule for rule in COOLDOWN_RULE_RECORDS if rule.ability_id == 187827)

    assert metamorphosis.ability_name == "Metamorphosis"
    assert metamorphosis.capacity_model == "form_state"
    assert metamorphosis.capacity_score_eligible is False
    assert metamorphosis.capacity_score_eligible_sql == 0


def test_capacity_model_marks_charge_based_defensives() -> None:
    charge_based_rules = {
        rule.ability_id: rule
        for rule in COOLDOWN_RULE_RECORDS
        if rule.capacity_model == "charge_based"
    }

    assert charge_based_rules[203720].ability_name == "Demon Spikes"
    assert charge_based_rules[203720].max_charges == 2
    assert charge_based_rules[22842].ability_name == "Frenzied Regeneration"
    assert charge_based_rules[22842].max_charges == 2
    assert charge_based_rules[61336].ability_name == "Survival Instincts"
    assert charge_based_rules[61336].max_charges == 2
    assert charge_based_rules[363916].ability_name == "Obsidian Scales"
    assert charge_based_rules[363916].max_charges == 2
