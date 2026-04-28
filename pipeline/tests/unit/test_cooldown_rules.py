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
            f"'{rule.allowed_spec_ids_sql}', '{rule.required_talent_spell_ids_sql}')"
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

    assert tuple(rule for rule in COOLDOWN_RULE_RECORDS if rule.category in {"personal", "personal_spec"}) == PERSONAL_DEFENSIVE_RULE_RECORDS
    assert ordered_ability_ids == [rule.ability_id for rule in PERSONAL_DEFENSIVE_RULE_RECORDS]
