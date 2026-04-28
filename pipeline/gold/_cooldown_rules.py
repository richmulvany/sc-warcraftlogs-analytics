# Shared constants for gold utility/wipe diagnostics notebooks.
#
# Mirrors the tables formerly defined inline in scripts/export_gold_tables.py.
# Both utility_products.py and wipe_diagnostics.py read from this module; update
# here only, so the two notebooks stay in lockstep.

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CooldownRuleRecord:
    category: str
    player_class: str
    ability_id: int
    ability_name: str
    cooldown_seconds: int
    active_seconds: int
    allowed_spec_ids: tuple[int, ...] = ()
    required_talent_spell_ids: tuple[int, ...] = ()

    @property
    def allowed_spec_ids_sql(self) -> str:
        return "|".join(str(spec_id) for spec_id in self.allowed_spec_ids)

    @property
    def required_talent_spell_ids_sql(self) -> str:
        return "|".join(str(spell_id) for spell_id in self.required_talent_spell_ids)


EXCLUDED_ZONES = ["Blackrock Depths"]

HEALTHSTONE_ABILITY_IDS = [6262]

HEALTH_POTION_ABILITY_IDS = [
    431416,
    431419,
    431422,
    370511,
    371024,
    371028,
    371033,
    371036,
    371039,
    371043,
    1238009,
    1262857,
    1234768,
]

UTILITY_ABILITY_NAMES = {
    6262: "Healthstone",
    431416: "Algari Healing Potion",
    431419: "Algari Healing Potion",
    431422: "Algari Healing Potion",
    370511: "Refreshing Healing Potion",
    371024: "Refreshing Healing Potion",
    371028: "Refreshing Healing Potion",
    371033: "Refreshing Healing Potion",
    371036: "Refreshing Healing Potion",
    371039: "Refreshing Healing Potion",
    371043: "Refreshing Healing Potion",
    1238009: "Invigorating Healing Potion",
    1262857: "Potent Healing Potion",
    1234768: "Silvermoon Health Potion",
    1230866: "Silvermoon Health Potion",
    1230867: "Silvermoon Health Potion",
    1230868: "Silvermoon Health Potion",
    1230869: "Silvermoon Health Potion",
    1230870: "Silvermoon Health Potion",
    1230871: "Silvermoon Health Potion",
    1230872: "Silvermoon Health Potion",
}


COOLDOWN_RULE_RECORDS: tuple[CooldownRuleRecord, ...] = (
    CooldownRuleRecord("personal", "DeathKnight", 48707, "Anti-Magic Shell", 60, 5),
    CooldownRuleRecord("personal", "DeathKnight", 48792, "Icebound Fortitude", 180, 8),
    CooldownRuleRecord("personal", "DeathKnight", 49039, "Lichborne", 120, 10, required_talent_spell_ids=(49039,)),
    CooldownRuleRecord("personal", "DeathKnight", 48743, "Death Pact", 120, 1, required_talent_spell_ids=(48743,)),
    CooldownRuleRecord("personal", "DemonHunter", 198589, "Blur", 60, 10, allowed_spec_ids=(577, 1480)),
    CooldownRuleRecord(
        "personal",
        "DemonHunter",
        196555,
        "Netherwalk",
        180,
        6,
        allowed_spec_ids=(577, 1480),
        required_talent_spell_ids=(196555,),
    ),
    CooldownRuleRecord("personal_spec", "DemonHunter", 203720, "Demon Spikes", 15, 6, allowed_spec_ids=(581,)),
    CooldownRuleRecord("personal_spec", "DemonHunter", 187827, "Metamorphosis", 240, 15, allowed_spec_ids=(581,)),
    CooldownRuleRecord("personal_spec", "DemonHunter", 204021, "Fiery Brand", 60, 8, allowed_spec_ids=(581,)),
    CooldownRuleRecord("personal", "Druid", 22812, "Barkskin", 60, 12),
    CooldownRuleRecord("personal", "Druid", 61336, "Survival Instincts", 180, 6, required_talent_spell_ids=(61336,)),
    CooldownRuleRecord("personal", "Druid", 108238, "Renewal", 90, 1, required_talent_spell_ids=(108238,)),
    CooldownRuleRecord("personal", "Evoker", 363916, "Obsidian Scales", 90, 12),
    CooldownRuleRecord("personal", "Evoker", 374348, "Renewing Blaze", 90, 8, required_talent_spell_ids=(374348,)),
    CooldownRuleRecord("personal", "Hunter", 186265, "Aspect of the Turtle", 180, 8),
    CooldownRuleRecord("personal", "Hunter", 264735, "Survival of the Fittest", 180, 6, required_talent_spell_ids=(264735,)),
    CooldownRuleRecord("personal", "Hunter", 109304, "Exhilaration", 120, 1),
    CooldownRuleRecord("personal", "Mage", 45438, "Ice Block", 240, 10),
    CooldownRuleRecord("personal", "Mage", 342245, "Alter Time", 60, 10, required_talent_spell_ids=(342245,)),
    CooldownRuleRecord("personal", "Mage", 55342, "Mirror Image", 120, 40, required_talent_spell_ids=(55342,)),
    CooldownRuleRecord("personal", "Mage", 110959, "Greater Invisibility", 120, 3, required_talent_spell_ids=(110959,)),
    CooldownRuleRecord("personal", "Monk", 115203, "Fortifying Brew", 360, 15),
    CooldownRuleRecord("personal", "Monk", 122783, "Diffuse Magic", 90, 6, required_talent_spell_ids=(122783,)),
    CooldownRuleRecord("personal", "Monk", 122278, "Dampen Harm", 120, 10, required_talent_spell_ids=(122278,)),
    CooldownRuleRecord("personal", "Monk", 122470, "Touch of Karma", 90, 10, allowed_spec_ids=(269,), required_talent_spell_ids=(122470,)),
    CooldownRuleRecord("personal", "Paladin", 642, "Divine Shield", 300, 8),
    CooldownRuleRecord("personal", "Paladin", 498, "Divine Protection", 60, 8, allowed_spec_ids=(65,)),
    CooldownRuleRecord("personal", "Paladin", 184662, "Shield of Vengeance", 90, 10, allowed_spec_ids=(70,)),
    CooldownRuleRecord("personal", "Priest", 19236, "Desperate Prayer", 90, 10, required_talent_spell_ids=(19236,)),
    CooldownRuleRecord("personal", "Priest", 47585, "Dispersion", 120, 6, allowed_spec_ids=(258,)),
    CooldownRuleRecord("personal", "Rogue", 31224, "Cloak of Shadows", 120, 5),
    CooldownRuleRecord("personal", "Rogue", 5277, "Evasion", 120, 10),
    CooldownRuleRecord("personal", "Shaman", 108271, "Astral Shift", 90, 8),
    CooldownRuleRecord("personal", "Warlock", 104773, "Unending Resolve", 180, 8),
    CooldownRuleRecord("personal", "Warlock", 108416, "Dark Pact", 60, 20, required_talent_spell_ids=(108416,)),
    CooldownRuleRecord("personal", "Warrior", 118038, "Die by the Sword", 120, 8, allowed_spec_ids=(71,), required_talent_spell_ids=(118038,)),
    CooldownRuleRecord("personal", "Warrior", 184364, "Enraged Regeneration", 120, 8, allowed_spec_ids=(72,), required_talent_spell_ids=(184364,)),
    CooldownRuleRecord("personal", "Warrior", 202168, "Impending Victory", 30, 1, required_talent_spell_ids=(202168,)),
    CooldownRuleRecord("personal_spec", "DeathKnight", 55233, "Vampiric Blood", 90, 10, allowed_spec_ids=(250,)),
    CooldownRuleRecord("personal_spec", "Druid", 22842, "Frenzied Regeneration", 36, 3, allowed_spec_ids=(104,)),
    CooldownRuleRecord("personal_spec", "Paladin", 31850, "Ardent Defender", 120, 8, allowed_spec_ids=(66,)),
    CooldownRuleRecord("personal_spec", "Paladin", 86659, "Guardian of Ancient Kings", 300, 8, allowed_spec_ids=(66,)),
    CooldownRuleRecord("personal_spec", "Warrior", 871, "Shield Wall", 240, 8, allowed_spec_ids=(73,)),
    CooldownRuleRecord("personal_spec", "Warrior", 12975, "Last Stand", 180, 15, allowed_spec_ids=(73,)),
    CooldownRuleRecord("raid", "DeathKnight", 51052, "Anti-Magic Zone", 120, 10, required_talent_spell_ids=(51052,)),
    CooldownRuleRecord("raid", "DemonHunter", 196718, "Darkness", 300, 8, required_talent_spell_ids=(196718,)),
    CooldownRuleRecord("raid", "Evoker", 374227, "Zephyr", 120, 8, required_talent_spell_ids=(374227,)),
    CooldownRuleRecord("raid", "Paladin", 31821, "Aura Mastery", 180, 8, allowed_spec_ids=(65,), required_talent_spell_ids=(31821,)),
    CooldownRuleRecord("raid", "Priest", 62618, "Power Word: Barrier", 180, 10, allowed_spec_ids=(256,), required_talent_spell_ids=(62618,)),
    CooldownRuleRecord("raid", "Shaman", 98008, "Spirit Link Totem", 180, 6, allowed_spec_ids=(264,), required_talent_spell_ids=(98008,)),
    CooldownRuleRecord("raid", "Warrior", 97462, "Rallying Cry", 180, 10),
    CooldownRuleRecord("external", "Druid", 102342, "Ironbark", 90, 12, allowed_spec_ids=(105,), required_talent_spell_ids=(102342,)),
    CooldownRuleRecord("external", "Evoker", 357170, "Time Dilation", 60, 8, allowed_spec_ids=(1468,), required_talent_spell_ids=(357170,)),
    CooldownRuleRecord("external", "Monk", 116849, "Life Cocoon", 120, 12, allowed_spec_ids=(270,), required_talent_spell_ids=(116849,)),
    CooldownRuleRecord("external", "Paladin", 6940, "Blessing of Sacrifice", 120, 12, allowed_spec_ids=(65,), required_talent_spell_ids=(6940,)),
    CooldownRuleRecord("external", "Priest", 33206, "Pain Suppression", 180, 8, allowed_spec_ids=(256,), required_talent_spell_ids=(33206,)),
    CooldownRuleRecord("external", "Priest", 47788, "Guardian Spirit", 180, 10, allowed_spec_ids=(257,), required_talent_spell_ids=(47788,)),
)

PERSONAL_DEFENSIVE_RULE_RECORDS: tuple[CooldownRuleRecord, ...] = tuple(
    rule for rule in COOLDOWN_RULE_RECORDS if rule.category in {"personal", "personal_spec"}
)

COOLDOWN_RULES = [
    (
        rule.category,
        rule.player_class,
        rule.ability_id,
        rule.ability_name,
        rule.cooldown_seconds,
        rule.active_seconds,
    )
    for rule in COOLDOWN_RULE_RECORDS
]
COOLDOWN_ALLOWED_SPECS = {
    rule.ability_id: list(rule.allowed_spec_ids) for rule in COOLDOWN_RULE_RECORDS if rule.allowed_spec_ids
}
COOLDOWN_REQUIRED_TALENT_SPELL_IDS = {
    rule.ability_id: list(rule.required_talent_spell_ids)
    for rule in COOLDOWN_RULE_RECORDS
    if rule.required_talent_spell_ids
}
PERSONAL_DEFENSIVE_RULES = [
    (
        rule.category,
        rule.player_class,
        rule.ability_id,
        rule.ability_name,
        rule.cooldown_seconds,
        rule.active_seconds,
    )
    for rule in PERSONAL_DEFENSIVE_RULE_RECORDS
]
TRACKED_PERSONAL_DEFENSIVE_RULES = PERSONAL_DEFENSIVE_RULES
DEFENSIVE_ABILITY_IDS = [rule.ability_id for rule in PERSONAL_DEFENSIVE_RULE_RECORDS]
DEFENSIVE_ABILITY_ID_TO_NAME = {rule.ability_id: rule.ability_name for rule in COOLDOWN_RULE_RECORDS}
DEFENSIVE_ABILITY_NAMES = sorted(rule.ability_name.lower() for rule in PERSONAL_DEFENSIVE_RULE_RECORDS)
UTILITY_ABILITY_ID_TO_NAME = {**UTILITY_ABILITY_NAMES, **DEFENSIVE_ABILITY_ID_TO_NAME}


def _sql_repr(value: str | int) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def _cooldown_rule_sql_rows(rules: tuple[CooldownRuleRecord, ...], *, include_category: bool) -> str:
    rows: list[str] = []
    for rule in rules:
        row_values: list[str | int] = []
        if include_category:
            row_values.append(rule.category)
        row_values.extend(
            [
                rule.player_class,
                rule.ability_id,
                rule.ability_name,
                rule.cooldown_seconds,
                rule.active_seconds,
                rule.allowed_spec_ids_sql,
                rule.required_talent_spell_ids_sql,
            ]
        )
        rows.append("(" + ", ".join(_sql_repr(value) for value in row_values) + ")")
    return ",\n            ".join(rows)


def defensive_cooldown_rules_sql() -> str:
    return _cooldown_rule_sql_rows(PERSONAL_DEFENSIVE_RULE_RECORDS, include_category=False)


def cooldown_rules_sql() -> str:
    return _cooldown_rule_sql_rows(COOLDOWN_RULE_RECORDS, include_category=True)


def utility_ability_name_sql() -> str:
    return " ".join(
        f"WHEN {ability_id} THEN {_sql_repr(name)}"
        for ability_id, name in sorted(UTILITY_ABILITY_ID_TO_NAME.items())
    )
