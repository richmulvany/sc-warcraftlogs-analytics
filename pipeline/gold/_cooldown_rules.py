# Shared constants for gold utility/wipe diagnostics notebooks.
#
# Mirrors the tables formerly defined inline in scripts/export_gold_tables.py.
# Both utility_products.py and wipe_diagnostics.py read from this module; update
# here only, so the two notebooks stay in lockstep.

EXCLUDED_ZONES = ["Blackrock Depths"]

HEALTHSTONE_ABILITY_IDS = [6262]

HEALTH_POTION_ABILITY_IDS = [
    431416, 431419, 431422,
    370511, 371024, 371028, 371033, 371036, 371039, 371043,
    1238009, 1262857, 1234768,
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

# (category, class, ability_id, ability_name, cooldown_seconds, active_seconds)
COOLDOWN_RULES = [
    ("personal", "DeathKnight", 48707, "Anti-Magic Shell", 60, 5),
    ("personal", "DeathKnight", 48792, "Icebound Fortitude", 180, 8),
    ("personal", "DeathKnight", 49039, "Lichborne", 120, 10),
    ("personal", "DeathKnight", 48743, "Death Pact", 120, 1),
    ("personal", "DemonHunter", 198589, "Blur", 60, 10),
    ("personal", "DemonHunter", 196555, "Netherwalk", 180, 6),
    ("personal", "Druid", 22812, "Barkskin", 60, 12),
    ("personal", "Druid", 61336, "Survival Instincts", 180, 6),
    ("personal", "Druid", 108238, "Renewal", 90, 1),
    ("personal", "Evoker", 363916, "Obsidian Scales", 90, 12),
    ("personal", "Evoker", 374348, "Renewing Blaze", 90, 8),
    ("personal", "Hunter", 186265, "Aspect of the Turtle", 180, 8),
    ("personal", "Hunter", 264735, "Survival of the Fittest", 180, 6),
    ("personal", "Hunter", 109304, "Exhilaration", 120, 1),
    ("personal", "Mage", 45438, "Ice Block", 240, 10),
    ("personal", "Mage", 342245, "Alter Time", 60, 10),
    ("personal", "Mage", 55342, "Mirror Image", 120, 40),
    ("personal", "Mage", 110959, "Greater Invisibility", 120, 3),
    ("personal", "Monk", 115203, "Fortifying Brew", 360, 15),
    ("personal", "Monk", 122783, "Diffuse Magic", 90, 6),
    ("personal", "Monk", 122278, "Dampen Harm", 120, 10),
    ("personal", "Monk", 122470, "Touch of Karma", 90, 10),
    ("personal", "Paladin", 642, "Divine Shield", 300, 8),
    ("personal", "Paladin", 498, "Divine Protection", 60, 8),
    ("personal", "Paladin", 184662, "Shield of Vengeance", 90, 10),
    ("personal", "Priest", 19236, "Desperate Prayer", 90, 10),
    ("personal", "Priest", 47585, "Dispersion", 120, 6),
    ("personal", "Rogue", 31224, "Cloak of Shadows", 120, 5),
    ("personal", "Rogue", 5277, "Evasion", 120, 10),
    ("personal", "Shaman", 108271, "Astral Shift", 90, 8),
    ("personal", "Warlock", 104773, "Unending Resolve", 180, 8),
    ("personal", "Warlock", 108416, "Dark Pact", 60, 20),
    ("personal", "Warrior", 118038, "Die by the Sword", 120, 8),
    ("personal", "Warrior", 184364, "Enraged Regeneration", 120, 8),
    ("personal_spec", "DeathKnight", 55233, "Vampiric Blood", 90, 10),
    ("personal_spec", "Druid", 22842, "Frenzied Regeneration", 36, 3),
    ("personal_spec", "Paladin", 31850, "Ardent Defender", 120, 8),
    ("personal_spec", "Paladin", 86659, "Guardian of Ancient Kings", 300, 8),
    ("personal_spec", "Warrior", 871, "Shield Wall", 240, 8),
    ("personal_spec", "Warrior", 12975, "Last Stand", 180, 15),
    ("raid", "DeathKnight", 51052, "Anti-Magic Zone", 120, 10),
    ("raid", "DemonHunter", 196718, "Darkness", 300, 8),
    ("raid", "Evoker", 374227, "Zephyr", 120, 8),
    ("raid", "Paladin", 31821, "Aura Mastery", 180, 8),
    ("raid", "Priest", 62618, "Power Word: Barrier", 180, 10),
    ("raid", "Shaman", 98008, "Spirit Link Totem", 180, 6),
    ("raid", "Warrior", 97462, "Rallying Cry", 180, 10),
    ("external", "Druid", 102342, "Ironbark", 90, 12),
    ("external", "Evoker", 357170, "Time Dilation", 60, 8),
    ("external", "Monk", 116849, "Life Cocoon", 120, 12),
    ("external", "Paladin", 6940, "Blessing of Sacrifice", 120, 12),
    ("external", "Priest", 33206, "Pain Suppression", 180, 8),
    ("external", "Priest", 47788, "Guardian Spirit", 180, 10),
]

COOLDOWN_ALLOWED_SPECS = {
    198589: [577, 1480],
    196555: [577, 1480],
    47585: [258],
    498: [65],
    184662: [70],
    102342: [105],
    357170: [1468],
    116849: [270],
    6940: [65],
    33206: [256],
    47788: [257],
    31821: [65],
    62618: [256],
    98008: [264],
    122470: [269],
    55233: [250],
    22842: [104],
    31850: [66],
    86659: [66],
    118038: [71],
    184364: [72],
    871: [73],
    12975: [73],
}

COOLDOWN_REQUIRED_TALENT_SPELL_IDS = {
    196555: [196555], 49039: [49039], 48743: [48743], 61336: [61336],
    108238: [108238], 374348: [374348], 264735: [264735], 342245: [342245],
    55342: [55342], 110959: [110959], 122783: [122783], 122278: [122278],
    122470: [122470], 19236: [19236], 108270: [108270], 108416: [108416],
    118038: [118038], 184364: [184364], 51052: [51052], 196718: [196718],
    374227: [374227], 31821: [31821], 62618: [62618], 98008: [98008],
    102342: [102342], 357170: [357170], 116849: [116849], 6940: [6940],
    33206: [33206], 47788: [47788],
}

PERSONAL_DEFENSIVE_RULES = [r for r in COOLDOWN_RULES if r[0] in {"personal", "personal_spec"}]
TRACKED_PERSONAL_DEFENSIVE_RULES = PERSONAL_DEFENSIVE_RULES
DEFENSIVE_ABILITY_IDS = [r[2] for r in TRACKED_PERSONAL_DEFENSIVE_RULES]
DEFENSIVE_ABILITY_ID_TO_NAME = {r[2]: r[3] for r in COOLDOWN_RULES}
DEFENSIVE_ABILITY_NAMES = sorted(r[3].lower() for r in TRACKED_PERSONAL_DEFENSIVE_RULES)
UTILITY_ABILITY_ID_TO_NAME = {**UTILITY_ABILITY_NAMES, **DEFENSIVE_ABILITY_ID_TO_NAME}


def _sql_repr(value):
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def defensive_cooldown_rules_sql() -> str:
    rows = []
    for _, player_class, ability_id, ability_name, cd, active in PERSONAL_DEFENSIVE_RULES:
        allowed = "|".join(str(s) for s in COOLDOWN_ALLOWED_SPECS.get(ability_id, []))
        talents = "|".join(str(s) for s in COOLDOWN_REQUIRED_TALENT_SPELL_IDS.get(ability_id, []))
        rows.append(
            f"({_sql_repr(player_class)}, {ability_id}, {_sql_repr(ability_name)}, "
            f"{cd}, {active}, {_sql_repr(allowed)}, {_sql_repr(talents)})"
        )
    return ",\n            ".join(rows)


def cooldown_rules_sql() -> str:
    rows = []
    for category, player_class, ability_id, ability_name, cd, active in COOLDOWN_RULES:
        allowed = "|".join(str(s) for s in COOLDOWN_ALLOWED_SPECS.get(ability_id, []))
        talents = "|".join(str(s) for s in COOLDOWN_REQUIRED_TALENT_SPELL_IDS.get(ability_id, []))
        rows.append(
            f"({_sql_repr(category)}, {_sql_repr(player_class)}, {ability_id}, "
            f"{_sql_repr(ability_name)}, {cd}, {active}, "
            f"{_sql_repr(allowed)}, {_sql_repr(talents)})"
        )
    return ",\n            ".join(rows)


def utility_ability_name_sql() -> str:
    return " ".join(
        f"WHEN {ability_id} THEN {_sql_repr(name)}"
        for ability_id, name in sorted(UTILITY_ABILITY_ID_TO_NAME.items())
    )
