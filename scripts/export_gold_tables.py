"""
Export frontend gold tables from Databricks SQL to static CSV files.

The React app reads CSVs from `frontend/public/data`, so this script exports the
same files the frontend consumes rather than the old JSON sample format.

Usage:
    python scripts/export_gold_tables.py
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from dotenv import load_dotenv

from ingestion.src.adapters.wcl.client import WarcraftLogsAdapter, WarcraftLogsConfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

CATALOG = os.environ.get("DATABRICKS_CATALOG", "04_sdp")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "warcraftlogs")
OVERRIDES_CATALOG = os.environ.get("OVERRIDES_DATABRICKS_CATALOG", "00_governance")
OVERRIDES_SCHEMA = os.environ.get("OVERRIDES_DATABRICKS_SCHEMA", "warcraftlogs_admin")
OVERRIDES_TABLE = os.environ.get("OVERRIDES_DATABRICKS_TABLE", "preparation_identity_overrides")
_output_dir = Path(os.environ.get("EXPORT_OUTPUT_DIR", "frontend/public/data"))
OUTPUT_DIR = _output_dir if _output_dir.is_absolute() else REPO_ROOT / _output_dir
FRONTEND_PUBLIC_DATA_DIR = REPO_ROOT / "frontend/public/data"
POLL_INTERVAL_SECONDS = float(os.environ.get("EXPORT_POLL_INTERVAL_SECONDS", "2"))
POLL_TIMEOUT_SECONDS = int(os.environ.get("EXPORT_POLL_TIMEOUT_SECONDS", "300"))
LIVE_ROSTER_SHEET_ID = (
    os.environ.get("LIVE_ROSTER_SHEET_ID") or "1fHtbnNTHrLVFqq5e7L7usN4qI4LGd1JKRKMdVuHnmRg"
)
LIVE_ROSTER_SHEET_GID = os.environ.get("LIVE_ROSTER_SHEET_GID") or "0"
LIVE_ROSTER_FILENAME = os.environ.get("LIVE_ROSTER_FILENAME") or "live_raid_roster.csv"
PREPARATION_OVERRIDES_FILENAME = (
    os.environ.get("PREPARATION_OVERRIDES_FILENAME") or "preparation_overrides.csv"
)
WCL_CLIENT_ID = os.environ.get("WCL_CLIENT_ID") or os.environ.get("WARCRAFTLOGS_CLIENT_ID") or ""
WCL_CLIENT_SECRET = (
    os.environ.get("WCL_CLIENT_SECRET") or os.environ.get("WARCRAFTLOGS_CLIENT_SECRET") or ""
)
WCL_GUILD_NAME = (
    os.environ.get("WCL_GUILD_NAME") or os.environ.get("GUILD_NAME") or "Student Council"
)
WCL_GUILD_SERVER_SLUG = (
    os.environ.get("WCL_GUILD_SERVER_SLUG")
    or os.environ.get("GUILD_SERVER_SLUG")
    or "twisting-nether"
)
WCL_GUILD_SERVER_REGION = (
    os.environ.get("WCL_GUILD_SERVER_REGION") or os.environ.get("GUILD_SERVER_REGION") or "EU"
)
GUILD_ZONE_RANKS_FILENAME = os.environ.get("GUILD_ZONE_RANKS_FILENAME") or "guild_zone_ranks.csv"
BLIZZARD_CLIENT_ID = (
    os.environ.get("BLIZZARD_CLIENT_ID_PROFILE")
    or os.environ.get("BLIZZARD_PROFILE_CLIENT_ID")
    or os.environ.get("BLIZZARD_CLIENT_ID")
    or os.environ.get("BLIZZARD_CLIENT_ID_ROSTER")
    or os.environ.get("BLIZZARD_CLIENTID")
    or os.environ.get("BLIZZARD_API_CLIENT_ID")
    or ""
)
BLIZZARD_CLIENT_SECRET = (
    os.environ.get("BLIZZARD_CLIENT_SECRET_PROFILE")
    or os.environ.get("BLIZZARD_PROFILE_CLIENT_SECRET")
    or os.environ.get("BLIZZARD_CLIENT_SECRET")
    or os.environ.get("BLIZZARD_CLIENT_SECRET_ROSTER")
    or os.environ.get("BLIZZARD_CLIENTSECRET")
    or os.environ.get("BLIZZARD_API_CLIENT_SECRET")
    or ""
)
BLIZZARD_REGION = (
    os.environ.get("BLIZZARD_REGION")
    or os.environ.get("GUILD_SERVER_REGION")
    or WCL_GUILD_SERVER_REGION
    or "EU"
).lower()
BLIZZARD_LOCALE = os.environ.get("BLIZZARD_LOCALE") or "en_GB"
BLIZZARD_PROFILE_EXPORT_CAP = int(os.environ.get("BLIZZARD_PROFILE_EXPORT_CAP", "80"))
PLAYER_CHARACTER_MEDIA_FILENAME = (
    os.environ.get("PLAYER_CHARACTER_MEDIA_FILENAME") or "player_character_media.csv"
)
PLAYER_CHARACTER_EQUIPMENT_FILENAME = (
    os.environ.get("PLAYER_CHARACTER_EQUIPMENT_FILENAME") or "player_character_equipment.csv"
)
PLAYER_RAID_ACHIEVEMENTS_FILENAME = (
    os.environ.get("PLAYER_RAID_ACHIEVEMENTS_FILENAME") or "player_raid_achievements.csv"
)

FRONTEND_TABLES: dict[str, str] = {
    "gold_raid_summary.csv": "gold_raid_summary",
    "gold_player_performance_summary.csv": "gold_player_performance_summary",
    "gold_boss_progression.csv": "gold_boss_progression",
    "gold_encounter_catalog.csv": "gold_encounter_catalog",
    "gold_boss_kill_roster.csv": "gold_boss_kill_roster",
    "gold_player_attendance.csv": "gold_player_attendance",
    "gold_player_utility_by_pull.csv": "gold_player_utility_by_pull",
    "gold_wipe_survival_events.csv": "gold_wipe_survival_events",
    "gold_wipe_cooldown_utilization.csv": "gold_wipe_cooldown_utilization",
    "gold_guild_roster.csv": "gold_guild_roster",
    "gold_weekly_activity.csv": "gold_weekly_activity",
    "gold_boss_wipe_analysis.csv": "gold_boss_wipe_analysis",
    "gold_player_survivability.csv": "gold_player_survivability",
    "gold_player_death_events.csv": "gold_player_death_events",
    "gold_progression_timeline.csv": "gold_progression_timeline",
    "gold_raid_team.csv": "gold_raid_team",
    "gold_best_kills.csv": "gold_best_kills",
    "gold_boss_mechanics.csv": "gold_boss_mechanics",
    "gold_player_boss_performance.csv": "gold_player_boss_performance",
    "gold_boss_progress_history.csv": "gold_boss_progress_history",
    "gold_boss_pull_history.csv": "gold_boss_pull_history",
    "gold_player_mplus_summary.csv": "gold_player_mplus_summary",
    "gold_player_mplus_score_history.csv": "gold_player_mplus_score_history",
    "gold_player_mplus_run_history.csv": "gold_player_mplus_run_history",
    "gold_player_mplus_weekly_activity.csv": "gold_player_mplus_weekly_activity",
    "gold_player_mplus_dungeon_breakdown.csv": "gold_player_mplus_dungeon_breakdown",
}
EXCLUDED_ZONES = {"Blackrock Depths"}
HEALTHSTONE_ABILITY_IDS = [
    6262,
]
HEALTH_POTION_ABILITY_IDS = [
    431416,  # Algari Healing Potion
    431419,  # Algari Healing Potion variant
    431422,  # Algari Healing Potion variant
    370511,  # Refreshing Healing Potion
    371024,  # Refreshing Healing Potion variant
    371028,  # Refreshing Healing Potion variant
    371033,  # Refreshing Healing Potion variant
    371036,  # Refreshing Healing Potion variant
    371039,  # Refreshing Healing Potion variant
    371043,  # Refreshing Healing Potion variant
    1238009,  # Invigorating Healing Potion
    1262857,  # Potent Healing Potion
    1234768,  # Silvermoon Health Potion
]
UTILITY_ABILITY_NAMES = {
    # Healthstone
    6262: "Healthstone",
    # --- Pre-Midnight ---
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
    # --- Midnight ---
    # Silvermoon Health Potion item-use effect
    1234768: "Silvermoon Health Potion",
    # Silvermoon Health Potion (rank variants)
    1230866: "Silvermoon Health Potion",
    1230867: "Silvermoon Health Potion",
    1230868: "Silvermoon Health Potion",
    1230869: "Silvermoon Health Potion",
    1230870: "Silvermoon Health Potion",
    1230871: "Silvermoon Health Potion",
    1230872: "Silvermoon Health Potion",
}
COOLDOWN_RULES = [
    # category, class, spell id, spell name, cooldown seconds, active/window seconds.
    # Personal rules feed the death-failure score. Raid/external rules are excluded
    # there and only feed cooldown capacity review panels.
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
    198589: [
        577,
        1480,
    ],  # Blur — Havoc Demon Hunter (1480 observed in WCL combatantInfo for Havoc pulls)
    196555: [577, 1480],  # Netherwalk — Havoc Demon Hunter
    47585: [258],  # Dispersion — Shadow Priest
    498: [65],  # Divine Protection — Holy Paladin
    184662: [70],  # Shield of Vengeance — Retribution Paladin
    102342: [105],  # Ironbark — Restoration Druid
    357170: [1468],  # Time Dilation — Preservation Evoker
    116849: [270],  # Life Cocoon — Mistweaver Monk
    6940: [65],  # Blessing of Sacrifice — Holy Paladin
    33206: [256],  # Pain Suppression — Discipline Priest
    47788: [257],  # Guardian Spirit — Holy Priest
    31821: [65],  # Aura Mastery — Holy Paladin
    62618: [256],  # Power Word: Barrier — Discipline Priest
    98008: [264],  # Spirit Link Totem — Restoration Shaman
    122470: [269],  # Touch of Karma — Windwalker Monk
    55233: [250],  # Vampiric Blood — Blood Death Knight
    22842: [104],  # Frenzied Regeneration — Guardian Druid
    31850: [66],  # Ardent Defender — Protection Paladin
    86659: [66],  # Guardian of Ancient Kings — Protection Paladin
    118038: [71],  # Die by the Sword — Arms Warrior
    184364: [72],  # Enraged Regeneration — Fury Warrior
    871: [73],  # Shield Wall — Protection Warrior
    12975: [73],  # Last Stand — Protection Warrior
}
# Talent-gated cooldowns are only considered available capacity when WCL
# CombatantInfo shows the matching talent node, or when the player actually
# cast the ability elsewhere in the report. This prevents false missed-cooldown
# rows for untalented spells such as Power Word: Barrier.
COOLDOWN_REQUIRED_TALENT_SPELL_IDS = {
    196555: [196555],  # Netherwalk
    49039: [49039],  # Lichborne
    48743: [48743],  # Death Pact
    61336: [61336],  # Survival Instincts
    108238: [108238],  # Renewal
    374348: [374348],  # Renewing Blaze
    264735: [264735],  # Survival of the Fittest
    342245: [342245],  # Alter Time
    55342: [55342],  # Mirror Image
    110959: [110959],  # Greater Invisibility
    122783: [122783],  # Diffuse Magic
    122278: [122278],  # Dampen Harm
    122470: [122470],  # Touch of Karma
    19236: [19236],  # Desperate Prayer
    108270: [108270],  # Stone Bulwark Totem
    108416: [108416],  # Dark Pact
    118038: [118038],  # Die by the Sword
    184364: [184364],  # Enraged Regeneration
    51052: [51052],  # Anti-Magic Zone
    196718: [196718],  # Darkness
    374227: [374227],  # Zephyr
    31821: [31821],  # Aura Mastery
    62618: [62618],  # Power Word: Barrier
    98008: [98008],  # Spirit Link Totem
    102342: [102342],  # Ironbark
    357170: [357170],  # Time Dilation
    116849: [116849],  # Life Cocoon
    6940: [6940],  # Blessing of Sacrifice
    33206: [33206],  # Pain Suppression
    47788: [47788],  # Guardian Spirit
}
PERSONAL_DEFENSIVE_RULES = [
    rule for rule in COOLDOWN_RULES if rule[0] in {"personal", "personal_spec"}
]
TRACKED_PERSONAL_DEFENSIVE_RULES = [
    rule for rule in COOLDOWN_RULES if rule[0] in {"personal", "personal_spec"}
]
NON_PERSONAL_COOLDOWN_RULES = [rule for rule in COOLDOWN_RULES if rule[0] in {"raid", "external"}]
DEFENSIVE_ABILITY_IDS = [rule[2] for rule in TRACKED_PERSONAL_DEFENSIVE_RULES]
DEFENSIVE_ABILITY_ID_TO_NAME = {rule[2]: rule[3] for rule in COOLDOWN_RULES}
DEFENSIVE_COOLDOWN_RULES_SQL = ",\n            ".join(
    (
        f"({player_class!r}, {ability_id}, {ability_name!r}, {cooldown_seconds}, "
        f"{active_seconds}, {'|'.join(str(spec_id) for spec_id in COOLDOWN_ALLOWED_SPECS.get(ability_id, []))!r}, "
        f"{'|'.join(str(spell_id) for spell_id in COOLDOWN_REQUIRED_TALENT_SPELL_IDS.get(ability_id, []))!r})"
    )
    for _, player_class, ability_id, ability_name, cooldown_seconds, active_seconds in PERSONAL_DEFENSIVE_RULES
)
COOLDOWN_RULES_SQL = ",\n            ".join(
    (
        f"({category!r}, {player_class!r}, {ability_id}, {ability_name!r}, {cooldown_seconds}, "
        f"{active_seconds}, {'|'.join(str(spec_id) for spec_id in COOLDOWN_ALLOWED_SPECS.get(ability_id, []))!r}, "
        f"{'|'.join(str(spell_id) for spell_id in COOLDOWN_REQUIRED_TALENT_SPELL_IDS.get(ability_id, []))!r})"
    )
    for category, player_class, ability_id, ability_name, cooldown_seconds, active_seconds in COOLDOWN_RULES
)
UTILITY_ABILITY_ID_TO_NAME = {
    **UTILITY_ABILITY_NAMES,
    **DEFENSIVE_ABILITY_ID_TO_NAME,
}
UTILITY_ABILITY_NAME_SQL = " ".join(
    f"WHEN {ability_id} THEN {name!r}"
    for ability_id, name in sorted(UTILITY_ABILITY_ID_TO_NAME.items())
)
DEFENSIVE_ABILITY_NAMES = sorted(rule[3].lower() for rule in TRACKED_PERSONAL_DEFENSIVE_RULES)

TABLE_EXPORT_STATEMENTS: dict[str, str] = {
    "gold_weekly_activity": f"""
        SELECT
          DATE_TRUNC('week', CAST(start_time_utc AS TIMESTAMP)) AS week_start,
          COUNT(*) AS raid_nights,
          SUM(COALESCE(boss_kills, 0)) AS total_boss_kills,
          SUM(COALESCE(total_wipes, 0)) AS total_wipes,
          SUM(COALESCE(total_pulls, 0)) AS total_pulls,
          SUM(COALESCE(total_fight_seconds, 0)) AS total_raid_seconds,
          ARRAY_SORT(COLLECT_SET(zone_name)) AS zones_raided
        FROM {CATALOG}.{SCHEMA}.gold_raid_summary
        WHERE zone_name IS NOT NULL
          AND zone_name NOT IN ({", ".join(repr(zone) for zone in sorted(EXCLUDED_ZONES))})
        GROUP BY DATE_TRUNC('week', CAST(start_time_utc AS TIMESTAMP))
        ORDER BY week_start
    """.strip(),
    "gold_player_utility_by_pull": f"""
        WITH latest_fight_casts AS (
          SELECT *
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY report_code ORDER BY _ingested_at DESC, _file_path DESC) AS rn
            FROM {CATALOG}.{SCHEMA}.bronze_fight_casts
          )
          WHERE rn = 1
        ),
        player_pulls AS (
          SELECT
            f.report_code,
            f.fight_id,
            f.encounter_id,
            f.boss_name,
            f.zone_name,
            f.difficulty,
            f.difficulty_label,
            f.raid_night_date,
            f.is_kill,
            a.player_name,
            a.player_class
          FROM (
            SELECT
              report_code,
              fight_id,
              encounter_id,
              boss_name,
              zone_name,
              difficulty,
              difficulty_label,
              raid_night_date,
              is_kill,
              EXPLODE(friendly_player_ids) AS actor_id
            FROM {CATALOG}.{SCHEMA}.silver_fight_events
          ) f
          INNER JOIN {CATALOG}.{SCHEMA}.silver_actor_roster a
            ON f.report_code = a.report_code
           AND f.actor_id = a.actor_id
          WHERE a.player_name IS NOT NULL
            AND a.player_name != ''
        ),
        casts AS (
          SELECT
            raw.report_code,
            event.fight AS fight_id,
            event.sourceID AS actor_id,
            event.abilityGameID AS ability_id,
            CASE event.abilityGameID
              {UTILITY_ABILITY_NAME_SQL}
              ELSE CAST(event.abilityGameID AS STRING)
            END AS ability_name
          FROM (
            SELECT
              report_code,
              EXPLODE(
                FROM_JSON(
                  events_json,
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,targetID:BIGINT,abilityGameID:BIGINT,fight:BIGINT>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
          ) raw
          WHERE event.sourceID IS NOT NULL
            AND event.abilityGameID IS NOT NULL
        ),
        casts_with_players AS (
          SELECT
            c.report_code,
            c.fight_id,
            a.player_name,
            a.player_class,
            c.ability_id,
            c.ability_name
          FROM casts c
          INNER JOIN {CATALOG}.{SCHEMA}.silver_actor_roster a
            ON c.report_code = a.report_code
           AND c.actor_id = a.actor_id
        ),
        classified AS (
          SELECT
            c.report_code,
            c.fight_id,
            f.encounter_id,
            f.boss_name,
            f.zone_name,
            f.difficulty,
            f.difficulty_label,
            f.raid_night_date,
            f.is_kill,
            c.player_name,
            c.player_class,
            COALESCE(c.ability_name, CAST(c.ability_id AS STRING)) AS ability_name,
            CASE
              WHEN c.ability_id IN ({", ".join(str(value) for value in HEALTHSTONE_ABILITY_IDS)})
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%healthstone%' THEN 'healthstone'
              WHEN c.ability_id IN ({", ".join(str(value) for value in HEALTH_POTION_ABILITY_IDS)})
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%healing potion%'
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%health potion%'
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%healing injector%' THEN 'health_potion'
              WHEN c.ability_id IN ({", ".join(str(value) for value in DEFENSIVE_ABILITY_IDS)})
                OR LOWER(COALESCE(c.ability_name, '')) IN ({", ".join(repr(value) for value in DEFENSIVE_ABILITY_NAMES)})
                THEN 'defensive'
            END AS utility_type
          FROM casts_with_players c
          INNER JOIN {CATALOG}.{SCHEMA}.silver_fight_events f
            ON c.report_code = f.report_code
           AND c.fight_id = f.fight_id
        ),
        utility_by_player_pull AS (
          SELECT
            report_code,
            fight_id,
            player_name,
            SUM(CASE WHEN utility_type = 'health_potion' THEN 1 ELSE 0 END) AS health_potion_uses,
            SUM(CASE WHEN utility_type = 'healthstone' THEN 1 ELSE 0 END) AS healthstone_casts,
            SUM(CASE WHEN utility_type = 'defensive' THEN 1 ELSE 0 END) AS defensive_casts,
            CONCAT_WS(
              ', ',
              ARRAY_SORT(COLLECT_SET(CASE WHEN utility_type = 'defensive' THEN ability_name END))
            ) AS defensive_abilities
          FROM classified
          WHERE utility_type IS NOT NULL
          GROUP BY report_code, fight_id, player_name
        )
        SELECT
          p.report_code,
          p.fight_id,
          p.encounter_id,
          p.boss_name,
          p.zone_name,
          p.difficulty,
          p.difficulty_label,
          p.raid_night_date,
          p.is_kill,
          p.player_name,
          p.player_class,
          COALESCE(u.health_potion_uses, 0) AS health_potion_uses,
          COALESCE(u.healthstone_casts, 0) AS healthstone_casts,
          COALESCE(u.defensive_casts, 0) AS defensive_casts,
          COALESCE(u.defensive_abilities, '') AS defensive_abilities
        FROM player_pulls p
        INNER JOIN (SELECT DISTINCT report_code, fight_id FROM casts) i
          ON p.report_code = i.report_code
         AND p.fight_id = i.fight_id
        LEFT JOIN utility_by_player_pull u
          ON p.report_code = u.report_code
         AND p.fight_id = u.fight_id
         AND p.player_name = u.player_name
        ORDER BY p.raid_night_date, p.boss_name, p.fight_id, p.player_name
    """.strip(),
    "gold_wipe_survival_events": f"""
        WITH latest_fight_casts AS (
          SELECT *
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY report_code ORDER BY _ingested_at DESC, _file_path DESC) AS rn
            FROM {CATALOG}.{SCHEMA}.bronze_fight_casts
          )
          WHERE rn = 1
        ),
        defensive_rules AS (
          SELECT *
          FROM VALUES
            {DEFENSIVE_COOLDOWN_RULES_SQL}
          AS defensive_rules(player_class, ability_id, ability_name, cooldown_seconds, active_seconds, allowed_spec_ids, required_talent_spell_ids)
        ),
        casts AS (
          SELECT
            raw.report_code,
            event.fight AS fight_id,
            event.timestamp AS cast_timestamp_ms,
            event.sourceID AS actor_id,
            event.abilityGameID AS ability_id,
            CASE event.abilityGameID
              {UTILITY_ABILITY_NAME_SQL}
              ELSE CAST(event.abilityGameID AS STRING)
            END AS ability_name
          FROM (
            SELECT
              report_code,
              EXPLODE(
                FROM_JSON(
                  events_json,
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,targetID:BIGINT,abilityGameID:BIGINT,fight:BIGINT>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
          ) raw
          WHERE event.sourceID IS NOT NULL
            AND event.abilityGameID IS NOT NULL
            AND event.timestamp IS NOT NULL
        ),
        combatant_info AS (
          SELECT
            raw.report_code,
            event.fight AS fight_id,
            event.sourceID AS actor_id,
            event.specID AS spec_id,
            FILTER(
              TRANSFORM(
                event.talentTree,
                talent -> CAST(COALESCE(talent.spellID, talent.id, talent.talentID) AS STRING)
              ),
              talent_id -> talent_id IS NOT NULL
            ) AS talent_spell_ids
          FROM (
            SELECT
              report_code,
              EXPLODE(
                FROM_JSON(
                  GET_JSON_OBJECT(TO_JSON(STRUCT(*)), '$.combatant_info_json'),
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,fight:BIGINT,specID:BIGINT,talentTree:ARRAY<STRUCT<spellID:BIGINT,id:BIGINT,talentID:BIGINT>>>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
            WHERE GET_JSON_OBJECT(TO_JSON(STRUCT(*)), '$.combatant_info_json') IS NOT NULL
          ) raw
          WHERE event.sourceID IS NOT NULL
            AND event.specID IS NOT NULL
        ),
        player_pull_specs AS (
          SELECT
            c.report_code,
            c.fight_id,
            a.player_name,
            MAX(c.spec_id) AS spec_id,
            ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(c.talent_spell_ids))) AS talent_spell_ids
          FROM combatant_info c
          INNER JOIN {CATALOG}.{SCHEMA}.silver_actor_roster a
            ON c.report_code = a.report_code
           AND c.actor_id = a.actor_id
          GROUP BY c.report_code, c.fight_id, a.player_name
        ),
        instrumented_pulls AS (
          SELECT DISTINCT report_code, fight_id
          FROM casts
        ),
        player_casts AS (
          SELECT
            c.report_code,
            c.fight_id,
            c.cast_timestamp_ms,
            a.player_name,
            a.player_class,
            c.ability_id,
            c.ability_name
          FROM casts c
          INNER JOIN {CATALOG}.{SCHEMA}.silver_actor_roster a
            ON c.report_code = a.report_code
           AND c.actor_id = a.actor_id
        ),
        report_player_casts AS (
          SELECT DISTINCT
            report_code,
            player_name,
            ability_id
          FROM player_casts
        ),
        wipe_deaths AS (
          SELECT
            d.report_code,
            d.fight_id,
            d.encounter_id,
            d.boss_name,
            d.zone_name,
            d.zone_id,
            d.difficulty,
            d.difficulty_label,
            d.raid_night_date,
            d.player_name,
            d.player_class,
            s.spec_id,
            s.talent_spell_ids,
            d.death_timestamp_ms,
            d.fight_start_ms,
            d.killing_blow_name,
            d.killing_blow_id
          FROM {CATALOG}.{SCHEMA}.gold_player_death_events d
          INNER JOIN instrumented_pulls i
            ON d.report_code = i.report_code
           AND d.fight_id = i.fight_id
          LEFT JOIN player_pull_specs s
            ON d.report_code = s.report_code
           AND d.fight_id = s.fight_id
           AND d.player_name = s.player_name
          WHERE COALESCE(d.is_kill, false) = false
        ),
        death_defensive_rules AS (
          SELECT
            d.*,
            r.ability_id AS defensive_ability_id,
            r.ability_name AS defensive_ability_name,
            r.cooldown_seconds,
            r.active_seconds,
            MAX(c_any.cast_timestamp_ms) AS last_cast_before_death_ms,
            MAX(c_pull.cast_timestamp_ms) AS last_cast_on_pull_before_death_ms
          FROM wipe_deaths d
          INNER JOIN defensive_rules r
            ON d.player_class = r.player_class
           AND (
             r.allowed_spec_ids = ''
             OR ARRAY_CONTAINS(SPLIT(r.allowed_spec_ids, '\\\\|'), CAST(d.spec_id AS STRING))
           )
          LEFT JOIN report_player_casts rpc
            ON d.report_code = rpc.report_code
           AND d.player_name = rpc.player_name
           AND r.ability_id = rpc.ability_id
          LEFT JOIN player_casts c_any
            ON d.report_code = c_any.report_code
           AND d.player_name = c_any.player_name
           AND r.ability_id = c_any.ability_id
           AND c_any.cast_timestamp_ms <= d.death_timestamp_ms
          LEFT JOIN player_casts c_pull
            ON d.report_code = c_pull.report_code
           AND d.fight_id = c_pull.fight_id
           AND d.player_name = c_pull.player_name
           AND r.ability_id = c_pull.ability_id
           AND c_pull.cast_timestamp_ms <= d.death_timestamp_ms
          WHERE
            r.required_talent_spell_ids = ''
            OR EXISTS(
              SPLIT(r.required_talent_spell_ids, '\\\\|'),
              talent_id -> d.talent_spell_ids IS NOT NULL AND ARRAY_CONTAINS(d.talent_spell_ids, talent_id)
            )
            OR rpc.ability_id IS NOT NULL
          GROUP BY
            d.report_code,
            d.fight_id,
            d.encounter_id,
            d.boss_name,
            d.zone_name,
            d.zone_id,
            d.difficulty,
            d.difficulty_label,
            d.raid_night_date,
            d.player_name,
            d.player_class,
            d.spec_id,
            d.talent_spell_ids,
            d.death_timestamp_ms,
            d.fight_start_ms,
            d.killing_blow_name,
            d.killing_blow_id,
            r.ability_id,
            r.ability_name,
            r.cooldown_seconds,
            r.active_seconds
        ),
        death_defensive_state AS (
          SELECT
            *,
            CASE
              WHEN last_cast_on_pull_before_death_ms IS NOT NULL
               AND death_timestamp_ms <= last_cast_on_pull_before_death_ms + (active_seconds * 1000)
              THEN 1 ELSE 0
            END AS was_active_at_death,
            CASE
              WHEN last_cast_before_death_ms IS NULL
                OR death_timestamp_ms >= last_cast_before_death_ms + (cooldown_seconds * 1000)
              THEN 1 ELSE 0
            END AS was_available_at_death
          FROM death_defensive_rules
        ),
        defensive_summary AS (
          SELECT
            report_code,
            fight_id,
            encounter_id,
            boss_name,
            zone_name,
            zone_id,
            difficulty,
            difficulty_label,
            raid_night_date,
            player_name,
            player_class,
            spec_id,
            death_timestamp_ms,
            fight_start_ms,
            killing_blow_name,
            killing_blow_id,
            MAX(was_active_at_death) AS defensive_active_at_death,
            MAX(was_available_at_death) AS defensive_available_at_death,
            MAX(last_cast_before_death_ms) AS last_defensive_cast_before_death_ms,
            CONCAT_WS(
              ', ',
              ARRAY_SORT(COLLECT_SET(CASE WHEN was_active_at_death = 1 THEN defensive_ability_name END))
            ) AS active_defensives,
            CONCAT_WS(
              ', ',
              ARRAY_SORT(COLLECT_SET(CASE WHEN was_available_at_death = 1 THEN defensive_ability_name END))
            ) AS available_defensives
          FROM death_defensive_state
          GROUP BY
            report_code,
            fight_id,
            encounter_id,
            boss_name,
            zone_name,
            zone_id,
            difficulty,
            difficulty_label,
            raid_night_date,
            player_name,
            player_class,
            spec_id,
            death_timestamp_ms,
            fight_start_ms,
            killing_blow_name,
            killing_blow_id
        ),
        recovery_summary AS (
          SELECT
            d.report_code,
            d.fight_id,
            d.player_name,
            d.death_timestamp_ms,
            SUM(CASE
              WHEN c.ability_id IN ({", ".join(str(value) for value in HEALTHSTONE_ABILITY_IDS)})
               AND c.cast_timestamp_ms <= d.death_timestamp_ms
              THEN 1 ELSE 0
            END) AS healthstone_before_death,
            SUM(CASE
              WHEN c.ability_id IN ({", ".join(str(value) for value in HEALTH_POTION_ABILITY_IDS)})
               AND c.cast_timestamp_ms <= d.death_timestamp_ms
              THEN 1 ELSE 0
            END) AS health_potion_before_death
          FROM wipe_deaths d
          LEFT JOIN player_casts c
            ON d.report_code = c.report_code
           AND d.fight_id = c.fight_id
           AND d.player_name = c.player_name
          GROUP BY d.report_code, d.fight_id, d.player_name, d.death_timestamp_ms
        )
        SELECT
          w.report_code,
          w.fight_id,
          w.encounter_id,
          w.boss_name,
          w.zone_name,
          w.zone_id,
          w.difficulty,
          w.difficulty_label,
          w.raid_night_date,
          w.player_name,
          w.player_class,
          w.spec_id,
          w.death_timestamp_ms,
          w.fight_start_ms,
          w.killing_blow_name,
          w.killing_blow_id,
          COALESCE(d.defensive_active_at_death, 0) AS defensive_active_at_death,
          COALESCE(d.defensive_available_at_death, 0) AS defensive_available_at_death,
          CASE
            WHEN COALESCE(d.defensive_active_at_death, 0) = 0
             AND COALESCE(d.defensive_available_at_death, 0) = 1
            THEN 1 ELSE 0
          END AS available_defensive_unused_at_death,
          d.last_defensive_cast_before_death_ms,
          d.active_defensives,
          d.available_defensives,
          CASE WHEN COALESCE(r.healthstone_before_death, 0) > 0 THEN 1 ELSE 0 END AS healthstone_before_death,
          CASE WHEN COALESCE(r.health_potion_before_death, 0) > 0 THEN 1 ELSE 0 END AS health_potion_before_death
        FROM wipe_deaths w
        LEFT JOIN defensive_summary d
          ON w.report_code = d.report_code
         AND w.fight_id = d.fight_id
         AND w.player_name = d.player_name
         AND w.death_timestamp_ms = d.death_timestamp_ms
        LEFT JOIN recovery_summary r
          ON w.report_code = r.report_code
         AND w.fight_id = r.fight_id
         AND w.player_name = r.player_name
         AND w.death_timestamp_ms = r.death_timestamp_ms
        ORDER BY w.raid_night_date, w.boss_name, w.fight_id, w.death_timestamp_ms, w.player_name
    """.strip(),
    "gold_wipe_cooldown_utilization": f"""
        WITH latest_fight_casts AS (
          SELECT *
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY report_code ORDER BY _ingested_at DESC, _file_path DESC) AS rn
            FROM {CATALOG}.{SCHEMA}.bronze_fight_casts
          )
          WHERE rn = 1
        ),
        cooldown_rules AS (
          SELECT *
          FROM VALUES
            {COOLDOWN_RULES_SQL}
          AS cooldown_rules(cooldown_category, player_class, ability_id, ability_name, cooldown_seconds, active_seconds, allowed_spec_ids, required_talent_spell_ids)
        ),
        instrumented_pulls AS (
          SELECT DISTINCT
            report_code,
            event.fight AS fight_id
          FROM (
            SELECT
              report_code,
              EXPLODE(
                FROM_JSON(
                  events_json,
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,targetID:BIGINT,abilityGameID:BIGINT,fight:BIGINT>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
          ) raw
          WHERE event.fight IS NOT NULL
        ),
        combatant_info AS (
          SELECT
            raw.report_code,
            event.fight AS fight_id,
            event.sourceID AS actor_id,
            event.specID AS spec_id,
            FILTER(
              TRANSFORM(
                event.talentTree,
                talent -> CAST(COALESCE(talent.spellID, talent.id, talent.talentID) AS STRING)
              ),
              talent_id -> talent_id IS NOT NULL
            ) AS talent_spell_ids
          FROM (
            SELECT
              report_code,
              EXPLODE(
                FROM_JSON(
                  GET_JSON_OBJECT(TO_JSON(STRUCT(*)), '$.combatant_info_json'),
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,fight:BIGINT,specID:BIGINT,talentTree:ARRAY<STRUCT<spellID:BIGINT,id:BIGINT,talentID:BIGINT>>>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
            WHERE GET_JSON_OBJECT(TO_JSON(STRUCT(*)), '$.combatant_info_json') IS NOT NULL
          ) raw
          WHERE event.sourceID IS NOT NULL
            AND event.specID IS NOT NULL
        ),
        player_pull_specs AS (
          SELECT
            c.report_code,
            c.fight_id,
            a.player_name,
            MAX(c.spec_id) AS spec_id,
            ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(c.talent_spell_ids))) AS talent_spell_ids
          FROM combatant_info c
          INNER JOIN {CATALOG}.{SCHEMA}.silver_actor_roster a
            ON c.report_code = a.report_code
           AND c.actor_id = a.actor_id
          GROUP BY c.report_code, c.fight_id, a.player_name
        ),
        casts AS (
          SELECT
            raw.report_code,
            event.fight AS fight_id,
            event.sourceID AS actor_id,
            event.abilityGameID AS ability_id
          FROM (
            SELECT
              report_code,
              EXPLODE(
                FROM_JSON(
                  events_json,
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,targetID:BIGINT,abilityGameID:BIGINT,fight:BIGINT>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
          ) raw
          WHERE event.sourceID IS NOT NULL
            AND event.abilityGameID IS NOT NULL
        ),
        player_casts AS (
          SELECT
            c.report_code,
            c.fight_id,
            a.player_name,
            c.ability_id,
            COUNT(*) AS actual_casts
          FROM casts c
          INNER JOIN {CATALOG}.{SCHEMA}.silver_actor_roster a
            ON c.report_code = a.report_code
           AND c.actor_id = a.actor_id
          GROUP BY c.report_code, c.fight_id, a.player_name, c.ability_id
        ),
        report_player_casts AS (
          SELECT DISTINCT
            report_code,
            player_name,
            ability_id
          FROM player_casts
        ),
        player_pulls AS (
          SELECT
            f.report_code,
            f.fight_id,
            f.encounter_id,
            f.boss_name,
            f.zone_name,
            f.difficulty,
            f.difficulty_label,
            f.raid_night_date,
            f.duration_seconds,
            a.player_name,
            a.player_class
          FROM (
            SELECT
              report_code,
              fight_id,
              encounter_id,
              boss_name,
              zone_name,
              difficulty,
              difficulty_label,
              raid_night_date,
              duration_seconds,
              is_kill,
              EXPLODE(friendly_player_ids) AS actor_id
            FROM {CATALOG}.{SCHEMA}.silver_fight_events
          ) f
          INNER JOIN {CATALOG}.{SCHEMA}.silver_actor_roster a
            ON f.report_code = a.report_code
           AND f.actor_id = a.actor_id
          WHERE COALESCE(f.is_kill, false) = false
            AND a.player_name IS NOT NULL
            AND a.player_name != ''
        ),
        tracked_cooldowns AS (
          SELECT
            p.report_code,
            p.fight_id,
            p.encounter_id,
            p.boss_name,
            p.zone_name,
            p.difficulty,
            p.difficulty_label,
            p.raid_night_date,
            p.duration_seconds,
            p.player_name,
            p.player_class,
            s.spec_id,
            r.cooldown_category,
            r.ability_id,
            r.ability_name,
            r.cooldown_seconds,
            r.active_seconds,
            CAST(FLOOR(COALESCE(p.duration_seconds, 0) / r.cooldown_seconds) + 1 AS BIGINT) AS possible_casts
          FROM player_pulls p
          INNER JOIN cooldown_rules r
            ON p.player_class = r.player_class
          INNER JOIN instrumented_pulls i
            ON p.report_code = i.report_code
           AND p.fight_id = i.fight_id
          INNER JOIN player_pull_specs s
            ON p.report_code = s.report_code
           AND p.fight_id = s.fight_id
           AND p.player_name = s.player_name
          LEFT JOIN report_player_casts rpc
            ON p.report_code = rpc.report_code
           AND p.player_name = rpc.player_name
           AND r.ability_id = rpc.ability_id
          WHERE COALESCE(p.duration_seconds, 0) > 0
            AND (
              r.allowed_spec_ids = ''
              OR ARRAY_CONTAINS(SPLIT(r.allowed_spec_ids, '\\\\|'), CAST(s.spec_id AS STRING))
            )
            AND (
              r.required_talent_spell_ids = ''
              OR EXISTS(
                SPLIT(r.required_talent_spell_ids, '\\\\|'),
                talent_id -> s.talent_spell_ids IS NOT NULL AND ARRAY_CONTAINS(s.talent_spell_ids, talent_id)
              )
              OR rpc.ability_id IS NOT NULL
            )
        )
        SELECT
          t.report_code,
          t.fight_id,
          t.encounter_id,
          t.boss_name,
          t.zone_name,
          t.difficulty,
          t.difficulty_label,
          t.raid_night_date,
          t.duration_seconds,
          t.player_name,
          t.player_class,
          t.spec_id,
          t.cooldown_category,
          t.ability_id,
          t.ability_name,
          t.cooldown_seconds,
          t.active_seconds,
          t.possible_casts,
          COALESCE(c.actual_casts, 0) AS actual_casts,
          GREATEST(t.possible_casts - COALESCE(c.actual_casts, 0), 0) AS missed_casts,
          CASE
            WHEN t.possible_casts > 0
            THEN ROUND((COALESCE(c.actual_casts, 0) / t.possible_casts) * 100, 1)
            ELSE 0
          END AS cast_efficiency_pct
        FROM tracked_cooldowns t
        LEFT JOIN player_casts c
          ON t.report_code = c.report_code
         AND t.fight_id = c.fight_id
         AND t.player_name = c.player_name
         AND t.ability_id = c.ability_id
        ORDER BY t.raid_night_date, t.boss_name, t.fight_id, t.cooldown_category, t.player_name, t.ability_name
    """.strip(),
}

PREPARATION_OVERRIDES_EXPORT_STATEMENT = f"""
    SELECT
      CAST(id AS STRING) AS id,
      CAST(mode AS STRING) AS mode,
      CAST(source_character AS STRING) AS source_character,
      CAST(target_character AS STRING) AS target_character,
      CAST(characters AS STRING) AS characters,
      CAST(display_name AS STRING) AS display_name,
      CAST(enabled AS STRING) AS enabled,
      CAST(notes AS STRING) AS notes,
      CAST(updated_by AS STRING) AS updated_by,
      CAST(updated_at AS STRING) AS updated_at
    FROM {OVERRIDES_CATALOG}.{OVERRIDES_SCHEMA}.{OVERRIDES_TABLE}
    ORDER BY id
""".strip()

LIVE_ROSTER_COLUMNS = {
    "name": 0,
    "roster_rank": 3,
    "player_class": 5,
    "race": 119,
    "note": 120,
}

MEDIA_FIELDNAMES = [
    "player_name",
    "realm_slug",
    "avatar_url",
    "inset_url",
    "main_url",
    "main_raw_url",
]
EQUIPMENT_FIELDNAMES = [
    "player_name",
    "realm_slug",
    "slot_type",
    "slot_name",
    "item_id",
    "item_name",
    "icon_url",
    "quality",
    "item_level",
    "inventory_type",
    "item_subclass",
    "binding",
    "transmog_name",
    "enchantments_json",
    "sockets_json",
    "stats_json",
    "spells_json",
    "raw_details_json",
]
RAID_ACHIEVEMENT_FIELDNAMES = [
    "player_name",
    "realm_slug",
    "achievement_id",
    "achievement_name",
    "completed_timestamp",
]

REALM_SLUG_OVERRIDES = {
    "twistingnether": "twisting-nether",
    "twisting-nether": "twisting-nether",
    "defiasbrotherhood": "defias-brotherhood",
    "defias-brotherhood": "defias-brotherhood",
    "argentdawn": "argent-dawn",
    "argent-dawn": "argent-dawn",
}


def _mirror_to_frontend_data(path: Path) -> None:
    if path.parent.resolve() == FRONTEND_PUBLIC_DATA_DIR.resolve():
        return

    mirror_path = FRONTEND_PUBLIC_DATA_DIR / path.name
    mirror_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, mirror_path)
    logger.info("Mirrored %s -> %s", path.name, mirror_path.relative_to(REPO_ROOT))


def _filter_exported_csv(output_path: Path) -> int | None:
    with output_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        if not fieldnames:
            return None

        rows = list(reader)

    original_count = len(rows)
    filtered_rows = rows
    changed = False

    if "zone_name" in fieldnames:
        filtered_rows = [
            row
            for row in filtered_rows
            if (row.get("zone_name") or "").strip() not in EXCLUDED_ZONES
        ]
        changed = changed or len(filtered_rows) != original_count

    if not changed:
        return original_count

    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_rows)

    logger.info(
        "Filtered %s excluded-zone rows from %s",
        original_count - len(filtered_rows),
        output_path.relative_to(REPO_ROOT),
    )
    return len(filtered_rows)


def _first_warehouse_id(client: WorkspaceClient) -> str:
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses found. Create or start one in Databricks.")
    warehouse_id = warehouses[0].id
    if not warehouse_id:
        raise RuntimeError("First SQL warehouse had no id.")
    return warehouse_id


def _wait_for_success(
    client: WorkspaceClient,
    response: Any,
) -> Any:
    statement_id = getattr(response, "statement_id", None)
    if not statement_id:
        return response

    deadline = time.time() + POLL_TIMEOUT_SECONDS
    current = response
    while time.time() < deadline:
        state = getattr(getattr(current, "status", None), "state", None)
        if state == sql.StatementState.SUCCEEDED:
            return current
        if state in {
            sql.StatementState.CANCELED,
            sql.StatementState.CLOSED,
            sql.StatementState.FAILED,
        }:
            raise RuntimeError(
                f"Statement {statement_id} ended in state {state}: {getattr(getattr(current, 'status', None), 'error', None)}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)
        current = client.statement_execution.get_statement(statement_id)

    raise TimeoutError(
        f"Timed out waiting for statement {statement_id} to finish after {POLL_TIMEOUT_SECONDS}s"
    )


def _iter_external_links(result_chunk: Any) -> list[str]:
    links = getattr(result_chunk, "external_links", None) or []
    urls: list[str] = []
    for link in links:
        url = getattr(link, "external_link", None) or getattr(link, "url", None)
        if url:
            urls.append(url)
    return urls


def _download_chunk(url: str) -> bytes:
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        response = client.get(url, headers={})
        response.raise_for_status()
        return response.content


def _download_text(url: str) -> str:
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def export_live_raid_roster(output_dir: Path) -> int:
    if not LIVE_ROSTER_SHEET_ID:
        logger.info("Skipping live raid roster export: no sheet id configured.")
        return 0

    sheet_url = (
        f"https://docs.google.com/spreadsheets/d/{LIVE_ROSTER_SHEET_ID}/export"
        f"?format=csv&gid={LIVE_ROSTER_SHEET_GID}"
    )
    logger.info("Exporting live raid roster -> %s", LIVE_ROSTER_FILENAME)
    payload = _download_text(sheet_url)
    rows = list(csv.reader(io.StringIO(payload)))
    if len(rows) < 3:
        raise RuntimeError("Live roster sheet returned too few rows to parse.")

    refreshed_at = rows[1][1].strip() if len(rows[1]) > 1 else ""
    seen_names: set[str] = set()
    normalised_rows: list[dict[str, str]] = []

    for row in rows[2:]:
        if not row:
            continue

        name = (
            row[LIVE_ROSTER_COLUMNS["name"]].strip()
            if len(row) > LIVE_ROSTER_COLUMNS["name"]
            else ""
        )
        if not name:
            continue

        key = name.casefold()
        if key in seen_names:
            continue
        seen_names.add(key)

        normalised_rows.append(
            {
                "name": name,
                "roster_rank": row[LIVE_ROSTER_COLUMNS["roster_rank"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["roster_rank"]
                else "",
                "player_class": row[LIVE_ROSTER_COLUMNS["player_class"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["player_class"]
                else "",
                "race": row[LIVE_ROSTER_COLUMNS["race"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["race"]
                else "",
                "note": row[LIVE_ROSTER_COLUMNS["note"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["note"]
                else "",
                "source_refreshed_at": refreshed_at,
            }
        )

    output_path = output_dir / LIVE_ROSTER_FILENAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "name",
                "roster_rank",
                "player_class",
                "race",
                "note",
                "source_refreshed_at",
            ],
        )
        writer.writeheader()
        writer.writerows(normalised_rows)

    _mirror_to_frontend_data(output_path)
    logger.info("  wrote %s live roster rows to %s", len(normalised_rows), output_path)
    return len(normalised_rows)


def export_guild_zone_ranks(client: WorkspaceClient, warehouse_id: str, output_dir: Path) -> int:
    if not WCL_CLIENT_ID or not WCL_CLIENT_SECRET:
        logger.info("Skipping guild zone ranks export: WCL client credentials not configured.")
        return 0

    logger.info("Exporting guild zone ranks -> %s", GUILD_ZONE_RANKS_FILENAME)
    zone_response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT DISTINCT CAST(zone_id AS STRING) AS zone_id, zone_name "
            f"FROM {CATALOG}.{SCHEMA}.gold_boss_progression "
            "WHERE zone_id IS NOT NULL AND zone_name IS NOT NULL "
            f"AND zone_name NOT IN ({', '.join(repr(zone) for zone in sorted(EXCLUDED_ZONES))})"
        ),
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    zone_response = _wait_for_success(client, zone_response)
    zone_rows = getattr(getattr(zone_response, "result", None), "data_array", None) or []

    adapter = WarcraftLogsAdapter(
        WarcraftLogsConfig(
            client_id=WCL_CLIENT_ID,
            client_secret=WCL_CLIENT_SECRET,
        )
    )
    adapter.authenticate()

    query = """
    query GuildZoneRanks(
      $guildName: String!
      $serverSlug: String!
      $serverRegion: String!
      $zoneId: Int!
    ) {
      guildData {
        guild(name: $guildName, serverSlug: $serverSlug, serverRegion: $serverRegion) {
          zoneRanking(zoneId: $zoneId) {
            progress(size: 20) {
              worldRank { number }
              regionRank { number }
              serverRank { number }
            }
          }
        }
      }
    }
    """

    rows_out: list[dict[str, str | int]] = []
    try:
        for row in zone_rows:
            zone_id = int(row[0])
            zone_name = str(row[1])
            result = adapter.fetch(
                "guild_zone_ranks",
                {
                    "query": query,
                    "variables": {
                        "guildName": WCL_GUILD_NAME,
                        "serverSlug": WCL_GUILD_SERVER_SLUG,
                        "serverRegion": WCL_GUILD_SERVER_REGION,
                        "zoneId": zone_id,
                    },
                },
            )
            data = result.records[0] if result.records else {}
            guild_data = data.get("guildData") or {}
            guild = guild_data.get("guild") or {}
            zone_ranking = guild.get("zoneRanking") or {}
            progress = zone_ranking.get("progress") or {}
            rows_out.append(
                {
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "world_rank": ((progress.get("worldRank") or {}).get("number")) or "",
                    "region_rank": ((progress.get("regionRank") or {}).get("number")) or "",
                    "server_rank": ((progress.get("serverRank") or {}).get("number")) or "",
                }
            )
    finally:
        adapter.close()

    output_path = output_dir / GUILD_ZONE_RANKS_FILENAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["zone_id", "zone_name", "world_rank", "region_rank", "server_rank"],
        )
        writer.writeheader()
        writer.writerows(rows_out)

    _mirror_to_frontend_data(output_path)
    logger.info("  wrote %s guild zone rank rows to %s", len(rows_out), output_path)
    return len(rows_out)


def _realm_to_slug(value: str | None) -> str:
    text = (value or WCL_GUILD_SERVER_SLUG or "").strip()
    if not text:
        return WCL_GUILD_SERVER_SLUG

    cleaned = text.replace("'", "").replace("_", "-").replace(" ", "-")
    cleaned = re.sub(r"(?<=[a-z])(?=[A-Z])", "-", cleaned).lower()
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return REALM_SLUG_OVERRIDES.get(cleaned.replace("-", ""), cleaned)


def _character_slug(value: str) -> str:
    return quote(value.strip().lower(), safe="")


def _blizzard_token() -> str:
    if not BLIZZARD_CLIENT_ID or not BLIZZARD_CLIENT_SECRET:
        raise RuntimeError("Blizzard client credentials are not configured.")

    response = httpx.post(
        "https://oauth.battle.net/token",
        auth=(BLIZZARD_CLIENT_ID, BLIZZARD_CLIENT_SECRET),
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _blizzard_get(
    http: httpx.Client,
    path: str,
    namespace: str | None = None,
) -> dict[str, Any] | None:
    response = http.get(
        f"https://{BLIZZARD_REGION}.api.blizzard.com{path}",
        params={
            "namespace": namespace or f"profile-{BLIZZARD_REGION}",
            "locale": BLIZZARD_LOCALE,
        },
    )
    if response.status_code in {403, 404}:
        return None
    response.raise_for_status()
    return response.json()


def _blizzard_item_icon_url(http: httpx.Client, item_id: Any, cache: dict[str, str]) -> str:
    item_key = str(item_id or "").strip()
    if not item_key:
        return ""
    if item_key in cache:
        return cache[item_key]

    payload = _blizzard_get(
        http,
        f"/data/wow/media/item/{item_key}",
        namespace=f"static-{BLIZZARD_REGION}",
    )
    assets = _asset_map(payload)
    cache[item_key] = assets.get("icon", "")
    return cache[item_key]


def _json_dump(value: Any) -> str:
    if not value:
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _simplify_enchantments(item: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "display_string": enchantment.get("display_string") or "",
            "source_item_name": ((enchantment.get("source_item") or {}).get("name")) or "",
            "enchantment_id": ((enchantment.get("enchantment_id") or enchantment.get("id")) or ""),
        }
        for enchantment in item.get("enchantments", [])
    ]


def _simplify_sockets(item: dict[str, Any]) -> list[dict[str, Any]]:
    sockets = []
    for socket in item.get("sockets", []):
        gem = socket.get("item") or {}
        sockets.append(
            {
                "socket_type": ((socket.get("socket_type") or {}).get("name")) or "",
                "item_id": gem.get("id") or "",
                "item_name": gem.get("name") or "",
                "display_string": socket.get("display_string") or "",
            }
        )
    return sockets


def _simplify_stats(item: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "type": ((stat.get("type") or {}).get("name")) or "",
            "value": stat.get("value") or "",
            "display": stat.get("display", {}).get("display_string")
            if isinstance(stat.get("display"), dict)
            else stat.get("display_string", ""),
            "is_negated": bool(stat.get("is_negated")),
        }
        for stat in item.get("stats", [])
    ]


def _simplify_spells(item: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for spell in item.get("spells", []):
        rows.append(
            {
                "spell_id": ((spell.get("spell") or {}).get("id")) or "",
                "spell_name": ((spell.get("spell") or {}).get("name")) or "",
                "description": spell.get("description") or "",
            }
        )
    return rows


def _asset_map(payload: dict[str, Any] | None) -> dict[str, str]:
    if not payload:
        return {}
    return {
        str(asset.get("key") or ""): str(asset.get("value") or "")
        for asset in payload.get("assets", [])
        if asset.get("key") and asset.get("value")
    }


def _looks_like_raid_feat(name: str) -> bool:
    lowered = name.lower()
    return "cutting edge:" in lowered or "famed slayer" in lowered or "famed bane" in lowered


def _fetch_blizzard_profile_candidates(
    client: WorkspaceClient, warehouse_id: str
) -> list[dict[str, str]]:
    statement = f"""
        WITH candidates AS (
          SELECT
            name AS player_name,
            COALESCE(NULLIF(realm, ''), '{WCL_GUILD_SERVER_SLUG}') AS realm,
            true AS is_raid_team,
            0 AS kills_tracked,
            last_raid_date AS latest_seen_date
          FROM {CATALOG}.{SCHEMA}.gold_raid_team
          WHERE name IS NOT NULL
            AND name != ''

          UNION ALL

          SELECT
            player_name,
            '{WCL_GUILD_SERVER_SLUG}' AS realm,
            false AS is_raid_team,
            COUNT(*) AS kills_tracked,
            MAX(raid_night_date) AS latest_seen_date
          FROM {CATALOG}.{SCHEMA}.gold_boss_kill_roster
          WHERE player_name IS NOT NULL
            AND player_name != ''
          GROUP BY player_name
        )
        SELECT
          player_name,
          realm
        FROM candidates
        GROUP BY player_name, realm
        ORDER BY MAX(is_raid_team) DESC,
                 MAX(kills_tracked) DESC,
                 MAX(latest_seen_date) DESC NULLS LAST,
                 player_name
        LIMIT {BLIZZARD_PROFILE_EXPORT_CAP}
    """.strip()

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    response = _wait_for_success(client, response)
    rows = getattr(getattr(response, "result", None), "data_array", None) or []

    seen: set[tuple[str, str]] = set()
    candidates: list[dict[str, str]] = []
    for row in rows:
        player_name = str(row[0] or "").strip()
        realm_slug = _realm_to_slug(str(row[1] or ""))
        key = (player_name.casefold(), realm_slug)
        if not player_name or key in seen:
            continue
        seen.add(key)
        candidates.append({"player_name": player_name, "realm_slug": realm_slug})
    return candidates


def export_blizzard_character_profiles(
    client: WorkspaceClient,
    warehouse_id: str,
    output_dir: Path,
) -> int:
    if not BLIZZARD_CLIENT_ID or not BLIZZARD_CLIENT_SECRET:
        logger.info("Skipping Blizzard character profile export: credentials not configured.")
        return 0

    candidates = _fetch_blizzard_profile_candidates(client, warehouse_id)
    logger.info("Exporting Blizzard character profiles for %s characters", len(candidates))
    token = _blizzard_token()

    media_rows: list[dict[str, Any]] = []
    equipment_rows: list[dict[str, Any]] = []
    achievement_rows: list[dict[str, Any]] = []
    item_icon_cache: dict[str, str] = {}

    with httpx.Client(
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=True,
        timeout=30,
    ) as http:
        for candidate in candidates:
            player_name = candidate["player_name"]
            realm_slug = candidate["realm_slug"]
            character_slug = _character_slug(player_name)
            base = f"/profile/wow/character/{realm_slug}/{character_slug}"

            try:
                media = _blizzard_get(http, f"{base}/character-media")
                if media is None:
                    logger.info("Blizzard profile not found for %s-%s", player_name, realm_slug)
                    time.sleep(0.05)
                    continue

                assets = _asset_map(media)
                media_rows.append(
                    {
                        "player_name": player_name,
                        "realm_slug": realm_slug,
                        "avatar_url": assets.get("avatar", ""),
                        "inset_url": assets.get("inset", ""),
                        "main_url": assets.get("main", ""),
                        "main_raw_url": assets.get("main-raw", ""),
                    }
                )

                equipment = _blizzard_get(http, f"{base}/equipment")
                for item in (equipment or {}).get("equipped_items", []):
                    item_id = ((item.get("item") or {}).get("id")) or ""
                    equipment_rows.append(
                        {
                            "player_name": player_name,
                            "realm_slug": realm_slug,
                            "slot_type": ((item.get("slot") or {}).get("type")) or "",
                            "slot_name": ((item.get("slot") or {}).get("name")) or "",
                            "item_id": item_id,
                            "item_name": item.get("name") or "",
                            "icon_url": _blizzard_item_icon_url(http, item_id, item_icon_cache),
                            "quality": ((item.get("quality") or {}).get("name")) or "",
                            "item_level": ((item.get("level") or {}).get("value")) or "",
                            "inventory_type": ((item.get("inventory_type") or {}).get("name"))
                            or "",
                            "item_subclass": ((item.get("item_subclass") or {}).get("name")) or "",
                            "binding": ((item.get("binding") or {}).get("name")) or "",
                            "transmog_name": (
                                ((item.get("transmog") or {}).get("item") or {}).get("name")
                            )
                            or "",
                            "enchantments_json": _json_dump(_simplify_enchantments(item)),
                            "sockets_json": _json_dump(_simplify_sockets(item)),
                            "stats_json": _json_dump(_simplify_stats(item)),
                            "spells_json": _json_dump(_simplify_spells(item)),
                            "raw_details_json": _json_dump(
                                {
                                    "name_description": item.get("name_description") or {},
                                    "requirements": item.get("requirements") or {},
                                    "durability": item.get("durability") or {},
                                    "limit_category": item.get("limit_category") or "",
                                }
                            ),
                        }
                    )

                achievements = _blizzard_get(http, f"{base}/achievements")
                for row in (achievements or {}).get("achievements", []):
                    achievement = row.get("achievement") or {}
                    name = str(achievement.get("name") or "")
                    criteria = row.get("criteria") or {}
                    completed = bool(criteria.get("is_completed")) or bool(
                        row.get("completed_timestamp")
                    )
                    if completed and _looks_like_raid_feat(name):
                        achievement_rows.append(
                            {
                                "player_name": player_name,
                                "realm_slug": realm_slug,
                                "achievement_id": achievement.get("id") or row.get("id") or "",
                                "achievement_name": name,
                                "completed_timestamp": row.get("completed_timestamp") or "",
                            }
                        )
                time.sleep(0.05)
            except Exception as exc:
                logger.warning(
                    "Skipping Blizzard profile for %s-%s: %s", player_name, realm_slug, exc
                )

    outputs = [
        (PLAYER_CHARACTER_MEDIA_FILENAME, MEDIA_FIELDNAMES, media_rows),
        (PLAYER_CHARACTER_EQUIPMENT_FILENAME, EQUIPMENT_FIELDNAMES, equipment_rows),
        (PLAYER_RAID_ACHIEVEMENTS_FILENAME, RAID_ACHIEVEMENT_FIELDNAMES, achievement_rows),
    ]
    for filename, fieldnames, rows in outputs:
        output_path = output_dir / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        _mirror_to_frontend_data(output_path)
        logger.info("  wrote %s rows to %s", len(rows), output_path)

    return len(media_rows) + len(equipment_rows) + len(achievement_rows)


def _write_csv_from_statement(
    client: WorkspaceClient, statement_response: Any, output_path: Path
) -> int:
    statement_id = getattr(statement_response, "statement_id", None)
    if not statement_id:
        raise RuntimeError("Statement response did not include a statement_id.")

    manifest = getattr(statement_response, "manifest", None)
    chunks = getattr(manifest, "chunks", None) or []
    chunk_count = len(chunks) if chunks else 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0

    with output_path.open("wb") as fh:
        for chunk_index in range(chunk_count):
            chunk = (
                getattr(statement_response, "result", None)
                if chunk_index == 0
                else client.statement_execution.get_statement_result_chunk_n(
                    statement_id, chunk_index
                )
            )

            urls = _iter_external_links(chunk)
            if not urls:
                raise RuntimeError(
                    f"No external CSV links returned for statement {statement_id} chunk {chunk_index}."
                )

            for url in urls:
                payload = _download_chunk(url)
                if chunk_index > 0 and payload.startswith(b"\xef\xbb\xbf"):
                    payload = payload[3:]
                fh.write(payload)

            row_count += int(getattr(chunks[chunk_index], "row_count", 0) or 0)

    return row_count


def export_table(client: WorkspaceClient, warehouse_id: str, filename: str, table_name: str) -> int:
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    logger.info("Exporting %s -> %s", full_name, filename)
    statement = TABLE_EXPORT_STATEMENTS.get(table_name, f"SELECT * FROM {full_name}")

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=sql.Disposition.EXTERNAL_LINKS,
        format=sql.Format.CSV,
        wait_timeout="10s",
        on_wait_timeout=sql.ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )

    response = _wait_for_success(client, response)
    output_path = OUTPUT_DIR / filename
    row_count = _write_csv_from_statement(client, response, output_path)
    filtered_count = _filter_exported_csv(output_path)
    if filtered_count is not None:
        row_count = filtered_count
    _mirror_to_frontend_data(output_path)
    logger.info("  wrote %s rows to %s", row_count or "unknown", output_path)
    return row_count


def export_external_statement(
    client: WorkspaceClient,
    warehouse_id: str,
    *,
    statement: str,
    filename: str,
    label: str,
) -> int:
    logger.info("Exporting %s -> %s", label, filename)
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=sql.Disposition.EXTERNAL_LINKS,
        format=sql.Format.CSV,
        wait_timeout="10s",
        on_wait_timeout=sql.ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )

    response = _wait_for_success(client, response)
    output_path = OUTPUT_DIR / filename
    row_count = _write_csv_from_statement(client, response, output_path)
    _mirror_to_frontend_data(output_path)
    logger.info("  wrote %s rows to %s", row_count or "unknown", output_path)
    return row_count


def main() -> None:
    client = WorkspaceClient()
    warehouse_id = _first_warehouse_id(client)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for filename, table_name in FRONTEND_TABLES.items():
        total_rows += export_table(client, warehouse_id, filename, table_name)

    try:
        total_rows += export_live_raid_roster(OUTPUT_DIR)
    except Exception as exc:
        logger.warning("Skipping live raid roster export: %s", exc)

    try:
        total_rows += export_guild_zone_ranks(client, warehouse_id, OUTPUT_DIR)
    except Exception as exc:
        logger.warning("Skipping guild zone ranks export: %s", exc)

    try:
        total_rows += export_blizzard_character_profiles(client, warehouse_id, OUTPUT_DIR)
    except Exception as exc:
        logger.warning("Skipping Blizzard character profile export: %s", exc)

    try:
        total_rows += export_external_statement(
            client,
            warehouse_id,
            statement=PREPARATION_OVERRIDES_EXPORT_STATEMENT,
            filename=PREPARATION_OVERRIDES_FILENAME,
            label=f"{OVERRIDES_CATALOG}.{OVERRIDES_SCHEMA}.{OVERRIDES_TABLE}",
        )
    except Exception as exc:
        logger.warning("Skipping preparation overrides export: %s", exc)

    logger.info(
        "Export complete. %s total rows across %s files.",
        total_rows or "unknown",
        len(FRONTEND_TABLES) + 6,
    )


if __name__ == "__main__":
    main()
