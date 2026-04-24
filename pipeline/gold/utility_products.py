# Databricks notebook source
# Gold layer — weekly raid activity and per-pull utility usage
#
# gold_weekly_activity        — week-grain summary of raid nights / kills / wipes,
#                               filtered to actual raid zones (excludes Blackrock Depths).
# gold_player_utility_by_pull — per-player per-pull counts of healthstones,
#                               health potions, and tracked defensive cooldowns.
#
# Both tables previously lived as embedded SQL in scripts/export_gold_tables.py.
# Migrating into DLT lets the export step become a pure CSV writer.

import sys
import os

# Make sibling module `_cooldown_rules` importable inside the DLT notebook
# (Databricks workspace files are not on sys.path by default).
_HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None
if _HERE and _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import dlt  # noqa: E402

from _cooldown_rules import (  # noqa: E402
    DEFENSIVE_ABILITY_IDS,
    DEFENSIVE_ABILITY_NAMES,
    EXCLUDED_ZONES,
    HEALTHSTONE_ABILITY_IDS,
    HEALTH_POTION_ABILITY_IDS,
    utility_ability_name_sql,
)


def _id_list(values) -> str:
    return ", ".join(str(v) for v in values)


def _quoted_list(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


_UTILITY_ABILITY_NAME_SQL = utility_ability_name_sql()


# ── gold_weekly_activity ───────────────────────────────────────────────────────

@dlt.table(
    name="03_gold.sc_analytics.gold_weekly_activity",
    comment=(
        "Week-grain raid activity rollup (raid nights, boss kills, wipes, pulls, "
        "raid seconds, distinct zones).  Excludes non-raid zones such as Blackrock Depths."
    ),
    table_properties={"quality": "gold"},
)
def gold_weekly_activity():
    excluded = _quoted_list(sorted(EXCLUDED_ZONES))
    return spark.sql(  # noqa: F821
        f"""
        SELECT
          DATE_TRUNC('week', CAST(start_time_utc AS TIMESTAMP)) AS week_start,
          COUNT(*) AS raid_nights,
          SUM(COALESCE(boss_kills, 0)) AS total_boss_kills,
          SUM(COALESCE(total_wipes, 0)) AS total_wipes,
          SUM(COALESCE(total_pulls, 0)) AS total_pulls,
          SUM(COALESCE(total_fight_seconds, 0)) AS total_raid_seconds,
          ARRAY_SORT(COLLECT_SET(zone_name)) AS zones_raided
        FROM 03_gold.sc_analytics.gold_raid_summary
        WHERE zone_name IS NOT NULL
          AND zone_name NOT IN ({excluded})
        GROUP BY DATE_TRUNC('week', CAST(start_time_utc AS TIMESTAMP))
        """
    )


# ── gold_player_utility_by_pull ────────────────────────────────────────────────

@dlt.table(
    name="03_gold.sc_analytics.gold_player_utility_by_pull",
    comment=(
        "Per-player per-pull tally of healthstones, health potions, and tracked "
        "defensive cooldowns.  Restricted to pulls where cast events were ingested."
    ),
    table_properties={"quality": "gold"},
)
def gold_player_utility_by_pull():
    healthstone_ids = _id_list(HEALTHSTONE_ABILITY_IDS)
    potion_ids = _id_list(HEALTH_POTION_ABILITY_IDS)
    defensive_ids = _id_list(DEFENSIVE_ABILITY_IDS)
    defensive_names = _quoted_list(DEFENSIVE_ABILITY_NAMES)
    return spark.sql(  # noqa: F821
        f"""
        WITH latest_fight_casts AS (
          SELECT *
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY report_code ORDER BY _ingested_at DESC, _file_path DESC) AS rn
            FROM 01_bronze.warcraftlogs.bronze_fight_casts
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
            FROM 02_silver.sc_analytics_warcraftlogs.silver_fight_events
          ) f
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
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
              {_UTILITY_ABILITY_NAME_SQL}
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
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
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
              WHEN c.ability_id IN ({healthstone_ids})
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%healthstone%' THEN 'healthstone'
              WHEN c.ability_id IN ({potion_ids})
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%healing potion%'
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%health potion%'
                OR LOWER(COALESCE(c.ability_name, '')) LIKE '%healing injector%' THEN 'health_potion'
              WHEN c.ability_id IN ({defensive_ids})
                OR LOWER(COALESCE(c.ability_name, '')) IN ({defensive_names})
                THEN 'defensive'
            END AS utility_type
          FROM casts_with_players c
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_fight_events f
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
        """
    )
