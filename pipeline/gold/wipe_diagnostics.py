# Databricks notebook source
# Gold layer — wipe-pull diagnostics
#
# gold_wipe_survival_events     — per-death record on wipe pulls with defensive
#                                 cooldown availability/usage and recovery items.
# gold_wipe_cooldown_utilization — per-pull tracked cooldown cast counts vs the
#                                  number of casts the cooldown could theoretically
#                                  have supported, restricted to spec/talent rules.
#
# Both tables previously lived as embedded SQL in scripts/export_gold_tables.py.

import sys
import os

_HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None
if _HERE and _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import dlt  # noqa: E402

from _cooldown_rules import (  # noqa: E402
    HEALTHSTONE_ABILITY_IDS,
    HEALTH_POTION_ABILITY_IDS,
    cooldown_rules_sql,
    defensive_cooldown_rules_sql,
    utility_ability_name_sql,
)


def _id_list(values) -> str:
    return ", ".join(str(v) for v in values)


_UTILITY_ABILITY_NAME_SQL = utility_ability_name_sql()
_DEFENSIVE_COOLDOWN_RULES_SQL = defensive_cooldown_rules_sql()
_COOLDOWN_RULES_SQL = cooldown_rules_sql()


# ── gold_wipe_survival_events ──────────────────────────────────────────────────

@dlt.table(
    name="03_gold.sc_analytics.gold_wipe_survival_events",
    comment=(
        "Per-death record on wipe pulls.  Flags whether the player had a tracked "
        "personal defensive available or active at the moment of death and whether "
        "any recovery items (healthstone, health potion) were used beforehand."
    ),
    table_properties={"quality": "gold"},
)
def gold_wipe_survival_events():
    healthstone_ids = _id_list(HEALTHSTONE_ABILITY_IDS)
    potion_ids = _id_list(HEALTH_POTION_ABILITY_IDS)
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
        defensive_rules AS (
          SELECT *
          FROM VALUES
            {_DEFENSIVE_COOLDOWN_RULES_SQL}
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
                  combatant_info_json,
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,fight:BIGINT,specID:BIGINT,talentTree:ARRAY<STRUCT<spellID:BIGINT,id:BIGINT,talentID:BIGINT>>>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
            WHERE combatant_info_json IS NOT NULL
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
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
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
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
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
          FROM 03_gold.sc_analytics.gold_player_death_events d
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
              WHEN c.ability_id IN ({healthstone_ids})
               AND c.cast_timestamp_ms <= d.death_timestamp_ms
              THEN 1 ELSE 0
            END) AS healthstone_before_death,
            SUM(CASE
              WHEN c.ability_id IN ({potion_ids})
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
        """
    )


# ── gold_wipe_cooldown_utilization ─────────────────────────────────────────────

@dlt.table(
    name="03_gold.sc_analytics.gold_wipe_cooldown_utilization",
    comment=(
        "Per-pull cast counts for tracked personal/raid/external cooldowns vs the "
        "number of casts the cooldown could have supported across the pull duration. "
        "Restricted to wipe pulls and respects spec / talent gating."
    ),
    table_properties={"quality": "gold"},
)
def gold_wipe_cooldown_utilization():
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
        cooldown_rules AS (
          SELECT *
          FROM VALUES
            {_COOLDOWN_RULES_SQL}
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
                  combatant_info_json,
                  'STRUCT<data:ARRAY<STRUCT<timestamp:BIGINT,type:STRING,sourceID:BIGINT,fight:BIGINT,specID:BIGINT,talentTree:ARRAY<STRUCT<spellID:BIGINT,id:BIGINT,talentID:BIGINT>>>>>'
                ).data
              ) AS event
            FROM latest_fight_casts
            WHERE combatant_info_json IS NOT NULL
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
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
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
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
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
            FROM 02_silver.sc_analytics_warcraftlogs.silver_fight_events
          ) f
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
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
        """
    )
