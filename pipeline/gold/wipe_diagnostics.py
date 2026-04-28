# Databricks notebook source
# ruff: noqa: I001
# Gold layer — wipe-pull diagnostics
#
# gold_wipe_survival_events      — per-death record on wipe pulls with defensive
#                                  cooldown availability/usage and recovery items.
# gold_wipe_cooldown_utilization — per-pull tracked cooldown cast counts vs the
#                                  number of casts the cooldown could theoretically
#                                  have supported, using shared silver cooldown capacity.

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None
if _HERE and _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from _cooldown_rules import (  # noqa: E402
    HEALTH_POTION_ABILITY_IDS,
    HEALTHSTONE_ABILITY_IDS,
)
import dlt  # noqa: E402


def _id_list(values) -> str:
    return ", ".join(str(v) for v in values)


def _instrumented_pulls_sql() -> str:
    return """
        SELECT DISTINCT report_code, fight_id
        FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events
        WHERE fight_id IS NOT NULL
    """


def _wipe_deaths_sql() -> str:
    return f"""
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
          d.death_timestamp_ms,
          d.fight_start_ms,
          d.killing_blow_name,
          d.killing_blow_id
        FROM 03_gold.sc_analytics.gold_player_death_events d
        INNER JOIN ({_instrumented_pulls_sql()}) i
          ON d.report_code = i.report_code
         AND d.fight_id = i.fight_id
        WHERE COALESCE(d.is_kill, false) = false
    """


def _tracked_defensive_capacity_sql() -> str:
    return """
        SELECT
          report_code,
          fight_id,
          player_name,
          spec_id,
          ability_id,
          ability_name,
          cooldown_seconds,
          active_seconds
        FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cooldown_capacity
        WHERE has_tracked_capacity = true
          AND cooldown_category IN ('personal', 'personal_spec')
    """


@dlt.table(
    name="03_gold.sc_analytics.gold_wipe_survival_events",
    comment=(
        "Per-death record on wipe pulls. Flags whether the player had a tracked "
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
        WITH wipe_deaths AS (
          {_wipe_deaths_sql()}
        ),
        tracked_defensives AS (
          {_tracked_defensive_capacity_sql()}
        ),
        death_defensive_state AS (
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
            t.spec_id,
            d.death_timestamp_ms,
            d.fight_start_ms,
            d.killing_blow_name,
            d.killing_blow_id,
            t.ability_name AS defensive_ability_name,
            MAX(c_any.cast_timestamp_ms) AS last_cast_before_death_ms,
            MAX(c_pull.cast_timestamp_ms) AS last_cast_on_pull_before_death_ms,
            MAX(
              CASE
                WHEN c_pull.cast_timestamp_ms IS NOT NULL
                 AND d.death_timestamp_ms <= c_pull.cast_timestamp_ms + (t.active_seconds * 1000)
                THEN 1 ELSE 0
              END
            ) AS was_active_at_death,
            MAX(
              CASE
                WHEN c_any.cast_timestamp_ms IS NULL
                  OR d.death_timestamp_ms >= c_any.cast_timestamp_ms + (t.cooldown_seconds * 1000)
                THEN 1 ELSE 0
              END
            ) AS was_available_at_death
          FROM wipe_deaths d
          LEFT JOIN tracked_defensives t
            ON d.report_code = t.report_code
           AND d.fight_id = t.fight_id
           AND d.player_name = t.player_name
          LEFT JOIN 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events c_any
            ON d.report_code = c_any.report_code
           AND d.player_name = c_any.player_name
           AND t.ability_id = c_any.ability_id
           AND c_any.cast_timestamp_ms <= d.death_timestamp_ms
          LEFT JOIN 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events c_pull
            ON d.report_code = c_pull.report_code
           AND d.fight_id = c_pull.fight_id
           AND d.player_name = c_pull.player_name
           AND t.ability_id = c_pull.ability_id
           AND c_pull.cast_timestamp_ms <= d.death_timestamp_ms
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
            t.spec_id,
            d.death_timestamp_ms,
            d.fight_start_ms,
            d.killing_blow_name,
            d.killing_blow_id,
            t.ability_name
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
            CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(CASE WHEN was_active_at_death = 1 THEN defensive_ability_name END))) AS active_defensives,
            CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(CASE WHEN was_available_at_death = 1 THEN defensive_ability_name END))) AS available_defensives
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
            SUM(CASE WHEN c.ability_id IN ({healthstone_ids}) AND c.cast_timestamp_ms <= d.death_timestamp_ms THEN 1 ELSE 0 END) AS healthstone_before_death,
            SUM(CASE WHEN c.ability_id IN ({potion_ids}) AND c.cast_timestamp_ms <= d.death_timestamp_ms THEN 1 ELSE 0 END) AS health_potion_before_death
          FROM wipe_deaths d
          LEFT JOIN 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events c
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
          d.spec_id,
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
        """
        WITH tracked_cooldowns AS (
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
            player_name,
            player_class,
            spec_id,
            cooldown_category,
            ability_id,
            ability_name,
            cooldown_seconds,
            active_seconds,
            possible_casts,
            actual_casts
          FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cooldown_capacity
          WHERE has_tracked_capacity = true
            AND COALESCE(is_kill, false) = false
            AND COALESCE(duration_seconds, 0) > 0
        )
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
          player_name,
          player_class,
          spec_id,
          cooldown_category,
          ability_id,
          ability_name,
          cooldown_seconds,
          active_seconds,
          possible_casts,
          actual_casts,
          GREATEST(possible_casts - actual_casts, 0) AS missed_casts,
          CASE
            WHEN possible_casts > 0
            THEN ROUND((actual_casts / possible_casts) * 100, 1)
            ELSE 0
          END AS cast_efficiency_pct
        FROM tracked_cooldowns
        """
    )
