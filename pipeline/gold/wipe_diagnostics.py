# Databricks notebook source
# ruff: noqa: I001
# Gold layer — wipe-pull diagnostics
#
# gold_wipe_survival_events      — per-death record on wipe pulls with defensive
#                                  cooldown availability/usage and recovery items.
# gold_wipe_cooldown_utilization — per-pull tracked cooldown cast counts vs the
#                                  number of casts the cooldown could theoretically
#                                  have supported, using shared silver cooldown capacity.
# gold_wipe_survival_discipline  — per-player wipe discipline scores for stable
#                                  dashboard scopes, including component metrics.

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
          CONCAT_WS(':', LOWER(TRIM(d.player_name)), LOWER(TRIM(COALESCE(d.player_class, 'unknown'))), LOWER(TRIM(COALESCE(a.realm, 'unknown')))) AS player_identity_key,
          d.player_name,
          d.player_class,
          COALESCE(a.realm, 'unknown') AS realm,
          d.death_timestamp_ms,
          d.fight_start_ms,
          d.killing_blow_name,
          d.killing_blow_id
        FROM 03_gold.sc_analytics.gold_player_death_events d
        LEFT JOIN (
          SELECT
            report_code,
            LOWER(player_name) AS player_name_key,
            LOWER(player_class) AS player_class_key,
            MAX(COALESCE(realm, 'unknown')) AS realm
          FROM 02_silver.sc_analytics_warcraftlogs.silver_actor_roster
          GROUP BY report_code, LOWER(player_name), LOWER(player_class)
        ) a
          ON d.report_code = a.report_code
         AND LOWER(d.player_name) = a.player_name_key
         AND LOWER(d.player_class) = a.player_class_key
        INNER JOIN ({_instrumented_pulls_sql()}) i
          ON d.report_code = i.report_code
         AND d.fight_id = i.fight_id
        WHERE COALESCE(d.is_kill, false) = false
    """


def _tracked_defensive_capacity_sql() -> str:
    return """
        SELECT
          c.report_code,
          c.fight_id,
          CONCAT_WS(':', LOWER(TRIM(c.player_name)), LOWER(TRIM(COALESCE(c.player_class, 'unknown'))), LOWER(TRIM(COALESCE(a.realm, 'unknown')))) AS player_identity_key,
          c.player_name,
          c.spec_id,
          c.ability_id,
          c.ability_name,
          c.cooldown_seconds,
          c.active_seconds
        FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cooldown_capacity c
        LEFT JOIN (
          SELECT
            report_code,
            LOWER(player_name) AS player_name_key,
            LOWER(player_class) AS player_class_key,
            MAX(COALESCE(realm, 'unknown')) AS realm
          FROM 02_silver.sc_analytics_warcraftlogs.silver_actor_roster
          GROUP BY report_code, LOWER(player_name), LOWER(player_class)
        ) a
          ON c.report_code = a.report_code
         AND LOWER(c.player_name) = a.player_name_key
         AND LOWER(c.player_class) = a.player_class_key
        WHERE c.has_tracked_capacity = true
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
            d.player_identity_key,
            d.player_name,
            d.player_class,
            d.realm,
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
           AND d.player_identity_key = t.player_identity_key
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
            d.player_identity_key,
            d.player_name,
            d.player_class,
            d.realm,
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
            player_identity_key,
            player_name,
            player_class,
            realm,
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
            player_identity_key,
            player_name,
            player_class,
            realm,
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
            d.player_identity_key,
            d.player_name,
            d.death_timestamp_ms,
            SUM(CASE WHEN c.ability_id IN ({healthstone_ids}) AND c.cast_timestamp_ms <= d.death_timestamp_ms THEN 1 ELSE 0 END) AS healthstone_before_death,
            SUM(CASE WHEN c.ability_id IN ({potion_ids}) AND c.cast_timestamp_ms <= d.death_timestamp_ms THEN 1 ELSE 0 END) AS health_potion_before_death
          FROM wipe_deaths d
          LEFT JOIN 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events c
            ON d.report_code = c.report_code
           AND d.fight_id = c.fight_id
           AND d.player_name = c.player_name
          GROUP BY d.report_code, d.fight_id, d.player_identity_key, d.player_name, d.death_timestamp_ms
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
          w.player_identity_key,
          w.player_name,
          w.player_class,
          w.realm,
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
         AND w.player_identity_key = d.player_identity_key
         AND w.death_timestamp_ms = d.death_timestamp_ms
        LEFT JOIN recovery_summary r
          ON w.report_code = r.report_code
         AND w.fight_id = r.fight_id
         AND w.player_identity_key = r.player_identity_key
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
        WITH actor_realms AS (
          SELECT
            report_code,
            LOWER(player_name) AS player_name_key,
            LOWER(player_class) AS player_class_key,
            MAX(COALESCE(realm, 'unknown')) AS realm
          FROM 02_silver.sc_analytics_warcraftlogs.silver_actor_roster
          GROUP BY report_code, LOWER(player_name), LOWER(player_class)
        ),
        tracked_cooldowns AS (
          SELECT
            c.report_code,
            c.fight_id,
            c.encounter_id,
            c.boss_name,
            c.zone_name,
            c.difficulty,
            c.difficulty_label,
            c.raid_night_date,
            c.duration_seconds,
            CONCAT_WS(':', LOWER(TRIM(c.player_name)), LOWER(TRIM(COALESCE(c.player_class, 'unknown'))), LOWER(TRIM(COALESCE(a.realm, 'unknown')))) AS player_identity_key,
            c.player_name,
            COALESCE(a.realm, 'unknown') AS realm,
            c.player_class,
            c.spec_id,
            c.cooldown_category,
            c.ability_id,
            c.ability_name,
            c.cooldown_seconds,
            c.active_seconds,
            c.capacity_model,
            c.max_charges,
            c.possible_casts,
            c.observed_casts,
            c.over_capacity_casts,
            c.actual_casts
          FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cooldown_capacity c
          LEFT JOIN actor_realms a
            ON c.report_code = a.report_code
           AND LOWER(c.player_name) = a.player_name_key
           AND LOWER(c.player_class) = a.player_class_key
          WHERE c.has_scored_capacity = true
            AND COALESCE(c.is_kill, false) = false
            AND COALESCE(c.duration_seconds, 0) > 0
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
          player_identity_key,
          player_name,
          realm,
          player_class,
          spec_id,
          cooldown_category,
          ability_id,
          ability_name,
          cooldown_seconds,
          active_seconds,
          capacity_model,
          max_charges,
          possible_casts,
          observed_casts,
          over_capacity_casts,
          actual_casts,
          possible_casts - actual_casts AS missed_casts,
          CASE
            WHEN possible_casts > 0
            THEN ROUND((actual_casts / possible_casts) * 100, 1)
            ELSE 0
          END AS cast_efficiency_pct
        FROM tracked_cooldowns
        """
    )


@dlt.table(
    name="03_gold.sc_analytics.gold_wipe_survival_discipline",
    comment=(
        "Per-player wipe survival discipline metrics and absolute score for stable "
        "dashboard filter scopes. Scope dimensions use 'All' sentinel values for "
        "pre-aggregated tier/boss/difficulty views."
    ),
    table_properties={"quality": "gold"},
)
def gold_wipe_survival_discipline():
    return spark.sql(  # noqa: F821
        """
        WITH role_by_player AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            MAX(realm) AS realm,
            MAX_BY(role, raid_night_date) AS role
          FROM 03_gold.sc_analytics.gold_boss_kill_roster
          GROUP BY player_identity_key
        ),
        wipe_pull_base AS (
          SELECT
            player_identity_key,
            player_name,
            player_class,
            realm,
            zone_name,
            encounter_id,
            boss_name,
            difficulty,
            difficulty_label,
            report_code,
            fight_id
          FROM 03_gold.sc_analytics.gold_player_utility_by_pull
          WHERE COALESCE(is_kill, false) = false
        ),
        wipe_pull_scopes AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            MAX(realm) AS realm,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(encounter_id) = 1 THEN CAST(NULL AS BIGINT) ELSE encounter_id END AS encounter_id,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty) = 1 THEN CAST(NULL AS BIGINT) ELSE difficulty END AS difficulty,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            COUNT(DISTINCT CONCAT(report_code, ':', CAST(fight_id AS STRING))) AS wipe_pulls_tracked
          FROM wipe_pull_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, encounter_id, boss_name, difficulty, difficulty_label),
            (player_identity_key, zone_name, difficulty, difficulty_label),
            (player_identity_key, difficulty, difficulty_label),
            (player_identity_key, zone_name, encounter_id, boss_name),
            (player_identity_key, zone_name),
            (player_identity_key)
          )
        ),
        death_base AS (
          SELECT
            player_identity_key,
            player_name,
            player_class,
            realm,
            zone_name,
            encounter_id,
            boss_name,
            difficulty,
            difficulty_label,
            report_code,
            fight_id,
            death_timestamp_ms,
            fight_start_ms,
            killing_blow_name,
            healthstone_before_death,
            health_potion_before_death,
            ROW_NUMBER() OVER (
              PARTITION BY report_code, fight_id
              ORDER BY death_timestamp_ms ASC, player_name ASC
            ) AS death_order
          FROM 03_gold.sc_analytics.gold_wipe_survival_events
        ),
        death_scopes AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            MAX(realm) AS realm,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(encounter_id) = 1 THEN CAST(NULL AS BIGINT) ELSE encounter_id END AS encounter_id,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty) = 1 THEN CAST(NULL AS BIGINT) ELSE difficulty END AS difficulty,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            COUNT(*) AS wipe_deaths,
            SUM(CASE WHEN death_order = 1 THEN 1 ELSE 0 END) AS first_deaths,
            SUM(CASE WHEN death_timestamp_ms - fight_start_ms <= 60000 THEN 1 ELSE 0 END) AS early_deaths,
            SUM(CASE WHEN COALESCE(healthstone_before_death, 0) > 0 THEN 1 ELSE 0 END) AS healthstone_uses,
            SUM(CASE WHEN COALESCE(health_potion_before_death, 0) > 0 THEN 1 ELSE 0 END) AS potion_uses,
            SUM(CASE WHEN COALESCE(healthstone_before_death, 0) = 0 THEN 1 ELSE 0 END) AS no_healthstone_deaths,
            SUM(CASE WHEN COALESCE(health_potion_before_death, 0) = 0 THEN 1 ELSE 0 END) AS no_health_potion_deaths
          FROM death_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, encounter_id, boss_name, difficulty, difficulty_label),
            (player_identity_key, zone_name, difficulty, difficulty_label),
            (player_identity_key, difficulty, difficulty_label),
            (player_identity_key, zone_name, encounter_id, boss_name),
            (player_identity_key, zone_name),
            (player_identity_key)
          )
        ),
        killing_blow_counts AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(encounter_id) = 1 THEN CAST(NULL AS BIGINT) ELSE encounter_id END AS encounter_id,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty) = 1 THEN CAST(NULL AS BIGINT) ELSE difficulty END AS difficulty,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            COALESCE(killing_blow_name, '') AS killing_blow_name,
            COUNT(*) AS killing_blow_count
          FROM death_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, encounter_id, boss_name, difficulty, difficulty_label, killing_blow_name),
            (player_identity_key, zone_name, difficulty, difficulty_label, killing_blow_name),
            (player_identity_key, difficulty, difficulty_label, killing_blow_name),
            (player_identity_key, zone_name, encounter_id, boss_name, killing_blow_name),
            (player_identity_key, zone_name, killing_blow_name),
            (player_identity_key, killing_blow_name)
          )
        ),
        top_killing_blow AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            zone_name,
            encounter_id,
            boss_name,
            difficulty,
            difficulty_label,
            MAX_BY(killing_blow_name, killing_blow_count) AS most_common_killing_blow,
            MAX(killing_blow_count) AS most_common_killing_blow_count
          FROM killing_blow_counts
          WHERE killing_blow_name != ''
          GROUP BY player_identity_key, zone_name, encounter_id, boss_name, difficulty, difficulty_label
        ),
        defensive_pull_base AS (
          SELECT
            player_identity_key,
            player_name,
            player_class,
            realm,
            zone_name,
            encounter_id,
            boss_name,
            difficulty,
            difficulty_label,
            report_code,
            fight_id,
            SUM(possible_casts) AS possible_casts,
            SUM(actual_casts) AS actual_casts,
            SUM(missed_casts) AS missed_casts
          FROM 03_gold.sc_analytics.gold_wipe_cooldown_utilization
          WHERE cooldown_category IN ('personal', 'personal_spec')
          GROUP BY
            player_identity_key,
            player_name,
            player_class,
            realm,
            zone_name,
            encounter_id,
            boss_name,
            difficulty,
            difficulty_label,
            report_code,
            fight_id
        ),
        defensive_scopes AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            MAX(realm) AS realm,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(encounter_id) = 1 THEN CAST(NULL AS BIGINT) ELSE encounter_id END AS encounter_id,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty) = 1 THEN CAST(NULL AS BIGINT) ELSE difficulty END AS difficulty,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            COUNT(DISTINCT CASE WHEN possible_casts > 0 THEN CONCAT(report_code, ':', CAST(fight_id AS STRING)) END) AS pulls_with_tracked_defensive_capacity,
            SUM(possible_casts) AS tracked_defensive_capacity,
            SUM(actual_casts) AS defensive_casts,
            SUM(missed_casts) AS defensive_missed_casts
          FROM defensive_pull_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, encounter_id, boss_name, difficulty, difficulty_label),
            (player_identity_key, zone_name, difficulty, difficulty_label),
            (player_identity_key, difficulty, difficulty_label),
            (player_identity_key, zone_name, encounter_id, boss_name),
            (player_identity_key, zone_name),
            (player_identity_key)
          )
        ),
        kill_base AS (
          SELECT
            player_identity_key,
            player_name,
            player_class,
            realm,
            zone_name,
            encounter_id,
            boss_name,
            difficulty,
            difficulty_label,
            report_code,
            fight_id
          FROM 03_gold.sc_analytics.gold_boss_kill_roster
        ),
        kill_scopes AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            MAX(realm) AS realm,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(encounter_id) = 1 THEN CAST(NULL AS BIGINT) ELSE encounter_id END AS encounter_id,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty) = 1 THEN CAST(NULL AS BIGINT) ELSE difficulty END AS difficulty,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            COUNT(DISTINCT CONCAT(report_code, ':', CAST(fight_id AS STRING))) AS kills_tracked
          FROM kill_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, encounter_id, boss_name, difficulty, difficulty_label),
            (player_identity_key, zone_name, difficulty, difficulty_label),
            (player_identity_key, difficulty, difficulty_label),
            (player_identity_key, zone_name, encounter_id, boss_name),
            (player_identity_key, zone_name),
            (player_identity_key)
          )
        ),
        kill_death_base AS (
          SELECT
            player_identity_key,
            player_name,
            player_class,
            realm,
            zone_name,
            encounter_id,
            boss_name,
            difficulty,
            difficulty_label
          FROM 03_gold.sc_analytics.gold_player_death_events
          WHERE COALESCE(is_kill, false) = true
        ),
        kill_death_scopes AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            MAX(realm) AS realm,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(encounter_id) = 1 THEN CAST(NULL AS BIGINT) ELSE encounter_id END AS encounter_id,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty) = 1 THEN CAST(NULL AS BIGINT) ELSE difficulty END AS difficulty,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            COUNT(*) AS kill_deaths
          FROM kill_death_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, encounter_id, boss_name, difficulty, difficulty_label),
            (player_identity_key, zone_name, difficulty, difficulty_label),
            (player_identity_key, difficulty, difficulty_label),
            (player_identity_key, zone_name, encounter_id, boss_name),
            (player_identity_key, zone_name),
            (player_identity_key)
          )
        ),
        joined AS (
          SELECT
            COALESCE(w.player_identity_key, d.player_identity_key, c.player_identity_key, k.player_identity_key, kd.player_identity_key) AS player_identity_key,
            COALESCE(w.player_name, d.player_name, c.player_name, k.player_name, kd.player_name) AS player_name,
            COALESCE(w.player_class, d.player_class, c.player_class, k.player_class, kd.player_class, r.player_class, 'Unknown') AS player_class,
            COALESCE(w.realm, d.realm, c.realm, k.realm, kd.realm, 'unknown') AS realm,
            r.role,
            COALESCE(w.zone_name, d.zone_name, c.zone_name, k.zone_name, kd.zone_name) AS zone_name,
            COALESCE(w.encounter_id, d.encounter_id, c.encounter_id, k.encounter_id, kd.encounter_id) AS encounter_id,
            COALESCE(w.boss_name, d.boss_name, c.boss_name, k.boss_name, kd.boss_name) AS boss_name,
            COALESCE(w.difficulty, d.difficulty, c.difficulty, k.difficulty, kd.difficulty) AS difficulty,
            COALESCE(w.difficulty_label, d.difficulty_label, c.difficulty_label, k.difficulty_label, kd.difficulty_label) AS difficulty_label,
            COALESCE(w.wipe_pulls_tracked, 0) AS wipe_pulls_tracked,
            COALESCE(d.wipe_deaths, 0) AS wipe_deaths,
            COALESCE(d.first_deaths, 0) AS first_deaths,
            COALESCE(d.early_deaths, 0) AS early_deaths,
            COALESCE(kd.kill_deaths, 0) AS kill_deaths,
            COALESCE(k.kills_tracked, 0) AS kills_tracked,
            COALESCE(c.pulls_with_tracked_defensive_capacity, 0) AS pulls_with_tracked_defensive_capacity,
            c.tracked_defensive_capacity,
            c.defensive_casts,
            c.defensive_missed_casts,
            COALESCE(d.healthstone_uses, 0) AS healthstone_uses,
            COALESCE(d.potion_uses, 0) AS potion_uses,
            COALESCE(d.no_healthstone_deaths, 0) AS no_healthstone_deaths,
            COALESCE(d.no_health_potion_deaths, 0) AS no_health_potion_deaths,
            COALESCE(t.most_common_killing_blow, '') AS most_common_killing_blow,
            COALESCE(t.most_common_killing_blow_count, 0) AS most_common_killing_blow_count
          FROM wipe_pull_scopes w
          FULL OUTER JOIN death_scopes d
            ON w.player_identity_key = d.player_identity_key
           AND w.zone_name = d.zone_name
           AND COALESCE(w.encounter_id, -1) = COALESCE(d.encounter_id, -1)
           AND w.boss_name = d.boss_name
           AND COALESCE(w.difficulty, -1) = COALESCE(d.difficulty, -1)
           AND w.difficulty_label = d.difficulty_label
          FULL OUTER JOIN defensive_scopes c
            ON COALESCE(w.player_identity_key, d.player_identity_key) = c.player_identity_key
           AND COALESCE(w.zone_name, d.zone_name) = c.zone_name
           AND COALESCE(COALESCE(w.encounter_id, d.encounter_id), -1) = COALESCE(c.encounter_id, -1)
           AND COALESCE(w.boss_name, d.boss_name) = c.boss_name
           AND COALESCE(COALESCE(w.difficulty, d.difficulty), -1) = COALESCE(c.difficulty, -1)
           AND COALESCE(w.difficulty_label, d.difficulty_label) = c.difficulty_label
          FULL OUTER JOIN kill_scopes k
            ON COALESCE(w.player_identity_key, d.player_identity_key, c.player_identity_key) = k.player_identity_key
           AND COALESCE(w.zone_name, d.zone_name, c.zone_name) = k.zone_name
           AND COALESCE(COALESCE(w.encounter_id, d.encounter_id, c.encounter_id), -1) = COALESCE(k.encounter_id, -1)
           AND COALESCE(w.boss_name, d.boss_name, c.boss_name) = k.boss_name
           AND COALESCE(COALESCE(w.difficulty, d.difficulty, c.difficulty), -1) = COALESCE(k.difficulty, -1)
           AND COALESCE(w.difficulty_label, d.difficulty_label, c.difficulty_label) = k.difficulty_label
          FULL OUTER JOIN kill_death_scopes kd
            ON COALESCE(w.player_identity_key, d.player_identity_key, c.player_identity_key, k.player_identity_key) = kd.player_identity_key
           AND COALESCE(w.zone_name, d.zone_name, c.zone_name, k.zone_name) = kd.zone_name
           AND COALESCE(COALESCE(w.encounter_id, d.encounter_id, c.encounter_id, k.encounter_id), -1) = COALESCE(kd.encounter_id, -1)
           AND COALESCE(w.boss_name, d.boss_name, c.boss_name, k.boss_name) = kd.boss_name
           AND COALESCE(COALESCE(w.difficulty, d.difficulty, c.difficulty, k.difficulty), -1) = COALESCE(kd.difficulty, -1)
           AND COALESCE(w.difficulty_label, d.difficulty_label, c.difficulty_label, k.difficulty_label) = kd.difficulty_label
          LEFT JOIN role_by_player r
            ON COALESCE(w.player_identity_key, d.player_identity_key, c.player_identity_key, k.player_identity_key, kd.player_identity_key) = r.player_identity_key
          LEFT JOIN top_killing_blow t
            ON COALESCE(w.player_identity_key, d.player_identity_key, c.player_identity_key, k.player_identity_key, kd.player_identity_key) = t.player_identity_key
           AND COALESCE(w.zone_name, d.zone_name, c.zone_name, k.zone_name, kd.zone_name) = t.zone_name
           AND COALESCE(COALESCE(w.encounter_id, d.encounter_id, c.encounter_id, k.encounter_id, kd.encounter_id), -1) = COALESCE(t.encounter_id, -1)
           AND COALESCE(w.boss_name, d.boss_name, c.boss_name, k.boss_name, kd.boss_name) = t.boss_name
           AND COALESCE(COALESCE(w.difficulty, d.difficulty, c.difficulty, k.difficulty, kd.difficulty), -1) = COALESCE(t.difficulty, -1)
           AND COALESCE(w.difficulty_label, d.difficulty_label, c.difficulty_label, k.difficulty_label, kd.difficulty_label) = t.difficulty_label
        ),
        base_metrics AS (
          SELECT
            *,
            CASE WHEN wipe_pulls_tracked > 0 THEN wipe_deaths / wipe_pulls_tracked ELSE 0 END AS deaths_per_wipe,
            CASE WHEN kills_tracked > 0 THEN kill_deaths / kills_tracked ELSE CAST(NULL AS DOUBLE) END AS deaths_per_kill,
            CASE
              WHEN tracked_defensive_capacity > 0
              THEN ROUND((defensive_casts / tracked_defensive_capacity) * 100, 1)
              ELSE CAST(NULL AS DOUBLE)
            END AS defensive_usage_rate,
            CASE WHEN wipe_deaths > 0 THEN ROUND((healthstone_uses / wipe_deaths) * 100, 1) ELSE CAST(NULL AS DOUBLE) END AS healthstone_usage_rate,
            CASE WHEN wipe_deaths > 0 THEN ROUND((potion_uses / wipe_deaths) * 100, 1) ELSE CAST(NULL AS DOUBLE) END AS potion_usage_rate,
            CASE WHEN wipe_deaths > 0 THEN ROUND((no_healthstone_deaths / wipe_deaths) * 100, 1) ELSE 0 END AS no_healthstone_pct,
            CASE WHEN wipe_deaths > 0 THEN ROUND((no_health_potion_deaths / wipe_deaths) * 100, 1) ELSE 0 END AS no_health_potion_pct,
            COALESCE(defensive_missed_casts, 0) * 0.5 + no_healthstone_deaths * 0.3 + no_health_potion_deaths * 0.2 AS weighted_failure_points,
            CASE
              WHEN tracked_defensive_capacity > 0 AND defensive_casts > 0 THEN 'tracked_used'
              WHEN tracked_defensive_capacity > 0 AND COALESCE(defensive_casts, 0) = 0 THEN 'tracked_zero_usage'
              WHEN wipe_pulls_tracked > 0 AND COALESCE(tracked_defensive_capacity, 0) = 0 THEN 'no_tracked_capacity'
              ELSE 'unknown'
            END AS defensive_tracking_status
          FROM joined
          WHERE COALESCE(player_name, '') != ''
        ),
        defensive_baselines AS (
          SELECT
            zone_name,
            COALESCE(encounter_id, -1) AS encounter_id_key,
            boss_name,
            COALESCE(difficulty, -1) AS difficulty_key,
            difficulty_label,
            player_class,
            PERCENTILE_APPROX(defensive_usage_rate, 0.5) AS defensive_class_baseline_pct
          FROM base_metrics
          WHERE defensive_usage_rate IS NOT NULL
          GROUP BY zone_name, COALESCE(encounter_id, -1), boss_name, COALESCE(difficulty, -1), difficulty_label, player_class
        ),
        component_scores AS (
          SELECT
            b.*,
            db.defensive_class_baseline_pct,
            CASE
              WHEN db.defensive_class_baseline_pct IS NULL OR b.defensive_usage_rate IS NULL
              THEN CAST(NULL AS DOUBLE)
              ELSE ROUND(b.defensive_usage_rate - db.defensive_class_baseline_pct, 1)
            END AS defensive_class_delta_pct,
            LEAST(100, GREATEST(0, 100 - b.deaths_per_wipe * 250)) AS death_pressure_score,
            CASE
              WHEN b.defensive_tracking_status IN ('tracked_used', 'tracked_zero_usage')
              THEN LEAST(
                100,
                GREATEST(
                  0,
                  b.defensive_usage_rate * 0.7
                  + LEAST(100, GREATEST(0, 50 + (b.defensive_usage_rate - COALESCE(db.defensive_class_baseline_pct, b.defensive_usage_rate)) * 1.5)) * 0.3
                )
              )
              WHEN b.defensive_tracking_status = 'no_tracked_capacity' THEN 50
              ELSE 50
            END AS defensive_component_score,
            CASE WHEN b.wipe_deaths > 0 THEN LEAST(100, GREATEST(0, 100 - b.no_healthstone_pct)) ELSE 100 END AS healthstone_component_score,
            CASE WHEN b.wipe_deaths > 0 THEN LEAST(100, GREATEST(0, 100 - b.no_health_potion_pct)) ELSE 100 END AS potion_component_score
          FROM base_metrics b
          LEFT JOIN defensive_baselines db
            ON b.zone_name = db.zone_name
           AND COALESCE(b.encounter_id, -1) = db.encounter_id_key
           AND b.boss_name = db.boss_name
           AND COALESCE(b.difficulty, -1) = db.difficulty_key
           AND b.difficulty_label = db.difficulty_label
           AND b.player_class = db.player_class
        ),
        scored AS (
          SELECT
            *,
            LEAST(
                100,
                GREATEST(
                  0,
                  defensive_component_score * 0.3
                  + healthstone_component_score * 0.3
                  + potion_component_score * 0.3
                  + death_pressure_score * 0.1
                )
              ) AS survival_discipline_score
          FROM component_scores
        )
        SELECT
          player_identity_key,
          player_name,
          player_class,
          realm,
          role,
          zone_name,
          encounter_id,
          boss_name,
          difficulty,
          difficulty_label,
          wipe_pulls_tracked,
          wipe_deaths,
          first_deaths,
          early_deaths,
          kill_deaths,
          kills_tracked,
          ROUND(deaths_per_kill, 4) AS deaths_per_kill,
          ROUND(deaths_per_wipe, 4) AS deaths_per_wipe,
          pulls_with_tracked_defensive_capacity,
          COALESCE(tracked_defensive_capacity, 0) AS tracked_defensive_capacity,
          COALESCE(defensive_casts, 0) AS defensive_casts,
          COALESCE(defensive_missed_casts, 0) AS defensive_missed_casts,
          healthstone_uses,
          potion_uses,
          no_healthstone_deaths,
          no_health_potion_deaths,
          defensive_usage_rate,
          healthstone_usage_rate,
          potion_usage_rate,
          no_healthstone_pct,
          no_health_potion_pct,
          ROUND(death_pressure_score, 1) AS death_pressure_score,
          ROUND(defensive_component_score, 1) AS defensive_component_score,
          ROUND(healthstone_component_score, 1) AS healthstone_component_score,
          ROUND(potion_component_score, 1) AS potion_component_score,
          ROUND(defensive_class_baseline_pct, 1) AS defensive_class_baseline_pct,
          defensive_class_delta_pct,
          ROUND(weighted_failure_points, 1) AS weighted_failure_points,
          ROUND(survival_discipline_score, 1) AS survival_failure_score,
          ROUND(survival_discipline_score, 1) AS survival_discipline_score,
          CASE
            WHEN LEAST(
              COALESCE(defensive_component_score, 100),
              healthstone_component_score,
              potion_component_score,
              death_pressure_score
            ) >= 95 THEN '—'
            WHEN COALESCE(defensive_component_score, 100) <= healthstone_component_score
             AND COALESCE(defensive_component_score, 100) <= potion_component_score
             AND COALESCE(defensive_component_score, 100) <= death_pressure_score THEN 'Defensive usage'
            WHEN death_pressure_score <= healthstone_component_score
             AND death_pressure_score <= potion_component_score THEN 'Wipe survival'
            WHEN healthstone_component_score <= potion_component_score THEN 'Healthstone'
            ELSE 'Health potion'
          END AS top_improvement_area,
          CASE
            WHEN LEAST(
              COALESCE(defensive_component_score, 100),
              healthstone_component_score,
              potion_component_score,
              death_pressure_score
            ) >= 95 THEN '—'
            WHEN COALESCE(defensive_component_score, 100) <= healthstone_component_score
             AND COALESCE(defensive_component_score, 100) <= potion_component_score
             AND COALESCE(defensive_component_score, 100) <= death_pressure_score THEN 'Defensive usage'
            WHEN death_pressure_score <= healthstone_component_score
             AND death_pressure_score <= potion_component_score THEN 'Wipe survival'
            WHEN healthstone_component_score <= potion_component_score THEN 'Healthstone'
            ELSE 'Health potion'
          END AS top_missing_category,
          defensive_tracking_status,
          defensive_tracking_status IN ('tracked_used', 'tracked_zero_usage') AS has_defensive_capacity_tracked,
          most_common_killing_blow,
          most_common_killing_blow_count
        FROM scored
        WHERE wipe_pulls_tracked > 0 OR wipe_deaths > 0
        """
    )
