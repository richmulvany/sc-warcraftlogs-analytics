# Databricks notebook source
# ruff: noqa: I001
# Gold layer — core fact tables
#
# fact_player_fight_performance — one row per player per kill fight with
#                                 performance metrics and WCL parse rankings
# fact_player_events            — one row per death event per player per report
#
# These are the backbone tables that downstream gold aggregation tables build on.
#
# NOTE on throughput:
#   throughput_per_second comes from silver_player_rankings.amount which is the
#   role-appropriate per-second value WCL uses for ranking — DPS for dps/tank
#   rows, HPS for healer rows (silver picks from the matching playerMetric
#   payload).  Already normalised per fight second.
#   silver_player_performance (playerDetails endpoint) does NOT carry damage totals.

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE)) if _HERE else None
if _REPO_ROOT and _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import dlt  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType  # noqa: E402

from pipeline.consumables import (  # noqa: E402
    MIDNIGHT_COMBAT_POTION_IDS as COMBAT_POTION_IDS,
    MIDNIGHT_COMBAT_POTION_NAMES as COMBAT_POTION_NAMES,
    merge_consumable_name_strings,
)

_BUFF_ABILITY_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("guid", LongType(), True),
])

_BUFF_EVENT_STRUCT = StructType([
    StructField("timestamp", LongType(), True),
    StructField("type", StringType(), True),
    StructField("sourceID", LongType(), True),
    StructField("targetID", LongType(), True),
    StructField("fight", LongType(), True),
    StructField("ability", _BUFF_ABILITY_STRUCT, True),
    StructField("abilityGameID", LongType(), True),
])

_BUFF_EVENTS_SCHEMA = StructType([
    StructField("data", ArrayType(_BUFF_EVENT_STRUCT), True),
])

_merge_consumable_names_udf = F.udf(merge_consumable_name_strings, StringType())


# ── Player Fight Performance Fact ──────────────────────────────────────────────
# Joins silver_player_performance (from player_details) to silver_fight_events
# (fight context: date, zone, boss, outcome) and silver_player_rankings (WCL
# parse percentiles and throughput amount).
#
# Only kill fights are included (is_kill = true) — this table is intended for
# performance assessment rather than wipe analysis (use silver_fight_events for
# the latter).

@dlt.table(
    name="03_gold.sc_analytics.fact_player_fight_performance",
    comment=(
        "Per-player performance on every boss kill. "
        "Joins fight context, player stats (gear, consumables, combat stats), "
        "and WCL parse rankings (throughput, percentile). "
        "One row per player per kill fight."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def fact_player_fight_performance():
    perf = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_player_performance")  # noqa: F821
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821
    rankings = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_player_rankings")  # noqa: F821
    buffs = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_player_combatant_buffs")  # noqa: F821
    raw_casts = spark.read.table("01_bronze.warcraftlogs.bronze_fight_casts")  # noqa: F821
    actors = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_actor_roster")  # noqa: F821

    # Kill fights only with full context
    kill_context = (
        fights
        .filter(F.col("is_kill") == True)  # noqa: E712
        .select(
            "report_code",
            "fight_id",
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "raid_night_date",
            "is_kill",
            "duration_seconds",
        )
    )

    # Rankings keyed on (report_code, fight_id, player_name).
    # amount = WCL role-appropriate metric (DPS for dps/tank, HPS for healer)
    # per-second throughput, already fight-normalised.
    rankings_slim = (
        rankings
        .select(
            F.col("report_code").alias("_r_report_code"),
            F.col("fight_id").alias("_r_fight_id"),
            F.col("player_name").alias("_r_player_name"),
            "rank_percent",
            "bracket_percent",
            "rank_string",   # "~1265" approximate rank position
            F.col("amount").cast("long").alias("throughput_per_second"),
        )
    )

    # Slim perf to player-specific columns only — fight-context cols come from kill_context.
    # Includes combatant stats and consumable usage from playerDetails endpoint.
    perf_slim = perf.select(
        "report_code",
        "fight_id",
        "role",
        "player_name",
        "player_class",
        "spec",
        "avg_item_level",
        "healthstone_use",
        "has_weapon_enhancement",
        "weapon_enhancement_names",
        "crit_rating",
        "haste_rating",
        "mastery_rating",
        "versatility_rating",
    )

    buffs_slim = buffs.select(
        F.col("report_code").alias("_b_report_code"),
        F.col("fight_id").alias("_b_fight_id"),
        F.col("player_name").alias("_b_player_name"),
        "has_food_buff",
        "food_buff_names",
        "has_flask_or_phial_buff",
        "flask_or_phial_names",
        "has_weapon_enhancement_aura",
        "weapon_enhancement_aura_names",
    )

    combat_potions_slim = (
        raw_casts
        .withColumn("parsed_buffs", F.from_json(F.col("buffs_json"), _BUFF_EVENTS_SCHEMA))
        .filter(F.col("parsed_buffs").isNotNull())
        .withColumn("event", F.explode("parsed_buffs.data"))
        .withColumn("fight_id", F.col("event.fight"))
        .withColumn(
            "ability_name_normalized",
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.lower(F.coalesce(F.col("event.ability.name"), F.lit(""))),
                            r"'",
                            "",
                        ),
                        r"\s+",
                        " ",
                    ),
                    r"^\s+|\s+$",
                    "",
                )
            ),
        )
        .withColumn("actor_id", F.coalesce(F.col("event.targetID"), F.col("event.sourceID")))
        .filter(
            F.col("ability_name_normalized").isin(COMBAT_POTION_NAMES)
            | F.col("event.ability.guid").isin(COMBAT_POTION_IDS)
            | F.col("event.abilityGameID").isin(COMBAT_POTION_IDS)
        )
        .join(
            actors.select(
                F.col("report_code").alias("_a_report_code"),
                F.col("actor_id").alias("_a_actor_id"),
                "player_name",
            ),
            (F.col("report_code") == F.col("_a_report_code"))
            & (F.col("actor_id") == F.col("_a_actor_id")),
            "inner",
        )
        .drop("_a_report_code", "_a_actor_id")
        .groupBy("report_code", "fight_id", "player_name")
        .agg(
            F.countDistinct("event.timestamp").alias("combat_potion_casts"),
            F.concat_ws(
                " | ",
                F.array_sort(F.collect_set(F.col("event.ability.name"))),
            ).alias("combat_potion_names"),
        )
        .select(
            F.col("report_code").alias("_p_report_code"),
            F.col("fight_id").alias("_p_fight_id"),
            F.col("player_name").alias("_p_player_name"),
            F.lit(1).alias("cast_potion_use"),
            "combat_potion_casts",
            "combat_potion_names",
        )
    )

    return (
        perf_slim
        # Inner join to kill_context — drops wipe performance rows
        .join(kill_context, ["report_code", "fight_id"], "inner")
        # Left join rankings — not all kill fights have WCL rankings available
        .join(
            rankings_slim,
            (perf_slim.report_code == rankings_slim._r_report_code)
            & (perf_slim.fight_id == rankings_slim._r_fight_id)
            & (perf_slim.player_name == rankings_slim._r_player_name),
            "left",
        )
        .join(
            buffs_slim,
            (perf_slim.report_code == buffs_slim._b_report_code)
            & (perf_slim.fight_id == buffs_slim._b_fight_id)
            & (perf_slim.player_name == buffs_slim._b_player_name),
            "left",
        )
        .join(
            combat_potions_slim,
            (perf_slim.report_code == combat_potions_slim._p_report_code)
            & (perf_slim.fight_id == combat_potions_slim._p_fight_id)
            & (perf_slim.player_name == combat_potions_slim._p_player_name),
            "left",
        )
        .drop("_r_report_code", "_r_fight_id", "_r_player_name")
        .drop("_b_report_code", "_b_fight_id", "_b_player_name")
        .drop("_p_report_code", "_p_fight_id", "_p_player_name")
        .withColumn("potion_use", F.coalesce(F.col("cast_potion_use"), F.lit(0)))
        .withColumn("has_food_buff", F.coalesce(F.col("has_food_buff"), F.lit(0)))
        .withColumn("has_flask_or_phial_buff", F.coalesce(F.col("has_flask_or_phial_buff"), F.lit(0)))
        .withColumn(
            "has_weapon_enhancement",
            F.greatest(
                F.coalesce(F.col("has_weapon_enhancement"), F.lit(0)),
                F.coalesce(F.col("has_weapon_enhancement_aura"), F.lit(0)),
            ),
        )
        .withColumn(
            "weapon_enhancement_names",
            _merge_consumable_names_udf(
                F.col("weapon_enhancement_names"),
                F.col("weapon_enhancement_aura_names"),
            ),
        )
        .select(
            "report_code",
            "fight_id",
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "raid_night_date",
            "is_kill",
            "duration_seconds",
            "player_name",
            "player_class",
            "role",
            "spec",
            "avg_item_level",
            "potion_use",
            "combat_potion_casts",
            "combat_potion_names",
            "healthstone_use",
            "has_food_buff",
            "food_buff_names",
            "has_flask_or_phial_buff",
            "flask_or_phial_names",
            "has_weapon_enhancement",
            "weapon_enhancement_names",
            "crit_rating",
            "haste_rating",
            "mastery_rating",
            "versatility_rating",
            "throughput_per_second",   # from rankings.amount (DPS for dps/tank, HPS for healer; nullable)
            "rank_percent",
            "bracket_percent",
            "rank_string",
        )
        .orderBy("raid_night_date", "encounter_id", "role", F.col("throughput_per_second").desc_nulls_last())
    )


# ── Player Events Fact (Deaths) ────────────────────────────────────────────────
# Event-level table — one row per death event.
# fight_id is available directly from silver_player_deaths (each WCL death entry
# carries the fight it occurred in).  Zone context is joined from
# silver_guild_reports via report_code.

@dlt.table(
    name="03_gold.sc_analytics.fact_player_events",
    comment=(
        "Per-player death events across all raid reports. "
        "One row per death event with fight_id, killing blow, and zone context."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def fact_player_events():
    deaths = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_player_deaths")  # noqa: F821
    reports = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_guild_reports")  # noqa: F821

    # Zone context from reports
    report_context = (
        reports
        .select(
            F.col("code").alias("_r_code"),
            F.col("zone_name"),
            F.col("zone_id"),
            F.to_date("start_time_utc").alias("raid_night_date"),
        )
    )

    return (
        deaths
        .join(
            report_context,
            deaths.report_code == report_context._r_code,
            "left",
        )
        .drop("_r_code")
        .select(
            "report_code",
            "fight_id",
            "player_name",
            "player_class",
            "death_timestamp_ms",
            "overkill",
            "killing_blow_name",
            "killing_blow_id",
            "zone_name",
            "zone_id",
            "raid_night_date",
        )
        .orderBy("report_code", "fight_id", "death_timestamp_ms")
    )
