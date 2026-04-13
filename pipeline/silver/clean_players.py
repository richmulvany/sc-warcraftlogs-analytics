# Databricks notebook source
# Silver layer — player actor roster and per-fight combatant info
#
# silver_actor_roster       — one row per player per report (class, realm)
# silver_player_performance — one row per player per boss kill with spec,
#                             item level, combat stats, and consumable usage.
#
# NOTE on throughput (DPS/HPS):
#   The playerDetails endpoint with includeCombatantInfo=true returns combatant
#   info (gear, stats, talents) but NOT damage/healing totals.  Throughput
#   comes from the rankings endpoint — use silver_player_rankings.amount.
#   fact_player_fight_performance joins both sources correctly.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schemas for playerDetails JSON blob ───────────────────────────────────────
# WCL playerDetails(includeCombatantInfo: true) structure:
# {"data": {"playerDetails": {"dps": [...], "healers": [...], "tanks": [...]}}}
#
# Each player entry contains combatant info (gear, talents, stats) and metadata
# but NOT damage/healing totals — those come from the rankings endpoint.

_SPEC_SCHEMA = StructType([
    StructField("spec",  StringType(), True),
    StructField("count", LongType(),   True),
])

_STAT_VALUE_STRUCT = StructType([
    StructField("min", LongType(), True),
    StructField("max", LongType(), True),
])

# Secondary stats present for all specs; primary stat varies by class.
_STATS_STRUCT = StructType([
    StructField("Crit",        _STAT_VALUE_STRUCT, True),
    StructField("Haste",       _STAT_VALUE_STRUCT, True),
    StructField("Mastery",     _STAT_VALUE_STRUCT, True),
    StructField("Versatility", _STAT_VALUE_STRUCT, True),
    StructField("Strength",    _STAT_VALUE_STRUCT, True),  # melee/tank primary
    StructField("Agility",     _STAT_VALUE_STRUCT, True),  # physical dps primary
    StructField("Intellect",   _STAT_VALUE_STRUCT, True),  # caster/healer primary
    StructField("Stamina",     _STAT_VALUE_STRUCT, True),
])

_COMBATANT_INFO_STRUCT = StructType([
    StructField("stats", _STATS_STRUCT, True),
    # talentTree and gear omitted — too complex to flatten usefully in silver
])

_PLAYER_ENTRY_SCHEMA = StructType([
    StructField("id",             LongType(),   True),
    StructField("name",           StringType(), True),
    StructField("type",           StringType(), True),        # WoW class name
    StructField("icon",           StringType(), True),        # "Class-Spec" icon slug
    StructField("specs",          ArrayType(_SPEC_SCHEMA), True),
    StructField("minItemLevel",   FloatType(),  True),
    StructField("maxItemLevel",   FloatType(),  True),
    StructField("potionUse",      LongType(),   True),        # 0 or 1 per fight
    StructField("healthstoneUse", LongType(),   True),        # 0 or 1 per fight
    StructField("combatantInfo",  _COMBATANT_INFO_STRUCT, True),
])

_PLAYER_DETAILS_SCHEMA = StructType([
    StructField("data", StructType([
        StructField("playerDetails", StructType([
            StructField("dps",     ArrayType(_PLAYER_ENTRY_SCHEMA), True),
            StructField("healers", ArrayType(_PLAYER_ENTRY_SCHEMA), True),
            StructField("tanks",   ArrayType(_PLAYER_ENTRY_SCHEMA), True),
        ]), True),
    ]), True),
])


# ── silver_actor_roster ───────────────────────────────────────────────────────

@dlt.table(
    name="silver_actor_roster",
    comment=(
        "Player actor roster per report from masterData. "
        "One row per player per report with class and realm."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_report_code", "report_code IS NOT NULL")
@dlt.expect_or_drop("valid_actor_id",    "actor_id IS NOT NULL")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL AND LENGTH(player_name) > 0")
def silver_actor_roster():
    return (
        dlt.read_stream("bronze_actor_roster")
        .filter(F.col("actors").isNotNull())
        .select(
            F.col("report_code"),
            F.explode("actors").alias("actor"),
            F.col("_ingested_at"),
        )
        .select(
            F.col("report_code"),
            F.col("actor.id").alias("actor_id"),
            F.trim(F.col("actor.name")).alias("player_name"),
            F.trim(F.col("actor.subType")).alias("player_class"),  # subType = class for Players
            F.col("actor.server").alias("realm"),
            F.col("_ingested_at"),
        )
        # Filter out empty names and "Unknown" class actors — these are usually NPCs
        # incorrectly typed as Players, or actors from very old log formats.
        .filter(F.col("player_name") != "")
        .dropDuplicates(["report_code", "actor_id"])
    )


# ── silver_player_performance ─────────────────────────────────────────────────

@dlt.table(
    name="silver_player_performance",
    comment=(
        "Per-player combatant info on each boss kill: spec, item level, "
        "combat stats (Crit/Haste/Mastery/Vers), and consumable usage. "
        "One row per player per kill fight. "
        "NOTE: throughput (DPS/HPS) is in silver_player_rankings.amount — "
        "not available from the playerDetails endpoint."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_fight_ref",   "report_code IS NOT NULL AND fight_id IS NOT NULL")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL AND LENGTH(player_name) > 0")
def silver_player_performance():
    raw = dlt.read("bronze_player_details")  # batch

    parsed = raw.withColumn(
        "pd",
        F.from_json(F.col("player_details_json"), _PLAYER_DETAILS_SCHEMA),
    )

    def _role_df(role_col: str, role_label: str):
        return (
            parsed
            .filter(F.size(F.col(f"pd.data.playerDetails.{role_col}")) > 0)
            .select(
                "report_code",
                "fight_id",
                "boss_name",
                "encounter_id",
                "difficulty",
                "zone_id",
                "zone_name",
                "duration_ms",
                "_ingested_at",
                F.explode(f"pd.data.playerDetails.{role_col}").alias("player"),
                F.lit(role_label).alias("role"),
            )
        )

    all_players = (
        _role_df("dps",     "dps")
        .union(_role_df("healers", "healer"))
        .union(_role_df("tanks",   "tank"))
    )

    return (
        all_players
        .select(
            F.col("report_code"),
            F.col("fight_id"),
            F.col("boss_name"),
            F.col("encounter_id"),
            F.col("difficulty"),
            F.when(F.col("difficulty") == 3, "Normal")
             .when(F.col("difficulty") == 4, "Heroic")
             .when(F.col("difficulty") == 5, "Mythic")
             .otherwise("Unknown")
             .alias("difficulty_label"),
            F.col("zone_id"),
            F.col("zone_name"),
            F.col("duration_ms"),
            F.col("role"),
            F.col("player.id").alias("actor_id"),
            F.trim(F.col("player.name")).alias("player_name"),
            F.col("player.type").alias("player_class"),
            # Primary spec from specs array (most-played spec this fight)
            F.when(
                F.size(F.col("player.specs")) > 0,
                F.col("player.specs")[0]["spec"],
            ).alias("spec"),
            F.round(
                (F.col("player.minItemLevel") + F.col("player.maxItemLevel")) / 2, 1
            ).alias("avg_item_level"),
            # Consumable usage — 0 = did not use, 1 = used
            F.col("player.potionUse").alias("potion_use"),
            F.col("player.healthstoneUse").alias("healthstone_use"),
            # Secondary combat stats (from combatantInfo — values are rating integers)
            F.col("player.combatantInfo.stats.Crit.min").alias("crit_rating"),
            F.col("player.combatantInfo.stats.Haste.min").alias("haste_rating"),
            F.col("player.combatantInfo.stats.Mastery.min").alias("mastery_rating"),
            F.col("player.combatantInfo.stats.Versatility.min").alias("versatility_rating"),
            F.col("_ingested_at"),
        )
        .filter(F.col("player_name").isNotNull() & (F.col("player_name") != ""))
        .dropDuplicates(["report_code", "fight_id", "player_name"])
    )
