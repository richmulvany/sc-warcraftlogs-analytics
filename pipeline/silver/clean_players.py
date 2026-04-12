# Databricks notebook source
# Silver layer — player actor roster and per-fight performance
#
# silver_actor_roster       — one row per player per report (class, realm)
# silver_player_performance — one row per player per boss kill with DPS/HPS,
#                             spec, item level, active time

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Schema for playerDetails JSON blob ────────────────────────────────────────
# WCL returns playerDetails as an opaque JSON scalar.  We stored it as a string
# in bronze; parse it here with an explicit schema so silver is fully typed.
# Structure: {"data": {"playerDetails": {"dps": [...], "healers": [...], "tanks": [...]}}}

_SPEC_SCHEMA = StructType([
    StructField("spec", StringType(), True),
    StructField("count", IntegerType(), True),
])

_PLAYER_ENTRY_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),       # class name
    StructField("icon", StringType(), True),        # spec icon slug
    StructField("specs", ArrayType(_SPEC_SCHEMA), True),
    StructField("minItemLevel", FloatType(), True),
    StructField("maxItemLevel", FloatType(), True),
    StructField("total", FloatType(), True),        # damage or healing total
    StructField("activeTime", FloatType(), True),   # ms active
    StructField("activeTimeReduced", FloatType(), True),
])

_PLAYER_DETAILS_SCHEMA = StructType([
    StructField("data", StructType([
        StructField("playerDetails", StructType([
            StructField("dps", ArrayType(_PLAYER_ENTRY_SCHEMA), True),
            StructField("healers", ArrayType(_PLAYER_ENTRY_SCHEMA), True),
            StructField("tanks", ArrayType(_PLAYER_ENTRY_SCHEMA), True),
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
@dlt.expect_or_drop("valid_actor_id", "actor_id IS NOT NULL")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
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
        .dropDuplicates(["report_code", "actor_id"])
    )


# ── silver_player_performance ─────────────────────────────────────────────────


@dlt.table(
    name="silver_player_performance",
    comment=(
        "Per-player performance on each boss kill: DPS/HPS totals, spec, item level, "
        "active time. One row per player per kill fight."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_fight_ref", "report_code IS NOT NULL AND fight_id IS NOT NULL")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
def silver_player_performance():
    raw = dlt.read("bronze_player_details")  # batch — avoids streaming union complexity

    parsed = raw.withColumn(
        "pd",
        F.from_json(F.col("player_details_json"), _PLAYER_DETAILS_SCHEMA),
    )

    # Explode each role array, tagging with the role label
    def _role_df(role_col: str, role_label: str):  # type: ignore[return]
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
        _role_df("dps", "dps")
        .union(_role_df("healers", "healer"))
        .union(_role_df("tanks", "tank"))
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
            # Primary spec: first entry in specs array
            F.when(
                F.size(F.col("player.specs")) > 0,
                F.col("player.specs")[0]["spec"],
            ).alias("spec"),
            F.col("player.minItemLevel").alias("min_item_level"),
            F.col("player.maxItemLevel").alias("max_item_level"),
            F.round(
                (F.col("player.minItemLevel") + F.col("player.maxItemLevel")) / 2, 1
            ).alias("avg_item_level"),
            F.col("player.total").alias("total_amount"),  # damage or healing in raw units
            # Active time as % of fight duration (avoid division by zero)
            F.round(
                F.col("player.activeTime") / F.greatest(F.col("duration_ms"), F.lit(1)) * 100, 1
            ).alias("active_time_pct"),
            F.col("_ingested_at"),
        )
        .filter(F.col("player_name").isNotNull())
        .dropDuplicates(["report_code", "fight_id", "player_name"])
    )
