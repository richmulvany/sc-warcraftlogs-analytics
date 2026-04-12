# Databricks notebook source
# Silver layer — parsed WCL fight rankings
#
# silver_player_rankings — one row per player per kill fight with WCL parse
#                          percentiles and performance metrics.  Sourced by
#                          parsing the opaque rankings_json scalar from bronze.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schema for the rankings JSON scalar ───────────────────────────────────────
# WCL report.rankings(compare: Parses) returns an array of fight-level objects,
# each of which contains a nested array of per-player ranking records.
#
# Example structure:
# [
#   {
#     "fightID": 1,
#     "difficulty": 5,
#     "encounterID": 2564,
#     "size": 20,
#     "rankings": [
#       {
#         "name": "PlayerName",
#         "class": "Warrior",
#         "spec": "Arms",
#         "amount": 123456.78,
#         "rankPercent": 75.32,
#         "bracketPercent": 68.15,
#         "medal": "silver",
#         "duration": 180000
#       }
#     ]
#   }
# ]

_PLAYER_RANKING_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("class", StringType(), True),
    StructField("spec", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("rankPercent", DoubleType(), True),
    StructField("bracketPercent", DoubleType(), True),
    StructField("medal", StringType(), True),
    StructField("duration", LongType(), True),
])

_FIGHT_RANKING_STRUCT = StructType([
    StructField("fightID", LongType(), True),
    StructField("difficulty", LongType(), True),
    StructField("encounterID", LongType(), True),
    StructField("size", LongType(), True),
    StructField("rankings", ArrayType(_PLAYER_RANKING_STRUCT), True),
])

_RANKINGS_SCHEMA = ArrayType(_FIGHT_RANKING_STRUCT)


# ── Parsed Player Rankings ─────────────────────────────────────────────────────


@dlt.table(
    name="silver_player_rankings",
    comment=(
        "WCL parse rankings per player per kill fight. "
        "One row per player per fight with percentile and performance data."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_ranking_ref", "report_code IS NOT NULL AND fight_id IS NOT NULL")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
def silver_player_rankings():
    # Read as batch — rankings are stable after a report is cleared; this is a
    # dimension-like table that downstream gold tables join to kill facts.
    raw = dlt.read("bronze_fight_rankings")

    parsed = (
        raw
        .withColumn("parsed", F.from_json(F.col("rankings_json"), _RANKINGS_SCHEMA))
        # Drop rows where JSON parsing failed (malformed or unexpected structure)
        .filter(F.col("parsed").isNotNull())
        # Explode outer array: one row per fight ranking entry
        .withColumn("fight_ranking", F.explode("parsed"))
        # Explode inner array: one row per player within that fight
        .withColumn("player_ranking", F.explode("fight_ranking.rankings"))
    )

    return (
        parsed
        .select(
            F.col("report_code"),
            F.col("fight_ranking.fightID").alias("fight_id"),
            F.col("fight_ranking.encounterID").alias("encounter_id"),
            F.col("fight_ranking.difficulty").alias("difficulty"),
            F.col("player_ranking.name").alias("player_name"),
            F.col("player_ranking.class").alias("player_class"),
            F.col("player_ranking.spec").alias("spec"),
            F.col("player_ranking.amount").alias("amount"),
            F.col("player_ranking.rankPercent").alias("rank_percent"),
            F.col("player_ranking.bracketPercent").alias("bracket_percent"),
            F.col("player_ranking.medal").alias("medal"),
            F.col("player_ranking.duration").alias("fight_duration_ms"),
        )
        .filter(F.col("player_name").isNotNull())
    )
