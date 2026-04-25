# Databricks notebook source
# Silver layer — parsed WCL fight rankings
#
# silver_player_rankings — one row per player per kill fight with WCL parse
#                          percentiles.  Sourced by parsing the opaque
#                          rankings_json scalar from bronze.
#
# Actual WCL rankings(compare: Parses) response structure:
# {
#   "data": [
#     {
#       "fightID": 1,
#       "encounter": {"id": 2587, "name": "Eranog"},
#       "difficulty": 5,
#       "size": 20,
#       "roles": {
#         "tanks":   {"characters": [{name, class, spec, amount, rankPercent, bracketPercent, rank, ...}]},
#         "healers": {"characters": [...]},
#         "dps":     {"characters": [...]}
#       }
#     }
#   ]
# }
#
# Note: there is NO top-level array — the root is an object with a "data" key.
# Note: there is NO "medal" field — rankings are expressed via rankPercent only.
# Note: "rank" is a string in "~1265" format, not a number.

import dlt
from pyspark.sql import Window, functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schemas ────────────────────────────────────────────────────────────────────

_CHARACTER_STRUCT = StructType([
    StructField("id",             LongType(),   True),
    StructField("name",           StringType(), True),
    StructField("class",          StringType(), True),
    StructField("spec",           StringType(), True),
    StructField("amount",         DoubleType(), True),
    StructField("rankPercent",    DoubleType(), True),
    StructField("bracketPercent", DoubleType(), True),
    StructField("rank",           StringType(), True),   # "~1265" — keep as string
    StructField("totalParses",    LongType(),   True),
])

_ROLE_STRUCT = StructType([
    StructField("name",       StringType(),              True),
    StructField("characters", ArrayType(_CHARACTER_STRUCT), True),
])

_ROLES_STRUCT = StructType([
    StructField("tanks",   _ROLE_STRUCT, True),
    StructField("healers", _ROLE_STRUCT, True),
    StructField("dps",     _ROLE_STRUCT, True),
])

_ENCOUNTER_STRUCT = StructType([
    StructField("id",   LongType(),   True),
    StructField("name", StringType(), True),
])

_FIGHT_ENTRY_STRUCT = StructType([
    StructField("fightID",    LongType(),   True),
    StructField("difficulty", LongType(),   True),
    StructField("zone",       LongType(),   True),
    StructField("encounter",  _ENCOUNTER_STRUCT, True),
    StructField("size",       LongType(),   True),
    StructField("duration",   LongType(),   True),
    StructField("roles",      _ROLES_STRUCT, True),
])

_RANKINGS_SCHEMA = StructType([
    StructField("data", ArrayType(_FIGHT_ENTRY_STRUCT), True),
])


# ── Parsed Player Rankings ─────────────────────────────────────────────────────

@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_player_rankings",
    comment=(
        "WCL parse rankings per player per kill fight. "
        "One row per player per fight with percentile, spec, and class. "
        "Sourced from rankings(compare: Parses) — roles-based structure."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_ranking_ref", "report_code IS NOT NULL AND fight_id IS NOT NULL")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
def silver_player_rankings():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_rankings")  # noqa: F821

    # If the ingestion job re-landed a rankings file (e.g. to backfill rankings
    # WCL hadn't computed yet), bronze can have multiple rows per report_code.
    # Keep only the most recently ingested payload per report.
    latest_window = Window.partitionBy("report_code").orderBy(F.col("_ingested_at").desc_nulls_last())
    raw_latest = (
        raw
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Parse the opaque JSON string into a typed struct
    fights = (
        raw_latest
        .withColumn("parsed", F.from_json(F.col("rankings_json"), _RANKINGS_SCHEMA))
        .filter(F.col("parsed").isNotNull())
        # Explode outer data array: one row per fight entry
        .withColumn("fight_entry", F.explode("parsed.data"))
    )

    # Extract characters from each role separately, then union.
    # Each role's characters array is independently exploded to avoid cross-product.
    def _role_df(role_key: str, role_label: str):
        chars_col = f"fight_entry.roles.{role_key}.characters"
        return (
            fights
            .filter(F.col(chars_col).isNotNull())
            .filter(F.size(F.col(chars_col)) > 0)
            .select(
                F.col("report_code"),
                F.col("fight_entry.fightID").alias("fight_id"),
                F.col("fight_entry.encounter.id").alias("encounter_id"),
                F.col("fight_entry.encounter.name").alias("encounter_name"),
                F.col("fight_entry.difficulty").alias("difficulty"),
                F.col("fight_entry.size").alias("fight_size"),
                F.explode(chars_col).alias("character"),
                F.lit(role_label).alias("role"),
            )
        )

    all_players = (
        _role_df("tanks",   "tank")
        .union(_role_df("healers", "healer"))
        .union(_role_df("dps",     "dps"))
    )

    return (
        all_players
        .select(
            F.col("report_code"),
            F.col("fight_id"),
            F.col("encounter_id"),
            F.col("encounter_name"),
            F.col("difficulty"),
            F.col("fight_size"),
            F.col("role"),
            F.col("character.name").alias("player_name"),
            F.col("character.class").alias("player_class"),
            F.col("character.spec").alias("spec"),
            F.col("character.amount").alias("amount"),
            F.col("character.rankPercent").alias("rank_percent"),
            F.col("character.bracketPercent").alias("bracket_percent"),
            F.col("character.rank").alias("rank_string"),   # "~1265" approximate rank
            F.col("character.totalParses").alias("total_parses"),
        )
        .filter(F.col("player_name").isNotNull())
        .dropDuplicates(["report_code", "fight_id", "player_name"])
    )
