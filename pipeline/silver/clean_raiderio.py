# Databricks notebook source
# Silver layer — cleaned Raider.IO Mythic+ profile data
#
# bronze_raiderio_character_profiles stores the full Raider.IO response as an
# opaque JSON string. These tables parse only the stable fields needed for M+
# score and run analytics while tolerating missing optional arrays.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

_SCORE_STRUCT = StructType([
    StructField("all", DoubleType(), True),
    StructField("dps", DoubleType(), True),
    StructField("healer", DoubleType(), True),
    StructField("tank", DoubleType(), True),
])

_SCORE_SEASON_STRUCT = StructType([
    StructField("season", StringType(), True),
    StructField("scores", _SCORE_STRUCT, True),
])

_RANK_POSITION_STRUCT = StructType([
    StructField("world", LongType(), True),
    StructField("region", LongType(), True),
    StructField("realm", LongType(), True),
])

_RUN_STRUCT = StructType([
    StructField("dungeon", StringType(), True),
    StructField("short_name", StringType(), True),
    StructField("mythic_level", LongType(), True),
    StructField("completed_at", StringType(), True),
    StructField("clear_time_ms", LongType(), True),
    StructField("par_time_ms", LongType(), True),
    StructField("num_keystone_upgrades", LongType(), True),
    StructField("map_challenge_mode_id", LongType(), True),
    StructField("zone_id", LongType(), True),
    StructField("score", DoubleType(), True),
    StructField("url", StringType(), True),
])

_PROFILE_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("realm", StringType(), True),
    StructField("profile_url", StringType(), True),
    StructField("last_crawled_at", StringType(), True),
    StructField("mythic_plus_scores_by_season", ArrayType(_SCORE_SEASON_STRUCT), True),
    StructField(
        "mythic_plus_ranks",
        StructType([
            StructField("overall", _RANK_POSITION_STRUCT, True),
            StructField("class", _RANK_POSITION_STRUCT, True),
            StructField("class_dps", _RANK_POSITION_STRUCT, True),
            StructField("class_healer", _RANK_POSITION_STRUCT, True),
            StructField("class_tank", _RANK_POSITION_STRUCT, True),
        ]),
        True,
    ),
    StructField("mythic_plus_recent_runs", ArrayType(_RUN_STRUCT), True),
    StructField("mythic_plus_best_runs", ArrayType(_RUN_STRUCT), True),
])


def _parsed_profiles():
    raw = dlt.read("bronze_raiderio_character_profiles")
    return (
        raw
        .withColumn("_profile", F.from_json("profile_json", _PROFILE_SCHEMA))
        .withColumn("_ingested_at_ts", F.to_timestamp("_ingested_at"))
    )


@dlt.table(
    name="silver_raiderio_player_scores",
    comment="Parsed Raider.IO Mythic+ score snapshots, one row per player per ingestion run.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
def silver_raiderio_player_scores():
    parsed = _parsed_profiles()
    return (
        parsed
        .withColumn("_season_row", F.explode_outer("_profile.mythic_plus_scores_by_season"))
        .select(
            F.col("player_name"),
            F.col("realm_slug"),
            F.lower(F.coalesce(F.col("region"), F.col("_profile.region"))).alias("region"),
            F.coalesce(F.col("profile_url"), F.col("_profile.profile_url")).alias("profile_url"),
            F.lit("current").alias("season"),
            F.col("_season_row.scores.`all`").alias("score_all"),
            F.col("_season_row.scores.dps").alias("score_dps"),
            F.col("_season_row.scores.healer").alias("score_healer"),
            F.col("_season_row.scores.tank").alias("score_tank"),
            F.col("_profile.mythic_plus_ranks.overall.world").alias("world_rank"),
            F.col("_profile.mythic_plus_ranks.overall.region").alias("region_rank"),
            F.col("_profile.mythic_plus_ranks.overall.realm").alias("realm_rank"),
            F.col("_profile.last_crawled_at").alias("raiderio_last_crawled_at"),
            F.col("_ingested_at_ts").alias("snapshot_at"),
            F.col("_ingested_at"),
        )
    )


@dlt.table(
    name="silver_raiderio_player_runs",
    comment="Parsed Raider.IO Mythic+ runs from recent and best run arrays.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
@dlt.expect_or_drop("valid_dungeon", "dungeon IS NOT NULL")
def silver_raiderio_player_runs():
    parsed = _parsed_profiles()

    recent = (
        parsed
        .withColumn("_run", F.explode_outer("_profile.mythic_plus_recent_runs"))
        .withColumn("source", F.lit("recent"))
    )
    best = (
        parsed
        .withColumn("_run", F.explode_outer("_profile.mythic_plus_best_runs"))
        .withColumn("source", F.lit("best"))
    )

    runs = recent.unionByName(best)

    selected = (
        runs
        .select(
            F.col("player_name"),
            F.col("realm_slug"),
            F.lower(F.coalesce(F.col("region"), F.col("_profile.region"))).alias("region"),
            F.lit("current").alias("season"),
            F.col("source"),
            F.col("_run.dungeon").alias("dungeon"),
            F.col("_run.short_name").alias("short_name"),
            F.col("_run.mythic_level").alias("mythic_level"),
            F.to_timestamp("_run.completed_at").alias("completed_at"),
            F.col("_run.clear_time_ms").alias("clear_time_ms"),
            F.col("_run.par_time_ms").alias("par_time_ms"),
            F.col("_run.num_keystone_upgrades").alias("num_keystone_upgrades"),
            F.col("_run.map_challenge_mode_id").alias("map_challenge_mode_id"),
            F.col("_run.zone_id").alias("zone_id"),
            F.col("_run.score").alias("score"),
            F.col("_run.url").alias("url"),
            # best_runs from Raider.IO are always timed by definition; trust source tag first
            F.when(F.col("source") == "best", F.lit(True))
            .when(
                (F.col("_run.clear_time_ms").isNotNull())
                & (F.col("_run.par_time_ms").isNotNull())
                & (F.col("_run.clear_time_ms") <= F.col("_run.par_time_ms")),
                F.lit(True),
            )
            .when(F.col("_run.num_keystone_upgrades") > 0, F.lit(True))
            .otherwise(F.lit(False))
            .cast(BooleanType())
            .alias("timed"),
            F.col("_ingested_at_ts").alias("snapshot_at"),
            F.col("_ingested_at"),
        )
        .filter(F.col("dungeon").isNotNull())
    )

    dedupe = Window.partitionBy(
        "player_name",
        "realm_slug",
        "region",
        "dungeon",
        "completed_at",
        "mythic_level",
    ).orderBy(
        F.when(F.col("source") == "best", F.lit(0)).otherwise(F.lit(1)),
        F.col("snapshot_at").desc(),
    )

    return (
        selected
        .withColumn("_rn", F.row_number().over(dedupe))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
