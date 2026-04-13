# Databricks notebook source
# Silver layer — cleaned guild member roster from Blizzard API
#
# silver_guild_members — deduplicated, enriched guild roster with rank labels
#                        and raid-team flags.  Slowly-changing dimension: read
#                        batch (dlt.read) so gold tables always see the latest
#                        snapshot.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Cleaned Guild Members ──────────────────────────────────────────────────────
# Deduplicates on player name keeping the most recent record (by _ingested_at).
# Adds human-readable rank labels and raid-team classification.


@dlt.table(
    name="silver_guild_members",
    comment=(
        "Deduplicated guild roster from Blizzard API. "
        "One row per character name with rank label and raid-team flag."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_rank", "rank IS NOT NULL")
def silver_guild_members():
    # Read as batch — this is a slowly-changing dimension; gold tables always
    # want the latest full snapshot rather than appended history.
    raw = dlt.read("bronze_guild_members")

    # Keep the most recent record per character name when multiple ingestion
    # runs are present in the bronze table.
    w = Window.partitionBy("name").orderBy(F.col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_file_path")
    )

    return (
        deduped
        .withColumn(
            "rank_label",
            F.when(F.col("rank") == 0, "Guild Master")
             .when(F.col("rank") == 1, "GM Alt")
             .when(F.col("rank") == 2, "Officer")
             .when(F.col("rank") == 3, "Officer Alt")
             .when(F.col("rank") == 4, "Officer Alt")
             .when(F.col("rank") == 5, "Raider")
             .when(F.col("rank") == 6, "Raider Alt")
             .when(F.col("rank") == 7, "Bestie")
             .when(F.col("rank") == 8, "Trial")
             .when(F.col("rank") == 9, "Social")
             .otherwise(F.concat(F.lit("Rank "), F.col("rank").cast("string"))),
        )
        .withColumn(
            "rank_category",
            F.when(F.col("rank").isin(0, 1), "GM")
             .when(F.col("rank").isin(2, 3, 4), "Officer")
             .when(F.col("rank") == 5, "Raider")
             .when(F.col("rank") == 6, "Raider Alt")
             .when(F.col("rank") == 7, "Bestie")
             .when(F.col("rank") == 8, "Trial")
             .otherwise("Social"),
        )
        .withColumn(
            "is_raid_team",
            F.col("rank").isin(0, 1, 2, 3, 4, 5, 8),
        )
        # Derive class name from Blizzard class_id — the Profile API does not
        # return a human-readable class name string, only the numeric class_id.
        .withColumn(
            "class_name",
            F.when(F.col("class_id") == 1,  "Warrior")
             .when(F.col("class_id") == 2,  "Paladin")
             .when(F.col("class_id") == 3,  "Hunter")
             .when(F.col("class_id") == 4,  "Rogue")
             .when(F.col("class_id") == 5,  "Priest")
             .when(F.col("class_id") == 6,  "Death Knight")
             .when(F.col("class_id") == 7,  "Shaman")
             .when(F.col("class_id") == 8,  "Mage")
             .when(F.col("class_id") == 9,  "Warlock")
             .when(F.col("class_id") == 10, "Monk")
             .when(F.col("class_id") == 11, "Druid")
             .when(F.col("class_id") == 12, "Demon Hunter")
             .when(F.col("class_id") == 13, "Evoker")
             .otherwise(F.lit(None)),
        )
        .select(
            "name",
            "realm_slug",
            "rank",
            "rank_label",
            "rank_category",
            "is_raid_team",
            "class_id",
            "class_name",
            "level",
            "_ingested_at",
        )
    )
