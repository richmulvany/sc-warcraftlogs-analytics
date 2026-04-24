# Databricks notebook source
# Silver layer — Blizzard character achievements
#
# silver_character_achievements — latest achievement payload per character.
# Keeps achievements_json opaque + exposes total_quantity / total_points headers
# for fast lookups in gold; downstream consumers parse the JSON for raid-specific
# achievement IDs.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType
from pyspark.sql.window import Window


_ACHIEVEMENTS_HEADER_SCHEMA = StructType([
    StructField("total_quantity", LongType(), True),
    StructField("total_points", LongType(), True),
])


@dlt.table(
    name="02_silver.sc_analytics_blizzard.silver_character_achievements",
    comment="Latest Blizzard character achievement payload per character (JSON kept opaque).",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
def silver_character_achievements():
    raw = spark.read.table("01_bronze.blizzard.bronze_character_achievements")  # noqa: F821
    w = Window.partitionBy("player_name", "realm_slug").orderBy(F.col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    parsed = deduped.withColumn("hdr", F.from_json("achievements_json", _ACHIEVEMENTS_HEADER_SCHEMA))

    return parsed.select(
        "player_name",
        "realm_slug",
        "achievements_json",
        F.col("hdr.total_quantity").alias("total_quantity"),
        F.col("hdr.total_points").alias("total_points"),
        F.to_timestamp("_ingested_at").alias("snapshot_at"),
        "_ingested_at",
    )
