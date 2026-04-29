# Databricks notebook source
# Silver layer — Blizzard character media (avatar / inset / main)
#
# silver_character_media — latest media payload per character. Parses the
# `assets` array into avatar_url / inset_url / main_url columns.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

_ASSET_STRUCT = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

_MEDIA_SCHEMA = StructType(
    [
        StructField("assets", ArrayType(_ASSET_STRUCT), True),
    ]
)


@dlt.table(
    name="02_silver.sc_analytics_blizzard.silver_character_media",
    comment="Latest Blizzard character-media URLs (avatar/inset/main) per character.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
def silver_character_media():
    raw = spark.read.table("01_bronze.blizzard.bronze_character_media")  # noqa: F821
    w = Window.partitionBy("player_name", "realm_slug").orderBy(F.col("_ingested_at").desc())
    deduped = raw.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    parsed = deduped.withColumn("media", F.from_json("media_json", _MEDIA_SCHEMA))

    def _asset(key: str):
        return F.expr(f"FILTER(media.assets, a -> a.key = '{key}')[0].value")

    return parsed.select(
        "player_name",
        "realm_slug",
        _asset("avatar").alias("avatar_url"),
        _asset("inset").alias("inset_url"),
        _asset("main").alias("main_url"),
        _asset("main-raw").alias("main_raw_url"),
        F.to_timestamp("_ingested_at").alias("snapshot_at"),
        "_ingested_at",
    )
