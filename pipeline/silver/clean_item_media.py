# Databricks notebook source
# Silver layer — Blizzard item media
#
# silver_item_media — latest icon URL per item id, parsed from the assets array
# in the raw Blizzard /data/wow/media/item/{id} payload.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


_ASSET_STRUCT = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
])

_MEDIA_SCHEMA = StructType([
    StructField("assets", ArrayType(_ASSET_STRUCT), True),
])


@dlt.table(
    name="02_silver.sc_analytics_blizzard.silver_item_media",
    comment="Latest icon URL per Blizzard item id (extracted from the media payload).",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_item_id", "item_id IS NOT NULL")
def silver_item_media():
    raw = spark.read.table("01_bronze.blizzard.bronze_item_media")  # noqa: F821
    w = Window.partitionBy("item_id").orderBy(F.col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    parsed = deduped.withColumn("media", F.from_json("media_json", _MEDIA_SCHEMA))

    return parsed.select(
        "item_id",
        F.expr("FILTER(media.assets, a -> a.key = 'icon')[0].value").alias("icon_url"),
        F.to_timestamp("_ingested_at").alias("snapshot_at"),
        "_ingested_at",
    )
