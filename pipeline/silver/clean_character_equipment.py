# Databricks notebook source
# Silver layer — Blizzard character equipment
#
# silver_character_equipment — latest equipped items per character with average
# item level computed from equipped_items[].level.value.  JSON kept as opaque
# field for callers that need full item detail.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

_LEVEL_STRUCT = StructType(
    [
        StructField("value", LongType(), True),
    ]
)

_ITEM_QUALITY_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

_ITEM_NAME_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
    ]
)

_SLOT_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

_EQUIPPED_ITEM_STRUCT = StructType(
    [
        StructField("slot", _SLOT_STRUCT, True),
        StructField("item", _ITEM_NAME_STRUCT, True),
        StructField("name", StringType(), True),
        StructField("level", _LEVEL_STRUCT, True),
        StructField("quality", _ITEM_QUALITY_STRUCT, True),
    ]
)

_EQUIPMENT_SCHEMA = StructType(
    [
        StructField("equipped_items", ArrayType(_EQUIPPED_ITEM_STRUCT), True),
    ]
)


@dlt.table(
    name="02_silver.sc_analytics_blizzard.silver_character_equipment",
    comment="Latest Blizzard character equipment payload per character with avg item level.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
def silver_character_equipment():
    raw = spark.read.table("01_bronze.blizzard.bronze_character_equipment")  # noqa: F821
    w = Window.partitionBy("player_name", "realm_slug").orderBy(F.col("_ingested_at").desc())
    deduped = raw.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    parsed = deduped.withColumn("eq", F.from_json("equipment_json", _EQUIPMENT_SCHEMA))

    return parsed.select(
        "player_name",
        "realm_slug",
        F.col("equipment_json"),
        F.expr(
            "round(aggregate(eq.equipped_items, 0L, (acc, x) -> acc + coalesce(x.level.value, 0L)) "
            "/ greatest(size(eq.equipped_items), 1), 1)"
        ).alias("avg_item_level"),
        F.size("eq.equipped_items").alias("equipped_item_count"),
        F.to_timestamp("_ingested_at").alias("snapshot_at"),
        "_ingested_at",
    )
