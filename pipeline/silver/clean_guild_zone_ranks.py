# Databricks notebook source
# Silver layer — WCL guild zone ranks
#
# silver_guild_zone_ranks — latest guildData.zoneRanking.progress payload per zone.
# Bronze stores progress_json as an opaque scalar; silver dedupes to most recent
# row per zone but keeps the JSON intact for gold consumers.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_guild_zone_ranks",
    comment=(
        "Latest WCL guildData.zoneRanking.progress payload per zone. "
        "JSON kept opaque; gold tables parse the structures they need."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_zone_id", "zone_id IS NOT NULL")
@dlt.expect_or_drop("valid_progress", "progress_json IS NOT NULL")
def silver_guild_zone_ranks():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_guild_zone_ranks")  # noqa: F821
    w = Window.partitionBy("zone_id").orderBy(F.col("_ingested_at").desc())
    return (
        raw.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "zone_id",
            "zone_name",
            "progress_json",
            F.to_timestamp("_ingested_at").alias("snapshot_at"),
            "_ingested_at",
        )
    )
