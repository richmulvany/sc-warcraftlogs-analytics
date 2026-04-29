# Databricks notebook source
# Gold layer monitoring — WCL parse completeness trend

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="03_gold.sc_analytics.gold_parse_completeness_daily",
    comment=(
        "Daily monitoring table for WCL parse completeness by raid night and role. "
        "Tracks null rank percentages from silver_player_rankings."
    ),
    table_properties={"quality": "gold"},
)
def gold_parse_completeness_daily():
    rankings = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_player_rankings")  # noqa: F821
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821

    rankings_with_date = rankings.join(
        fights.select("report_code", "fight_id", "raid_night_date"),
        on=["report_code", "fight_id"],
        how="inner",
    )

    return (
        rankings_with_date.groupBy("raid_night_date", "role")
        .agg(
            F.count("*").alias("player_rows"),
            F.sum(F.when(F.col("rank_percent").isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
                "null_rank_rows"
            ),
        )
        .withColumn(
            "null_rank_pct",
            F.when(
                F.col("player_rows") > 0, F.col("null_rank_rows") / F.col("player_rows")
            ).otherwise(F.lit(0.0)),
        )
        .orderBy(F.col("raid_night_date").desc(), F.col("role"))
    )
