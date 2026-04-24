# Databricks notebook source
# Silver layer — cleaned and exploded raid attendance
#
# silver_raid_attendance — one row per player per raid report with presence
#                          status, class, zone context, and raid night date.

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_raid_attendance",
    comment=(
        "Exploded player attendance: one row per player per raid report. "
        "Includes zone context and raid night date joined from silver_guild_reports."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_report_code", "report_code IS NOT NULL AND LENGTH(report_code) > 0")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL AND LENGTH(player_name) > 0")
def silver_raid_attendance():
    # Join to silver_guild_reports (batch) for zone and date context.
    # Earlier attendance files don't carry zone/startTime fields, so we resolve
    # those from the guild reports dimension which reliably has them.
    reports = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_guild_reports")  # noqa: F821

    return (
        spark.readStream.table("01_bronze.warcraftlogs.bronze_raid_attendance")  # noqa: F821
        .filter(F.col("players").isNotNull())
        .select(
            F.col("code").alias("report_code"),
            F.explode("players").alias("player"),
            F.col("_ingested_at"),
        )
        .select(
            F.col("report_code"),
            F.trim(F.col("player.name")).alias("player_name"),
            F.col("player.presence").cast("integer").alias("presence"),
            F.trim(F.col("player.type")).alias("player_class"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "presence_status",
            F.when(F.col("presence") == 1, "present")
             .when(F.col("presence") == 2, "benched")
             .otherwise("absent"),
        )
        .join(
            reports.select(
                F.col("code").alias("_r_code"),
                F.col("zone_id"),
                F.col("zone_name"),
                F.col("start_time_utc").alias("report_start_utc"),
                F.to_date("start_time_utc").alias("raid_night_date"),
            ),
            F.col("report_code") == F.col("_r_code"),
            "left",
        )
        .drop("_r_code")
        .dropDuplicates(["report_code", "player_name"])
    )
