# Databricks notebook source
# Silver layer — cleaned and exploded raid attendance
#
# Reads from bronze_raid_attendance (one record per report with a nested
# players array) and explodes it into one row per player per raid.

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_raid_attendance",
    comment="Exploded player attendance: one row per player per raid report.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_report_code", "report_code IS NOT NULL AND LENGTH(report_code) > 0")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL AND LENGTH(player_name) > 0")
def silver_raid_attendance():
    return (
        dlt.read_stream("bronze_raid_attendance")
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
        # A player can appear multiple times if the same report is ingested again;
        # keep only the first occurrence per (report, player) pair.
        .dropDuplicates(["report_code", "player_name"])
    )
