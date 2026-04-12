# Databricks notebook source
# Gold layer — player-focused data products
#
# gold_player_attendance  — per-player attendance stats across all raids
# gold_weekly_activity    — raid frequency and boss kill counts by ISO week

import dlt
from pyspark.sql import functions as F

# ── Player Attendance Summary ──────────────────────────────────────────────────
# "Who is turning up to raids and how often?"


@dlt.table(
    name="gold_player_attendance",
    comment="Per-player attendance rates and raid counts across all ingested reports.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_attendance():
    attendance = dlt.read("silver_raid_attendance")
    reports = dlt.read("silver_guild_reports")

    return (
        attendance.join(reports, attendance.report_code == reports.code, "left")
        .groupBy("player_name", "player_class")
        .agg(
            F.count("*").alias("total_raids_tracked"),
            F.sum(F.when(F.col("presence") == 1, 1).otherwise(0)).alias("raids_present"),
            F.sum(F.when(F.col("presence") == 2, 1).otherwise(0)).alias("raids_benched"),
            F.sum(F.when(F.col("presence") == 3, 1).otherwise(0)).alias("raids_absent"),
            F.max("start_time_utc").alias("last_raid_date"),
            F.min("start_time_utc").alias("first_raid_date"),
        )
        .withColumn(
            "attendance_rate_pct",
            F.round(
                F.col("raids_present") / F.greatest(F.col("total_raids_tracked"), F.lit(1)) * 100,
                1,
            ),
        )
        .orderBy(F.col("attendance_rate_pct").desc(), F.col("player_name"))
    )


# ── Weekly Raid Activity ───────────────────────────────────────────────────────
# "How many raids did we run each week, and how much progress did we make?"


@dlt.table(
    name="gold_weekly_activity",
    comment="Raid count and boss kill totals grouped by ISO week.",
    table_properties={"quality": "gold"},
)
def gold_weekly_activity():
    reports = dlt.read("silver_guild_reports")
    fights = dlt.read("silver_fight_events")

    fight_stats = fights.groupBy("report_code").agg(
        F.sum(F.col("is_kill").cast("integer")).alias("boss_kills"),
        F.sum((~F.col("is_kill")).cast("integer")).alias("total_wipes"),
        F.count("*").alias("total_pulls"),
        F.sum("duration_seconds").alias("total_time_seconds"),
    )

    return (
        reports.join(fight_stats, reports.code == fight_stats.report_code, "left")
        .select(
            F.date_trunc("week", F.col("start_time_utc")).alias("week_start"),
            F.col("code"),
            F.col("zone_name"),
            F.coalesce(F.col("boss_kills"), F.lit(0)).alias("boss_kills"),
            F.coalesce(F.col("total_wipes"), F.lit(0)).alias("total_wipes"),
            F.coalesce(F.col("total_pulls"), F.lit(0)).alias("total_pulls"),
            F.coalesce(F.col("total_time_seconds"), F.lit(0)).alias("total_time_seconds"),
        )
        .groupBy("week_start")
        .agg(
            F.count("code").alias("raid_nights"),
            F.sum("boss_kills").alias("total_boss_kills"),
            F.sum("total_wipes").alias("total_wipes"),
            F.sum("total_pulls").alias("total_pulls"),
            F.sum("total_time_seconds").alias("total_raid_seconds"),
            F.collect_set("zone_name").alias("zones_raided"),
        )
        .orderBy("week_start")
    )
