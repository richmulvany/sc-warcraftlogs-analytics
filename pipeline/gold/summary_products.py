# Databricks notebook source
# Gold layer — business-ready data products consumed by the frontend
#
# TEMPLATE USAGE:
#   - These are your "data products" — think of each table as an API endpoint
#   - Optimise for read performance (ZORDER, partitioning)
#   - Each gold table should map to a clear business question

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Boss Progression Summary ───────────────────────────────────────────────────
# "Which bosses have we killed and how many attempts did it take?"


@dlt.table(
    name="gold_boss_progression",
    comment="Per-boss kill/wipe summary across all raids.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "boss_name",
    },
)
def gold_boss_progression():
    fights = dlt.read("silver_fight_events")
    return (
        fights.groupBy("boss_name", "difficulty")
        .agg(
            F.count("*").alias("total_pulls"),
            F.sum(F.col("is_kill").cast("integer")).alias("total_kills"),
            F.sum((~F.col("is_kill")).cast("integer")).alias("total_wipes"),
            F.min(F.when(F.col("is_kill"), F.col("duration_seconds"))).alias("best_kill_seconds"),
            F.avg("duration_seconds").alias("avg_pull_duration_seconds"),
            F.max(F.when(F.col("is_kill"), F.lit(True))).alias("is_killed"),
        )
        .withColumn(
            "wipe_to_kill_ratio",
            F.round(F.col("total_wipes") / F.greatest(F.col("total_kills"), F.lit(1)), 2),
        )
    )


# ── Raid Summary ───────────────────────────────────────────────────────────────
# "How did each raid go at a high level?"


@dlt.table(
    name="gold_raid_summary",
    comment="One row per raid report with aggregate kill/wipe/duration stats.",
    table_properties={"quality": "gold"},
)
def gold_raid_summary():
    reports = dlt.read("silver_guild_reports")
    fights = dlt.read("silver_fight_events")

    fight_stats = fights.groupBy("report_code").agg(
        F.count("*").alias("total_pulls"),
        F.sum(F.col("is_kill").cast("integer")).alias("boss_kills"),
        F.sum((~F.col("is_kill")).cast("integer")).alias("total_wipes"),
        F.sum("duration_seconds").alias("total_time_seconds"),
        F.countDistinct("boss_name").alias("unique_bosses_engaged"),
    )

    return (
        reports.join(fight_stats, reports.code == fight_stats.report_code, "left")
        .select(
            reports.code.alias("report_code"),
            reports.title.alias("report_title"),
            reports.zone_name,
            reports.start_time_utc,
            fight_stats.total_pulls,
            fight_stats.boss_kills,
            fight_stats.total_wipes,
            fight_stats.total_time_seconds,
            fight_stats.unique_bosses_engaged,
        )
        .orderBy(F.col("start_time_utc").desc())
    )


# ── Rolling Kill Progression ───────────────────────────────────────────────────
# "How has our progression developed week over week?"


@dlt.table(
    name="gold_progression_timeline",
    comment="Cumulative boss kills over time for progression chart.",
    table_properties={"quality": "gold"},
)
def gold_progression_timeline():
    fights = dlt.read("silver_fight_events")
    reports = dlt.read("silver_guild_reports")

    kills = (
        fights.filter(F.col("is_kill"))
        .join(reports, fights.report_code == reports.code)
        .select(
            "boss_name",
            "difficulty",
            "start_time_utc",
        )
        .dropDuplicates(["boss_name", "difficulty"])  # First kill only
        .orderBy("start_time_utc")
    )

    window = Window.orderBy("start_time_utc").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    return kills.withColumn("cumulative_kills", F.count("boss_name").over(window))
