# Databricks notebook source
# Gold layer — survivability and boss mechanics data products
#
# gold_player_survivability — per-player death statistics and most common killing blows
# gold_boss_mechanics       — enhanced wipe analysis with phase breakdown, duration
#                             buckets, and weekly progress trend

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Player Survivability ───────────────────────────────────────────────────────
# Per-player death statistics derived from fact_player_events (death events) and
# fact_player_fight_performance (kill counts for the deaths_per_kill metric).
#
# NOTE: deaths_per_kill is an approximation because deaths are aggregated at
# report level (not per-fight) by the WCL table API.  A player who died on a
# wipe in the same report will inflate this metric relative to kills.

@dlt.table(
    name="gold_player_survivability",
    comment=(
        "Per-player death statistics across all raids. "
        "deaths_per_kill is an approximation — see note in table comment. "
        "Ordered by deaths_per_kill descending (most deaths first)."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_survivability():
    deaths = dlt.read("fact_player_events")
    perf = dlt.read("fact_player_fight_performance")

    # Total deaths per player
    death_counts = (
        deaths
        .filter(F.col("player_name").isNotNull())
        .groupBy("player_name", "player_class")
        .agg(
            F.count("*").alias("total_deaths"),
            F.max("zone_name").alias("last_zone"),  # most recent zone (last ingested)
            F.collect_set("zone_name").alias("zones_died_in"),
        )
    )

    # Most common killing blow per player (mode)
    w_blow = Window.partitionBy("player_name").orderBy(F.col("blow_count").desc())
    killing_blow_counts = (
        deaths
        .filter(F.col("killing_blow_name").isNotNull())
        .groupBy("player_name", "killing_blow_name")
        .agg(F.count("*").alias("blow_count"))
    )
    top_killing_blow = (
        killing_blow_counts
        .withColumn("_rn", F.row_number().over(w_blow))
        .filter(F.col("_rn") == 1)
        .select(
            "player_name",
            F.col("killing_blow_name").alias("most_common_killing_blow"),
            F.col("blow_count").alias("most_common_killing_blow_count"),
        )
    )

    # Last death date (max zone-level date proxy using report join in fact_player_events)
    last_death = (
        deaths
        .groupBy("player_name")
        .agg(F.max("death_timestamp_ms").alias("last_death_timestamp_ms"))
    )

    # Kill count per player from performance facts
    kill_counts = (
        perf
        .groupBy("player_name")
        .agg(F.count("*").alias("kills_tracked"))
    )

    return (
        death_counts
        .join(top_killing_blow, "player_name", "left")
        .join(last_death, "player_name", "left")
        .join(kill_counts, "player_name", "left")
        .withColumn(
            "deaths_per_kill",
            F.round(
                F.col("total_deaths")
                / F.greatest(F.coalesce(F.col("kills_tracked"), F.lit(0)).cast("double"), F.lit(1)),
                2,
            ),
        )
        .select(
            "player_name",
            "player_class",
            "total_deaths",
            F.coalesce(F.col("kills_tracked"), F.lit(0)).alias("kills_tracked"),
            "deaths_per_kill",
            "most_common_killing_blow",
            "most_common_killing_blow_count",
            "zones_died_in",
            "last_death_timestamp_ms",
        )
        .orderBy(F.col("deaths_per_kill").desc(), F.col("total_deaths").desc())
    )


# ── Boss Mechanics ─────────────────────────────────────────────────────────────
# Enhanced wipe analysis beyond gold_boss_wipe_analysis.
# Analyses wipe patterns to surface progress and help teams understand where
# they are dying in each encounter.
#
# Metrics:
#   - Phase breakdown: % of wipes ending in each phase bucket
#   - Duration buckets: % of wipes by duration range
#   - Weekly pull counts: pulls per boss per ISO week
#   - progress_trend: last week's avg boss_percentage vs overall avg
#     (positive = improving, negative = regressing)

@dlt.table(
    name="gold_boss_mechanics",
    comment=(
        "Enhanced wipe analysis per boss encounter. "
        "Phase breakdown, duration buckets, weekly pull counts, and progress trend."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_boss_mechanics():
    fights = dlt.read("silver_fight_events")

    # Boss wipes only (raid difficulties, valid encounter, not a kill)
    wipes = (
        fights
        .filter(F.col("is_kill") == False)  # noqa: E712
        .filter(F.col("encounter_id").isNotNull() & (F.col("encounter_id") > 0))
        .filter(F.col("difficulty").isin(3, 4, 5))
    )

    # ISO week for weekly trend calculation
    wipes_with_week = (
        wipes
        .withColumn("iso_week", F.date_trunc("week", F.col("raid_night_date")))
    )

    # Phase breakdown — classify wipes by last_phase into buckets
    wipes_with_phase = (
        wipes_with_week
        .withColumn(
            "phase_bucket",
            F.when(F.col("last_phase") <= 1, "Phase 1")
             .when(F.col("last_phase") == 2, "Phase 2")
             .when(F.col("last_phase") >= 3, "Phase 3+")
             .otherwise("Unknown"),
        )
    )

    # Duration buckets
    wipes_with_duration_bucket = (
        wipes_with_phase
        .withColumn(
            "duration_bucket",
            F.when(F.col("duration_seconds") < 60, "< 1 min")
             .when(F.col("duration_seconds") < 180, "1-3 min")
             .when(F.col("duration_seconds") < 300, "3-5 min")
             .otherwise("5+ min"),
        )
    )

    # Overall aggregation per encounter + difficulty
    overall = (
        wipes_with_duration_bucket
        .groupBy("encounter_id", "boss_name", "zone_name", "difficulty", "difficulty_label")
        .agg(
            F.count("*").alias("total_wipes"),
            F.avg("boss_percentage").alias("avg_boss_pct"),
            # Phase breakdown counts
            F.sum(F.when(F.col("phase_bucket") == "Phase 1", 1).otherwise(0)).alias("wipes_phase_1"),
            F.sum(F.when(F.col("phase_bucket") == "Phase 2", 1).otherwise(0)).alias("wipes_phase_2"),
            F.sum(F.when(F.col("phase_bucket") == "Phase 3+", 1).otherwise(0)).alias("wipes_phase_3_plus"),
            # Duration breakdown counts
            F.sum(F.when(F.col("duration_bucket") == "< 1 min", 1).otherwise(0)).alias("wipes_lt_1min"),
            F.sum(F.when(F.col("duration_bucket") == "1-3 min", 1).otherwise(0)).alias("wipes_1_3min"),
            F.sum(F.when(F.col("duration_bucket") == "3-5 min", 1).otherwise(0)).alias("wipes_3_5min"),
            F.sum(F.when(F.col("duration_bucket") == "5+ min", 1).otherwise(0)).alias("wipes_5plus_min"),
        )
        .withColumn(
            "pct_wipes_phase_1",
            F.round(F.col("wipes_phase_1") / F.greatest(F.col("total_wipes"), F.lit(1)) * 100, 1),
        )
        .withColumn(
            "pct_wipes_phase_2",
            F.round(F.col("wipes_phase_2") / F.greatest(F.col("total_wipes"), F.lit(1)) * 100, 1),
        )
        .withColumn(
            "pct_wipes_phase_3_plus",
            F.round(F.col("wipes_phase_3_plus") / F.greatest(F.col("total_wipes"), F.lit(1)) * 100, 1),
        )
    )

    # Weekly pull counts per boss — window function for trend
    weekly_pulls = (
        wipes_with_week
        .groupBy("encounter_id", "difficulty", "iso_week")
        .agg(F.count("*").alias("weekly_pulls"))
    )

    # Last week vs overall boss_percentage trend
    # Identify "last week" as the most recent ISO week with any wipes for each boss
    w_week = Window.partitionBy("encounter_id", "difficulty").orderBy(F.col("iso_week").desc())

    last_week_wipes = (
        wipes_with_week
        .withColumn("_week_rn", F.dense_rank().over(w_week))
        .filter(F.col("_week_rn") == 1)
        .groupBy("encounter_id", "difficulty")
        .agg(F.avg("boss_percentage").alias("last_week_avg_boss_pct"))
    )

    # progress_trend = last_week_avg - overall_avg (positive means boss % going up = improving)
    return (
        overall
        .join(last_week_wipes, ["encounter_id", "difficulty"], "left")
        .withColumn(
            "progress_trend",
            F.round(
                F.col("last_week_avg_boss_pct") - F.col("avg_boss_pct"),
                2,
            ),
        )
        .select(
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "total_wipes",
            F.round("avg_boss_pct", 1).alias("avg_boss_pct"),
            "pct_wipes_phase_1",
            "pct_wipes_phase_2",
            "pct_wipes_phase_3_plus",
            "wipes_lt_1min",
            "wipes_1_3min",
            "wipes_3_5min",
            "wipes_5plus_min",
            F.round("last_week_avg_boss_pct", 1).alias("last_week_avg_boss_pct"),
            "progress_trend",
        )
        .orderBy("zone_name", "difficulty", "encounter_id")
    )
