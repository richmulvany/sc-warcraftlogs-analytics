# Databricks notebook source
# Gold layer — player-focused data products
#
# gold_player_attendance        — per-player attendance rates across all raids
# gold_weekly_activity          — raid frequency and boss kills by ISO week
# gold_roster                   — active player roster with class and realm
# gold_player_performance_summary — aggregated throughput per player across kills
# gold_boss_kill_roster         — per-player stats on every boss kill
# gold_player_boss_performance  — per-player per-boss performance aggregated across kills

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Player Attendance Summary ──────────────────────────────────────────────────
# "Who is turning up to raids and how often?"

@dlt.table(
    name="gold_player_attendance",
    comment="Per-player attendance rates and raid counts, enriched with zone and date context.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_attendance():
    attendance = dlt.read("silver_raid_attendance")
    return (
        attendance
        .groupBy("player_name", "player_class")
        .agg(
            F.count("*").alias("total_raids_tracked"),
            F.sum(F.when(F.col("presence") == 1, 1).otherwise(0)).alias("raids_present"),
            F.sum(F.when(F.col("presence") == 2, 1).otherwise(0)).alias("raids_benched"),
            F.sum(F.when(F.col("presence") == 3, 1).otherwise(0)).alias("raids_absent"),
            F.max("raid_night_date").alias("last_raid_date"),
            F.min("raid_night_date").alias("first_raid_date"),
            F.collect_set("zone_name").alias("zones_attended"),
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
# "How many raids did we run each week and how much progress did we make?"

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
        F.collect_set("zone_name").alias("zones_raided"),
    )

    return (
        reports
        .join(fight_stats, reports.code == fight_stats.report_code, "left")
        .select(
            F.date_trunc("week", F.col("start_time_utc")).alias("week_start"),
            F.col("code"),
            F.coalesce(F.col("boss_kills"), F.lit(0)).alias("boss_kills"),
            F.coalesce(F.col("total_wipes"), F.lit(0)).alias("total_wipes"),
            F.coalesce(F.col("total_pulls"), F.lit(0)).alias("total_pulls"),
            F.coalesce(F.col("total_time_seconds"), F.lit(0)).alias("total_time_seconds"),
            F.col("zones_raided"),
        )
        .groupBy("week_start")
        .agg(
            F.count("code").alias("raid_nights"),
            F.sum("boss_kills").alias("total_boss_kills"),
            F.sum("total_wipes").alias("total_wipes"),
            F.sum("total_pulls").alias("total_pulls"),
            F.sum("total_time_seconds").alias("total_raid_seconds"),
            F.flatten(F.collect_list("zones_raided")).alias("zones_raided_flat"),
        )
        .withColumn("zones_raided", F.array_distinct(F.col("zones_raided_flat")))
        .drop("zones_raided_flat")
        .orderBy("week_start")
    )


# ── Guild Roster ───────────────────────────────────────────────────────────────
# "Who is in the guild? What class/realm is each player?"

@dlt.table(
    name="gold_roster",
    comment=(
        "Active guild roster derived from masterData actor logs and attendance records. "
        "One row per player with latest-seen class, realm, and attendance summary."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_roster():
    actors = dlt.read("silver_actor_roster")
    attendance = dlt.read("silver_raid_attendance")

    # Most-recent class/realm snapshot per player from actor logs
    w = Window.partitionBy("player_name").orderBy(F.col("_ingested_at").desc())
    latest_actor = (
        actors
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("player_name", "player_class", "realm")
    )

    att_summary = (
        attendance
        .groupBy("player_name")
        .agg(
            F.count("*").alias("total_raids_tracked"),
            F.sum(F.when(F.col("presence") == 1, 1).otherwise(0)).alias("raids_present"),
            F.max("raid_night_date").alias("last_seen"),
            F.min("raid_night_date").alias("first_seen"),
        )
        .withColumn(
            "attendance_rate_pct",
            F.round(
                F.col("raids_present") / F.greatest(F.col("total_raids_tracked"), F.lit(1)) * 100, 1
            ),
        )
    )

    return (
        latest_actor
        .join(att_summary, "player_name", "left")
        .orderBy(F.col("last_seen").desc(), F.col("player_name"))
    )


# ── Player Performance Summary ─────────────────────────────────────────────────
# "How is each player performing on kill fights overall?"
# Reads from fact_player_fight_performance which already contains throughput
# (from WCL rankings.amount) and fight context.

@dlt.table(
    name="gold_player_performance_summary",
    comment=(
        "Aggregated per-player performance across all boss kills: avg/best DPS/HPS, "
        "spec, item level, kill count, and WCL parse percentile. "
        "throughput_per_second = WCL rankings.amount (DPS/HPS, null when no ranking)."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_performance_summary():
    perf = dlt.read("fact_player_fight_performance")
    actors = dlt.read("silver_actor_roster")

    # Most-recent realm per player
    w = Window.partitionBy("player_name").orderBy(F.col("_ingested_at").desc())
    realm_lookup = (
        actors
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("player_name", "realm")
    )

    # Most common spec per player-role
    spec_counts = (
        perf
        .filter(F.col("spec").isNotNull())
        .groupBy("player_name", "role", "spec")
        .agg(F.count("*").alias("spec_count"))
    )
    w2 = Window.partitionBy("player_name", "role").orderBy(F.col("spec_count").desc())
    primary_specs = (
        spec_counts
        .withColumn("_rn", F.row_number().over(w2))
        .filter(F.col("_rn") == 1)
        .select("player_name", "role", F.col("spec").alias("primary_spec"))
    )

    agg = (
        perf
        .groupBy("player_name", "player_class", "role")
        .agg(
            F.count("*").alias("kills_tracked"),
            # throughput_per_second = WCL DPS/HPS; null when no ranking available
            F.avg("throughput_per_second").cast("long").alias("avg_throughput_per_second"),
            F.max("throughput_per_second").cast("long").alias("best_throughput_per_second"),
            F.avg("rank_percent").alias("avg_rank_percent"),
            F.max("rank_percent").alias("best_rank_percent"),
            F.avg("avg_item_level").alias("avg_item_level"),
            F.max("raid_night_date").alias("last_seen_date"),
        )
    )

    return (
        agg
        .join(primary_specs, ["player_name", "role"], "left")
        .join(realm_lookup, "player_name", "left")
        .select(
            "player_name",
            "player_class",
            "realm",
            "role",
            "primary_spec",
            "kills_tracked",
            "avg_throughput_per_second",
            "best_throughput_per_second",
            F.round("avg_rank_percent", 1).alias("avg_rank_percent"),
            F.round("best_rank_percent", 1).alias("best_rank_percent"),
            F.round("avg_item_level", 1).alias("avg_item_level"),
            "last_seen_date",
        )
        .orderBy("role", F.col("avg_throughput_per_second").desc_nulls_last())
    )


# ── Boss Kill Roster ───────────────────────────────────────────────────────────
# "Who was present on each boss kill and how did they perform?"
# Reads directly from fact_player_fight_performance — fight context already joined.

@dlt.table(
    name="gold_boss_kill_roster",
    comment=(
        "Per-player performance on every boss kill. "
        "One row per player per kill. Queryable by boss, player, or date. "
        "throughput_per_second from WCL rankings (null when ranking unavailable)."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_boss_kill_roster():
    return (
        dlt.read("fact_player_fight_performance")
        .select(
            "report_code",
            "fight_id",
            "boss_name",
            "encounter_id",
            "difficulty",
            "difficulty_label",
            "zone_name",
            "raid_night_date",
            "duration_seconds",
            "player_name",
            "player_class",
            "role",
            "spec",
            "avg_item_level",
            "potion_use",
            "healthstone_use",
            "throughput_per_second",
            "rank_percent",
            "bracket_percent",
            "rank_string",
        )
        .orderBy("raid_night_date", "boss_name", "role", F.col("throughput_per_second").desc_nulls_last())
    )


# ── Player Boss Performance ────────────────────────────────────────────────────
# "How does each player perform on a specific boss, and are they improving?"

@dlt.table(
    name="gold_player_boss_performance",
    comment=(
        "Per-player per-boss kill performance aggregated across all kills. "
        "One row per (player, encounter, difficulty) with trend indicators. "
        "throughput_per_second from WCL rankings (null when ranking unavailable)."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_player_boss_performance():
    perf = dlt.read("fact_player_fight_performance")

    # Aggregate across all kills of the same boss per player
    agg = (
        perf
        .groupBy(
            "player_name", "player_class", "role",
            "encounter_id", "boss_name", "zone_name", "difficulty", "difficulty_label",
        )
        .agg(
            F.count("*").alias("kills_on_boss"),
            F.avg("throughput_per_second").cast("long").alias("avg_throughput_per_second"),
            F.max("throughput_per_second").cast("long").alias("best_throughput_per_second"),
            F.avg("rank_percent").alias("avg_rank_percent"),
            F.max("rank_percent").alias("best_rank_percent"),
            F.avg("avg_item_level").alias("avg_item_level"),
            F.min("raid_night_date").alias("first_kill_date"),
            F.max("raid_night_date").alias("latest_kill_date"),
            # Most played spec on this boss
            F.first("spec", ignorenulls=True).alias("primary_spec"),
        )
    )

    # Latest-kill throughput using a window so we can get the ordered last value.
    w_latest = Window.partitionBy(
        "player_name", "encounter_id", "difficulty"
    ).orderBy("raid_night_date")

    latest_kill = (
        perf
        .withColumn(
            "_latest_throughput",
            F.last("throughput_per_second", ignorenulls=True).over(
                w_latest.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        )
        .groupBy("player_name", "encounter_id", "difficulty")
        .agg(F.first("_latest_throughput").cast("long").alias("latest_throughput_per_second"))
    )

    return (
        agg
        .join(latest_kill, ["player_name", "encounter_id", "difficulty"], "left")
        .withColumn(
            "throughput_trend",
            F.when(
                F.col("kills_on_boss") > 1,
                F.round(
                    (F.col("latest_throughput_per_second") - F.col("avg_throughput_per_second"))
                    / F.greatest(F.col("avg_throughput_per_second").cast("double"), F.lit(1)) * 100,
                    1,
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
        .select(
            "player_name",
            "player_class",
            "role",
            "primary_spec",
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "kills_on_boss",
            "avg_throughput_per_second",
            "best_throughput_per_second",
            "latest_throughput_per_second",
            "throughput_trend",
            F.round("avg_rank_percent", 1).alias("avg_rank_percent"),
            F.round("best_rank_percent", 1).alias("best_rank_percent"),
            F.round("avg_item_level", 1).alias("avg_item_level"),
            "first_kill_date",
            "latest_kill_date",
        )
        .orderBy("encounter_id", "difficulty", "role", F.col("avg_throughput_per_second").desc_nulls_last())
    )
