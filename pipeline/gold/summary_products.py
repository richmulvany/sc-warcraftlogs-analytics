# Databricks notebook source
# Gold layer — raid and boss analytics data products
#
# gold_boss_progression     — kill/wipe counts per encounter across all raids
# gold_raid_summary         — one row per raid night with aggregate stats
# gold_progression_timeline — cumulative first-kills over time
# gold_best_kills           — fastest kill per encounter per difficulty
# gold_boss_wipe_analysis   — phase/pct breakdown of wipes per boss
# gold_encounter_catalog    — encounter + zone reference (from silver_zone_catalog)

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Boss Progression ───────────────────────────────────────────────────────────
# "Which bosses have we killed and how many attempts did it take?"
# Grouped by encounter_id (stable across patches) + difficulty.
# zone_name and boss_name carried through for display.

@dlt.table(
    name="gold_boss_progression",
    comment="Per-encounter kill/wipe summary across all raids, grouped by encounter and difficulty.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_boss_progression():
    fights = dlt.read("silver_fight_events")
    return (
        fights
        .groupBy("encounter_id", "boss_name", "zone_id", "zone_name", "difficulty", "difficulty_label")
        .agg(
            F.count("*").alias("total_pulls"),
            F.sum(F.col("is_kill").cast("integer")).alias("total_kills"),
            F.sum((~F.col("is_kill")).cast("integer")).alias("total_wipes"),
            F.min(F.when(F.col("is_kill"), F.col("duration_seconds"))).alias("best_kill_seconds"),
            F.avg("duration_seconds").alias("avg_pull_duration_seconds"),
            F.max(F.when(F.col("is_kill"), F.lit(True))).alias("is_killed"),
            F.min(F.when(F.col("is_kill"), F.col("raid_night_date"))).alias("first_kill_date"),
            F.max(F.col("raid_night_date")).alias("last_attempt_date"),
        )
        .withColumn(
            "wipe_to_kill_ratio",
            F.round(F.col("total_wipes") / F.greatest(F.col("total_kills"), F.lit(1)), 2),
        )
        .orderBy("zone_name", "difficulty", "first_kill_date")
    )


# ── Raid Summary ───────────────────────────────────────────────────────────────
# "How did each raid night go?"

@dlt.table(
    name="gold_raid_summary",
    comment="One row per raid night with aggregate boss kill, wipe, and time stats.",
    table_properties={"quality": "gold"},
)
def gold_raid_summary():
    reports = dlt.read("silver_guild_reports")
    fights = dlt.read("silver_fight_events")

    fight_stats = fights.groupBy("report_code").agg(
        F.count("*").alias("total_pulls"),
        F.sum(F.col("is_kill").cast("integer")).alias("boss_kills"),
        F.sum((~F.col("is_kill")).cast("integer")).alias("total_wipes"),
        F.sum("duration_seconds").alias("total_fight_seconds"),
        F.countDistinct("encounter_id").alias("unique_bosses_engaged"),
        F.countDistinct(
            F.when(F.col("is_kill"), F.col("encounter_id"))
        ).alias("unique_bosses_killed"),
        F.first("zone_name").alias("zone_name"),
        F.first("zone_id").alias("zone_id"),
        F.first("difficulty_label").alias("primary_difficulty"),
        F.first("raid_night_date").alias("raid_night_date"),
    )

    return (
        reports
        .join(fight_stats, reports.code == fight_stats.report_code, "left")
        .select(
            reports.code.alias("report_code"),
            reports.title.alias("report_title"),
            reports.start_time_utc,
            reports.end_time_utc,
            fight_stats.zone_id,
            fight_stats.zone_name,
            fight_stats.raid_night_date,
            fight_stats.primary_difficulty,
            fight_stats.total_pulls,
            fight_stats.boss_kills,
            fight_stats.total_wipes,
            fight_stats.total_fight_seconds,
            fight_stats.unique_bosses_engaged,
            fight_stats.unique_bosses_killed,
        )
        .orderBy(F.col("start_time_utc").desc())
    )


# ── Progression Timeline ───────────────────────────────────────────────────────
# "How has our progression developed over time?"

@dlt.table(
    name="gold_progression_timeline",
    comment="Cumulative boss first-kills over time, per difficulty.",
    table_properties={"quality": "gold"},
)
def gold_progression_timeline():
    fights = dlt.read("silver_fight_events")

    first_kills = (
        fights
        .filter(F.col("is_kill"))
        .select("encounter_id", "boss_name", "zone_name", "difficulty", "difficulty_label", "raid_night_date")
        .dropDuplicates(["encounter_id", "difficulty"])  # one row = first ever kill
        .orderBy("raid_night_date")
    )

    window = Window.partitionBy("difficulty").orderBy("raid_night_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    return first_kills.withColumn("cumulative_kills", F.count("encounter_id").over(window))


# ── Best Kill Times ────────────────────────────────────────────────────────────
# "What is our fastest recorded kill for each boss?"

@dlt.table(
    name="gold_best_kills",
    comment="Fastest kill duration per encounter per difficulty, with date context.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_best_kills():
    fights = dlt.read("silver_fight_events")

    return (
        fights
        .filter(F.col("is_kill"))
        .groupBy("encounter_id", "boss_name", "zone_name", "difficulty", "difficulty_label")
        .agg(
            F.min("duration_seconds").alias("best_kill_seconds"),
            F.avg("duration_seconds").alias("avg_kill_seconds"),
            F.count("*").alias("total_kills"),
            F.min("raid_night_date").alias("first_kill_date"),
            F.max("raid_night_date").alias("latest_kill_date"),
        )
        .withColumn("best_kill_mm_ss",
            F.concat(
                F.floor(F.col("best_kill_seconds") / 60).cast("string"),
                F.lit("m "),
                F.lpad((F.col("best_kill_seconds") % 60).cast("string"), 2, "0"),
                F.lit("s"),
            )
        )
        .orderBy("zone_name", "difficulty", "boss_name")
    )


# ── Boss Wipe Analysis ────────────────────────────────────────────────────────
# "On which bosses are we wiping most, at what phase, and are we improving?"
# The most actionable table for a raid leader — shows where the guild is
# struggling and whether each raid night is making progress.

@dlt.table(
    name="gold_boss_wipe_analysis",
    comment=(
        "Per-boss wipe breakdown: phase distribution, average wipe %, "
        "and progression trend across raid nights."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_boss_wipe_analysis():
    fights = dlt.read("silver_fight_events")
    wipes = fights.filter(~F.col("is_kill"))

    return (
        wipes
        .groupBy("encounter_id", "boss_name", "zone_name", "difficulty", "difficulty_label")
        .agg(
            F.count("*").alias("total_wipes"),
            # Best wipe = lowest boss HP% reached on a wipe (closest to kill)
            F.min("boss_percentage").alias("best_wipe_pct"),
            F.avg("boss_percentage").alias("avg_wipe_pct"),
            # Phase breakdown — proportion of wipes ending in each phase
            F.avg("last_phase").alias("avg_last_phase"),
            F.max("last_phase").alias("max_phase_reached"),
            # Duration stats — shorter avg wipe = dying earlier = further from kill
            F.avg("duration_seconds").alias("avg_wipe_duration_seconds"),
            F.max("duration_seconds").alias("longest_wipe_seconds"),
            # Date range
            F.min("raid_night_date").alias("first_wipe_date"),
            F.max("raid_night_date").alias("latest_wipe_date"),
            F.countDistinct("raid_night_date").alias("raid_nights_attempted"),
        )
        .withColumn(
            "avg_wipe_pct_rounded",
            F.round("avg_wipe_pct", 1),
        )
        .orderBy("zone_name", "difficulty", F.col("best_wipe_pct").asc())
    )


# ── Boss Progress History ─────────────────────────────────────────────────────
# "How has the guild's best pull on each boss changed over time?"
# One row per report / boss / difficulty, keeping the best boss HP remaining
# reached on that night (or 0 if the boss died in that report).

@dlt.table(
    name="gold_boss_progress_history",
    comment=(
        "Per-report boss progression history with best boss HP remaining over time. "
        "Used for encounter detail charts and progression log tables."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id,report_code",
    },
)
def gold_boss_progress_history():
    fights = dlt.read("silver_fight_events")
    reports = dlt.read("silver_guild_reports")

    report_context = (
        reports
        .select(
            F.col("code").alias("_report_code"),
            F.col("title").alias("report_title"),
            F.col("start_time_utc"),
            F.col("end_time_utc"),
        )
    )

    return (
        fights
        .groupBy(
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "raid_night_date",
            "report_code",
        )
        .agg(
            F.count("*").alias("pulls_on_night"),
            F.sum(F.col("is_kill").cast("integer")).alias("kills_on_night"),
            F.sum((~F.col("is_kill")).cast("integer")).alias("wipes_on_night"),
            F.min(F.when(~F.col("is_kill"), F.col("boss_percentage"))).alias("best_wipe_pct_on_night"),
            F.avg(F.when(~F.col("is_kill"), F.col("boss_percentage"))).alias("avg_wipe_pct_on_night"),
            F.max(F.when(F.col("is_kill"), F.lit(True)).otherwise(F.lit(False))).alias("is_kill_on_night"),
            F.max("duration_seconds").alias("longest_pull_seconds"),
            F.max(F.when(F.col("is_kill"), F.col("duration_seconds"))).alias("kill_duration_seconds"),
        )
        .withColumn(
            "best_boss_hp_remaining",
            F.when(F.col("is_kill_on_night"), F.lit(0.0))
            .otherwise(F.round(F.col("best_wipe_pct_on_night"), 2)),
        )
        .withColumn(
            "avg_wipe_pct_on_night",
            F.round(F.col("avg_wipe_pct_on_night"), 2),
        )
        .join(report_context, F.col("report_code") == F.col("_report_code"), "left")
        .drop("_report_code")
        .select(
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "raid_night_date",
            "report_code",
            "report_title",
            "start_time_utc",
            "end_time_utc",
            "pulls_on_night",
            "kills_on_night",
            "wipes_on_night",
            "best_wipe_pct_on_night",
            "avg_wipe_pct_on_night",
            "best_boss_hp_remaining",
            "is_kill_on_night",
            "kill_duration_seconds",
            "longest_pull_seconds",
        )
        .orderBy("raid_night_date", "report_code")
    )


@dlt.table(
    name="gold_boss_pull_history",
    comment=(
        "Per-pull boss progression history with boss HP remaining for every tracked attempt. "
        "Used for pull-by-pull progression charts."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id,report_code",
    },
)
def gold_boss_pull_history():
    fights = dlt.read("silver_fight_events")
    reports = dlt.read("silver_guild_reports")

    report_context = (
        reports
        .select(
            F.col("code").alias("_report_code"),
            F.col("title").alias("report_title"),
            F.col("start_time_utc"),
            F.col("end_time_utc"),
        )
    )

    return (
        fights
        .withColumn(
            "boss_hp_remaining",
            F.when(F.col("is_kill"), F.lit(0.0)).otherwise(F.col("boss_percentage").cast("double")),
        )
        .join(report_context, F.col("report_code") == F.col("_report_code"), "left")
        .drop("_report_code")
        .select(
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "raid_night_date",
            "report_code",
            "report_title",
            "start_time_utc",
            "end_time_utc",
            "fight_id",
            "is_kill",
            "boss_percentage",
            "boss_hp_remaining",
            "duration_seconds",
            "last_phase",
        )
        .orderBy("raid_night_date", "start_time_utc", "report_code", "fight_id")
    )


# ── Encounter Catalog ──────────────────────────────────────────────────────────
# "What encounters and zones exist?" (reference table for frontend filters)

@dlt.table(
    name="gold_encounter_catalog",
    comment="Zone and encounter reference catalog for frontend dropdowns and boss ID resolution.",
    table_properties={"quality": "gold"},
)
def gold_encounter_catalog():
    return (
        dlt.read("silver_zone_catalog")
        .filter(~F.col("zone_frozen"))  # active raid tiers only
        .select(
            "zone_id",
            "zone_name",
            "encounter_id",
            "encounter_name",
            "difficulty_names",
        )
        .orderBy("zone_name", "encounter_name")
    )
