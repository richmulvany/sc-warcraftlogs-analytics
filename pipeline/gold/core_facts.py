# Databricks notebook source
# Gold layer — core fact tables
#
# fact_player_fight_performance — one row per player per kill fight with
#                                 performance metrics and WCL parse rankings
# fact_player_events            — one row per death event per player per report
#
# These are the backbone tables that downstream gold aggregation tables build on.

import dlt
from pyspark.sql import functions as F


# ── Player Fight Performance Fact ──────────────────────────────────────────────
# Joins silver_player_performance (from player_details) to silver_fight_events
# (fight context: date, zone, boss, outcome) and silver_player_rankings (WCL
# parse percentiles).
#
# Only kill fights are included (is_kill = true) — this table is intended for
# performance assessment rather than wipe analysis (use silver_fight_events for
# the latter).
#
# throughput_per_second = total_amount / duration_seconds avoids re-aggregating
# the raw total_amount which varies with fight length.

@dlt.table(
    name="fact_player_fight_performance",
    comment=(
        "Per-player performance on every boss kill. "
        "Joins fight context, player stats, and WCL parse rankings. "
        "One row per player per kill fight."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def fact_player_fight_performance():
    perf = dlt.read("silver_player_performance")
    fights = dlt.read("silver_fight_events")
    rankings = dlt.read("silver_player_rankings")

    # Kill fights only with full context
    kill_context = (
        fights
        .filter(F.col("is_kill") == True)  # noqa: E712
        .select(
            "report_code",
            "fight_id",
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "raid_night_date",
            "is_kill",
            "duration_seconds",
        )
    )

    # Rankings keyed on (report_code, fight_id, player_name) — one row per player per fight
    rankings_slim = (
        rankings
        .select(
            F.col("report_code").alias("_r_report_code"),
            F.col("fight_id").alias("_r_fight_id"),
            F.col("player_name").alias("_r_player_name"),
            "rank_percent",
            "bracket_percent",
            "medal",
        )
    )

    # Slim perf to player-specific columns only — fight-context cols (encounter_id,
    # boss_name, zone_name, difficulty, difficulty_label) come from kill_context.
    perf_slim = perf.select(
        "report_code",
        "fight_id",
        "role",
        "player_name",
        "player_class",
        "spec",
        "avg_item_level",
        "total_amount",
        "active_time_pct",
    )

    return (
        perf_slim
        # Inner join to kill_context — drops wipe performance rows
        .join(kill_context, ["report_code", "fight_id"], "inner")
        # Left join rankings — not all kill fights have WCL rankings available
        .join(
            rankings_slim,
            (perf_slim.report_code == rankings_slim._r_report_code)
            & (perf_slim.fight_id == rankings_slim._r_fight_id)
            & (perf_slim.player_name == rankings_slim._r_player_name),
            "left",
        )
        .drop("_r_report_code", "_r_fight_id", "_r_player_name")
        .withColumn(
            "throughput_per_second",
            F.round(
                F.col("total_amount")
                / F.greatest(F.col("duration_seconds").cast("double"), F.lit(1)),
                0,
            ).cast("long"),
        )
        .select(
            "report_code",
            "fight_id",
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "raid_night_date",
            "is_kill",
            "duration_seconds",
            "player_name",
            "player_class",
            "role",
            "spec",
            "avg_item_level",
            "total_amount",
            "active_time_pct",
            "throughput_per_second",
            "rank_percent",
            "bracket_percent",
            "medal",
        )
        .orderBy("raid_night_date", "encounter_id", "role", F.col("throughput_per_second").desc())
    )


# ── Player Events Fact (Deaths) ────────────────────────────────────────────────
# Event-level table — one row per death event.
# Zone context is joined from silver_guild_reports via report_code.
#
# NOTE: fight_id is NOT available in the deaths data.  The WCL table(dataType:
# Deaths) API aggregates all deaths across the requested fight IDs into a single
# response without per-fight attribution.  Use silver_fight_events for
# fight-level analysis; use this table for player survivability patterns.

@dlt.table(
    name="fact_player_events",
    comment=(
        "Per-player death events across all raid reports. "
        "One row per death event with zone context from guild reports. "
        "NOTE: fight_id is not available — deaths are aggregated per report by the WCL table API."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def fact_player_events():
    deaths = dlt.read("silver_player_deaths")
    reports = dlt.read("silver_guild_reports")

    # Zone context from reports
    report_context = (
        reports
        .select(
            F.col("code").alias("_r_code"),
            F.col("zone_name"),
            F.col("zone_id"),
        )
    )

    return (
        deaths
        .join(
            report_context,
            deaths.report_code == report_context._r_code,
            "left",
        )
        .drop("_r_code")
        .select(
            "report_code",
            "player_name",
            "player_class",
            "death_timestamp_ms",
            "killing_blow_name",
            "killing_blow_id",
            "zone_name",
            "zone_id",
        )
        .orderBy("report_code", "player_name", "death_timestamp_ms")
    )
