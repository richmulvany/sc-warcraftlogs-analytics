# Databricks notebook source
# Silver layer — cleaned guild reports and fight events
#
# silver_guild_reports  — one row per report with parsed timestamps and zone
# silver_fight_events   — one row per boss pull, raid-only (difficulty 3/4/5),
#                         enriched with zone and difficulty label from the report

import dlt
from pyspark.sql import functions as F

RAID_DIFFICULTIES = (3, 4, 5)  # Normal=3, Heroic=4, Mythic=5  (M+=10 excluded)


# ── Cleaned Guild Reports ──────────────────────────────────────────────────────


@dlt.table(
    name="silver_guild_reports",
    comment="Cleaned guild reports: timestamps parsed to UTC, zone fields extracted.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_report_code", "code IS NOT NULL AND LENGTH(code) > 0")
@dlt.expect("valid_start_time", "start_time_utc IS NOT NULL")
def silver_guild_reports():
    return (
        dlt.read_stream("bronze_guild_reports")
        .select(
            F.col("code"),
            F.col("title"),
            (F.col("startTime") / 1000).cast("timestamp").alias("start_time_utc"),
            (F.col("endTime") / 1000).cast("timestamp").alias("end_time_utc"),
            F.col("zone.id").alias("zone_id"),
            F.col("zone.name").alias("zone_name"),
            F.col("_ingested_at"),
            F.col("_source"),
        )
        .dropDuplicates(["code"])
    )


# ── Cleaned Fight Events ───────────────────────────────────────────────────────
# Reads from bronze_report_fights (streaming) and joins to silver_guild_reports
# (batch dimension) to carry zone and raid-night date through to every fight row.
# Only raid boss pulls are retained (difficulty IN (3,4,5) AND encounterID > 0).


@dlt.table(
    name="silver_fight_events",
    comment=(
        "Normalised boss pull events — raid encounters only (Normal/Heroic/Mythic). "
        "One row per pull with duration, outcome, zone, and difficulty label."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_fight_id", "fight_id IS NOT NULL")
@dlt.expect_or_drop("valid_report_code", "report_code IS NOT NULL")
@dlt.expect_or_drop("is_raid_encounter", "encounter_id IS NOT NULL AND encounter_id > 0")
@dlt.expect_or_drop("is_raid_difficulty", "difficulty IN (3, 4, 5)")
def silver_fight_events():
    reports = dlt.read("silver_guild_reports")  # batch dimension join

    return (
        dlt.read_stream("bronze_report_fights")
        .select(
            F.col("code").alias("report_code"),
            F.col("title").alias("report_title"),
            F.explode("fights").alias("fight"),
            F.col("_ingested_at"),
        )
        .select(
            F.col("report_code"),
            F.col("report_title"),
            F.col("fight.id").alias("fight_id"),
            F.col("fight.name").alias("boss_name"),
            F.col("fight.encounterID").alias("encounter_id"),
            F.col("fight.kill").cast("boolean").alias("is_kill"),
            F.col("fight.difficulty").alias("difficulty"),
            F.col("fight.startTime").alias("fight_start_ms"),
            ((F.col("fight.endTime") - F.col("fight.startTime")) / 1000)
            .cast("integer")
            .alias("duration_seconds"),
            F.col("fight.bossPercentage").alias("boss_percentage"),
            F.col("fight.lastPhase").alias("last_phase"),
            F.col("fight.size").alias("fight_size"),
            F.col("fight.friendlyPlayers").alias("friendly_player_ids"),
            F.col("_ingested_at"),
        )
        # ── Raid-only filter ──────────────────────────────────────────────
        # NULL-safe: old JSONL files (pre schema extension) have null encounterID;
        # those rows must be explicitly excluded rather than relying on null > 0
        # evaluating to false (which is not guaranteed across all Spark contexts).
        .filter(F.col("encounter_id").isNotNull() & (F.col("encounter_id") > 0))
        .filter(F.col("difficulty").isNotNull() & F.col("difficulty").isin(list(RAID_DIFFICULTIES)))
        # ── Derived columns ───────────────────────────────────────────────
        .withColumn(
            "difficulty_label",
            F.when(F.col("difficulty") == 3, "Normal")
             .when(F.col("difficulty") == 4, "Heroic")
             .when(F.col("difficulty") == 5, "Mythic")
             .otherwise("Unknown"),
        )
        .withColumn("outcome", F.when(F.col("is_kill"), "kill").otherwise("wipe"))
        # ── Join zone + date from silver_guild_reports (batch dimension) ──
        # Zone is reliably present there; the fight files from before the
        # schema extension don't carry zone at the report level.
        .join(
            reports.select(
                F.col("code").alias("_r_code"),
                F.col("zone_id"),
                F.col("zone_name"),
                F.col("start_time_utc"),
                F.to_date("start_time_utc").alias("raid_night_date"),
            ),
            F.col("report_code") == F.col("_r_code"),
            "left",
        )
        .drop("_r_code")
    )
