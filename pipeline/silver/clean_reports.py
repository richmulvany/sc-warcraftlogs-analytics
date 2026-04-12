# Databricks notebook source
# Silver layer — cleaned and normalised entities
#
# TEMPLATE USAGE:
#   - Replace transformations with logic appropriate for your data
#   - Maintain the pattern: read from bronze, clean, write to silver
#   - Keep DLT expectations (EXPECT clauses) for all critical business rules

import dlt
from pyspark.sql import functions as F

# ── Cleaned Guild Reports ──────────────────────────────────────────────────────


@dlt.table(
    name="silver_guild_reports",
    comment="Cleaned guild report records: timestamps parsed, nulls handled.",
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
            # Convert millisecond epoch to timestamp
            (F.col("startTime") / 1000).cast("timestamp").alias("start_time_utc"),
            F.col("zone.name").alias("zone_name"),
            F.col("_ingested_at"),
            F.col("_source"),
        )
        .dropDuplicates(["code"])
    )


# ── Cleaned Fight Events ───────────────────────────────────────────────────────


@dlt.table(
    name="silver_fight_events",
    comment="Normalised fight-level data with duration and outcome columns.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_fight_id", "fight_id IS NOT NULL")
@dlt.expect("valid_report_code", "report_code IS NOT NULL")
def silver_fight_events():
    return (
        dlt.read_stream("bronze_report_fights")
        # Explode the nested fights array from the report JSON
        .select(
            F.col("code").alias("report_code"),
            F.col("title").alias("report_title"),
            F.explode("fights").alias("fight"),
        )
        .select(
            F.col("report_code"),
            F.col("report_title"),
            F.col("fight.id").alias("fight_id"),
            F.col("fight.name").alias("boss_name"),
            F.col("fight.kill").cast("boolean").alias("is_kill"),
            F.col("fight.difficulty").alias("difficulty"),
            # Duration in seconds
            ((F.col("fight.endTime") - F.col("fight.startTime")) / 1000)
            .cast("integer")
            .alias("duration_seconds"),
            F.col("fight.bossPercentage").alias("boss_percentage"),
            F.col("fight.fightPercentage").alias("fight_percentage"),
        )
        .withColumn(
            "outcome",
            F.when(F.col("is_kill"), "kill").otherwise("wipe"),
        )
    )
