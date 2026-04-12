# Databricks notebook source
# Bronze layer — raw ingestion via Auto Loader
#
# Three tables land data from the WarcraftLogs ingestion job, which writes JSONL
# files into a Unity Catalog Volume at /Volumes/{catalog}/{schema}/landing/.
#
# DLT Docs: https://docs.databricks.com/en/delta-live-tables/index.html

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("pipelines.catalog", "04_sdp")  # type: ignore[name-defined]  # noqa: F821
SCHEMA = spark.conf.get("pipelines.schema", "warcraftlogs")  # noqa: F821
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"


# ── Guild Reports ──────────────────────────────────────────────────────────────
# One JSONL file per ingestion run, one record per report.
# Fields: code, title, startTime (ms epoch), endTime (ms epoch), zone {id, name}


@dlt.table(
    name="bronze_guild_reports",
    comment="Raw guild report records from WarcraftLogs API, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_guild_reports():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING}/guild_reports/_schema")
        .load(f"{LANDING}/guild_reports/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Report Fights ──────────────────────────────────────────────────────────────
# One JSONL file per report code (named {report_code}.jsonl), one record per file.
# Fields: code, title, startTime, endTime, fights[] (boss encounters only)


@dlt.table(
    name="bronze_report_fights",
    comment="Raw fight-level data per raid report, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_report_fights():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING}/report_fights/_schema")
        .load(f"{LANDING}/report_fights/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Raid Attendance ────────────────────────────────────────────────────────────
# One JSONL file per ingestion run, one record per report.
# Fields: code (report code), players[] {name, presence, type (class)}


@dlt.table(
    name="bronze_raid_attendance",
    comment="Raw raid attendance records per report, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_raid_attendance():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING}/raid_attendance/_schema")
        .load(f"{LANDING}/raid_attendance/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )
