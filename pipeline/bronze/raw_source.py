# Databricks notebook source
# Bronze layer — raw ingestion via Auto Loader
#
# TEMPLATE USAGE:
#   - Replace `guild_reports` and `report_fights` with your own entity names
#   - Update the Volume path to match your catalog/schema
#   - Add/remove tables to match your source adapter's output
#
# DLT Docs: https://docs.databricks.com/en/delta-live-tables/index.html

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("pipelines.catalog", "main")  # type: ignore[name-defined]  # noqa: F821
SCHEMA = spark.conf.get("pipelines.schema", "pipeline_dev")  # noqa: F821
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"


# ── Guild Reports ──────────────────────────────────────────────────────────────


@dlt.table(
    name="bronze_guild_reports",
    comment="Raw guild report records from source API, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_guild_reports():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING}/guild_reports/_schema")
        .load(f"{LANDING}/guild_reports/")
        .withColumn("_file_path", F.input_file_name())
    )


# ── Report Fights ──────────────────────────────────────────────────────────────


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
        .withColumn("_file_path", F.input_file_name())
    )
