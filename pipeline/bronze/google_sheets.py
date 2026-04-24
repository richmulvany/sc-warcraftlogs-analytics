# Databricks notebook source
# Bronze layer — Google Sheets
#
# Publishes to 01_bronze.google_sheets via fully-qualified `name=` overrides.
# Pre-create: CREATE SCHEMA IF NOT EXISTS 01_bronze.google_sheets;
#
# Reads from the source-matched bronze landing volume at
#   /Volumes/01_bronze/google_sheets/landing/live_raid_roster/

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

LANDING = "/Volumes/01_bronze/google_sheets/landing"
TARGET_SCHEMA = "01_bronze.google_sheets"


_LIVE_RAID_ROSTER_SCHEMA = StructType([
    StructField("sheet_id", StringType(), True),
    StructField("gid", StringType(), True),
    StructField("csv_text", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_live_raid_roster",
    comment="Raw Google Sheets CSV export for the live raid roster.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_csv_text", "csv_text IS NOT NULL")
def bronze_live_raid_roster():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_LIVE_RAID_ROSTER_SCHEMA)
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            f"{LANDING}/live_raid_roster/_schema",
        )
        .load(f"{LANDING}/live_raid_roster/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )
