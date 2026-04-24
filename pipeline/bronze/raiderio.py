# Databricks notebook source
# Bronze layer — Raider.IO
#
# Publishes to 01_bronze.raiderio via fully-qualified `name=` overrides.
# Pre-create: CREATE SCHEMA IF NOT EXISTS 01_bronze.raiderio;
#
# Reads from the source-matched bronze landing volume at
#   /Volumes/01_bronze/raiderio/landing/raiderio_character_profiles/

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

LANDING = "/Volumes/01_bronze/raiderio/landing"
TARGET_SCHEMA = "01_bronze.raiderio"


_RAIDERIO_CHARACTER_PROFILE_SCHEMA = StructType([
    StructField("player_name", StringType(), True),
    StructField("realm_slug", StringType(), True),
    StructField("region", StringType(), True),
    StructField("profile_url", StringType(), True),
    StructField("profile_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_raiderio_character_profiles",
    comment="Raw Raider.IO character profile payloads for Mythic+ analysis.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_player_name", "player_name IS NOT NULL")
@dlt.expect("has_profile_json", "profile_json IS NOT NULL")
def bronze_raiderio_character_profiles():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_RAIDERIO_CHARACTER_PROFILE_SCHEMA)
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            f"{LANDING}/raiderio_character_profiles/_schema",
        )
        .load(f"{LANDING}/raiderio_character_profiles/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )
