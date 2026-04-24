# Databricks notebook source
# Bronze layer — Blizzard Profile API
#
# Publishes to 01_bronze.blizzard via fully-qualified `name=` overrides.
# Pre-create: CREATE SCHEMA IF NOT EXISTS 01_bronze.blizzard;
#
# Reads from the source-matched bronze landing volume at
#   /Volumes/01_bronze/blizzard/landing/<subdir>/

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

LANDING = "/Volumes/01_bronze/blizzard/landing"
TARGET_SCHEMA = "01_bronze.blizzard"


def _autoload(subdir: str, schema: StructType):
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(schema)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/{subdir}/_schema")
        .load(f"{LANDING}/{subdir}/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Guild Members ──────────────────────────────────────────────────────────────

_GUILD_MEMBERS_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("realm_slug", StringType(), True),
    StructField("rank", LongType(), True),
    StructField("class_id", LongType(), True),
    StructField("class_name", StringType(), True),
    StructField("level", LongType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_guild_members",
    comment="Raw guild roster from Blizzard Profile API.",
    table_properties={"quality": "bronze"},
)
def bronze_guild_members():
    return _autoload("guild_members", _GUILD_MEMBERS_SCHEMA)


# ── Character Media (NEW — migrated from export script) ───────────────────────

_CHARACTER_MEDIA_SCHEMA = StructType([
    StructField("player_name", StringType(), True),
    StructField("realm_slug", StringType(), True),
    StructField("media_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_character_media",
    comment="Raw Blizzard character-media payloads (avatar/inset/main artwork).",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_player_name", "player_name IS NOT NULL")
def bronze_character_media():
    return _autoload("character_media", _CHARACTER_MEDIA_SCHEMA)


# ── Character Equipment (NEW) ─────────────────────────────────────────────────

_CHARACTER_EQUIPMENT_SCHEMA = StructType([
    StructField("player_name", StringType(), True),
    StructField("realm_slug", StringType(), True),
    StructField("equipment_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_character_equipment",
    comment="Raw Blizzard character equipment payloads.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_player_name", "player_name IS NOT NULL")
def bronze_character_equipment():
    return _autoload("character_equipment", _CHARACTER_EQUIPMENT_SCHEMA)


# ── Character Achievements (NEW) ──────────────────────────────────────────────

_CHARACTER_ACHIEVEMENTS_SCHEMA = StructType([
    StructField("player_name", StringType(), True),
    StructField("realm_slug", StringType(), True),
    StructField("achievements_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_character_achievements",
    comment="Raw Blizzard character achievement payloads.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_player_name", "player_name IS NOT NULL")
def bronze_character_achievements():
    return _autoload("character_achievements", _CHARACTER_ACHIEVEMENTS_SCHEMA)


# ── Item Media (NEW) ──────────────────────────────────────────────────────────

_ITEM_MEDIA_SCHEMA = StructType([
    StructField("item_id", LongType(), True),
    StructField("media_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_item_media",
    comment="Raw Blizzard item media payloads (icon URL per item id).",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_item_id", "item_id IS NOT NULL")
def bronze_item_media():
    return _autoload("item_media", _ITEM_MEDIA_SCHEMA)
