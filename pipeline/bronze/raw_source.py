# Databricks notebook source
# Bronze layer — raw ingestion via Auto Loader
#
# All six tables read JSONL files from a Unity Catalog Volume landing zone:
#   /Volumes/{catalog}/{schema}/landing/{subdir}/
#
# Explicit schemas are provided on every table so that:
#   a) tables with empty directories can initialise (no inference needed)
#   b) old JSONL files missing new fields get null-filled rather than failing
#   c) the Delta table schema is predictable regardless of schema checkpoint state
#
# DLT Docs: https://docs.databricks.com/en/delta-live-tables/index.html

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CATALOG = spark.conf.get("pipelines.catalog", "04_sdp")  # type: ignore[name-defined]  # noqa: F821
SCHEMA = spark.conf.get("pipelines.schema", "warcraftlogs")  # noqa: F821
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# ── Shared sub-schemas ─────────────────────────────────────────────────────────
# All integer fields use LongType — Spark's JSON reader always infers integers
# as Long, so schemas must match to avoid incompatible merge errors on existing
# Delta tables that were previously created via schema inference.

_ZONE_STRUCT = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

_ACTOR_STRUCT = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("subType", StringType(), True),   # class name for Players
    StructField("server", StringType(), True),
])

_FIGHT_STRUCT = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("encounterID", LongType(), True),   # 0 for trash; >0 for bosses
    StructField("kill", BooleanType(), True),
    StructField("startTime", LongType(), True),     # ms relative to report start
    StructField("endTime", LongType(), True),
    StructField("difficulty", LongType(), True),    # 3=Normal 4=Heroic 5=Mythic 10=M+
    StructField("fightPercentage", DoubleType(), True),
    StructField("bossPercentage", DoubleType(), True),
    StructField("lastPhase", LongType(), True),
    StructField("size", LongType(), True),
    StructField("friendlyPlayers", ArrayType(LongType()), True),
])

_PLAYER_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("presence", LongType(), True),   # 1=present 2=benched 3=absent
    StructField("type", StringType(), True),     # class name
])

_ENCOUNTER_STRUCT = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

_DIFFICULTY_STRUCT = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("sizes", ArrayType(LongType()), True),
])


# ── Guild Reports ──────────────────────────────────────────────────────────────

_GUILD_REPORTS_SCHEMA = StructType([
    StructField("code", StringType(), True),
    StructField("title", StringType(), True),
    StructField("startTime", LongType(), True),
    StructField("endTime", LongType(), True),
    StructField("zone", _ZONE_STRUCT, True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


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
        .schema(_GUILD_REPORTS_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/guild_reports/_schema")
        .load(f"{LANDING}/guild_reports/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Report Fights ──────────────────────────────────────────────────────────────
# Old JSONL files (fetched before the schema extension) won't have encounterID,
# size, friendlyPlayers, or zone — those fields will be null-filled.
# The silver filter (encounterID > 0) cleanly drops those incomplete rows.

_REPORT_FIGHTS_SCHEMA = StructType([
    StructField("code", StringType(), True),
    StructField("title", StringType(), True),
    StructField("startTime", LongType(), True),
    StructField("endTime", LongType(), True),
    StructField("zone", _ZONE_STRUCT, True),
    StructField("masterData", StructType([
        StructField("actors", ArrayType(_ACTOR_STRUCT), True),
    ]), True),
    StructField("fights", ArrayType(_FIGHT_STRUCT), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


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
        .schema(_REPORT_FIGHTS_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/report_fights/_schema")
        .load(f"{LANDING}/report_fights/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Raid Attendance ────────────────────────────────────────────────────────────

_RAID_ATTENDANCE_SCHEMA = StructType([
    StructField("code", StringType(), True),
    StructField("startTime", LongType(), True),
    StructField("zone", _ZONE_STRUCT, True),
    StructField("players", ArrayType(_PLAYER_STRUCT), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


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
        .schema(_RAID_ATTENDANCE_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/raid_attendance/_schema")
        .load(f"{LANDING}/raid_attendance/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Actor Roster ───────────────────────────────────────────────────────────────

_ACTOR_ROSTER_SCHEMA = StructType([
    StructField("report_code", StringType(), True),
    StructField("actors", ArrayType(_ACTOR_STRUCT), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name="bronze_actor_roster",
    comment="Raw player actor roster per report from masterData, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_actor_roster():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_ACTOR_ROSTER_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/actor_roster/_schema")
        .load(f"{LANDING}/actor_roster/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Player Details ─────────────────────────────────────────────────────────────
# player_details_json is an opaque JSON string — parsed in silver with an
# explicit StructType so the complex nested shape doesn't complicate bronze.

_PLAYER_DETAILS_SCHEMA = StructType([
    StructField("report_code", StringType(), True),
    StructField("fight_id", LongType(), True),
    StructField("boss_name", StringType(), True),
    StructField("encounter_id", LongType(), True),
    StructField("difficulty", LongType(), True),
    StructField("is_kill", BooleanType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("zone_id", LongType(), True),
    StructField("zone_name", StringType(), True),
    StructField("player_details_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name="bronze_player_details",
    comment="Raw per-player performance JSON per boss kill fight, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_fight_id", "fight_id IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_player_details():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_PLAYER_DETAILS_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/player_details/_schema")
        .load(f"{LANDING}/player_details/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Zone Catalog ───────────────────────────────────────────────────────────────

_ZONE_CATALOG_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("frozen", BooleanType(), True),
    StructField("encounters", ArrayType(_ENCOUNTER_STRUCT), True),
    StructField("difficulties", ArrayType(_DIFFICULTY_STRUCT), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name="bronze_zone_catalog",
    comment="WCL zone and encounter reference catalog, refreshed each ingestion run.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_zone_id", "id IS NOT NULL")
def bronze_zone_catalog():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_ZONE_CATALOG_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/zone_catalog/_schema")
        .load(f"{LANDING}/zone_catalog/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Guild Members ──────────────────────────────────────────────────────────────
# Ingested from Blizzard Profile API — live guild roster with rank and class.
# All integer fields use LongType to match JSON inference behaviour.

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
    name="bronze_guild_members",
    comment="Raw guild roster from Blizzard Profile API, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_guild_members():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_GUILD_MEMBERS_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/guild_members/_schema")
        .load(f"{LANDING}/guild_members/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Raider.IO Character Profiles ──────────────────────────────────────────────
# profile_json is an opaque JSON string parsed in silver. Keeping bronze narrow
# protects the pipeline from optional Raider.IO fields changing shape.

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
    name="bronze_raiderio_character_profiles",
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
        .option("cloudFiles.schemaLocation", f"{LANDING}/raiderio_character_profiles/_schema")
        .load(f"{LANDING}/raiderio_character_profiles/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Fight Rankings ─────────────────────────────────────────────────────────────
# rankings_json is an opaque JSON string from the WCL rankings scalar.
# Parsed in silver with an explicit schema.

_FIGHT_RANKINGS_SCHEMA = StructType([
    StructField("report_code", StringType(), True),
    StructField("rankings_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name="bronze_fight_rankings",
    comment="Raw WCL parse rankings per report (opaque JSON scalar), ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_rankings_data", "rankings_json IS NOT NULL")
def bronze_fight_rankings():
    # allowOverwrites=true lets the ingestion job re-land a rankings file when
    # WCL filled in previously-null `rankPercent` values. Silver dedupes per
    # (report, fight, player) keeping the row with the latest `_ingested_at`.
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_FIGHT_RANKINGS_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING}/fight_rankings/_schema")
        .load(f"{LANDING}/fight_rankings/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Fight Deaths ───────────────────────────────────────────────────────────────
# table_json is an opaque JSON string from the WCL table(dataType: Deaths) scalar.
# fight_ids records which boss fights were aggregated in this fetch.
# Parsed in silver (clean_events.py) with an explicit schema.

_FIGHT_DEATHS_SCHEMA = StructType([
    StructField("report_code", StringType(), True),
    StructField("fight_ids", ArrayType(LongType()), True),
    StructField("table_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name="bronze_fight_deaths",
    comment="Raw WCL death table per report (opaque JSON scalar), ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_deaths_data", "table_json IS NOT NULL")
def bronze_fight_deaths():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_FIGHT_DEATHS_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/fight_deaths/_schema")
        .load(f"{LANDING}/fight_deaths/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )


# ── Fight Casts ────────────────────────────────────────────────────────────────
# events_json is an opaque JSON string from the WCL events(dataType: Casts)
# paginator. Parsed in silver (clean_events.py) with an explicit schema.

_FIGHT_CASTS_SCHEMA = StructType([
    StructField("report_code", StringType(), True),
    StructField("fight_ids", ArrayType(LongType()), True),
    StructField("events_json", StringType(), True),
    StructField("buffs_json", StringType(), True),
    StructField("combatant_info_json", StringType(), True),
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


@dlt.table(
    name="bronze_fight_casts",
    comment="Raw WCL cast events for raid boss pulls, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_cast_data", "events_json IS NOT NULL")
def bronze_fight_casts():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(_FIGHT_CASTS_SCHEMA)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/fight_casts/_schema")
        .load(f"{LANDING}/fight_casts/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )
