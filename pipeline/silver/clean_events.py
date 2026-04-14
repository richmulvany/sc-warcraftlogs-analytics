# Databricks notebook source
# Silver layer — parsed WCL fight death events
#
# silver_player_deaths — one row per death event per player per report.
#
# Actual WCL table(dataType: Deaths) response structure:
# {
#   "data": {
#     "entries": [
#       {
#         "name": "PlayerName",
#         "id": 7,
#         "type": "DeathKnight",     <- WoW class
#         "icon": "DeathKnight-Blood",
#         "timestamp": 1073684,      <- death timestamp (ms from report start)
#         "fight": 4,                <- fight ID (available per death!)
#         "overkill": 0,
#         "events": [                <- damage window leading to death, newest first
#           {
#             "timestamp": 1073346,
#             "type": "damage",
#             "sourceIsFriendly": false,
#             "ability": {"name": "Caustic Phlegm", "guid": 1246653},
#             ...
#           },
#           ...
#         ]
#       }
#     ]
#   }
# }
#
# Key findings vs original assumptions:
#   - Each entry is ONE death event (not a player summary with a deathEvents array)
#   - fight_id IS available on each entry via the "fight" field
#   - events[] are sorted newest-first; events[0] from a non-friendly source = killing blow
#   - No "killingBlow" top-level field — derived from events[0]

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schema for the table(dataType: Deaths) JSON scalar ────────────────────────

_ABILITY_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("guid", LongType(),   True),
])

_EVENT_STRUCT = StructType([
    StructField("timestamp",        LongType(),    True),
    StructField("type",             StringType(),  True),  # "damage", "heal", etc.
    StructField("sourceIsFriendly", BooleanType(), True),
    StructField("ability",          _ABILITY_STRUCT, True),
])

_ENTRY_STRUCT = StructType([
    StructField("name",       StringType(),           True),  # player name
    StructField("id",         LongType(),             True),  # actor ID
    StructField("type",       StringType(),           True),  # WoW class
    StructField("icon",       StringType(),           True),  # "ClassName-Spec"
    StructField("timestamp",  LongType(),             True),  # ms from report start
    StructField("fight",      LongType(),             True),  # fight ID
    StructField("overkill",   LongType(),             True),
    StructField("events",     ArrayType(_EVENT_STRUCT), True),
])

_TABLE_DATA_STRUCT = StructType([
    StructField("entries", ArrayType(_ENTRY_STRUCT), True),
])

_TABLE_SCHEMA = StructType([
    StructField("data", _TABLE_DATA_STRUCT, True),
])


# ── Parsed Player Death Events ─────────────────────────────────────────────────

@dlt.table(
    name="silver_player_deaths",
    comment=(
        "WCL death events per player per report. "
        "One row per death event with fight_id and killing blow. "
        "fight_id is available — each entry in the Deaths table corresponds "
        "to a single death in a specific fight."
    ),
    table_properties={"quality": "silver"},
)
def silver_player_deaths():
    raw = dlt.read("bronze_fight_deaths")  # batch — stable once ingested

    return (
        raw
        .withColumn("parsed", F.from_json(F.col("table_json"), _TABLE_SCHEMA))
        # Drop rows where JSON parsing failed
        .filter(F.col("parsed").isNotNull())
        # Explode entries: one row per death event
        .withColumn("entry", F.explode("parsed.data.entries"))
        .select(
            F.col("report_code"),
            F.col("entry.fight").alias("fight_id"),
            F.col("entry.name").alias("player_name"),
            F.col("entry.type").alias("player_class"),
            F.col("entry.timestamp").alias("death_timestamp_ms"),
            F.col("entry.overkill").alias("overkill"),
            # Killing blow: filter events to non-friendly damage sources (newest first),
            # take element [0] — the hit closest to the death timestamp.
            # FILTER() is a Spark SQL higher-order function available in Spark 3.x+.
            F.expr(
                "FILTER(entry.events, e -> e.type = 'damage' AND e.sourceIsFriendly = false)[0]"
            ).alias("kb_event"),
        )
        .withColumn("killing_blow_name", F.col("kb_event.ability.name"))
        .withColumn("killing_blow_id",   F.col("kb_event.ability.guid"))
        .drop("kb_event")
        .filter(F.col("player_name").isNotNull())
    )
