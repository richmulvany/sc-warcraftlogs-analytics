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
import re
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
    name="02_silver.sc_analytics_warcraftlogs.silver_player_deaths",
    comment=(
        "WCL death events per player per report. "
        "One row per death event with fight_id and killing blow. "
        "fight_id is available — each entry in the Deaths table corresponds "
        "to a single death in a specific fight."
    ),
    table_properties={"quality": "silver"},
)
def silver_player_deaths():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_deaths")  # noqa: F821

    # Legacy bronze death files stored many fights in one record; WCL truncates
    # those responses on long reports. Once a report has any single-fight death
    # records, prefer those and ignore the older multi-fight payloads.
    report_modes = (
        raw
        .groupBy("report_code")
        .agg(
            F.max(
                F.when(F.size(F.coalesce(F.col("fight_ids"), F.array().cast("array<long>"))) == 1, F.lit(1))
                .otherwise(F.lit(0))
            ).alias("has_single_fight_records")
        )
    )

    filtered_raw = (
        raw
        .join(report_modes, on="report_code", how="left")
        .filter(
            (F.col("has_single_fight_records") == 0)
            | (F.size(F.coalesce(F.col("fight_ids"), F.array().cast("array<long>"))) == 1)
        )
        .drop("has_single_fight_records")
    )

    return (
        filtered_raw
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


# ── Schema for events(dataType: Casts) JSON scalar ────────────────────────────

_CAST_ABILITY_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("guid", LongType(), True),
    StructField("type", LongType(), True),
    StructField("abilityIcon", StringType(), True),
])

_CAST_EVENT_STRUCT = StructType([
    StructField("timestamp", LongType(), True),
    StructField("type", StringType(), True),
    StructField("sourceID", LongType(), True),
    StructField("targetID", LongType(), True),
    StructField("fight", LongType(), True),
    StructField("abilityGameID", LongType(), True),
    StructField("ability", _CAST_ABILITY_STRUCT, True),
])

_CAST_EVENTS_SCHEMA = StructType([
    StructField("data", ArrayType(_CAST_EVENT_STRUCT), True),
])

_COMBATANT_INFO_AURA_STRUCT = StructType([
    StructField("abilityGameId", LongType(), True),
    StructField("icon", StringType(), True),
    StructField("name", StringType(), True),
    StructField("stacks", LongType(), True),
])

_COMBATANT_SOURCE_STRUCT = StructType([
    StructField("id", LongType(), True),
])

_COMBATANT_INFO_EVENT_STRUCT = StructType([
    StructField("timestamp", LongType(), True),
    StructField("type", StringType(), True),
    StructField("sourceID", LongType(), True),
    StructField("source", _COMBATANT_SOURCE_STRUCT, True),
    StructField("fight", LongType(), True),
    StructField("specID", LongType(), True),
    StructField("auras", ArrayType(_COMBATANT_INFO_AURA_STRUCT), True),
])

_COMBATANT_INFO_EVENTS_SCHEMA = StructType([
    StructField("data", ArrayType(_COMBATANT_INFO_EVENT_STRUCT), True),
])

MIDNIGHT_FLASK_OR_PHIAL_NAMES = {
    "flask of thalassian resistance",
    "flask of the blood knights",
    "flask of the magisters",
    "flask of the shattered sun",
    "vicious thalassian flask of honor",
    "haranir phial of perception",
    "haranir phial of ingenuity",
    "haranir phial of finesse",
}
MIDNIGHT_WEAPON_ENHANCEMENT_NAMES = {
    "thalassian phoenix oil",
    "smuggler's enchanted edge",
    "oil of dawn",
    "refulgent weightstone",
    "refulgent whetstone",
    "refulgent razorstone",
    "laced zoomshots",
    "weighted boomshots",
    "smuggler's lynxeye",
    "farstrider's hawkeye",
    "flametongue weapon",
    "windfury weapon",
    "earthliving weapon",
}
MIDNIGHT_COMBAT_POTION_NAMES = {
    "light's potential",
    "potion of recklessness",
    "potion of zealotry",
    "draught of rampant abandon",
}
MIDNIGHT_FOOD_NAMES = {
    "silvermoon parade",
    "harandar celebration",
    "quel'dorei medley",
    "blooming feast",
    "royal roast",
    "impossibly royal roast",
    "flora frenzy",
    "champion's bento",
    "warped wise wings",
    "void-kissed fish rolls",
    "sun-seared lumifin",
    "null and void plate",
    "glitter skewers",
    "fel-kissed filet",
    "buttered root crab",
    "arcano cutlets",
    "tasty smoked tetra",
    "crimson calamari",
    "braised blood hunter",
    "sunwell delight",
    "hearthflame supper",
    "fried bloomtail",
    "felberry figs",
    "eversong pudding",
    "bloodthistle-wrapped cutlets",
    "wise tails",
    "twilight angler's medley",
    "spellfire filet",
    "spiced biscuits",
    "silvermoon standard",
    "quick sandwich",
    "portable snack",
    "mana-infused stew",
    "forager's medley",
    "farstrider rations",
    "bloom skewers",
}
_SPACE_RE = re.compile(r"\s+")


def _normalize_name(value: str | None) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", value.strip().lower())


def _unique_preserve(values):
    seen = set()
    ordered = []
    for value in values:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def _classify(names, matcher):
    if not names:
        return []
    matched = []
    for name in names:
        trimmed = (name or "").strip()
        if trimmed and matcher(trimmed):
            matched.append(trimmed)
    return _unique_preserve(matched)


def classify_food_names(names):
    def matcher(name: str) -> bool:
        normalized = _normalize_name(name)
        return normalized in MIDNIGHT_FOOD_NAMES or "well fed" in normalized or "feast" in normalized
    return _classify(names, matcher)


def classify_flask_or_phial_names(names):
    def matcher(name: str) -> bool:
        normalized = _normalize_name(name)
        return normalized in MIDNIGHT_FLASK_OR_PHIAL_NAMES or "flask" in normalized or "phial" in normalized
    return _classify(names, matcher)


def classify_weapon_enhancement_names(names):
    def matcher(name: str) -> bool:
        normalized = _normalize_name(name)
        return normalized in MIDNIGHT_WEAPON_ENHANCEMENT_NAMES or any(
            keyword in normalized
            for keyword in (
                " oil",
                "oil ",
                "whetstone",
                "weightstone",
                "razorstone",
                "shots",
                "enchanted edge",
                "lynxeye",
                "hawkeye",
            )
        )
    return _classify(names, matcher)


def classify_combat_potion_names(names):
    def matcher(name: str) -> bool:
        normalized = _normalize_name(name)
        return normalized in MIDNIGHT_COMBAT_POTION_NAMES
    return _classify(names, matcher)


def join_consumable_names(names):
    cleaned = _unique_preserve(
        (name or "").strip()
        for name in (names or [])
        if (name or "").strip()
    )
    return " | ".join(cleaned) if cleaned else None


_classify_food_udf = F.udf(classify_food_names, ArrayType(StringType()))
_classify_flask_udf = F.udf(classify_flask_or_phial_names, ArrayType(StringType()))
_classify_weapon_udf = F.udf(classify_weapon_enhancement_names, ArrayType(StringType()))
_join_consumable_names_udf = F.udf(join_consumable_names, StringType())


# ── Parsed Player Cast Events ─────────────────────────────────────────────────

@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_player_cast_events",
    comment=(
        "WCL player cast events for raid boss pulls. "
        "Used for health potion, healthstone, and defensive usage analysis."
    ),
    table_properties={"quality": "silver"},
)
def silver_player_cast_events():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_casts")  # noqa: F821
    actors = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_actor_roster")  # noqa: F821
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821

    parsed = (
        raw
        .withColumn("parsed", F.from_json(F.col("events_json"), _CAST_EVENTS_SCHEMA))
        .filter(F.col("parsed").isNotNull())
        .withColumn("event", F.explode("parsed.data"))
        .select(
            F.col("report_code"),
            F.col("event.fight").alias("event_fight_id"),
            F.col("event.timestamp").alias("cast_timestamp_ms"),
            F.col("event.sourceID").alias("actor_id"),
            F.col("event.targetID").alias("target_actor_id"),
            F.col("event.ability.name").alias("ability_name"),
            F.coalesce(F.col("event.ability.guid"), F.col("event.abilityGameID")).alias("ability_id"),
            F.col("event.ability.abilityIcon").alias("ability_icon"),
            F.col("_ingested_at"),
        )
        .filter(F.col("actor_id").isNotNull())
        .filter(F.col("ability_name").isNotNull() | F.col("ability_id").isNotNull())
    )

    actor_lookup = actors.select(
        F.col("report_code").alias("_a_report_code"),
        F.col("actor_id").alias("_a_actor_id"),
        "player_name",
        "player_class",
    )

    fight_lookup = fights.select(
        F.col("report_code").alias("_f_report_code"),
        F.col("fight_id").alias("_f_fight_id"),
        F.col("fight_start_ms").alias("_f_start_ms"),
        (F.col("fight_start_ms") + (F.col("duration_seconds") * 1000)).alias("_f_end_ms"),
    )

    with_fight = (
        parsed
        .join(
            fight_lookup,
            (parsed.report_code == fight_lookup._f_report_code)
            & (
                (parsed.event_fight_id == fight_lookup._f_fight_id)
                | (
                    parsed.event_fight_id.isNull()
                    & (parsed.cast_timestamp_ms >= fight_lookup._f_start_ms)
                    & (parsed.cast_timestamp_ms <= fight_lookup._f_end_ms)
                )
            ),
            "left",
        )
        .withColumn("fight_id", F.coalesce(F.col("event_fight_id"), F.col("_f_fight_id")))
        .drop("_f_report_code", "_f_fight_id", "_f_start_ms", "_f_end_ms", "event_fight_id")
    )

    return (
        with_fight
        .join(
            actor_lookup,
            (with_fight.report_code == actor_lookup._a_report_code)
            & (with_fight.actor_id == actor_lookup._a_actor_id),
            "left",
        )
        .drop("_a_report_code", "_a_actor_id")
        .filter(F.col("player_name").isNotNull())
        .dropDuplicates(["report_code", "fight_id", "cast_timestamp_ms", "actor_id", "ability_id"])
    )


@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_player_combatant_buffs",
    comment=(
        "Per-player pull-start preparation signals from CombatantInfo events. "
        "Tracks food buffs, flask/phial buffs, and weapon enhancements."
    ),
    table_properties={"quality": "silver"},
)
def silver_player_combatant_buffs():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_casts")  # noqa: F821
    actors = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_actor_roster")  # noqa: F821
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821

    parsed = (
        raw
        .withColumn("parsed", F.from_json(F.col("combatant_info_json"), _COMBATANT_INFO_EVENTS_SCHEMA))
        .filter(F.col("parsed").isNotNull())
        .withColumn("event", F.explode("parsed.data"))
        .select(
            F.col("report_code"),
            F.col("event.fight").alias("event_fight_id"),
            F.col("event.timestamp").alias("combatant_timestamp_ms"),
            F.coalesce(F.col("event.sourceID"), F.col("event.source.id")).alias("actor_id"),
            F.col("event.specID").alias("spec_id"),
            F.expr("transform(coalesce(event.auras, array()), aura -> trim(aura.name))").alias("aura_names_raw"),
            F.col("_ingested_at"),
        )
        .filter(F.col("actor_id").isNotNull())
        .dropDuplicates(["report_code", "event_fight_id", "combatant_timestamp_ms", "actor_id"])
    )

    actor_lookup = actors.select(
        F.col("report_code").alias("_a_report_code"),
        F.col("actor_id").alias("_a_actor_id"),
        "player_name",
        "player_class",
    )

    fight_lookup = fights.select(
        F.col("report_code").alias("_f_report_code"),
        F.col("fight_id").alias("_f_fight_id"),
        F.col("fight_start_ms").alias("_f_start_ms"),
        (F.col("fight_start_ms") + (F.col("duration_seconds") * 1000)).alias("_f_end_ms"),
    )

    with_fight = (
        parsed
        .join(
            fight_lookup,
            (parsed.report_code == fight_lookup._f_report_code)
            & (
                (parsed.event_fight_id == fight_lookup._f_fight_id)
                | (
                    parsed.event_fight_id.isNull()
                    & (parsed.combatant_timestamp_ms >= fight_lookup._f_start_ms)
                    & (parsed.combatant_timestamp_ms <= fight_lookup._f_end_ms)
                )
            ),
            "left",
        )
        .withColumn("fight_id", F.coalesce(F.col("event_fight_id"), F.col("_f_fight_id")))
        .drop("_f_report_code", "_f_fight_id", "_f_start_ms", "_f_end_ms", "event_fight_id")
    )

    grouped = (
        with_fight
        .join(
            actor_lookup,
            (with_fight.report_code == actor_lookup._a_report_code)
            & (with_fight.actor_id == actor_lookup._a_actor_id),
            "left",
        )
        .drop("_a_report_code", "_a_actor_id")
        .filter(F.col("player_name").isNotNull())
        .groupBy("report_code", "fight_id", "player_name", "player_class")
        .agg(
            F.max("spec_id").alias("spec_id"),
            F.flatten(F.collect_list("aura_names_raw")).alias("aura_names"),
        )
    )

    classified = (
        grouped
        .withColumn("food_buff_names_array", _classify_food_udf(F.col("aura_names")))
        .withColumn("flask_or_phial_names_array", _classify_flask_udf(F.col("aura_names")))
        .withColumn("weapon_enhancement_names_array", _classify_weapon_udf(F.col("aura_names")))
    )

    return (
        classified
        .select(
            "report_code",
            "fight_id",
            "player_name",
            "player_class",
            "spec_id",
            F.when(F.size(F.col("food_buff_names_array")) > 0, F.lit(1)).otherwise(F.lit(0)).alias("has_food_buff"),
            _join_consumable_names_udf(F.col("food_buff_names_array")).alias("food_buff_names"),
            F.when(F.size(F.col("flask_or_phial_names_array")) > 0, F.lit(1)).otherwise(F.lit(0)).alias("has_flask_or_phial_buff"),
            _join_consumable_names_udf(F.col("flask_or_phial_names_array")).alias("flask_or_phial_names"),
            F.when(F.size(F.col("weapon_enhancement_names_array")) > 0, F.lit(1)).otherwise(F.lit(0)).alias("has_weapon_enhancement_aura"),
            _join_consumable_names_udf(F.col("weapon_enhancement_names_array")).alias("weapon_enhancement_aura_names"),
        )
    )
