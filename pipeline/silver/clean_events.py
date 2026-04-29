# Databricks notebook source
# ruff: noqa: E402, I001
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

import os
import sys


def _ensure_repo_root_on_syspath() -> None:
    candidates = [os.getcwd()]

    module_file = globals().get("__file__")
    if module_file:
        candidates.append(os.path.abspath(module_file))

    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()  # noqa: F821
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        candidates.append(notebook_path)
    except Exception:
        pass

    for candidate in candidates:
        current = candidate if os.path.isdir(candidate) else os.path.dirname(candidate)
        while current and current != os.path.dirname(current):
            pipeline_dir = (
                current
                if os.path.basename(current) == "pipeline"
                else os.path.join(current, "pipeline")
            )
            if os.path.isfile(os.path.join(pipeline_dir, "__init__.py")):
                repo_root = os.path.dirname(pipeline_dir)
                if repo_root not in sys.path:
                    sys.path.insert(0, repo_root)
                return
            current = os.path.dirname(current)


_ensure_repo_root_on_syspath()

import dlt  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pipeline.consumables import (  # noqa: E402
    MIDNIGHT_FLASK_OR_PHIAL_NAMES,
    MIDNIGHT_FOOD_NAMES,
    MIDNIGHT_WEAPON_ENHANCEMENT_NAMES,
)
from pipeline.expectations.common_expectations import (
    INGESTED_AT_PRESENT,
    REPORT_FIGHT_PLAYER_UNIQUE,
)  # noqa: E402
from pipeline.gold._cooldown_rules import cooldown_rules_sql  # noqa: E402

# ── Schema for the table(dataType: Deaths) JSON scalar ────────────────────────

_ABILITY_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("guid", LongType(), True),
    ]
)

_EVENT_STRUCT = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("type", StringType(), True),  # "damage", "heal", etc.
        StructField("sourceIsFriendly", BooleanType(), True),
        StructField("ability", _ABILITY_STRUCT, True),
    ]
)

_ENTRY_STRUCT = StructType(
    [
        StructField("name", StringType(), True),  # player name
        StructField("id", LongType(), True),  # actor ID
        StructField("type", StringType(), True),  # WoW class
        StructField("icon", StringType(), True),  # "ClassName-Spec"
        StructField("timestamp", LongType(), True),  # ms from report start
        StructField("fight", LongType(), True),  # fight ID
        StructField("overkill", LongType(), True),
        StructField("events", ArrayType(_EVENT_STRUCT), True),
    ]
)

_TABLE_DATA_STRUCT = StructType(
    [
        StructField("entries", ArrayType(_ENTRY_STRUCT), True),
    ]
)

_TABLE_SCHEMA = StructType(
    [
        StructField("data", _TABLE_DATA_STRUCT, True),
    ]
)


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
@dlt.expect(*REPORT_FIGHT_PLAYER_UNIQUE)
def silver_player_deaths():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_deaths")  # noqa: F821

    # Legacy bronze death files stored many fights in one record; WCL truncates
    # those responses on long reports. Once a report has any single-fight death
    # records, prefer those and ignore the older multi-fight payloads.
    report_modes = raw.groupBy("report_code").agg(
        F.max(
            F.when(
                F.size(F.coalesce(F.col("fight_ids"), F.array().cast("array<long>"))) == 1, F.lit(1)
            ).otherwise(F.lit(0))
        ).alias("has_single_fight_records")
    )

    filtered_raw = (
        raw.join(report_modes, on="report_code", how="left")
        .filter(
            (F.col("has_single_fight_records") == 0)
            | (F.size(F.coalesce(F.col("fight_ids"), F.array().cast("array<long>"))) == 1)
        )
        .drop("has_single_fight_records")
    )

    return (
        filtered_raw.withColumn("parsed", F.from_json(F.col("table_json"), _TABLE_SCHEMA))
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
            F.col("_ingested_at"),
            # Killing blow: filter events to non-friendly damage sources (newest first),
            # take element [0] — the hit closest to the death timestamp.
            # FILTER() is a Spark SQL higher-order function available in Spark 3.x+.
            F.expr(
                "FILTER(entry.events, e -> e.type = 'damage' AND e.sourceIsFriendly = false)[0]"
            ).alias("kb_event"),
        )
        .withColumn("killing_blow_name", F.col("kb_event.ability.name"))
        .withColumn("killing_blow_id", F.col("kb_event.ability.guid"))
        .withColumn(
            "_duplicate_count",
            F.count(F.lit(1)).over(
                Window.partitionBy("report_code", "fight_id", "player_name", "death_timestamp_ms")
            ),
        )
        .drop("kb_event")
        .filter(F.col("player_name").isNotNull())
    )


# ── Schema for events(dataType: Casts) JSON scalar ────────────────────────────

_CAST_ABILITY_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("guid", LongType(), True),
        StructField("type", LongType(), True),
        StructField("abilityIcon", StringType(), True),
    ]
)

_CAST_EVENT_STRUCT = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("type", StringType(), True),
        StructField("sourceID", LongType(), True),
        StructField("targetID", LongType(), True),
        StructField("fight", LongType(), True),
        StructField("abilityGameID", LongType(), True),
        StructField("ability", _CAST_ABILITY_STRUCT, True),
    ]
)

_CAST_EVENTS_SCHEMA = StructType(
    [
        StructField("data", ArrayType(_CAST_EVENT_STRUCT), True),
    ]
)

_COMBATANT_INFO_TALENT_STRUCT = StructType(
    [
        StructField("spellID", LongType(), True),
        StructField("id", LongType(), True),
        StructField("talentID", LongType(), True),
    ]
)

_COMBATANT_INFO_AURA_STRUCT = StructType(
    [
        StructField("abilityGameId", LongType(), True),
        StructField("icon", StringType(), True),
        StructField("name", StringType(), True),
        StructField("stacks", LongType(), True),
    ]
)

_COMBATANT_SOURCE_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
    ]
)

_COMBATANT_INFO_EVENT_STRUCT = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("type", StringType(), True),
        StructField("sourceID", LongType(), True),
        StructField("source", _COMBATANT_SOURCE_STRUCT, True),
        StructField("fight", LongType(), True),
        StructField("specID", LongType(), True),
        StructField("talentTree", ArrayType(_COMBATANT_INFO_TALENT_STRUCT), True),
        StructField("auras", ArrayType(_COMBATANT_INFO_AURA_STRUCT), True),
    ]
)

_COMBATANT_INFO_EVENTS_SCHEMA = StructType(
    [
        StructField("data", ArrayType(_COMBATANT_INFO_EVENT_STRUCT), True),
    ]
)

_COOLDOWN_RULES_SQL = cooldown_rules_sql()


def _sql_string_array(values: set[str]) -> str:
    return (
        "array(" + ", ".join("'" + value.replace("'", "''") + "'" for value in sorted(values)) + ")"
    )


def _normalized_name_sql(name_sql: str) -> str:
    return (
        "trim(regexp_replace(regexp_replace(lower(coalesce("
        f"{name_sql}, '')), \"'\", ''), '\\\\s+', ' '))"
    )


def _classified_name_array_sql(
    source_col: str, explicit_names: set[str], keywords: tuple[str, ...]
) -> str:
    normalized_name = _normalized_name_sql("name")
    clauses = [f"array_contains({_sql_string_array(explicit_names)}, {normalized_name})"]
    clauses.extend(
        f"{normalized_name} LIKE '%{keyword_sql}%'"
        for keyword_sql in (keyword.replace("'", "''") for keyword in keywords)
    )
    return (
        f"filter({source_col}, name -> {normalized_name} <> '' AND (" + " OR ".join(clauses) + "))"
    )


def _joined_name_column(array_col: str) -> F.Column:
    return F.when(F.size(F.col(array_col)) > 0, F.array_join(F.col(array_col), " | ")).otherwise(
        F.lit(None)
    )


# ── Parsed Player Cast Events ─────────────────────────────────────────────────


@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_player_cast_events",
    comment=(
        "WCL player cast events for raid boss pulls. "
        "Used for health potion, healthstone, and defensive usage analysis."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect(*INGESTED_AT_PRESENT)
def silver_player_cast_events():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_casts")  # noqa: F821
    actors = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_actor_roster")  # noqa: F821
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821

    parsed = (
        raw.withColumn("parsed", F.from_json(F.col("events_json"), _CAST_EVENTS_SCHEMA))
        .filter(F.col("parsed").isNotNull())
        .withColumn("event", F.explode("parsed.data"))
        .select(
            F.col("report_code"),
            F.col("event.fight").alias("event_fight_id"),
            F.col("event.timestamp").alias("cast_timestamp_ms"),
            F.col("event.sourceID").alias("actor_id"),
            F.col("event.targetID").alias("target_actor_id"),
            F.col("event.ability.name").alias("ability_name"),
            F.coalesce(F.col("event.ability.guid"), F.col("event.abilityGameID")).alias(
                "ability_id"
            ),
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
        parsed.join(
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
        with_fight.join(
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
@dlt.expect(*INGESTED_AT_PRESENT)
def silver_player_combatant_buffs():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_casts")  # noqa: F821
    actors = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_actor_roster")  # noqa: F821
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821

    parsed = (
        raw.withColumn(
            "parsed", F.from_json(F.col("combatant_info_json"), _COMBATANT_INFO_EVENTS_SCHEMA)
        )
        .filter(F.col("parsed").isNotNull())
        .withColumn("event", F.explode("parsed.data"))
        .select(
            F.col("report_code"),
            F.col("event.fight").alias("event_fight_id"),
            F.col("event.timestamp").alias("combatant_timestamp_ms"),
            F.coalesce(F.col("event.sourceID"), F.col("event.source.id")).alias("actor_id"),
            F.col("event.specID").alias("spec_id"),
            F.expr("transform(coalesce(event.auras, array()), aura -> trim(aura.name))").alias(
                "aura_names_raw"
            ),
            F.expr(
                "filter(transform(coalesce(event.talentTree, array()), "
                "talent -> cast(coalesce(talent.spellID, talent.id, talent.talentID) as string)), "
                "talent_id -> talent_id is not null)"
            ).alias("talent_spell_ids_raw"),
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
        parsed.join(
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
        with_fight.join(
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
            F.array_distinct(F.flatten(F.collect_list("talent_spell_ids_raw"))).alias(
                "talent_spell_ids"
            ),
            F.max("_ingested_at").alias("_ingested_at"),
        )
    )

    classified = (
        grouped.withColumn(
            "food_buff_names_array",
            F.expr(
                _classified_name_array_sql(
                    "aura_names",
                    MIDNIGHT_FOOD_NAMES,
                    ("well fed", "feast"),
                )
            ),
        )
        .withColumn(
            "flask_or_phial_names_array",
            F.expr(
                _classified_name_array_sql(
                    "aura_names",
                    MIDNIGHT_FLASK_OR_PHIAL_NAMES,
                    ("flask", "phial"),
                )
            ),
        )
        .withColumn(
            "weapon_enhancement_names_array",
            F.expr(
                _classified_name_array_sql(
                    "aura_names",
                    MIDNIGHT_WEAPON_ENHANCEMENT_NAMES,
                    (
                        " oil",
                        "oil ",
                        "whetstone",
                        "weightstone",
                        "razorstone",
                        "shots",
                        "enchanted edge",
                        "lynxeye",
                        "hawkeye",
                    ),
                )
            ),
        )
    )

    return classified.select(
        "report_code",
        "fight_id",
        "player_name",
        "player_class",
        "spec_id",
        "talent_spell_ids",
        F.when(F.size(F.col("food_buff_names_array")) > 0, F.lit(1))
        .otherwise(F.lit(0))
        .alias("has_food_buff"),
        _joined_name_column("food_buff_names_array").alias("food_buff_names"),
        F.when(F.size(F.col("flask_or_phial_names_array")) > 0, F.lit(1))
        .otherwise(F.lit(0))
        .alias("has_flask_or_phial_buff"),
        _joined_name_column("flask_or_phial_names_array").alias("flask_or_phial_names"),
        F.when(F.size(F.col("weapon_enhancement_names_array")) > 0, F.lit(1))
        .otherwise(F.lit(0))
        .alias("has_weapon_enhancement_aura"),
        _joined_name_column("weapon_enhancement_names_array").alias(
            "weapon_enhancement_aura_names"
        ),
        "_ingested_at",
    )


# Materialized because both gold wipe diagnostics tables reuse the same spec/talent gating join.
@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_player_cooldown_capacity",
    comment=(
        "Per-player pull cooldown tracking state for wipe diagnostics. "
        "Materializes spec/talent-gated cooldown availability, cast evidence, "
        "and pull-level capacity so downstream gold products do not re-derive it."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect(*INGESTED_AT_PRESENT)
def silver_player_cooldown_capacity():
    return spark.sql(  # noqa: F821
        f"""
        WITH cooldown_rules AS (
          SELECT *
          FROM VALUES
            {_COOLDOWN_RULES_SQL}
          AS cooldown_rules(cooldown_category, player_class, ability_id, ability_name, cooldown_seconds, active_seconds, allowed_spec_ids, required_talent_spell_ids)
        ),
        instrumented_pulls AS (
          SELECT DISTINCT report_code, fight_id
          FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events
          WHERE fight_id IS NOT NULL
        ),
        report_player_casts AS (
          SELECT DISTINCT report_code, player_name, ability_id
          FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events
        ),
        player_casts AS (
          SELECT
            report_code,
            fight_id,
            player_name,
            ability_id,
            COUNT(*) AS actual_casts,
            MAX(cast_timestamp_ms) AS last_cast_on_pull_ms
          FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events
          GROUP BY report_code, fight_id, player_name, ability_id
        ),
        player_pulls AS (
          SELECT
            f.report_code,
            f.fight_id,
            f.encounter_id,
            f.boss_name,
            f.zone_name,
            f.zone_id,
            f.difficulty,
            f.difficulty_label,
            f.raid_night_date,
            f.duration_seconds,
            f.is_kill,
            f._ingested_at AS _ingested_at,
            a.player_name,
            a.player_class
          FROM (
            SELECT
              report_code,
              fight_id,
              encounter_id,
              boss_name,
              zone_name,
              zone_id,
              difficulty,
              difficulty_label,
              raid_night_date,
              duration_seconds,
              is_kill,
              _ingested_at,
              EXPLODE(friendly_player_ids) AS actor_id
            FROM 02_silver.sc_analytics_warcraftlogs.silver_fight_events
          ) f
          INNER JOIN 02_silver.sc_analytics_warcraftlogs.silver_actor_roster a
            ON f.report_code = a.report_code
           AND f.actor_id = a.actor_id
          INNER JOIN instrumented_pulls i
            ON f.report_code = i.report_code
           AND f.fight_id = i.fight_id
          WHERE a.player_name IS NOT NULL
            AND a.player_name != ''
        ),
        player_pull_state AS (
          SELECT
            p.*,
            b.spec_id,
            b.talent_spell_ids,
            CASE WHEN b.report_code IS NOT NULL THEN true ELSE false END AS has_combatant_info
          FROM player_pulls p
          LEFT JOIN 02_silver.sc_analytics_warcraftlogs.silver_player_combatant_buffs b
            ON p.report_code = b.report_code
           AND p.fight_id = b.fight_id
           AND p.player_name = b.player_name
        )
        SELECT
          p.report_code,
          p.fight_id,
          p.encounter_id,
          p.boss_name,
          p.zone_name,
          p.zone_id,
          p.difficulty,
          p.difficulty_label,
          p.raid_night_date,
          p.duration_seconds,
          p.is_kill,
          p._ingested_at AS _ingested_at,
          p.player_name,
          p.player_class,
          p.spec_id,
          r.cooldown_category,
          r.ability_id,
          r.ability_name,
          r.cooldown_seconds,
          r.active_seconds,
          r.allowed_spec_ids,
          r.required_talent_spell_ids,
          p.talent_spell_ids,
          p.has_combatant_info,
          CASE
            WHEN p.spec_id IS NULL THEN CAST(NULL AS BOOLEAN)
            WHEN r.allowed_spec_ids = '' THEN true
            ELSE ARRAY_CONTAINS(SPLIT(r.allowed_spec_ids, '\\\\|'), CAST(p.spec_id AS STRING))
          END AS spec_eligible,
          CASE
            WHEN r.required_talent_spell_ids = '' THEN true
            WHEN p.talent_spell_ids IS NULL THEN CAST(NULL AS BOOLEAN)
            ELSE EXISTS(
              SPLIT(r.required_talent_spell_ids, '\\\\|'),
              talent_id -> ARRAY_CONTAINS(p.talent_spell_ids, talent_id)
            )
          END AS talent_present,
          CASE WHEN rpc.ability_id IS NOT NULL THEN true ELSE false END AS cast_seen_in_report,
          COALESCE(pc.actual_casts, 0) AS actual_casts,
          pc.last_cast_on_pull_ms,
          CASE
            WHEN COALESCE(p.duration_seconds, 0) > 0
            THEN CAST(FLOOR(COALESCE(p.duration_seconds, 0) / r.cooldown_seconds) + 1 AS BIGINT)
            ELSE CAST(0 AS BIGINT)
          END AS possible_casts,
          CASE
            WHEN p.has_combatant_info = false OR p.spec_id IS NULL THEN 'unknown_missing_combatant_info'
            WHEN r.allowed_spec_ids <> ''
             AND NOT ARRAY_CONTAINS(SPLIT(r.allowed_spec_ids, '\\\\|'), CAST(p.spec_id AS STRING))
            THEN 'untracked_spec_ineligible'
            WHEN r.required_talent_spell_ids <> ''
             AND NOT EXISTS(
               SPLIT(r.required_talent_spell_ids, '\\\\|'),
               talent_id -> p.talent_spell_ids IS NOT NULL AND ARRAY_CONTAINS(p.talent_spell_ids, talent_id)
             )
             AND rpc.ability_id IS NULL
            THEN 'untracked_talent_missing'
            ELSE 'tracked'
          END AS cooldown_tracking_state,
          CASE
            WHEN p.has_combatant_info = false OR p.spec_id IS NULL THEN false
            WHEN r.allowed_spec_ids <> ''
             AND NOT ARRAY_CONTAINS(SPLIT(r.allowed_spec_ids, '\\\\|'), CAST(p.spec_id AS STRING))
            THEN false
            WHEN r.required_talent_spell_ids <> ''
             AND NOT EXISTS(
               SPLIT(r.required_talent_spell_ids, '\\\\|'),
               talent_id -> p.talent_spell_ids IS NOT NULL AND ARRAY_CONTAINS(p.talent_spell_ids, talent_id)
             )
             AND rpc.ability_id IS NULL
            THEN false
            ELSE true
          END AS has_tracked_capacity
        FROM player_pull_state p
        INNER JOIN cooldown_rules r
          ON p.player_class = r.player_class
        LEFT JOIN report_player_casts rpc
          ON p.report_code = rpc.report_code
         AND p.player_name = rpc.player_name
         AND r.ability_id = rpc.ability_id
        LEFT JOIN player_casts pc
          ON p.report_code = pc.report_code
         AND p.fight_id = pc.fight_id
         AND p.player_name = pc.player_name
         AND r.ability_id = pc.ability_id
        """
    )
