# Databricks notebook source
# Gold layer — survivability and boss mechanics data products
#
# gold_player_survivability — per-player death statistics and most common killing blows
# gold_boss_mechanics       — enhanced wipe analysis with phase breakdown, duration
#                             buckets, and weekly progress trend

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _player_identity_key(name_col: str, class_col: str, realm_col: str) -> F.Column:
    return F.concat_ws(
        ":",
        F.lower(F.trim(F.col(name_col))),
        F.lower(F.trim(F.coalesce(F.col(class_col), F.lit("unknown")))),
        F.lower(F.trim(F.coalesce(F.col(realm_col), F.lit("unknown")))),
    )


def _with_player_identity(dataframe, actors):
    actor_realms = (
        actors.filter(F.col("realm").isNotNull() & (F.trim(F.col("realm")) != ""))
        .groupBy(
            F.col("report_code").alias("_actor_report_code"),
            F.lower(F.col("player_name")).alias("_player_name_lower"),
            F.lower(F.col("player_class")).alias("_player_class_lower"),
        )
        .agg(F.max(F.trim(F.col("realm"))).alias("realm"))
    )
    return (
        dataframe.join(
            actor_realms,
            (dataframe.report_code == F.col("_actor_report_code"))
            & (F.lower(dataframe.player_name) == F.col("_player_name_lower"))
            & (F.lower(dataframe.player_class) == F.col("_player_class_lower")),
            "left",
        )
        .drop("_actor_report_code", "_player_name_lower", "_player_class_lower")
        .withColumn("realm", F.coalesce(F.col("realm"), F.lit("unknown")))
        .withColumn("player_identity_key", _player_identity_key("player_name", "player_class", "realm"))
    )

# ── Player Survivability ───────────────────────────────────────────────────────
# Per-player death statistics derived from fact_player_events (death events) and
# fact_player_fight_performance (kill counts for the deaths_per_kill metric).
#
# NOTE: deaths_per_kill is an approximation because deaths are aggregated at
# report level (not per-fight) by the WCL table API.  A player who died on a
# wipe in the same report will inflate this metric relative to kills.


@dlt.table(
    name="03_gold.sc_analytics.gold_player_survivability",
    comment=(
        "Per-player death statistics across all raids. "
        "deaths_per_kill is an approximation — see note in table comment. "
        "Ordered by deaths_per_kill descending (most deaths first)."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_survivability():
    deaths = spark.read.table("03_gold.sc_analytics.fact_player_events")  # noqa: F821
    perf = spark.read.table("03_gold.sc_analytics.fact_player_fight_performance")  # noqa: F821
    actors = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_actor_roster")  # noqa: F821
    deaths = _with_player_identity(deaths, actors)
    perf = _with_player_identity(perf, actors)

    # Total deaths per player
    death_counts = (
        deaths.filter(F.col("player_name").isNotNull())
        .groupBy("player_identity_key", "player_name", "player_class", "realm")
        .agg(
            F.count("*").alias("total_deaths"),
            F.max("zone_name").alias("last_zone"),  # most recent zone (last ingested)
            F.collect_set("zone_name").alias("zones_died_in"),
        )
    )

    # Most common killing blow per player (mode)
    w_blow = Window.partitionBy("player_identity_key").orderBy(F.col("blow_count").desc())
    killing_blow_counts = (
        deaths.filter(F.col("killing_blow_name").isNotNull())
        .groupBy("player_identity_key", "killing_blow_name")
        .agg(F.count("*").alias("blow_count"))
    )
    top_killing_blow = (
        killing_blow_counts.withColumn("_rn", F.row_number().over(w_blow))
        .filter(F.col("_rn") == 1)
        .select(
            "player_identity_key",
            F.col("killing_blow_name").alias("most_common_killing_blow"),
            F.col("blow_count").alias("most_common_killing_blow_count"),
        )
    )
    top_killing_blows = (
        killing_blow_counts.withColumn("_rn", F.row_number().over(w_blow))
        .filter(F.col("_rn") <= 3)
        .groupBy("player_identity_key")
        .agg(
            F.to_json(
                F.expr(
                    """
                    transform(
                      sort_array(collect_list(named_struct(
                        'rank', _rn,
                        'name', killing_blow_name,
                        'count', blow_count
                      ))),
                      x -> named_struct('name', x.name, 'count', x.count)
                    )
                    """
                )
            ).alias("top_killing_blows_json")
        )
    )

    # Last death date (max zone-level date proxy using report join in fact_player_events)
    last_death = deaths.groupBy("player_identity_key").agg(
        F.max("death_timestamp_ms").alias("last_death_timestamp_ms")
    )

    # Kill count per player from performance facts
    kill_counts = perf.groupBy("player_identity_key").agg(F.count("*").alias("kills_tracked"))

    return (
        death_counts.join(top_killing_blow, "player_identity_key", "left")
        .join(top_killing_blows, "player_identity_key", "left")
        .join(last_death, "player_identity_key", "left")
        .join(kill_counts, "player_identity_key", "left")
        .withColumn(
            "deaths_per_kill",
            F.round(
                F.col("total_deaths")
                / F.greatest(F.coalesce(F.col("kills_tracked"), F.lit(0)).cast("double"), F.lit(1)),
                2,
            ),
        )
        .select(
            "player_identity_key",
            "player_name",
            "player_class",
            "realm",
            "total_deaths",
            F.coalesce(F.col("kills_tracked"), F.lit(0)).alias("kills_tracked"),
            "deaths_per_kill",
            "most_common_killing_blow",
            "most_common_killing_blow_count",
            "top_killing_blows_json",
            "zones_died_in",
            "last_death_timestamp_ms",
        )
        .orderBy(F.col("deaths_per_kill").desc(), F.col("total_deaths").desc())
    )


# ── Player Death Events With Boss Context ──────────────────────────────────────
# Frontend-facing death event grain used for scoped player pages.  The aggregate
# gold_player_survivability table is intentionally all-time; this table keeps the
# boss, difficulty, and zone dimensions needed for page filters.


@dlt.table(
    name="03_gold.sc_analytics.gold_player_death_events",
    comment=(
        "One row per player death joined to boss fight context. "
        "Used by frontend player profiles for tier/difficulty/boss-scoped survivability."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name,encounter_id",
    },
)
def gold_player_death_events():
    deaths = spark.read.table("03_gold.sc_analytics.fact_player_events")  # noqa: F821
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821
    actors = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_actor_roster")  # noqa: F821

    fight_context = fights.select(
        F.col("report_code").alias("_report_code"),
        F.col("fight_id").alias("_fight_id"),
        "encounter_id",
        "boss_name",
        F.col("zone_name").alias("_zone_name"),
        F.col("zone_id").alias("_zone_id"),
        "difficulty",
        "difficulty_label",
        F.col("raid_night_date").alias("_raid_night_date"),
        "is_kill",
        F.col("fight_start_ms").alias("_fight_start_ms"),
    ).dropDuplicates(["_report_code", "_fight_id"])

    death_key_window = Window.partitionBy(
        "report_code",
        "fight_id",
        "player_identity_key",
        "death_timestamp_ms",
    ).orderBy(
        F.col("killing_blow_id").asc_nulls_last(),
        F.col("killing_blow_name").asc_nulls_last(),
        F.col("overkill").asc_nulls_last(),
    )

    return (
        _with_player_identity(deaths.drop("zone_name", "zone_id", "raid_night_date"), actors)
        .join(
            fight_context,
            (F.col("report_code") == F.col("_report_code"))
            & (F.col("fight_id") == F.col("_fight_id")),
            "left",
        )
        .drop("_report_code", "_fight_id")
        .filter(F.col("encounter_id").isNotNull() & (F.col("encounter_id") > 0))
        .withColumn("_death_duplicate_ordinal", F.row_number().over(death_key_window))
        .withColumn(
            "death_event_key",
            F.format_string(
                "%016x",
                F.xxhash64(
                    F.col("report_code"),
                    F.col("fight_id"),
                    F.col("player_identity_key"),
                    F.col("death_timestamp_ms"),
                    F.col("_death_duplicate_ordinal"),
                ),
            ),
        )
        .select(
            "death_event_key",
            "report_code",
            "fight_id",
            "encounter_id",
            "boss_name",
            F.col("_zone_name").alias("zone_name"),
            F.col("_zone_id").alias("zone_id"),
            "difficulty",
            "difficulty_label",
            F.col("_raid_night_date").alias("raid_night_date"),
            "is_kill",
            "player_identity_key",
            "player_name",
            "player_class",
            "realm",
            "death_timestamp_ms",
            F.col("_fight_start_ms").alias("fight_start_ms"),
            "overkill",
            "killing_blow_name",
            "killing_blow_id",
        )
        .orderBy("raid_night_date", "report_code", "fight_id", "death_timestamp_ms")
    )


@dlt.table(
    name="03_gold.sc_analytics.gold_player_survivability_rankings",
    comment=(
        "Scoped raid-team survivability rankings by deaths per kill. Scope "
        "dimensions use 'All' sentinel values for dashboard tier/boss/difficulty filters."
    ),
    table_properties={"quality": "gold"},
)
def gold_player_survivability_rankings():
    return spark.sql(  # noqa: F821
        """
        WITH raid_team AS (
          SELECT LOWER(name) AS player_key
          FROM 03_gold.sc_analytics.gold_raid_team
          WHERE name IS NOT NULL AND name != ''
        ),
        raid_team_count AS (
          SELECT COUNT(*) AS row_count FROM raid_team
        ),
        kill_base AS (
          SELECT
            k.player_identity_key,
            MAX(k.player_name) AS player_name,
            MAX(k.player_class) AS player_class,
            k.zone_name,
            k.encounter_id,
            k.boss_name,
            k.difficulty,
            k.difficulty_label,
            COUNT(*) AS kills
          FROM 03_gold.sc_analytics.gold_boss_kill_roster k
          CROSS JOIN raid_team_count rtc
          LEFT JOIN raid_team rt ON LOWER(k.player_name) = rt.player_key
          WHERE k.player_name IS NOT NULL
            AND k.player_name != ''
            AND k.zone_name != 'Blackrock Depths'
            AND (rtc.row_count = 0 OR rt.player_key IS NOT NULL)
          GROUP BY
            k.player_identity_key,
            k.zone_name,
            k.encounter_id,
            k.boss_name,
            k.difficulty,
            k.difficulty_label
        ),
        death_base AS (
          SELECT
            d.player_identity_key,
            MAX(d.player_name) AS player_name,
            MAX(d.player_class) AS player_class,
            d.zone_name,
            d.encounter_id,
            d.boss_name,
            d.difficulty,
            d.difficulty_label,
            COUNT(*) AS deaths
          FROM 03_gold.sc_analytics.gold_player_death_events d
          CROSS JOIN raid_team_count rtc
          LEFT JOIN raid_team rt ON LOWER(d.player_name) = rt.player_key
          WHERE d.player_name IS NOT NULL
            AND d.player_name != ''
            AND d.zone_name != 'Blackrock Depths'
            AND (rtc.row_count = 0 OR rt.player_key IS NOT NULL)
          GROUP BY
            d.player_identity_key,
            d.zone_name,
            d.encounter_id,
            d.boss_name,
            d.difficulty,
            d.difficulty_label
        ),
        scoped_kills AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            CASE
              WHEN GROUPING(boss_name) = 1 OR COUNT(DISTINCT encounter_id) != 1 THEN CAST(NULL AS BIGINT)
              ELSE MAX(encounter_id)
            END AS encounter_id,
            CASE
              WHEN GROUPING(difficulty_label) = 1 OR COUNT(DISTINCT difficulty) != 1 THEN CAST(NULL AS BIGINT)
              ELSE MAX(difficulty)
            END AS difficulty,
            SUM(kills) AS kills
          FROM kill_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, boss_name, difficulty_label),
            (player_identity_key, zone_name, difficulty_label),
            (player_identity_key, boss_name, difficulty_label),
            (player_identity_key, difficulty_label),
            (player_identity_key, zone_name, boss_name),
            (player_identity_key, boss_name),
            (player_identity_key, zone_name),
            (player_identity_key)
          )
        ),
        scoped_deaths AS (
          SELECT
            player_identity_key,
            MAX(player_name) AS player_name,
            MAX(player_class) AS player_class,
            CASE WHEN GROUPING(zone_name) = 1 THEN 'All' ELSE zone_name END AS zone_name,
            CASE WHEN GROUPING(boss_name) = 1 THEN 'All' ELSE boss_name END AS boss_name,
            CASE WHEN GROUPING(difficulty_label) = 1 THEN 'All' ELSE difficulty_label END AS difficulty_label,
            CASE
              WHEN GROUPING(boss_name) = 1 OR COUNT(DISTINCT encounter_id) != 1 THEN CAST(NULL AS BIGINT)
              ELSE MAX(encounter_id)
            END AS encounter_id,
            CASE
              WHEN GROUPING(difficulty_label) = 1 OR COUNT(DISTINCT difficulty) != 1 THEN CAST(NULL AS BIGINT)
              ELSE MAX(difficulty)
            END AS difficulty,
            SUM(deaths) AS deaths
          FROM death_base
          GROUP BY GROUPING SETS (
            (player_identity_key, zone_name, boss_name, difficulty_label),
            (player_identity_key, zone_name, difficulty_label),
            (player_identity_key, boss_name, difficulty_label),
            (player_identity_key, difficulty_label),
            (player_identity_key, zone_name, boss_name),
            (player_identity_key, boss_name),
            (player_identity_key, zone_name),
            (player_identity_key)
          )
        ),
        joined AS (
          SELECT
            k.player_identity_key,
            k.player_name,
            k.player_class,
            k.zone_name,
            k.encounter_id,
            k.boss_name,
            k.difficulty,
            k.difficulty_label,
            k.kills,
            COALESCE(d.deaths, 0) AS deaths,
            COALESCE(d.deaths, 0) / GREATEST(k.kills, 1) AS deaths_per_kill
          FROM scoped_kills k
          LEFT JOIN scoped_deaths d
            ON k.player_identity_key = d.player_identity_key
           AND k.zone_name = d.zone_name
           AND k.boss_name = d.boss_name
           AND k.difficulty_label = d.difficulty_label
          WHERE k.kills > 0
        ),
        ranked AS (
          SELECT
            *,
            RANK() OVER (
              PARTITION BY zone_name, boss_name, difficulty_label
              ORDER BY deaths_per_kill ASC, deaths ASC, player_identity_key ASC
            ) AS survivability_rank,
            COUNT(*) OVER (
              PARTITION BY zone_name, boss_name, difficulty_label
            ) AS survivability_rank_total
          FROM joined
        )
        SELECT
          player_identity_key,
          player_name,
          player_class,
          zone_name,
          encounter_id,
          boss_name,
          difficulty,
          difficulty_label,
          deaths,
          kills,
          ROUND(deaths_per_kill, 4) AS deaths_per_kill,
          survivability_rank,
          survivability_rank_total,
          CASE
            WHEN survivability_rank_total <= 1 THEN 100.0
            ELSE ROUND(((survivability_rank_total - survivability_rank) / (survivability_rank_total - 1)) * 100, 1)
          END AS survivability_rank_percentile
        FROM ranked
        """
    )


# ── Boss Mechanics ─────────────────────────────────────────────────────────────
# Enhanced wipe analysis beyond gold_boss_wipe_analysis.
# Analyses wipe patterns to surface progress and help teams understand where
# they are dying in each encounter.
#
# Metrics:
#   - Phase breakdown: % of wipes ending in each phase bucket
#   - Duration buckets: % of wipes by duration range
#   - Weekly pull counts: pulls per boss per ISO week
#   - progress_trend: last week's avg boss_percentage vs overall avg
#     (positive = improving, negative = regressing)


@dlt.table(
    name="03_gold.sc_analytics.gold_boss_mechanics",
    comment=(
        "Enhanced wipe analysis per boss encounter. "
        "Phase breakdown, duration buckets, weekly pull counts, and progress trend."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_boss_mechanics():
    fights = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_fight_events")  # noqa: F821

    # Boss wipes only (raid difficulties, valid encounter, not a kill)
    wipes = (
        fights.filter(F.col("is_kill") == False)  # noqa: E712
        .filter(F.col("encounter_id").isNotNull() & (F.col("encounter_id") > 0))
        .filter(F.col("difficulty").isin(3, 4, 5))
    )

    # ISO week for weekly trend calculation
    wipes_with_week = wipes.withColumn("iso_week", F.date_trunc("week", F.col("raid_night_date")))

    # Phase breakdown — classify wipes by last_phase into buckets
    wipes_with_phase = wipes_with_week.withColumn(
        "phase_bucket",
        F.when(F.col("last_phase") <= 1, "Phase 1")
        .when(F.col("last_phase") == 2, "Phase 2")
        .when(F.col("last_phase") >= 3, "Phase 3+")
        .otherwise("Unknown"),
    )

    # Duration buckets
    wipes_with_duration_bucket = wipes_with_phase.withColumn(
        "duration_bucket",
        F.when(F.col("duration_seconds") < 60, "< 1 min")
        .when(F.col("duration_seconds") < 180, "1-3 min")
        .when(F.col("duration_seconds") < 300, "3-5 min")
        .otherwise("5+ min"),
    )

    # Overall aggregation per encounter + difficulty
    overall = (
        wipes_with_duration_bucket.groupBy(
            "encounter_id", "boss_name", "zone_name", "difficulty", "difficulty_label"
        )
        .agg(
            F.count("*").alias("total_wipes"),
            F.avg("boss_percentage").alias("avg_boss_pct"),
            # Phase breakdown counts
            F.sum(F.when(F.col("phase_bucket") == "Phase 1", 1).otherwise(0)).alias(
                "wipes_phase_1"
            ),
            F.sum(F.when(F.col("phase_bucket") == "Phase 2", 1).otherwise(0)).alias(
                "wipes_phase_2"
            ),
            F.sum(F.when(F.col("phase_bucket") == "Phase 3+", 1).otherwise(0)).alias(
                "wipes_phase_3_plus"
            ),
            # Duration breakdown counts
            F.sum(F.when(F.col("duration_bucket") == "< 1 min", 1).otherwise(0)).alias(
                "wipes_lt_1min"
            ),
            F.sum(F.when(F.col("duration_bucket") == "1-3 min", 1).otherwise(0)).alias(
                "wipes_1_3min"
            ),
            F.sum(F.when(F.col("duration_bucket") == "3-5 min", 1).otherwise(0)).alias(
                "wipes_3_5min"
            ),
            F.sum(F.when(F.col("duration_bucket") == "5+ min", 1).otherwise(0)).alias(
                "wipes_5plus_min"
            ),
        )
        .withColumn(
            "pct_wipes_phase_1",
            F.round(F.col("wipes_phase_1") / F.greatest(F.col("total_wipes"), F.lit(1)) * 100, 1),
        )
        .withColumn(
            "pct_wipes_phase_2",
            F.round(F.col("wipes_phase_2") / F.greatest(F.col("total_wipes"), F.lit(1)) * 100, 1),
        )
        .withColumn(
            "pct_wipes_phase_3_plus",
            F.round(
                F.col("wipes_phase_3_plus") / F.greatest(F.col("total_wipes"), F.lit(1)) * 100, 1
            ),
        )
    )

    # Last week vs overall boss_percentage trend
    # Identify "last week" as the most recent ISO week with any wipes for each boss
    w_week = Window.partitionBy("encounter_id", "difficulty").orderBy(F.col("iso_week").desc())

    last_week_wipes = (
        wipes_with_week.withColumn("_week_rn", F.dense_rank().over(w_week))
        .filter(F.col("_week_rn") == 1)
        .groupBy("encounter_id", "difficulty")
        .agg(F.avg("boss_percentage").alias("last_week_avg_boss_pct"))
    )

    # progress_trend = last_week_avg - overall_avg (positive means boss % going up = improving)
    return (
        overall.join(last_week_wipes, ["encounter_id", "difficulty"], "left")
        .withColumn(
            "progress_trend",
            F.round(
                F.col("last_week_avg_boss_pct") - F.col("avg_boss_pct"),
                2,
            ),
        )
        .select(
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "total_wipes",
            F.round("avg_boss_pct", 1).alias("avg_boss_pct"),
            "pct_wipes_phase_1",
            "pct_wipes_phase_2",
            "pct_wipes_phase_3_plus",
            "wipes_lt_1min",
            "wipes_1_3min",
            "wipes_3_5min",
            "wipes_5plus_min",
            F.round("last_week_avg_boss_pct", 1).alias("last_week_avg_boss_pct"),
            "progress_trend",
        )
        .orderBy("zone_name", "difficulty", "encounter_id")
    )
