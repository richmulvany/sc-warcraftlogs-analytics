# Databricks notebook source
# Gold layer — raid preparation and mechanics data products
#
# gold_player_consumables   — per-player consumable compliance on boss kills
#                             (potion and healthstone usage rates)
# gold_player_combat_stats  — per-player avg combat stat ratings across kills
#                             (Crit/Haste/Mastery/Versatility by boss)
# gold_boss_ability_deaths  — which abilities are killing players most on each boss
#                             (sourced from death events joined to fight context)
#
# These tables power "are we prepared?" and "what is killing us?" questions.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Player Consumable Compliance ───────────────────────────────────────────────
# "Is each player using potions and healthstones on boss kills?"
#
# potion_use and healthstone_use are 0/1 per kill fight (from playerDetails).
# Usage rate = kills with at least one use / total kills tracked.

@dlt.table(
    name="gold_player_consumables",
    comment=(
        "Per-player consumable usage rates across all boss kills. "
        "potion_use_rate and healthstone_use_rate are fractions 0.0-1.0. "
        "Also broken down per-boss for targeted coaching."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_consumables():
    perf = dlt.read("fact_player_fight_performance")

    # Overall consumable compliance per player-role
    overall = (
        perf
        .filter(F.col("potion_use").isNotNull() | F.col("healthstone_use").isNotNull())
        .groupBy("player_name", "player_class", "role")
        .agg(
            F.count("*").alias("kills_tracked"),
            F.sum(F.when(F.col("potion_use") >= 1, 1).otherwise(0)).alias("kills_with_potion"),
            F.sum(F.when(F.col("healthstone_use") >= 1, 1).otherwise(0)).alias("kills_with_healthstone"),
        )
        .withColumn(
            "potion_use_rate",
            F.round(
                F.col("kills_with_potion") / F.greatest(F.col("kills_tracked"), F.lit(1)),
                2,
            ),
        )
        .withColumn(
            "healthstone_use_rate",
            F.round(
                F.col("kills_with_healthstone") / F.greatest(F.col("kills_tracked"), F.lit(1)),
                2,
            ),
        )
    )

    # Per-boss breakdown to catch boss-specific behaviour (e.g. never potting on farm)
    per_boss = (
        perf
        .filter(F.col("potion_use").isNotNull() | F.col("healthstone_use").isNotNull())
        .groupBy("player_name", "encounter_id", "boss_name", "difficulty_label")
        .agg(
            F.count("*").alias("kills_on_boss"),
            F.sum(F.when(F.col("potion_use") >= 1, 1).otherwise(0)).alias("boss_kills_with_potion"),
            F.sum(F.when(F.col("healthstone_use") >= 1, 1).otherwise(0)).alias("boss_kills_with_healthstone"),
        )
        .withColumn(
            "boss_potion_rate",
            F.round(
                F.col("boss_kills_with_potion") / F.greatest(F.col("kills_on_boss"), F.lit(1)),
                2,
            ),
        )
        .withColumn(
            "boss_healthstone_rate",
            F.round(
                F.col("boss_kills_with_healthstone") / F.greatest(F.col("kills_on_boss"), F.lit(1)),
                2,
            ),
        )
        # Flag bosses where the player has a significantly lower pot rate than their overall average.
        # Collect as an array of structs so one row per player in the outer table.
        .select(
            "player_name",
            F.struct(
                "encounter_id",
                "boss_name",
                "difficulty_label",
                "kills_on_boss",
                "boss_potion_rate",
                "boss_healthstone_rate",
            ).alias("boss_consumable_detail"),
        )
    )

    boss_details_agg = (
        per_boss
        .groupBy("player_name")
        .agg(F.collect_list("boss_consumable_detail").alias("boss_consumable_details"))
    )

    return (
        overall
        .join(boss_details_agg, "player_name", "left")
        .select(
            "player_name",
            "player_class",
            "role",
            "kills_tracked",
            "kills_with_potion",
            "kills_with_healthstone",
            "potion_use_rate",
            "healthstone_use_rate",
            "boss_consumable_details",
        )
        .orderBy("role", F.col("potion_use_rate").asc())
    )


# ── Player Combat Stats ────────────────────────────────────────────────────────
# "What are each player's combat stat ratings and are they optimised for their spec?"
#
# Secondary stat ratings (Crit/Haste/Mastery/Versatility) come from
# combatantInfo.stats in the playerDetails endpoint.  Averaged across kills.

@dlt.table(
    name="gold_player_combat_stats",
    comment=(
        "Per-player average combat stat ratings (Crit/Haste/Mastery/Versatility) "
        "across all kill fights. Useful for identifying stat-distribution outliers. "
        "Ratings are integer values from the WCL combatantInfo endpoint."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_combat_stats():
    perf = dlt.read("fact_player_fight_performance")

    # Only include rows where at least one stat is populated
    has_stats = (
        F.col("crit_rating").isNotNull()
        | F.col("haste_rating").isNotNull()
        | F.col("mastery_rating").isNotNull()
        | F.col("versatility_rating").isNotNull()
    )

    # Per-player overall average (most recent snapshot weighted by kill count)
    overall_stats = (
        perf
        .filter(has_stats)
        .groupBy("player_name", "player_class", "role", "spec")
        .agg(
            F.count("*").alias("kills_tracked"),
            F.avg("crit_rating").cast("long").alias("avg_crit_rating"),
            F.avg("haste_rating").cast("long").alias("avg_haste_rating"),
            F.avg("mastery_rating").cast("long").alias("avg_mastery_rating"),
            F.avg("versatility_rating").cast("long").alias("avg_versatility_rating"),
            F.avg("avg_item_level").alias("avg_item_level"),
            F.max("raid_night_date").alias("latest_kill_date"),
        )
    )

    # Latest snapshot (most recent kill) for current gearing reference
    w_latest = Window.partitionBy("player_name", "spec").orderBy(F.col("raid_night_date").desc())
    latest_snapshot = (
        perf
        .filter(has_stats)
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("player_name").alias("_ls_player_name"),
            F.col("spec").alias("_ls_spec"),
            F.col("crit_rating").alias("latest_crit_rating"),
            F.col("haste_rating").alias("latest_haste_rating"),
            F.col("mastery_rating").alias("latest_mastery_rating"),
            F.col("versatility_rating").alias("latest_versatility_rating"),
            F.col("avg_item_level").alias("latest_avg_item_level"),
        )
    )

    return (
        overall_stats
        .join(
            latest_snapshot,
            (overall_stats.player_name == latest_snapshot._ls_player_name)
            & (overall_stats.spec == latest_snapshot._ls_spec),
            "left",
        )
        .drop("_ls_player_name", "_ls_spec")
        .select(
            "player_name",
            "player_class",
            "role",
            "spec",
            "kills_tracked",
            # Most recent gear snapshot
            "latest_avg_item_level",
            "latest_crit_rating",
            "latest_haste_rating",
            "latest_mastery_rating",
            "latest_versatility_rating",
            # All-time averages (smooths out single outlier kills)
            F.round("avg_item_level", 1).alias("avg_item_level"),
            "avg_crit_rating",
            "avg_haste_rating",
            "avg_mastery_rating",
            "avg_versatility_rating",
            "latest_kill_date",
        )
        .orderBy("role", "player_name", "spec")
    )


# ── Boss Ability Deaths ────────────────────────────────────────────────────────
# "What is actually killing players on each boss?"
#
# Joins fact_player_events (death events with killing_blow_name) to
# silver_fight_events to get boss context (encounter_id, boss_name) from fight_id.
# Aggregates death counts by boss + ability to surface the most dangerous mechanics.

@dlt.table(
    name="gold_boss_ability_deaths",
    comment=(
        "Death counts per ability per boss encounter across all raids. "
        "Shows which boss mechanics are killing the most players. "
        "One row per (encounter, difficulty, killing_blow_name). "
        "killing_blow_name is NULL when the killing hit was from a friendly source "
        "or when the events array was empty."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "encounter_id",
    },
)
def gold_boss_ability_deaths():
    deaths = dlt.read("fact_player_events")
    fights = dlt.read("silver_fight_events")

    # Fight context — just the boss identity columns keyed by (report_code, fight_id)
    fight_context = (
        fights
        .filter(F.col("encounter_id").isNotNull() & (F.col("encounter_id") > 0))
        .select(
            "report_code",
            "fight_id",
            "encounter_id",
            "boss_name",
            "difficulty",
            "difficulty_label",
            "zone_name",
            "is_kill",
        )
        .dropDuplicates(["report_code", "fight_id"])
    )

    # Drop columns from deaths that fight_context will supply to avoid ambiguous refs.
    # fact_player_events carries zone_name/zone_id from the report-level guild_reports join;
    # fight_context has the same values from the fight-level silver_fight_events join.
    deaths_slim = deaths.drop("zone_name", "zone_id")

    # Join deaths to fight context — deaths can occur on wipes OR kills
    deaths_with_boss = (
        deaths_slim
        .join(fight_context, ["report_code", "fight_id"], "inner")
    )

    # Aggregate: death counts per boss ability (includes wipes + kills for full picture)
    ability_deaths = (
        deaths_with_boss
        .groupBy(
            "encounter_id", "boss_name", "zone_name", "difficulty", "difficulty_label",
            "killing_blow_name", "killing_blow_id",
        )
        .agg(
            F.count("*").alias("total_deaths"),
            F.countDistinct("player_name").alias("unique_players_killed"),
            F.countDistinct("report_code").alias("reports_with_deaths"),
            F.sum(F.when(F.col("is_kill") == True, 1).otherwise(0)).alias("deaths_on_kills"),  # noqa: E712
            F.sum(F.when(F.col("is_kill") == False, 1).otherwise(0)).alias("deaths_on_wipes"),  # noqa: E712
        )
    )

    # Rank abilities within each encounter by total deaths
    w = Window.partitionBy("encounter_id", "difficulty").orderBy(F.col("total_deaths").desc())

    return (
        ability_deaths
        .withColumn("death_rank", F.row_number().over(w))
        .select(
            "encounter_id",
            "boss_name",
            "zone_name",
            "difficulty",
            "difficulty_label",
            "death_rank",
            "killing_blow_name",
            "killing_blow_id",
            "total_deaths",
            "unique_players_killed",
            "deaths_on_kills",
            "deaths_on_wipes",
            "reports_with_deaths",
        )
        .orderBy("zone_name", "encounter_id", "difficulty", "death_rank")
    )
