# Databricks notebook source
# Gold layer — Raider.IO Mythic+ data products

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="03_gold.sc_analytics.gold_player_mplus_score_history",
    comment="Raider.IO Mythic+ score snapshots over time.",
    table_properties={"quality": "gold"},
)
def gold_player_mplus_score_history():
    scores = spark.read.table("02_silver.sc_analytics_raiderio.silver_raiderio_player_scores")  # noqa: F821
    return (
        scores.select(
            "player_name",
            "realm_slug",
            "region",
            "profile_url",
            "season",
            F.col("snapshot_at").cast("timestamp").alias("snapshot_at"),
            F.to_date("snapshot_at").alias("snapshot_date"),
            F.round("score_all", 1).alias("score_all"),
            F.round("score_dps", 1).alias("score_dps"),
            F.round("score_healer", 1).alias("score_healer"),
            F.round("score_tank", 1).alias("score_tank"),
            "world_rank",
            "region_rank",
            "realm_rank",
            "raiderio_last_crawled_at",
        )
        .filter(F.col("player_name").isNotNull())
        .orderBy("player_name", "snapshot_at")
    )


@dlt.table(
    name="03_gold.sc_analytics.gold_player_mplus_run_history",
    comment="Governed Raider.IO Mythic+ run history from recent/best run payloads.",
    table_properties={"quality": "gold"},
)
def gold_player_mplus_run_history():
    runs = spark.read.table("02_silver.sc_analytics_raiderio.silver_raiderio_player_runs")  # noqa: F821
    return (
        runs.select(
            "player_name",
            "realm_slug",
            "region",
            "season",
            "source",
            "dungeon",
            "short_name",
            "mythic_level",
            "score",
            "completed_at",
            F.to_date("completed_at").alias("completed_date"),
            "clear_time_ms",
            "par_time_ms",
            "num_keystone_upgrades",
            "timed",
            "url",
            "snapshot_at",
        )
        .filter(F.col("dungeon").isNotNull())
        .orderBy(F.col("completed_at").desc_nulls_last(), "player_name")
    )


@dlt.table(
    name="03_gold.sc_analytics.gold_player_mplus_summary",
    comment="Latest Raider.IO Mythic+ summary per player.",
    table_properties={"quality": "gold"},
)
def gold_player_mplus_summary():
    scores = spark.read.table("02_silver.sc_analytics_raiderio.silver_raiderio_player_scores")  # noqa: F821
    runs = spark.read.table("02_silver.sc_analytics_raiderio.silver_raiderio_player_runs")  # noqa: F821

    latest_scores = (
        scores.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("player_name", "realm_slug", "region", "season").orderBy(
                    F.col("snapshot_at").desc_nulls_last()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    run_counts = runs.groupBy("player_name", "realm_slug", "region", "season").agg(
        F.count("*").alias("total_runs"),
        F.sum(F.when(F.col("timed") == True, F.lit(1)).otherwise(F.lit(0))).alias("timed_runs"),  # noqa: E712
        F.sum(F.when(~F.col("timed"), F.lit(1)).otherwise(F.lit(0))).alias("untimed_runs"),
        F.max(F.when(F.col("timed") == True, F.col("mythic_level"))).alias("highest_timed_level"),  # noqa: E712
        F.max(F.when(~F.col("timed"), F.col("mythic_level"))).alias("highest_untimed_level"),
    )

    best_run = (
        runs.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("player_name", "realm_slug", "region", "season").orderBy(
                    F.col("timed").desc(),
                    F.col("mythic_level").desc_nulls_last(),
                    F.col("score").desc_nulls_last(),
                    F.col("completed_at").desc_nulls_last(),
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "player_name",
            "realm_slug",
            "region",
            "season",
            F.col("dungeon").alias("best_run_dungeon"),
            F.col("short_name").alias("best_run_short_name"),
            F.col("mythic_level").alias("best_run_level"),
            F.col("score").alias("best_run_score"),
            F.col("timed").alias("best_run_timed"),
            F.col("completed_at").alias("best_run_completed_at"),
            F.col("url").alias("best_run_url"),
        )
    )

    key_counts = runs.groupBy("player_name", "realm_slug", "region", "season", "mythic_level").agg(
        F.count("*").alias("_key_count")
    )
    most_common_key = (
        key_counts.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("player_name", "realm_slug", "region", "season").orderBy(
                    F.col("_key_count").desc(), F.col("mythic_level").desc_nulls_last()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "player_name",
            "realm_slug",
            "region",
            "season",
            F.col("mythic_level").alias("most_common_key_level"),
            F.col("_key_count").alias("most_common_key_count"),
        )
    )

    return (
        latest_scores.join(run_counts, ["player_name", "realm_slug", "region", "season"], "left")
        .join(best_run, ["player_name", "realm_slug", "region", "season"], "left")
        .join(most_common_key, ["player_name", "realm_slug", "region", "season"], "left")
        .select(
            "player_name",
            "realm_slug",
            "region",
            "profile_url",
            "season",
            "snapshot_at",
            F.round("score_all", 1).alias("score_all"),
            F.round("score_dps", 1).alias("score_dps"),
            F.round("score_healer", 1).alias("score_healer"),
            F.round("score_tank", 1).alias("score_tank"),
            "world_rank",
            "region_rank",
            "realm_rank",
            F.coalesce(F.col("total_runs"), F.lit(0)).alias("total_runs"),
            F.coalesce(F.col("timed_runs"), F.lit(0)).alias("timed_runs"),
            F.coalesce(F.col("untimed_runs"), F.lit(0)).alias("untimed_runs"),
            "highest_timed_level",
            "highest_untimed_level",
            "most_common_key_level",
            F.coalesce(F.col("most_common_key_count"), F.lit(0)).alias("most_common_key_count"),
            "best_run_dungeon",
            "best_run_short_name",
            "best_run_level",
            F.round("best_run_score", 1).alias("best_run_score"),
            "best_run_timed",
            "best_run_completed_at",
            "best_run_url",
        )
        .orderBy(F.col("score_all").desc_nulls_last(), "player_name")
    )


@dlt.table(
    name="03_gold.sc_analytics.gold_player_mplus_weekly_activity",
    comment="Weekly Mythic+ activity derived from Raider.IO run payloads.",
    table_properties={"quality": "gold"},
)
def gold_player_mplus_weekly_activity():
    runs = spark.read.table("02_silver.sc_analytics_raiderio.silver_raiderio_player_runs")  # noqa: F821
    key_counts = (
        runs.withColumn("week_start", F.to_date(F.date_trunc("week", F.col("completed_at"))))
        .groupBy("player_name", "realm_slug", "region", "season", "week_start", "mythic_level")
        .agg(F.count("*").alias("_key_count"))
    )
    most_common_key = (
        key_counts.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(
                    "player_name", "realm_slug", "region", "season", "week_start"
                ).orderBy(F.col("_key_count").desc(), F.col("mythic_level").desc_nulls_last())
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "player_name",
            "realm_slug",
            "region",
            "season",
            "week_start",
            F.col("mythic_level").alias("most_common_key_level"),
        )
    )

    weekly = (
        runs.withColumn("week_start", F.to_date(F.date_trunc("week", F.col("completed_at"))))
        .groupBy("player_name", "realm_slug", "region", "season", "week_start")
        .agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("timed") == True, F.lit(1)).otherwise(F.lit(0))).alias("timed_runs"),  # noqa: E712
            F.sum(F.when(~F.col("timed"), F.lit(1)).otherwise(F.lit(0))).alias("untimed_runs"),
            F.max("mythic_level").alias("highest_key_level"),
            F.countDistinct("dungeon").alias("unique_dungeons"),
        )
    )

    return weekly.join(
        most_common_key, ["player_name", "realm_slug", "region", "season", "week_start"], "left"
    ).orderBy("player_name", "week_start")


@dlt.table(
    name="03_gold.sc_analytics.gold_player_mplus_dungeon_breakdown",
    comment="Per-player per-dungeon Mythic+ summary from Raider.IO run payloads.",
    table_properties={"quality": "gold"},
)
def gold_player_mplus_dungeon_breakdown():
    runs = spark.read.table("02_silver.sc_analytics_raiderio.silver_raiderio_player_runs")  # noqa: F821

    best_runs = (
        runs.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(
                    "player_name", "realm_slug", "region", "season", "dungeon"
                ).orderBy(
                    F.col("timed").desc(),
                    F.col("mythic_level").desc_nulls_last(),
                    F.col("score").desc_nulls_last(),
                    F.col("completed_at").desc_nulls_last(),
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "player_name",
            "realm_slug",
            "region",
            "season",
            "dungeon",
            F.col("short_name").alias("best_short_name"),
            F.col("mythic_level").alias("best_key_level"),
            F.col("score").alias("best_score"),
            F.col("timed").alias("best_timed"),
            F.col("clear_time_ms").alias("best_clear_time_ms"),
            F.col("par_time_ms").alias("best_par_time_ms"),
            F.col("completed_at").alias("best_completed_at"),
            F.col("url").alias("best_run_url"),
        )
    )

    agg = runs.groupBy("player_name", "realm_slug", "region", "season", "dungeon").agg(
        F.max("mythic_level").alias("highest_key_level"),
        F.max(F.when(F.col("timed") == True, F.col("mythic_level"))).alias("highest_timed_level"),  # noqa: E712
        F.count("*").alias("total_runs"),
        F.sum(F.when(F.col("timed") == True, F.lit(1)).otherwise(F.lit(0))).alias("timed_runs"),  # noqa: E712
        F.sum(F.when(F.col("timed") == False, F.lit(1)).otherwise(F.lit(0))).alias("untimed_runs"),  # noqa: E712
        F.max("completed_at").alias("latest_completed_at"),
    )

    return (
        agg.join(best_runs, ["player_name", "realm_slug", "region", "season", "dungeon"], "left")
        .select(
            "player_name",
            "realm_slug",
            "region",
            "season",
            "dungeon",
            "best_short_name",
            "highest_key_level",
            "highest_timed_level",
            "total_runs",
            "timed_runs",
            "untimed_runs",
            "latest_completed_at",
            "best_key_level",
            F.round("best_score", 1).alias("best_score"),
            "best_timed",
            "best_clear_time_ms",
            "best_par_time_ms",
            "best_completed_at",
            "best_run_url",
        )
        .orderBy("player_name", F.col("best_score").desc_nulls_last(), "dungeon")
    )
