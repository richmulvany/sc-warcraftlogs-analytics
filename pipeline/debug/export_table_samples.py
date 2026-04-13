# Databricks notebook source
# Debug utility — exports row-count summaries and 50-row CSV samples for every
# pipeline table to debug_exports/ in the repo root so they can be committed
# and read outside Databricks.
#
# Run this notebook manually after a pipeline update to share table state.
# Output path: {repo_root}/debug_exports/{layer}/{table_name}.csv
#
# Usage: Run All.  Check the printed summary, then `git add debug_exports/`
#        and commit on your branch.

# COMMAND ----------
import os
import sys

# Resolve repo root from this notebook's workspace path.
# Notebook is at: pipeline/debug/export_table_samples
# [:-3] strips the last 3 path segments → repo root.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_nb_path = _ctx.notebookPath().get()
_repo_root = "/Workspace" + "/".join(_nb_path.split("/")[:-3])

EXPORT_DIR = f"{_repo_root}/debug_exports"
SAMPLE_ROWS = 50

catalog = spark.conf.get("pipelines.catalog", "04_sdp")   # noqa: F821
schema  = spark.conf.get("pipelines.schema",  "warcraftlogs")  # noqa: F821

print(f"Repo root  : {_repo_root}")
print(f"Export dir : {EXPORT_DIR}")
print(f"Catalog    : {catalog}.{schema}")

# COMMAND ----------
# Table inventory — (layer, table_name) pairs in dependency order.
# Add or remove tables here as the pipeline evolves.

TABLES = [
    # ── Bronze ────────────────────────────────────────────────────────────────
    ("bronze", "bronze_guild_reports"),
    ("bronze", "bronze_report_fights"),
    ("bronze", "bronze_raid_attendance"),
    ("bronze", "bronze_actor_roster"),
    ("bronze", "bronze_player_details"),
    ("bronze", "bronze_zone_catalog"),
    ("bronze", "bronze_guild_members"),
    ("bronze", "bronze_fight_rankings"),
    ("bronze", "bronze_fight_deaths"),
    # ── Silver ────────────────────────────────────────────────────────────────
    ("silver", "silver_guild_reports"),
    ("silver", "silver_fight_events"),
    ("silver", "silver_raid_attendance"),
    ("silver", "silver_actor_roster"),
    ("silver", "silver_player_performance"),
    ("silver", "silver_player_rankings"),
    ("silver", "silver_player_deaths"),
    ("silver", "silver_zone_catalog"),
    ("silver", "silver_guild_members"),
    # ── Gold — dimensions & facts ─────────────────────────────────────────────
    ("gold", "dim_encounter"),
    ("gold", "dim_player"),
    ("gold", "dim_guild_member"),
    ("gold", "fact_player_fight_performance"),
    ("gold", "fact_player_events"),
    # ── Gold — products ───────────────────────────────────────────────────────
    ("gold", "gold_guild_roster"),
    ("gold", "gold_raid_team"),
    ("gold", "gold_player_profile"),
    ("gold", "gold_player_attendance"),
    ("gold", "gold_weekly_activity"),
    ("gold", "gold_roster"),
    ("gold", "gold_player_performance_summary"),
    ("gold", "gold_boss_kill_roster"),
    ("gold", "gold_player_boss_performance"),
    ("gold", "gold_boss_progression"),
    ("gold", "gold_raid_summary"),
    ("gold", "gold_progression_timeline"),
    ("gold", "gold_best_kills"),
    ("gold", "gold_boss_wipe_analysis"),
    ("gold", "gold_boss_mechanics"),
    ("gold", "gold_encounter_catalog"),
    ("gold", "gold_player_survivability"),
]

# COMMAND ----------
# Create output directories
for layer in ("bronze", "silver", "gold"):
    os.makedirs(f"{EXPORT_DIR}/{layer}", exist_ok=True)

# COMMAND ----------
summary_rows = []

for layer, table in TABLES:
    full_name = f"`{catalog}`.`{schema}`.`{table}`"
    out_path   = f"{EXPORT_DIR}/{layer}/{table}.csv"

    try:
        df = spark.table(full_name)  # noqa: F821
        row_count = df.count()

        # Write a pandas CSV sample directly to the workspace filesystem.
        # toPandas() on a 50-row limit is safe even for wide schemas.
        sample_pd = (
            df.limit(SAMPLE_ROWS)
            .toPandas()
        )
        sample_pd.to_csv(out_path, index=False)

        status = "ok"
        note = f"{row_count:,} rows"

    except Exception as exc:
        row_count = -1
        status = "ERROR"
        note = str(exc)[:120]
        # Write an error placeholder so the file always exists after a run
        with open(out_path, "w") as fh:
            fh.write(f"error,message\n{status},{note}\n")

    summary_rows.append({
        "layer":      layer,
        "table":      table,
        "row_count":  row_count,
        "status":     status,
        "note":       note,
    })
    print(f"  [{status:5s}] {table:<45s} {note}")

# COMMAND ----------
# Print a tidy summary table and write it as a top-level CSV.
import pandas as pd  # noqa: E402

summary_df = pd.DataFrame(summary_rows)

summary_csv = f"{EXPORT_DIR}/pipeline_summary.csv"
summary_df.to_csv(summary_csv, index=False)

print("\n── Pipeline table summary ──────────────────────────────────────")
print(summary_df.to_string(index=False))
print(f"\nSummary written to: {summary_csv}")
print(f"Samples written to: {EXPORT_DIR}/{{bronze,silver,gold}}/<table>.csv")
print("\nNext steps: git add debug_exports/ && git commit -m 'chore: export table samples'")
