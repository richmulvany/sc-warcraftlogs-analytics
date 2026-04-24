# Runbook: Re-run Ingestion

## When to use

- New WarcraftLogs reports have been published and are not in the dashboard yet
- A prior ingestion job failed mid-run
- Cached landing files are stale or incomplete
- Death counts look suspiciously low on long reports and you need a death backfill

## Normal re-run

The ingestion job is incremental. Re-running it should only fetch missing or stale assets.

```bash
databricks bundle run nightly_ingestion
```

Then run the DLT update and re-export the frontend CSVs.

## Important current behaviour

- Most per-report assets are cached by filename in the landing volume
- `zone_catalog` and `guild_members` are refreshed each run
- `fight_deaths` is now special:
  - legacy multi-fight death files can be stale because WCL truncates long `Deaths` table responses
  - the ingestion job now detects stale legacy death files
  - when detected, it writes fresh timestamped single-fight files instead of reusing the old file
- `silver_player_deaths` prefers the new single-fight death files whenever they exist for a report

## Common failure causes

| Symptom | Cause | Fix |
|---------|-------|-----|
| HTTP 429 in logs | WCL rate limit hit | Re-run the job. Retry handling is built in, but an interrupted run may still need another pass. |
| HTTP 401 in logs | Invalid or rotated credentials | Update secrets in the `warcraftlogs` scope, then re-run ingestion. |
| `ArchivedReportError` in logs | WCL report is archived | Expected. A skip marker is written under `/Volumes/01_bronze/warcraftlogs/landing/archived/`. |
| Missing later wipe deaths on long reports | Legacy multi-fight `Deaths` payload truncated by WCL | Re-run ingestion after deploying the one-fight-at-a-time death fetch fix. |
| Empty or stale silver/gold after successful ingestion | DLT update not run, or pipeline state stale | Run a pipeline update. Use full refresh only if a normal update does not recover. |

## Force re-fetch a specific report

Use this when one report is clearly wrong and you want a focused backfill.

Run in a Databricks notebook:

```python
report_code = "aBcD1234"
base = "/Volumes/01_bronze/warcraftlogs/landing"

# Per-report cached files
for subdir in ["report_fights", "actor_roster", "fight_rankings", "fight_casts"]:
    for f in dbutils.fs.ls(f"{base}/{subdir}/"):
        if f.name.startswith(report_code):
            dbutils.fs.rm(f.path)

# Death backfills can exist under legacy and timestamped names
for f in dbutils.fs.ls(f"{base}/fight_deaths/"):
    if f.name.startswith(report_code):
        dbutils.fs.rm(f.path)

# Player details are per-fight
for f in dbutils.fs.ls(f"{base}/player_details/"):
    if f.name.startswith(report_code):
        dbutils.fs.rm(f.path)
```

Then:
1. run ingestion again
2. run the DLT update
3. rerun exports

## Backfill death data after the WCL truncation fix

Use this when wipe death counts are too low on longer reports.

Recommended sequence:

1. Deploy the updated ingestion and pipeline code.
2. Re-run ingestion normally.
   - the job should log `stale/incomplete — refetching` for affected `fight_deaths`
3. Run the DLT update.
4. Re-export:
   - `gold_player_death_events.csv`
   - `gold_wipe_survival_events.csv`
   - `gold_wipe_cooldown_utilization.csv`

Validation SQL:

```sql
SELECT
  report_code,
  COUNT(*) AS bronze_rows,
  COLLECT_SET(CAST(fight_ids[0] AS INT)) AS single_fight_ids
FROM 01_bronze.warcraftlogs.bronze_fight_deaths
WHERE report_code IN ('VvyhHrk3P4Z18NKT', 'b9Afa8tD3GRmvr26')
  AND size(fight_ids) = 1
GROUP BY report_code;
```

```sql
SELECT
  report_code,
  SORT_ARRAY(COLLECT_SET(fight_id)) AS death_fights
FROM 03_gold.sc_analytics.gold_player_death_events
WHERE boss_name = 'Vaelgor & Ezzorak'
  AND difficulty_label = 'Mythic'
  AND report_code IN ('VvyhHrk3P4Z18NKT', 'b9Afa8tD3GRmvr26')
GROUP BY report_code;
```

## Remove an archived report marker

If a report is no longer archived:

```python
report_code = "aBcD1234"
dbutils.fs.rm(f"/Volumes/01_bronze/warcraftlogs/landing/archived/{report_code}")
```

Then rerun ingestion.

## Full landing reset

Only use this when the landing volume is broadly corrupted and a targeted backfill is not enough.

```python
base = "/Volumes/01_bronze/warcraftlogs/landing"

for subdir in [
    "guild_reports",
    "report_fights",
    "actor_roster",
    "player_details",
    "raid_attendance",
    "fight_rankings",
    "fight_deaths",
    "fight_casts",
    "zone_catalog",
    "guild_members",
    "raiderio_character_profiles",
]:
    try:
        for f in dbutils.fs.ls(f"{base}/{subdir}/"):
            dbutils.fs.rm(f.path)
    except Exception:
        pass
```

Do not delete `archived/` unless you intentionally want archived reports retried.

After reset:
1. rerun ingestion
2. run a DLT full refresh if needed
3. rerun exports
