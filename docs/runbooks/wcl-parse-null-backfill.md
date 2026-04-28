# Runbook: WCL Parse-Null Backfill

## When to use

- `silver_player_rankings` has a spike in null `rank_percent`
- `gold_parse_completeness_daily` shows a sustained regression
- a newly uploaded report still has incomplete parse rows after the first ingestion pass

## What happens

The WCL rankings payload can arrive before Warcraft Logs finishes asynchronous parse calculation. The ingestion job writes the payload, measures null completeness, and re-fetches incomplete report files until either:

- all fight rows are populated, or
- the file ages past `RANKINGS_BACKFILL_MAX_AGE_DAYS`

Decision logs look like:

```text
fight_rankings_decision decision=refetch report_code=... incomplete_fights=... total_fights=... null_chars=... total_chars=... file_age_days=...
```

## Re-run only the WCL stage

Re-run only the `ingest_wcl` task from the Databricks Jobs UI, or run
[ingestion/jobs/ingest_wcl.py](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/ingestion/jobs/ingest_wcl.py)
directly as a notebook.

## Force a specific report to refetch rankings

Delete the cached rankings file for the report:

```python
report_code = "aBcD1234"
for f in dbutils.fs.ls("/Volumes/01_bronze/warcraftlogs/landing/fight_rankings/"):
    if f.name.startswith(report_code):
        dbutils.fs.rm(f.path)
```

Then rerun `ingest_wcl` and the DLT pipeline.

## Validation

```sql
SELECT raid_night_date, role, null_rank_pct
FROM 03_gold.sc_analytics.gold_parse_completeness_daily
ORDER BY raid_night_date DESC, role;
```

```sql
SELECT count(*)
FROM 02_silver.sc_analytics_warcraftlogs.silver_player_rankings
WHERE rank_percent IS NULL;
```
