# Runbook: Re-run Ingestion

## When to use
- Pipeline tables are empty or stale after a failed run
- New reports were added to WarcraftLogs and haven't been ingested yet
- You want to force a re-fetch of previously cached data

---

## Normal re-run (incremental)

Simply trigger the job again. Ingestion is idempotent — it skips any report that already has a JSONL file in the landing volume.

```bash
databricks bundle run nightly_ingestion
```

New reports since the last run will be picked up automatically.

---

## Common failure causes

| Symptom | Cause | Fix |
|---------|-------|-----|
| HTTP 429 in logs | WCL rate limit hit | Ingestion retries automatically via Retry-After header. If job exits mid-run, re-run it — cached files prevent duplicates. |
| HTTP 401 in logs | Token expired or invalid client credentials | Re-run — token refresh is proactive. If persistent, rotate secrets: `databricks secrets put-secret pipeline-secrets wcl_client_id --string-value "..."` |
| `ArchivedReportError` in logs | WCL report is archived | Expected behaviour. Skip marker written to `landing/archived/{code}`. Report won't be retried. |
| `AMBIGUOUS_REFERENCE` in DLT | Column name clash in a gold table join | Fix the join in the relevant gold Python file. See `docs/architecture/overview.md`. |
| `TABLE_DOES_NOT_EXIST: Staging Table` | Stale DLT pipeline state | Run `databricks bundle destroy && databricks bundle deploy`, then full-refresh. |
| Empty `silver_fight_events` | Old report_fights JSONL files missing `encounterID` | Delete `landing/report_fights/` files and re-run ingestion. |

---

## Force re-fetch a specific report

Delete the cached JSONL files for that report from the landing volume, then re-run ingestion:

```python
# Run in a Databricks notebook
report_code = "aBcD1234"
catalog, schema = "04_sdp", "warcraftlogs"
base = f"/Volumes/{catalog}/{schema}/landing"

for subdir in ["report_fights", "actor_roster", "fight_rankings", "fight_deaths"]:
    try:
        dbutils.fs.rm(f"{base}/{subdir}/{report_code}.jsonl")
    except Exception:
        pass

# player_details are per fight — delete all for this report
for f in dbutils.fs.ls(f"{base}/player_details/"):
    if f.name.startswith(report_code):
        dbutils.fs.rm(f.path)
```

---

## Remove an archived report marker

If WCL un-archives a report, delete its skip marker:

```python
dbutils.fs.rm(f"/Volumes/04_sdp/warcraftlogs/landing/archived/{report_code}")
```

Then re-run ingestion.

---

## Full landing reset

**Warning**: Deletes all cached landing data. Only use if tables are completely broken.

```python
catalog, schema = "04_sdp", "warcraftlogs"
base = f"/Volumes/{catalog}/{schema}/landing"

for subdir in ["guild_reports", "report_fights", "actor_roster", "player_details",
               "raid_attendance", "fight_rankings", "fight_deaths", "zone_catalog", "guild_members"]:
    try:
        for f in dbutils.fs.ls(f"{base}/{subdir}/"):
            dbutils.fs.rm(f.path)
    except Exception:
        pass
# Note: archived/ markers are NOT deleted — skip markers are intentional
```

Then trigger pipeline full-refresh in the UI (Pipeline → Full Refresh).

---

## Full pipeline state reset (last resort)

If full-refresh doesn't clear DLT staging table errors:

```bash
databricks bundle destroy
databricks bundle deploy
# Then Full Refresh in UI
```
