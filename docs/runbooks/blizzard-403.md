# Runbook: Blizzard 403 / Auth Failures

## Symptoms

- `guild_members` is missing after ingestion
- Blizzard profile media / equipment / achievements are stale
- ingestion logs show `403`, `401`, or OAuth token failures from Blizzard

## First check

Blizzard failures should no longer block the other ingestion stages. Confirm whether only the Blizzard task failed:

```bash
databricks bundle run ingestion_daily
```

In the Databricks Jobs UI, inspect `ingest_blizzard`.

## Common causes

- expired or rotated `blizzard_client_id` / `blizzard_client_secret`
- wrong region for the guild realm
- Blizzard temporary permission or availability issue

## Recovery

1. Rotate the Blizzard secrets in the `warcraftlogs` Databricks secret scope.
2. Re-run only the Blizzard stage.

Re-run only `ingest_blizzard` from the Databricks Jobs UI, or run
[ingestion/jobs/ingest_blizzard.py](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/ingestion/jobs/ingest_blizzard.py)
directly as a notebook.

3. Re-run the DLT pipeline if guild membership or Blizzard profile dimensions need to refresh immediately.

## Validation

```sql
SELECT max(snapshot_at) FROM 02_silver.sc_analytics_blizzard.silver_character_media;
SELECT max(snapshot_at) FROM 02_silver.sc_analytics_blizzard.silver_character_equipment;
SELECT max(snapshot_at) FROM 02_silver.sc_analytics_blizzard.silver_character_achievements;
```
