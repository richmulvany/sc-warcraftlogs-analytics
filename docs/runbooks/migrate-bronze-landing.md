# Runbook: Migrate Bronze Landing Volumes

> Archive-only: this migration is complete. Keep this document only for historical recovery or environment rebuilds.

## Goal

Move the existing raw landing data from the shared legacy volume:

- `/Volumes/04_sdp/warcraftlogs/landing/*`

into the source-matched bronze landing volumes:

- `/Volumes/01_bronze/warcraftlogs/landing/*`
- `/Volumes/01_bronze/blizzard/landing/*`
- `/Volumes/01_bronze/raiderio/landing/*`
- `/Volumes/01_bronze/google_sheets/landing/*`

without rerunning hours of ingestion.

## One-time sequence

1. Deploy the bundle so Databricks has the migration notebook:

```bash
databricks bundle deploy
```

2. Run the copy job:

```bash
databricks bundle run migrate_bronze_landing_volumes
```

Or run [ingestion/jobs/migrate_bronze_landing_volumes.py](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/ingestion/jobs/migrate_bronze_landing_volumes.py) directly as a notebook.

3. Confirm the copy succeeded.

Spot checks:

```python
for path in [
    "/Volumes/01_bronze/warcraftlogs/landing/guild_reports",
    "/Volumes/01_bronze/blizzard/landing/guild_members",
    "/Volumes/01_bronze/raiderio/landing/raiderio_character_profiles",
    "/Volumes/01_bronze/google_sheets/landing/live_raid_roster",
]:
    print(path, len(dbutils.fs.ls(path)))
```

4. Run the DLT pipeline as a **full refresh**.

This is required because the bronze streaming tables are now reading from different source paths. A normal incremental run risks treating the copied files as new input and appending duplicates.

5. After the full refresh completes, run the ingestion job normally.

At that point new raw files will land directly in the split bronze volumes and the legacy landing volume is no longer on the hot path.

## Copy mapping

- WarcraftLogs subdirs stay in `01_bronze.warcraftlogs`
- `guild_members`, `character_*`, `item_media` move to `01_bronze.blizzard`
- `raiderio_character_profiles` moves to `01_bronze.raiderio`
- `live_raid_roster` moves to `01_bronze.google_sheets`
- `archived` markers move to `01_bronze.warcraftlogs`

## Validation

After the full refresh:

```sql
SELECT COUNT(*) FROM 01_bronze.warcraftlogs.bronze_guild_reports;
SELECT COUNT(*) FROM 01_bronze.blizzard.bronze_guild_members;
SELECT COUNT(*) FROM 01_bronze.raiderio.bronze_raiderio_character_profiles;
SELECT COUNT(*) FROM 01_bronze.google_sheets.bronze_live_raid_roster;
```

And verify that new ingestion writes appear under the split paths rather than `/Volumes/04_sdp/warcraftlogs/landing/`.
