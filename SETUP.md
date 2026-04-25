# Setup Guide

This guide is for bringing up the current WarcraftLogs analytics project, not a generic template.

## Prerequisites

- Databricks workspace with Unity Catalog and serverless DLT/Lakeflow support
- Databricks CLI configured locally
- Python 3.11+
- Node 18+
- WarcraftLogs API credentials
- Blizzard API credentials

## 1. Local environment

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cd frontend && npm install && cd ..
```

Optional local checks:

```bash
.venv/bin/ruff check .
python3 -m py_compile scripts/export_gold_tables.py scripts/publish_dashboard_assets.py ingestion/src/adapters/wcl/client.py ingestion/src/adapters/blizzard/client.py
cd frontend && npm run build && cd ..
```

## 2. Databricks CLI

Configure the CLI and verify access:

```bash
databricks configure --token
databricks auth test
```

## 3. Secrets

Important: the ingestion notebook currently reads secrets from the `warcraftlogs` scope.

Create that scope and add the required keys:

```bash
databricks secrets create-scope warcraftlogs
databricks secrets put-secret warcraftlogs client_id --string-value "..."
databricks secrets put-secret warcraftlogs client_secret --string-value "..."
databricks secrets put-secret warcraftlogs blizzard_client_id --string-value "..."
databricks secrets put-secret warcraftlogs blizzard_client_secret --string-value "..."
```

Notes:
- `client_id` / `client_secret` are for WarcraftLogs
- `blizzard_client_id` / `blizzard_client_secret` are for Blizzard profile exports
- Raider.IO does not require credentials

## 4. Bundle variables

Review `databricks.yml` and set the guild-specific values you actually want to ingest:

- `catalog`
- `schema`
- `profile_candidate_catalog`
- `profile_candidate_schema`
- `guild_name`
- `guild_server_slug`
- `guild_server_region`
- `developer_user`

The current defaults are wired to the maintainer's development environment and guild.

## 5. Deploy pipeline and job

```bash
databricks bundle deploy
```

This deploys:
- the DLT pipeline
- the nightly ingestion job

## 6. First ingestion run

Run the ingestion job once to populate the split bronze landing volumes:

```bash
databricks bundle run nightly_ingestion
```

What it writes:
- `/Volumes/01_bronze/warcraftlogs/landing/{guild_reports,report_fights,actor_roster,player_details,raid_attendance,zone_catalog,fight_rankings,fight_casts,fight_deaths,guild_zone_ranks,archived}/`
- `/Volumes/01_bronze/blizzard/landing/{guild_members,character_media,character_equipment,character_achievements,item_media}/`
- `/Volumes/01_bronze/raiderio/landing/raiderio_character_profiles/`
- `/Volumes/01_bronze/google_sheets/landing/live_raid_roster/`

If you are migrating from the old shared landing volume, run:

```bash
databricks bundle run migrate_bronze_landing_volumes
```

Then run the DLT pipeline as a **full refresh** once so the bronze streaming tables rebuild from the new source paths.

## 7. Run the DLT pipeline

Start an update from Databricks after ingestion has landed files.

Expected high-level flow:
- Bronze ingests raw JSONL from the landing volume
- Silver parses and normalises
- Gold builds frontend-facing analytics tables

If you are recovering from broken state or major source changes, use a full refresh only when required.

## 8. Publish frontend datasets

The preferred path is:

- Databricks gold tables
- `scripts/publish_dashboard_assets.py`
- JSON datasets + `manifest.json` in `/Volumes/03_gold/sc_analytics/dashboard_exports/`
- GitHub Actions publishes those files to Cloudflare R2
- the frontend reads them at runtime via `VITE_DASHBOARD_DATA_BASE_URL`

Run:

```bash
databricks bundle run publish_dashboard_assets
```

This writes dashboard-ready JSON assets from persisted gold tables in `03_gold.sc_analytics` into the UC Volume export path.

Local CSV export still exists as a compatibility fallback:

```bash
.venv/bin/python scripts/export_gold_tables.py
```

Use that only when you explicitly want local static assets for development or rollback.

## 9. Run the frontend

```bash
cd frontend
npm run dev
```

Remote JSON mode:

```bash
cp .env.example .env.local
```

Set:

```bash
VITE_DASHBOARD_DATA_BASE_URL=https://data.sc-analytics.org/latest
```

If `VITE_DASHBOARD_DATA_BASE_URL` is not set, the frontend falls back to local static CSVs under `/data`.

For a production check:

```bash
npm run build
```

## 10. Current operational gotchas

- `fight_deaths` must be fetched one fight at a time. Multi-fight WCL death table requests truncate on long reports.
- `silver_player_deaths` prefers single-fight death records when both legacy and backfilled files exist for a report.
- Databricks Free object limits matter. Hidden DLT materialization tables count against the schema quota.
- The pipeline publishes into split catalogs/schemas. Leave the top-level bundle `catalog/schema` alone for the existing pipeline unless you are intentionally recreating it.

## First validation checks

After a full first run, confirm:

```sql
SELECT COUNT(*) FROM 02_silver.sc_analytics_warcraftlogs.silver_fight_events;
SELECT COUNT(*) FROM 02_silver.sc_analytics_warcraftlogs.silver_player_cast_events;
SELECT COUNT(*) FROM 02_silver.sc_analytics_warcraftlogs.silver_player_deaths;
SELECT COUNT(*) FROM 03_gold.sc_analytics.gold_player_death_events;
```

And in the published dashboard export:

```bash
databricks fs ls dbfs:/Volumes/03_gold/sc_analytics/dashboard_exports/latest
```

You should see `manifest.json` plus the exported dataset JSON files.

## When setup is not enough

Use these next:
- [README.md](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/README.md)
- [docs/architecture/overview.md](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/docs/architecture/overview.md)
- [docs/runbooks/rerun-ingestion.md](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/docs/runbooks/rerun-ingestion.md)
