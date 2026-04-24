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
python3 -m py_compile scripts/export_gold_tables.py ingestion/src/adapters/wcl/client.py pipeline/silver/clean_events.py
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

Run the ingestion job once to populate the landing volume:

```bash
databricks bundle run nightly_ingestion
```

What it writes:
- `/Volumes/{catalog}/{schema}/landing/guild_reports/`
- `/Volumes/{catalog}/{schema}/landing/report_fights/`
- `/Volumes/{catalog}/{schema}/landing/actor_roster/`
- `/Volumes/{catalog}/{schema}/landing/player_details/`
- `/Volumes/{catalog}/{schema}/landing/raid_attendance/`
- `/Volumes/{catalog}/{schema}/landing/guild_members/`
- `/Volumes/{catalog}/{schema}/landing/raiderio_character_profiles/`
- `/Volumes/{catalog}/{schema}/landing/fight_rankings/`
- `/Volumes/{catalog}/{schema}/landing/fight_casts/`
- `/Volumes/{catalog}/{schema}/landing/fight_deaths/`

## 7. Run the DLT pipeline

Start an update from Databricks after ingestion has landed files.

Expected high-level flow:
- Bronze ingests raw JSONL from the landing volume
- Silver parses and normalises
- Gold builds frontend-facing analytics tables

If you are recovering from broken state or major source changes, use a full refresh only when required.

## 8. Export frontend datasets

The frontend reads static CSVs from `frontend/public/data`.

Run:

```bash
.venv/bin/python scripts/export_gold_tables.py
```

This exports the governed datasets used by the dashboard, including:
- progression and wipe analysis CSVs
- Raider.IO exports
- player profile media/equipment exports
- wipe utility exports such as `gold_wipe_survival_events.csv`
- cooldown capacity exports such as `gold_wipe_cooldown_utilization.csv`

## 9. Run the frontend

```bash
cd frontend
npm run dev
```

For a production check:

```bash
npm run build
```

## 10. Current operational gotchas

- `fight_deaths` must be fetched one fight at a time. Multi-fight WCL death table requests truncate on long reports.
- `silver_player_deaths` prefers single-fight death records when both legacy and backfilled files exist for a report.
- The Wipe Analysis utility panels are export-derived. Avoid creating extra DLT gold tables for them unless there is a strong reason.
- Databricks Free object limits matter. Hidden DLT materialization tables count against the schema quota.

## First validation checks

After a full first run, confirm:

```sql
SELECT COUNT(*) FROM 04_sdp.warcraftlogs.silver_fight_events;
SELECT COUNT(*) FROM 04_sdp.warcraftlogs.silver_player_cast_events;
SELECT COUNT(*) FROM 04_sdp.warcraftlogs.silver_player_deaths;
SELECT COUNT(*) FROM 04_sdp.warcraftlogs.gold_player_death_events;
```

And locally:

```bash
ls frontend/public/data
```

You should see the exported CSVs used by the dashboard.

## When setup is not enough

Use these next:
- [README.md](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/README.md)
- [docs/architecture/overview.md](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/docs/architecture/overview.md)
- [docs/runbooks/rerun-ingestion.md](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/docs/runbooks/rerun-ingestion.md)
