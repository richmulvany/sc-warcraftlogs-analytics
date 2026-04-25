# Architecture Overview

## System Design

Bronze → Silver → Gold medallion architecture on Databricks serverless DLT. A nightly ingestion job pulls from WarcraftLogs, Blizzard, Raider.IO, and Google Sheets, landing raw data as JSONL in source-matched Unity Catalog Volumes under `01_bronze`. DLT Auto Loader streams these into Delta tables. A publish step materialises dashboard-ready JSON assets plus `manifest.json` into a UC Volume, GitHub Actions mirrors those assets to Cloudflare R2, and the React dashboard fetches them at runtime.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Databricks Workspace                      │
│                                                                   │
│  ┌─────────────────┐    ┌───────────────────────────────────┐   │
│  │  ingest_primary │    │     DLT Pipeline (Lakeflow)        │   │
│  │  (Workflow job) │───>│  Bronze ──> Silver ──> Gold        │   │
│  │  source fan-out │    │ source-split source-split 40+ tables│  │
│  └─────────────────┘    └──────────────────┬──────────────────┘  │
│         ▲                                  │                      │
│  ┌──────┴──────┐                           │                      │
│  │  WCL API    │                           │                      │
│  │  Blizzard   │                           │                      │
│  │  Raider.IO  │                           │ dashboard asset      │
│  │  Google     │                           │ publisher            │
└──┴─────────────┴───────────────────────────┼──────────────────────┘
                                             │
                                             ▼
                                    ┌──────────────────────┐
                                    │  UC Volume JSON      │
                                    │  manifest + datasets │
                                    └───────┬──────────────┘
                                            │ GitHub Actions
                                            ▼
                                  ┌──────────────────────┐
                                  │   Cloudflare R2      │
                                  │ public static assets │
                                  └──────────┬───────────┘
                                             │
                                             ▼
                                    ┌─────────────────┐
                                    │ React Frontend  │
                                    │ (Vite + TS)     │
                                    └─────────────────┘
```

## Layer Responsibilities

### Ingestion (`ingestion/jobs/ingest_primary.py`)
- Orchestrates report discovery plus per-source fetch steps
- Authenticates via OAuth2 client credentials where required (WCL + Blizzard)
- Fetches Raider.IO current-season Mythic+ profiles through the public character profile API
- Writes raw records as JSONL to source-matched Unity Catalog Volume landing zones in `01_bronze`
- Handles rate limiting (Retry-After header on 429), 5xx backoff, token refresh
- Marks archived WCL reports with skip files to prevent re-processing
- Caches per-report data (skips if JSONL already present for that report code)
- Fetches WCL death tables one fight at a time to avoid multi-fight truncation on long reports
- Fetches WCL cast events and combatant info for utility and cooldown analysis

### Bronze (`pipeline/bronze/*.py`)
- Auto Loader streaming tables reading from source-matched Volume landing zones
- Explicit `StructType` schemas — never `inferSchema` (required for empty-directory tolerance)
- Adds `_ingested_at` (file modification time) and `_file_path` metadata columns
- `@dlt.expect_or_drop` on critical keys (report_code, ingestion timestamp)
- Opaque JSON scalars (playerDetails, rankings, deaths) stored as raw strings for silver to parse
- Cast event payloads are landed separately for downstream utility analysis

### Silver (`pipeline/silver/clean_*.py`)
- Parses opaque JSON strings with explicit `StructType` schemas via `F.from_json()`
- Explodes role-based arrays (dps/healers/tanks) separately then unions — avoids cross-products
- Deduplicates on natural keys (report_code + fight_id + player_name for performance data)
- `@dlt.expect_or_drop` for data quality enforcement
- Derives human-readable labels: rank names from rank IDs, class names from class IDs, difficulty names from difficulty integers (3=Normal, 4=Heroic, 5=Mythic)
- Enriches fact tables with dimension context (zone_name, raid_night_date joined from silver_guild_reports)
- Parses Raider.IO opaque profile JSON into score snapshots and run-level Mythic+ rows
- `silver_player_deaths` prefers single-fight death backfills over legacy multi-fight bronze rows for the same report
- `silver_player_cast_events` normalises cast streams for healthstone, potion, and cooldown analysis

### Gold (`pipeline/gold/`)
- Gold tables publish into `03_gold.sc_analytics`
- **Dimension tables** (`core_dimensions.py`): deduplicated, enriched, case-insensitive joins for guild membership
- **Fact tables** (`core_facts.py`): join player performance + fight context + WCL rankings into a single denormalised row per player per kill fight
- **Aggregation tables**: group/window on facts and dimensions to produce business metrics
- **Mythic+ tables** (`mplus_products.py`): latest Raider.IO score summary, score history, run history, weekly activity, and dungeon breakdown
- Z-ordered on high-cardinality join columns (`encounter_id`, `player_name`) for query performance

## Key Design Decisions

See [ADR index](../adr/README.md) for documented decisions. Highlights:

| Decision | Choice | Reason |
|----------|--------|--------|
| Pipeline engine | DLT (Lakeflow) | Declarative, built-in lineage, serverless-compatible |
| Frontend data | Published JSON manifest + datasets | No backend needed; CDN-cached; frontend remains statically hostable |
| Ingestion pattern | Pluggable adapters + JSONL landing | Decouples API shape from pipeline; easy to add sources |
| JSON scalar handling | Store as string in bronze, parse in silver | Preserves raw payload; allows schema changes in silver without touching bronze |
| Type handling | All integers as LongType | Spark JSON inference always uses Long; avoids cast errors |

## Data Flow (detailed)

```
1. ingest_primary.py (Databricks Workflow, nightly)
   ├── Step 1: zone catalog   → /Volumes/01_bronze/warcraftlogs/landing/zone_catalog/
   ├── Step 2: guild reports  → /Volumes/01_bronze/warcraftlogs/landing/guild_reports/
   ├── Step 3: per report:
   │     fight manifest       → /Volumes/01_bronze/warcraftlogs/landing/report_fights/{code}.jsonl
   │     actor roster         → /Volumes/01_bronze/warcraftlogs/landing/actor_roster/{code}.jsonl
   │     player details       → /Volumes/01_bronze/warcraftlogs/landing/player_details/{code}_{fight}.jsonl
   ├── Step 4: attendance     → /Volumes/01_bronze/warcraftlogs/landing/raid_attendance/
   ├── Step 5: guild members  → /Volumes/01_bronze/blizzard/landing/guild_members/
   ├── Step 6: Raider.IO      → /Volumes/01_bronze/raiderio/landing/raiderio_character_profiles/
   ├── Step 7: rankings       → /Volumes/01_bronze/warcraftlogs/landing/fight_rankings/{code}.jsonl
   ├── Step 8: casts          → /Volumes/01_bronze/warcraftlogs/landing/fight_casts/{code}.jsonl
   ├── Step 9: deaths         → /Volumes/01_bronze/warcraftlogs/landing/fight_deaths/{code}_{ts}.jsonl
   ├── Step 10: live roster   → /Volumes/01_bronze/google_sheets/landing/live_raid_roster/
   └── Step 11: profiles      → /Volumes/01_bronze/blizzard/landing/{character_media,character_equipment,character_achievements,item_media}/

2. DLT Pipeline (triggered after ingestion)
   Bronze:  Auto Loader reads new JSONL files → bronze_* tables
   Silver:  Parse, clean, deduplicate         → silver_* tables
   Gold:    Aggregate, join, enrich           → 40+ gold_* tables

3. Dashboard publishing
   Read persisted gold tables through Spark/Databricks SQL → write JSON datasets + `manifest.json` to `/Volumes/03_gold/sc_analytics/dashboard_exports/`

4. Distribution
   GitHub Actions downloads `latest/` from the UC Volume → uploads to Cloudflare R2 → frontend fetches `manifest.json` from the public base URL at runtime
```

## Ingestion Caching Strategy

Per-report files are written with the report code in the filename. Before fetching, the job checks whether the file already exists in the Volume. If it does, the step is skipped. This means:
- Re-running the ingestion job is safe and idempotent
- New reports are ingested; old reports are skipped (unless files are deleted)
- Exception: `guild_members` and `zone_catalog` are always refreshed (no caching)
- `fight_deaths` is stricter: stale legacy multi-fight death files are detected and backfilled with timestamped single-fight files

## Ambiguous Column Reference Prevention

When joining DataFrames in DLT, the same column name in both DataFrames causes `AMBIGUOUS_REFERENCE` errors. Patterns used across the codebase:

1. **List-key joins**: `df1.join(df2, ["report_code", "fight_id"])` — Spark deduplicates keys automatically
2. **Pre-join aliasing**: right-side columns prefixed (`_r_report_code`, `_pc_player_class`) then dropped after join
3. **Pre-join drop**: columns duplicated from both sides dropped from one before joining (e.g. `deaths.drop("zone_name", "zone_id")` before joining to fight_context)
4. **Qualified selects**: use `df.col_name` syntax in select after unqualified joins
