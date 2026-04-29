# sc-analytics

[![CI](https://github.com/richmulvany/sc-warcraftlogs-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/richmulvany/sc-warcraftlogs-analytics/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-grade **Databricks medallion pipeline** that ingests WoW raid data from the [WarcraftLogs v2 GraphQL API](https://www.warcraftlogs.com/api/docs), [Blizzard Profile API](https://develop.battle.net/documentation), [Raider.IO API](https://raider.io/api), and Google Sheets, processes it through a Bronze → Silver → Gold architecture, and serves it to a React dashboard through static JSON assets published from Databricks to Cloudflare R2.

Built to run on **Databricks Free Edition** (serverless Lakeflow / DLT).

---

## What it does

Pulls data from WarcraftLogs, Blizzard, Raider.IO, and Google Sheets nightly, transforms it into 40+ analytics tables, and surfaces insights across:

- **Progression** — boss kill timelines, wipe analysis, best kill times
- **Performance** — DPS/HPS parse percentiles, throughput trends, spec breakdowns
- **Attendance** — who shows up, how often, which nights
- **Roster** — guild rank structure, active raid team, alt detection
- **Survivability** — who's dying, to what, how often
- **Preparation** — experimental current-tier raid-readiness ranking with attendance, food, flask/phial, weapon enhancement, and combat potion coverage
- **Wipe diagnosis** — first deaths, repeat deaths, wipe survival discipline, raid cooldown capacity, healer external capacity
- **Mythic+** — Raider.IO score snapshots, timed/untimed keys, dungeon breakdowns
- **Character profiles** — Blizzard profile portraits, standing renders, equipped gear, enchants, gems, raid feats

Recommended public data URL:

- `https://data.sc-analytics.org/latest`

---

## Architecture

```
WarcraftLogs API          Blizzard Profile API          Raider.IO API
(GraphQL, OAuth2)         (REST, OAuth2)                (REST, public)
        │                         │                           │
        └──────────┬──────────────┴──────────────┬────────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   ingest_primary.py  │  Databricks Workflow Job
        │ source-split landing │  JSONL → UC Volumes in 01_bronze
        └──────────┬───────────┘
                   │ /Volumes/01_bronze/{source}/landing/
                   ▼
        ┌──────────────────────┐
        │   Bronze (DLT)       │  Auto Loader streams JSONL into Delta
        │ source-split schemas │  Schema enforcement, metadata columns
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   Silver (DLT)       │  Parse JSON, explode arrays, deduplicate
        │ source-split schemas │  Explicit StructType schemas, DLT expectations
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   Gold (DLT)         │  Fact tables, dimensions, aggregations
        │   40+ tables         │  Business-ready, frontend-focused products
        └──────────┬───────────┘
                   │ Publish manifest + JSON
                   ▼
        ┌──────────────────────┐
        │  UC Volume / R2      │  manifest.json + dataset JSON files
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   React Frontend     │  Static dashboard (Vite + TypeScript)
        │                      │  Fetches published JSON assets at runtime
        └──────────────────────┘
```

---

## Repository Structure

```
sc-analytics/
├── ingestion/
│   ├── jobs/
│   │   └── ingest_primary.py     # ingestion orchestrator (Databricks job)
│   ├── src/
│   │   ├── adapters/
│   │   │   ├── base.py           # Abstract BaseAdapter interface
│   │   │   ├── wcl/client.py     # WarcraftLogs GraphQL client (OAuth2, retry)
│   │   │   ├── blizzard/client.py# Blizzard REST client (guild + character/item APIs)
│   │   │   └── raiderio/client.py# Raider.IO character profile client
│   │   └── utils/helpers.py
│   └── config/
│       ├── source_config.yml     # API endpoints, rate limits
│       └── rank_config.yml       # Guild rank definitions
├── pipeline/
│   ├── bronze/
│   │   ├── warcraftlogs.py       # 01_bronze.warcraftlogs.*
│   │   ├── blizzard.py           # 01_bronze.blizzard.*
│   │   ├── raiderio.py           # 01_bronze.raiderio.*
│   │   └── google_sheets.py      # 01_bronze.google_sheets.*
│   ├── silver/
│   │   ├── clean_reports.py      # guild_reports, fight_events
│   │   ├── clean_players.py      # actor_roster, player_performance
│   │   ├── clean_rankings.py     # player_rankings (WCL parse percentiles)
│   │   ├── clean_events.py       # player_deaths + player_cast_events
│   │   ├── clean_attendance.py   # raid_attendance
│   │   ├── clean_zone_catalog.py # zone_catalog
│   │   ├── clean_guild_members.py# guild_members (rank labels, class names)
│   │   ├── clean_raiderio.py     # Raider.IO M+ scores and runs
│   │   ├── clean_live_raid_roster.py
│   │   ├── clean_guild_zone_ranks.py
│   │   ├── clean_character_media.py
│   │   ├── clean_character_equipment.py
│   │   ├── clean_character_achievements.py
│   │   └── clean_item_media.py
│   └── gold/
│       ├── core_facts.py         # fact_player_fight_performance, fact_player_events
│       ├── core_dimensions.py    # dim_encounter, dim_player, dim_guild_member
│       ├── player_products.py    # attendance, performance, boss roster
│       ├── summary_products.py   # progression, best kills, wipe analysis
│       ├── survivability_products.py # player survivability, boss mechanics
│       ├── roster_products.py    # guild roster, raid team, player profile
│       ├── mplus_products.py        # Raider.IO Mythic+ gold products
│       ├── utility_products.py      # weekly activity, utility by pull
│       ├── wipe_diagnostics.py      # wipe survival + cooldown utilization
│       ├── profile_products.py      # live roster, zone ranks, media, equipment, achievements
│       └── _cooldown_rules.py       # shared cooldown/spec metadata
├── frontend/                     # React + Vite + TypeScript dashboard
├── scripts/dev/export_gold_tables.py # legacy CSV export for local fallback
├── scripts/publish_dashboard_assets.py  # gold tables -> UC Volume JSON assets
├── docs/                         # Architecture, data dictionary, runbooks, ADRs
├── databricks.yml                # Databricks Asset Bundle (pipeline + job config)
├── pyproject.toml                # Python deps, ruff, mypy config
└── CLAUDE.md                     # AI session context (architecture, patterns, gotchas)
```

---

## Data Sources

### WarcraftLogs v2 GraphQL API
- **Auth**: OAuth2 client credentials (`/oauth/token`)
- **Rate limit**: ~30 req/min (manual retry loop with `Retry-After` header)
- **Endpoints used**:
  - `reportData.reports` — guild raid reports (paginated)
  - `reportData.report.fights` — individual boss pulls per report
  - `reportData.report.masterData.actors` — player roster per report
  - `reportData.report.table(dataType:Summary)` — per-player combat stats (gear, consumables, stat ratings)
  - `reportData.report.rankings(compare:Parses)` — WCL parse percentiles and DPS/HPS
  - `reportData.report.table(dataType:Deaths)` — death events per fight
  - `reportData.report.events(dataType:Casts)` — full player cast stream for utility tracking
  - `reportData.report.events(dataType:CombatantInfo)` — spec/talent context for cooldown gating
  - `reportData.report.table(dataType:Casts, sourceID:0)` — attendance data
  - `guildData.zoneRanking` — guild progression rank by raid zone
  - `worldData.zones` — zone/encounter reference catalog

---

## Preparation Page

The `Preparation` page is now a current-tier-only raid-readiness dashboard rather
than a thin wrapper around the old consumables/stat exports.

What it currently does:
- scopes to the current raid tier from `gold_raid_summary`
- scopes to the current raid team from `live_raid_roster.csv` with `gold_raid_team.csv` fallback
- scores readiness from current-tier attendance plus preparation coverage
- tracks food, flask/phial, weapon enhancement, and combat potion coverage
- shows combat potion usage for every role, but only counts it in readiness for DPS
- supports identity overrides for same-raider character swaps via `preparation_overrides.csv`

Implementation notes:
- the frontend reads preparation metrics from `gold_boss_kill_roster.csv`
- Midnight consumable detection is derived in the pipeline from combatant buffs,
  weapon enhancements, and buff-based combat potion detection
- shared identity overrides come from
  `00_governance.warcraftlogs_admin.preparation_identity_overrides` and are exported
  by `scripts/dev/export_gold_tables.py`

## Mythic+ Page

The `Mythic+` page is now split between a fixed raid-team KPI strip and a
switchable analysis scope for the main panels.

What it currently does:
- keeps the top KPI strip (`Active Raiders`, `Avg Score`, `Top Scorer`, `Keys This Reset`)
  scoped to active raid-team members with non-zero current Raider.IO score
- resolves raid-team membership from `live_raid_roster.csv` when present, with
  `gold_raid_team.csv` fallback
- lets the main page panels toggle between `Guild` and `Raid Team` scope
- applies the selected scope to leaderboard, vault progress, dungeon coverage,
  score trajectory, and push-candidate panels
- rescales the dungeon coverage heatmap against the currently visible filtered
  key range instead of a fixed global key band
- uses a relative y-axis for score trajectory so filtered score deltas remain
  readable while keeping absolute score labels and tooltips

Implementation notes:
- Mythic+ page data comes from `gold_player_mplus_summary.csv`,
  `gold_player_mplus_score_history.csv`, `gold_player_mplus_weekly_activity.csv`,
  and `gold_player_mplus_dungeon_breakdown.csv`
- raid-team scope follows the same live-roster-first resolution pattern already
  used by the roster/preparation views
- Raider.IO score history starts from the first successful nightly ingestion, so
  early trend lines are relative only to captured snapshots, not true season start

### Blizzard Profile API
- **Auth**: OAuth2 client credentials (basic auth)
- **In Databricks ingestion**:
  - `/data/wow/guild/{realm}/{guild}/roster`
  - `/profile/wow/character/{realm}/{name}/character-media`
  - `/profile/wow/character/{realm}/{name}/equipment`
  - `/profile/wow/character/{realm}/{name}/achievements`
  - `/data/wow/media/item/{item_id}`
- Returns guild members with rank (0–9), class_id, character media, equipment, gems/enchants, selected raid feats, and item icon metadata

### Raider.IO API
- **Auth**: public unauthenticated character profile endpoint
- **Endpoint**: `/api/v1/characters/profile`
- **Fields used**: current-season Mythic+ scores, ranks, recent runs, best runs
- **Landing**: `/Volumes/01_bronze/raiderio/landing/raiderio_character_profiles/`
- **Scope**: all guild characters from Blizzard roster, with prior `silver_guild_members` as a fallback seed
- 404 per character is non-fatal; score history starts from the first successful nightly ingestion

---

## Tables Produced

### Bronze (16+ tables — raw, schema-enforced, source-split under `01_bronze`)
| Table | Source |
|-------|--------|
| `bronze_guild_reports` | WCL guild reports |
| `bronze_report_fights` | WCL fight + masterData JSON |
| `bronze_raid_attendance` | WCL table(Casts) attendance |
| `bronze_actor_roster` | WCL masterData actors |
| `bronze_player_details` | WCL table(Summary) playerDetails |
| `bronze_fight_rankings` | WCL rankings(compare:Parses) |
| `bronze_fight_deaths` | WCL table(Deaths), one record per fight after backfill-safe ingestion |
| `bronze_fight_casts` | WCL events(Casts + CombatantInfo) |
| `bronze_zone_catalog` | WCL worldData zones |
| `bronze_guild_zone_ranks` | WCL guild zone rank payloads |
| `bronze_guild_members` | Blizzard guild roster |
| `bronze_character_media` | Blizzard character media payloads |
| `bronze_character_equipment` | Blizzard equipment payloads |
| `bronze_character_achievements` | Blizzard achievements payloads |
| `bronze_item_media` | Blizzard item media payloads |
| `bronze_raiderio_character_profiles` | Raider.IO character profile payloads |
| `bronze_live_raid_roster` | Google Sheets live raid roster CSV payload |

### Silver (source-split under `02_silver`)
| Table | Key transformations |
|-------|---------------------|
| `silver_guild_reports` | UTC timestamps, zone extracted, deduplicated |
| `silver_fight_events` | Fights exploded, raid difficulties filtered (3/4/5), difficulty labels |
| `silver_actor_roster` | Player actors per report, class/realm extracted |
| `silver_player_performance` | playerDetails JSON parsed; role arrays exploded & unioned; spec, item level, combatant stats (Crit/Haste/Mastery/Vers), consumable use |
| `silver_player_rankings` | Rankings JSON parsed; role arrays exploded & unioned; WCL DPS/HPS amount, rank percentile |
| `silver_player_deaths` | Deaths JSON parsed; one row per death; prefers single-fight backfills over legacy multi-fight payloads |
| `silver_player_cast_events` | Cast event stream joined to actor roster and fight context for utility analysis |
| `silver_raid_attendance` | Attendance exploded; presence codes mapped to labels |
| `silver_zone_catalog` | Zone/encounter reference; difficulty names collected |
| `silver_guild_zone_ranks` | Progress JSON preserved per zone for downstream flattening |
| `silver_guild_members` | Deduplicated; rank labels (GM→Social); class names from Blizzard class_id enum |
| `silver_character_media` | Latest avatar / inset / main / main_raw URLs per character |
| `silver_character_equipment` | Latest raw equipment JSON per character |
| `silver_character_achievements` | Latest raw achievements JSON per character |
| `silver_item_media` | `item_id` → icon URL lookup |
| `silver_raiderio_player_scores` | Raider.IO current-season score snapshots and ranks |
| `silver_raiderio_player_runs` | Deduped Mythic+ recent/best runs, timed flag, scores, URLs |
| `silver_live_raid_roster` | Parsed Google Sheets live roster rows |

### Gold (40+ tables — business-ready)

**Dimensions**
| Table | Description |
|-------|-------------|
| `dim_encounter` | Boss encounter reference (active tiers only) |
| `dim_player` | Canonical player identity; guild membership; class enriched from perf data |
| `dim_guild_member` | Authoritative Blizzard roster; attendance stats; `is_active` flag |

**Facts**
| Table | Description |
|-------|-------------|
| `fact_player_fight_performance` | One row per player per kill fight; throughput (WCL rankings.amount), parse %, gear, consumables, stat ratings |
| `fact_player_events` | One row per death event; fight_id, killing blow, zone context |

**Aggregations**
| Table | Question answered |
|-------|-------------------|
| `gold_player_attendance` | Who shows up and how often? |
| `gold_weekly_activity` | How many raids per week, how much progress? |
| `gold_player_performance_summary` | How does each player perform across all kills? |
| `gold_boss_kill_roster` | Who was on each kill and how did they do? |
| `gold_player_boss_performance` | Per-boss performance per player with trend indicator |
| `gold_boss_progression` | Kill/wipe counts per encounter |
| `gold_raid_summary` | One row per raid night with aggregate stats |
| `gold_progression_timeline` | Cumulative first-kills over time |
| `gold_best_kills` | Fastest kill per encounter with mm:ss formatting |
| `gold_boss_wipe_analysis` | Wipe phase/% breakdown per boss |
| `gold_boss_mechanics` | Enhanced wipe analysis — phase buckets, duration buckets, weekly trend |
| `gold_player_survivability` | Deaths per kill, most common killing blow per player |
| `gold_encounter_catalog` | Zone/encounter reference for frontend filters |
| `gold_guild_roster` | Full Blizzard guild roster with class, attendance |
| `gold_live_raid_roster` | Current live raid roster from Google Sheets |
| `gold_guild_zone_ranks` | World / region / server rank per zone |
| `gold_player_character_media` | Character avatar / inset / render artwork |
| `gold_player_character_equipment` | Equipped items joined to icon URLs |
| `gold_player_raid_achievements` | Selected raid feats per character |
| `gold_player_utility_by_pull` | Pull-level healthstone / potion / defensive usage |
| `gold_wipe_survival_events` | Wipe death events with survival/recovery context |
| `gold_wipe_cooldown_utilization` | Pull-scoped cooldown capacity and actual usage |
| `gold_raid_team` | Active raid team with possible alt flags |
| `gold_player_mplus_summary` | Latest Raider.IO score, ranks, best run, timed/untimed counts |
| `gold_player_mplus_score_history` | Nightly Raider.IO score snapshots |
| `gold_player_mplus_run_history` | Governed Mythic+ run-level table |
| `gold_player_mplus_weekly_activity` | Weekly M+ run counts, timed/untimed split, highest key |
| `gold_player_mplus_dungeon_breakdown` | Per-player per-dungeon M+ summary |

The JSON publishing path is now the recommended deployment flow. The legacy CSV export remains available as a local/dev fallback during migration.

---

## Setup

See [SETUP.md](SETUP.md) for the full step-by-step guide. Summary:

### Prerequisites
- [Databricks Free Edition](https://www.databricks.com/try-databricks) workspace
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.210+ configured
- Python 3.11+

### 1. Configure secrets

Important: the ingestion notebook currently reads from the `warcraftlogs` secret scope.

```bash
databricks secrets create-scope warcraftlogs
databricks secrets put-secret warcraftlogs client_id --string-value "..."
databricks secrets put-secret warcraftlogs client_secret --string-value "..."
databricks secrets put-secret warcraftlogs blizzard_client_id --string-value "..."
databricks secrets put-secret warcraftlogs blizzard_client_secret --string-value "..."
```

### 2. Deploy

```bash
databricks bundle deploy
databricks bundle run daily_orchestrator   # full end-to-end run on demand
```

The bundle defines five jobs plus the parent orchestrator. The orchestrator
owns the daily 22:15 (Europe/London) schedule and chains the four stages with
explicit dependencies.

| Stage | Resource key | Display name | Action |
|---|---|---|---|
| 1 | `ingestion_daily`         | `sc-analytics-ingestion-daily-2215`     | WCL / Blizzard / Raider.IO / Sheets ingestion |
| 2 | `sdp_post_ingestion`      | `sc-analytics-sdp-daily-post-ingestion` | DLT pipeline update |
| 3 | `write_dashboard_assets`  | `sc-analytics-write-post-sdp`           | Write JSON assets to UC Volume |
| 4 | `publish_post_write`      | `sc-analytics-publish-post-write`       | Dispatch GitHub Actions → Cloudflare R2 |

To run a single stage for backfills or troubleshooting:

```bash
databricks bundle run ingestion_daily
databricks bundle run sdp_post_ingestion
databricks bundle run write_dashboard_assets
databricks bundle run publish_post_write
```

The publish stage requires two extra Databricks secrets (one-off setup):

```bash
databricks secrets put-secret github github_token  # PAT with actions:write
databricks secrets put-secret github github_repo   # owner/repo
```

### 3. Customise for your guild

Edit `databricks.yml` variables:
```yaml
variables:
  guild_name: "Your Guild Name"
  guild_server_slug: "realm-name"
  guild_server_region: EU   # EU / US / KR / TW
```

---

## Dashboard Data Hosting

The recommended production flow is:

Databricks gold tables -> UC Volume JSON assets -> GitHub Actions -> Cloudflare R2 -> frontend runtime fetch

The frontend reads the public data base URL from `VITE_DASHBOARD_DATA_BASE_URL`.
Frontend hosting is handled outside the active GitHub Actions deployment workflows.

See [docs/architecture/dashboard_data_publishing.md](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/docs/architecture/dashboard_data_publishing.md).

---

## Key Implementation Details

### Throughput metric
`throughput_per_second` in all performance tables comes from `silver_player_rankings.amount` — the WCL DPS/HPS value used for ranking (already per-second normalised). The `playerDetails` endpoint provides combatant info (gear, stats, consumables) but **not** damage/healing totals.

### Archived reports
WCL eventually archives old reports. `ArchivedReportError` is caught at ingestion time and a skip marker file written to `landing/archived/{report_code}`. Archived reports are never retried.

### Death ingestion truncation
WCL `table(dataType: Deaths, fightIDs: [...])` can truncate long multi-fight reports. The current ingestion path fetches deaths one fight at a time and writes one bronze death record per fight. If you are recovering old data, re-run ingestion so stale legacy death files are backfilled.

### Rate limiting
Manual retry loop reads `Retry-After` header on 429 responses. 5xx errors use exponential backoff (3 attempts, max 30s). Token refresh is proactive (5 min before expiry).

### Guild rank mapping (Blizzard API)
Blizzard returns 0-indexed rank IDs. Our guild mapping:

| Rank ID | Label |
|---------|-------|
| 0 | Guild Master |
| 1 | GM Alt |
| 2 | Officer |
| 3 | Officer Alt |
| 4 | Officer Alt |
| 5 | Raider |
| 6 | Raider Alt |
| 7 | Bestie |
| 8 | Trial |
| 9 | Social |

Raid team = ranks 0, 1, 2, 3, 4, 5, 8.

### WoW Class IDs
Blizzard returns `class_id` (integer). Silver layer maps to name: 1=Warrior, 2=Paladin, 3=Hunter, 4=Rogue, 5=Priest, 6=Death Knight, 7=Shaman, 8=Mage, 9=Warlock, 10=Monk, 11=Druid, 12=Demon Hunter, 13=Evoker.

---

## Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Lint + type check
ruff check .
mypy ingestion/

# Tests
pytest ingestion/tests/

# Deploy pipeline changes
databricks bundle deploy
```

## Frontend diagnosis panels

The Wipe Analysis page is the main surface for survival utility review.

Current utility diagnostics are built from export-layer datasets rather than extra gold tables:
- wipe survival discipline on wipe deaths
- raid defensive capacity
- healer external cooldown capacity

The score on the survival panel is presence-normalised to wipe pulls in scope, not a raw count.

---

## Documentation

- [Setup Guide](SETUP.md)
- [Architecture Overview](docs/architecture/overview.md)
- [Data Dictionary](docs/data_dictionary/README.md)
- [Runbooks](docs/runbooks/README.md)
- [ADR Index](docs/adr/README.md)
- [AI Session Context](CLAUDE.md)
