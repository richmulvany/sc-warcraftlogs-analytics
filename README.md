# WarcraftLogs Guild Analytics

[![CI](https://github.com/richmulvany/sc-warcraftlogs-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/richmulvany/sc-warcraftlogs-analytics/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-grade **Databricks medallion pipeline** that ingests WoW raid data from the [WarcraftLogs v2 GraphQL API](https://www.warcraftlogs.com/api/docs), [Blizzard Profile API](https://develop.battle.net/documentation), and [Raider.IO API](https://raider.io/api), processes it through a Bronze → Silver → Gold architecture, and serves it to a React dashboard.

Built to run on **Databricks Free Edition** (serverless DLT).

---

## What it does

Pulls data from WarcraftLogs, Blizzard, and Raider.IO nightly, transforms it into 40+ analytics tables, and surfaces insights across:

- **Progression** — boss kill timelines, wipe analysis, best kill times
- **Performance** — DPS/HPS parse percentiles, throughput trends, spec breakdowns
- **Attendance** — who shows up, how often, which nights
- **Roster** — guild rank structure, active raid team, alt detection
- **Survivability** — who's dying, to what, how often
- **Preparation** — consumable usage rates, combat stat distributions per player
- **Mythic+** — Raider.IO score snapshots, timed/untimed keys, dungeon breakdowns
- **Character profiles** — Blizzard profile portraits, standing renders, equipped gear, enchants, gems, raid feats

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
        │   8+ ingestion steps │  JSONL → Unity Catalog Volume
        └──────────┬───────────┘
                   │ /Volumes/{catalog}/{schema}/landing/
                   ▼
        ┌──────────────────────┐
        │   Bronze (DLT)       │  Auto Loader streams JSONL into Delta
        │   10 raw tables      │  Schema enforcement, metadata columns
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   Silver (DLT)       │  Parse JSON, explode arrays, deduplicate
        │   11 clean tables    │  Explicit StructType schemas, DLT expectations
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   Gold (DLT)         │  Fact tables, dimensions, aggregations
        │   40+ tables         │  Business-ready, frontend-focused products
        └──────────┬───────────┘
                   │ Static CSV export
                   ▼
        ┌──────────────────────┐
        │   React Frontend     │  Static dashboard (Vite + TypeScript)
        │                      │  Served via GitHub Pages / Vercel
        └──────────────────────┘
```

---

## Repository Structure

```
sc-warcraftlogs-analytics/
├── ingestion/
│   ├── jobs/
│   │   └── ingest_primary.py     # 7-step ingestion orchestrator (Databricks job)
│   ├── src/
│   │   ├── adapters/
│   │   │   ├── base.py           # Abstract BaseAdapter interface
│   │   │   ├── wcl/client.py     # WarcraftLogs GraphQL client (OAuth2, retry)
│   │   │   ├── blizzard/client.py# Blizzard REST client (guild roster)
│   │   │   └── raiderio/client.py# Raider.IO character profile client
│   │   └── utils/helpers.py
│   └── config/
│       ├── source_config.yml     # API endpoints, rate limits
│       └── rank_config.yml       # Guild rank definitions
├── pipeline/
│   ├── bronze/raw_source.py      # 10 Auto Loader table definitions
│   ├── silver/
│   │   ├── clean_reports.py      # guild_reports, fight_events
│   │   ├── clean_players.py      # actor_roster, player_performance
│   │   ├── clean_rankings.py     # player_rankings (WCL parse percentiles)
│   │   ├── clean_events.py       # player_deaths (killing blow extraction)
│   │   ├── clean_attendance.py   # raid_attendance
│   │   ├── clean_zone_catalog.py # zone_catalog
│   │   ├── clean_guild_members.py# guild_members (rank labels, class names)
│   │   └── clean_raiderio.py     # Raider.IO M+ scores and runs
│   └── gold/
│       ├── core_facts.py         # fact_player_fight_performance, fact_player_events
│       ├── core_dimensions.py    # dim_encounter, dim_player, dim_guild_member
│       ├── player_products.py    # attendance, weekly activity, performance, boss roster
│       ├── summary_products.py   # progression, best kills, wipe analysis
│       ├── survivability_products.py # player survivability, boss mechanics
│       ├── roster_products.py    # guild roster, raid team, player profile
│       ├── preparation_products.py  # consumables, combat stats, boss ability deaths
│       └── mplus_products.py        # Raider.IO Mythic+ gold products
├── frontend/                     # React + Vite + TypeScript dashboard
├── scripts/export_gold_tables.py  # Databricks SQL → static frontend CSV export
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
  - `reportData.report.table(dataType:Casts, sourceID:0)` — attendance data
  - `worldData.zones` — zone/encounter reference catalog

### Blizzard Profile API
- **Auth**: OAuth2 client credentials (basic auth)
- **In Databricks ingestion**: `/data/wow/guild/{realm}/{guild}/roster`
- **In frontend export helper**: character media, equipment, and achievements for player profile pages
- Returns guild members with rank (0–9), class_id, character media, equipment, gems/enchants, and selected raid feats

### Raider.IO API
- **Auth**: public unauthenticated character profile endpoint
- **Endpoint**: `/api/v1/characters/profile`
- **Fields used**: current-season Mythic+ scores, ranks, recent runs, best runs
- **Landing**: `landing/raiderio_character_profiles/`
- **Scope**: all guild characters from Blizzard roster, with prior `silver_guild_members` as a fallback seed
- 404 per character is non-fatal; score history starts from the first successful nightly ingestion

---

## Tables Produced

### Bronze (10 tables — raw, schema-enforced)
| Table | Source |
|-------|--------|
| `bronze_guild_reports` | WCL guild reports |
| `bronze_report_fights` | WCL fight + masterData JSON |
| `bronze_raid_attendance` | WCL table(Casts) attendance |
| `bronze_actor_roster` | WCL masterData actors |
| `bronze_player_details` | WCL table(Summary) playerDetails |
| `bronze_fight_rankings` | WCL rankings(compare:Parses) |
| `bronze_fight_deaths` | WCL table(Deaths) |
| `bronze_zone_catalog` | WCL worldData zones |
| `bronze_guild_members` | Blizzard guild roster |
| `bronze_raiderio_character_profiles` | Raider.IO character profile payloads |

### Silver (11 tables — cleaned, normalised)
| Table | Key transformations |
|-------|---------------------|
| `silver_guild_reports` | UTC timestamps, zone extracted, deduplicated |
| `silver_fight_events` | Fights exploded, raid difficulties filtered (3/4/5), difficulty labels |
| `silver_actor_roster` | Player actors per report, class/realm extracted |
| `silver_player_performance` | playerDetails JSON parsed; role arrays exploded & unioned; spec, item level, combatant stats (Crit/Haste/Mastery/Vers), consumable use |
| `silver_player_rankings` | Rankings JSON parsed; role arrays exploded & unioned; WCL DPS/HPS amount, rank percentile |
| `silver_player_deaths` | Deaths JSON parsed; one row per death; killing blow = first non-friendly damage event via `FILTER()` |
| `silver_raid_attendance` | Attendance exploded; presence codes mapped to labels |
| `silver_zone_catalog` | Zone/encounter reference; difficulty names collected |
| `silver_guild_members` | Deduplicated; rank labels (GM→Social); class names from Blizzard class_id enum |
| `silver_raiderio_player_scores` | Raider.IO current-season score snapshots and ranks |
| `silver_raiderio_player_runs` | Deduped Mythic+ recent/best runs, timed flag, scores, URLs |

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
| `gold_boss_ability_deaths` | What abilities kill players most per boss |
| `gold_player_consumables` | Potion/healthstone usage rate per player, per boss |
| `gold_player_combat_stats` | Crit/Haste/Mastery/Vers ratings per player (latest + avg) |
| `gold_encounter_catalog` | Zone/encounter reference for frontend filters |
| `gold_guild_roster` | Full Blizzard guild roster with class, attendance |
| `gold_raid_team` | Active raid team with possible alt flags |
| `gold_player_profile` | Comprehensive per-player identity + performance summary |
| `gold_roster` | Active roster from WCL actor logs |
| `gold_player_mplus_summary` | Latest Raider.IO score, ranks, best run, timed/untimed counts |
| `gold_player_mplus_score_history` | Nightly Raider.IO score snapshots |
| `gold_player_mplus_run_history` | Governed Mythic+ run-level table |
| `gold_player_mplus_weekly_activity` | Weekly M+ run counts, timed/untimed split, highest key |
| `gold_player_mplus_dungeon_breakdown` | Per-player per-dungeon M+ summary |

---

## Setup

See [SETUP.md](SETUP.md) for the full step-by-step guide. Summary:

### Prerequisites
- [Databricks Free Edition](https://www.databricks.com/try-databricks) workspace
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.210+ configured
- Python 3.11+

### 1. Configure secrets

In your Databricks workspace, create a secret scope named `pipeline-secrets`:

```bash
databricks secrets create-scope pipeline-secrets
databricks secrets put-secret pipeline-secrets wcl_client_id       --string-value "..."
databricks secrets put-secret pipeline-secrets wcl_client_secret    --string-value "..."
databricks secrets put-secret pipeline-secrets blizzard_client_id   --string-value "..."
databricks secrets put-secret pipeline-secrets blizzard_client_secret --string-value "..."
```

### 2. Deploy

```bash
databricks bundle deploy
databricks bundle run nightly_ingestion   # first ingestion run
```

The DLT pipeline runs automatically after ingestion completes.

### 3. Customise for your guild

Edit `databricks.yml` variables:
```yaml
variables:
  guild_name: "Your Guild Name"
  guild_server_slug: "realm-name"
  guild_server_region: EU   # EU / US / KR / TW
```

---

## Key Implementation Details

### Throughput metric
`throughput_per_second` in all performance tables comes from `silver_player_rankings.amount` — the WCL DPS/HPS value used for ranking (already per-second normalised). The `playerDetails` endpoint provides combatant info (gear, stats, consumables) but **not** damage/healing totals.

### Archived reports
WCL eventually archives old reports. `ArchivedReportError` is caught at ingestion time and a skip marker file written to `landing/archived/{report_code}`. Archived reports are never retried.

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

---

## Documentation

- [Setup Guide](SETUP.md)
- [Architecture Overview](docs/architecture/overview.md)
- [Data Dictionary](docs/data_dictionary/README.md)
- [Runbooks](docs/runbooks/README.md)
- [ADR Index](docs/adr/README.md)
- [AI Session Context](CLAUDE.md)
