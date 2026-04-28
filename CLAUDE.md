# CLAUDE.md

## Purpose

This file is working context for Claude Code in this repository.

The goal is to help Claude make small, safe, high-quality changes without wasting tokens exploring the whole project.

This repository contains a Databricks medallion pipeline and a React analytics dashboard for WarcraftLogs, Blizzard Profile, Raider.IO, and Google Sheets data.

Claude should treat this project as a production-style data engineering + analytics frontend project, not as a toy app.

---

## Project summary

WarcraftLogs Guild Analytics is a portfolio-grade analytics platform for a World of Warcraft guild.

It includes:

- Python ingestion jobs for WarcraftLogs, Blizzard, Raider.IO, and Google Sheets
- Databricks Lakeflow / DLT Bronze → Silver → Gold pipeline
- 40+ frontend-focused gold data products
- static dashboard data assets published from Databricks to Cloudflare R2
- React + Vite + TypeScript + Tailwind dashboard frontend

Recommended public dashboard data base URL:

    https://data.sc-analytics.org/latest

The frontend currently consumes published dashboard assets, primarily JSON. Legacy CSV export paths may still exist for local/dev fallback.

---

## Repository map

    sc-warcraftlogs-analytics/
    ├── ingestion/                     # Python API ingestion jobs/adapters
    │   ├── jobs/
    │   │   └── ingest_primary.py
    │   ├── src/
    │   │   ├── adapters/
    │   │   │   ├── base.py
    │   │   │   ├── wcl/client.py
    │   │   │   ├── blizzard/client.py
    │   │   │   └── raiderio/client.py
    │   │   └── utils/
    │   └── config/
    │
    ├── pipeline/                      # Databricks DLT / Lakeflow pipeline code
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    │
    ├── frontend/                      # React + Vite + TypeScript dashboard
    │
    ├── scripts/
    │   ├── dev/
    │   │   └── export_gold_tables.py   # legacy/local CSV export support
    │   └── publish_dashboard_assets.py # recommended JSON asset publishing
    │
    ├── docs/
    ├── databricks.yml
    ├── pyproject.toml
    └── CLAUDE.md

---

## Default working behaviour

Claude should optimise for targeted, minimal, reviewable changes.

Default rules:

1. Start with the file or area explicitly named by the user.
2. Do not browse the repo broadly “for context”.
3. Do not inspect backend/pipeline/frontend all at once unless the task genuinely crosses those boundaries.
4. Prefer targeted reads, grep, and direct file inspection.
5. Do not read every file in a directory.
6. Do not change unrelated files.
7. Do not rewrite whole files unless the user explicitly asks or the file is small enough that this is safer.
8. Preserve existing comments unless they are wrong or obsolete.
9. Preserve existing visual style and naming conventions.
10. Avoid clever rewrites when a small patch would solve the issue.
11. After two failed attempts, stop and explain the blocker rather than thrashing.

When uncertain, ask:

> What is the smallest change that satisfies the request?

---

## Token budget rules

This repo can burn context quickly. Be careful.

Do:

- inspect the target file first
- inspect directly imported helpers/components only if needed
- use grep for specific symbols
- summarise findings briefly
- make narrow edits
- stop once the requested outcome is achieved

Avoid:

- reading all pages
- reading all pipeline files
- opening barrel files such as `index.ts` unless an import error requires it
- reading generated/exported data unless the task is specifically about data content
- running broad architectural reviews during implementation tasks
- carrying old assumptions forward after the code shows otherwise

For planning tasks, do not edit files.

For implementation tasks, do not perform a full design review unless asked.

---

## Output style

Unless the user asks otherwise, respond with:

1. changed files
2. what changed
3. any assumptions or data behaviour affected
4. tests/checks run, or why they were not run
5. any follow-up needed

Keep summaries concise and practical.

Do not produce long essays after simple code changes.

---

## Hard boundaries by task type

### Frontend task

Stay in:

    frontend/

Only inspect backend/pipeline files if the user explicitly asks about data production or a frontend field cannot be explained from published data docs/types.

### Pipeline task

Stay in:

    pipeline/

Only inspect frontend files if the user asks how a pipeline field is used in the dashboard.

### Ingestion task

Stay in:

    ingestion/

Only inspect pipeline files if the landing/output shape is needed.

### Data publishing task

Relevant areas are usually:

    scripts/publish_dashboard_assets.py
    scripts/dev/export_gold_tables.py
    databricks.yml
    frontend/
    docs/architecture/dashboard_data_publishing.md

Do not assume CSV is the primary production path.

### Documentation task

Edit only the requested docs unless stale documentation would directly mislead future work.

---

## Frontend context

The frontend is a data-dense analytics dashboard, not a marketing site.

Stack:

- React 18
- Vite
- TypeScript
- TailwindCSS
- Recharts

Common dashboard surfaces include:

- Progression
- Performance
- Attendance
- Roster
- Survivability
- Preparation
- Wipe Analysis
- Mythic+
- Player/Profile pages

Frontend priorities:

1. analytical clarity
2. readability
3. responsive usability
4. fast interaction
5. visual consistency
6. honest representation of incomplete data

Avoid making the UI generic, over-animated, or decorative at the expense of insight.

---

## Frontend responsive/layout rules

Mobile, desktop, and ultra-wide screens all matter.

Default responsive goals:

- Mobile must be usable without the desktop sidebar occupying the screen.
- Tablet should avoid cramped two-column layouts.
- Desktop should remain dense and efficient.
- Wide and ultra-wide screens should not let panels stretch indefinitely.
- Text must remain readable at every breakpoint.
- Tables may scroll horizontally on small screens.
- Charts should resize predictably and keep labels/tooltips readable.

Layout principles:

- Fix global layout causes before patching individual pages.
- Prefer shared layout primitives over repeated per-page width hacks.
- Use page-level max-widths for readable content.
- Use full-width layouts only where useful for grids, charts, or tables.
- Avoid `w-screen` unless there is a specific reason.
- Avoid fixed pixel widths that cause overflow.
- Avoid shrinking text to solve density problems.
- Use responsive grids with sensible minimum card widths.
- On mobile, navigation should collapse into a drawer/top-nav pattern rather than preserving the desktop sidebar.
- Sticky/fixed elements must not cover content.
- Use `min-w-0` where flex/grid children need to shrink.
- Use `overflow-x-auto` for dense tables instead of crushing columns.
- Prefer responsive padding such as `px-4 sm:px-6 lg:px-8`.
- Prefer controlled containers such as `max-w-screen-2xl` or equivalent design-system wrappers.
- Avoid arbitrary one-off breakpoint hacks unless the page truly needs them.

When working on responsive issues:

1. inspect the app shell/layout first
2. inspect sidebar/navigation next
3. inspect shared card/grid/page primitives
4. then inspect representative pages
5. only then make page-specific adjustments

Representative pages for responsive QA:

- Mythic+
- Preparation
- Wipe Analysis
- Player Details/Profile
- any dashboard landing/overview page

---

## Frontend design-system guidance

Use or create shared primitives only when they reduce repeated bugs.

Useful primitives may include:

- `DashboardLayout`
- `PageShell`
- `PageContainer`
- `SectionHeader`
- `DashboardCard`
- `ChartCard`
- `StatCard`
- `ResponsiveGrid`
- `FilterBar`
- `DataTableWrapper`
- `MobileNav`
- `Sidebar`

Do not create abstractions just for neatness.

A good shared component should make future pages harder to break.

---

## Frontend chart/table rules

Charts:

- Keep chart heights usable on mobile.
- Avoid tiny axis labels.
- Prefer hiding or reducing non-essential axis detail on small screens.
- Preserve tooltip usefulness.
- Use relative domains only when appropriate and do not mislead users.
- If using relative y-axes, keep absolute values visible in labels/tooltips.
- Do not imply a trend starts before captured data exists.

Tables:

- Use horizontal scrolling on small screens.
- Do not shrink text until it is unreadable.
- Keep key identity columns visible where possible.
- Prefer concise columns on mobile if the existing design supports it.
- Do not remove important analytic fields without user approval.

Cards/grids:

- Use `min-w-0` in grid/flex children.
- Avoid cards with fixed widths unless necessary.
- Use responsive columns rather than viewport-dependent hacks.
- Prefer consistent card spacing and alignment over page-specific exceptions.

---

## Frontend data rules

Do not invent fields.

If a field is missing, nullable, stale, or only partially supported:

- handle it explicitly
- label it honestly
- avoid silently converting null to zero
- avoid implying precision that does not exist

Important formatting rule:

- Do not use `Number(value) || 0` for nullable analytics fields.
- `Number(null) === 0`, which silently turns missing values into real zeros.
- Use existing helpers such as `toFiniteNumber()` and `meanIgnoringNulls()` from `frontend/src/utils/format.ts` where available.

---

## Data architecture summary

The data pipeline follows:

    Source APIs
      ↓
    Bronze raw/source-split Delta tables
      ↓
    Silver cleaned and normalised tables
      ↓
    Gold facts/dimensions/analytics products
      ↓
    Published dashboard assets
      ↓
    React frontend

Main source APIs:

- WarcraftLogs v2 GraphQL API
- Blizzard Profile API
- Raider.IO API
- Google Sheets

Production dashboard data path:

    Databricks gold tables
      → UC Volume dashboard assets
      → Cloudflare R2
      → React runtime fetch

Legacy CSV export still exists but should not be assumed to be the production source of truth.

---

## Unity Catalog conventions

Gold tables are produced under:

    03_gold.sc_analytics.<table_name>

Current conceptual layers:

    01_bronze
    02_silver
    03_gold

Databricks Free Edition object quotas matter. Do not collapse bronze/silver/gold into one schema.

---

## Important data products

### Core facts

- `fact_player_fight_performance`
  - one row per player per boss kill fight
  - primary fact table for performance analysis
  - throughput comes from WCL rankings amount
  - parse percentile fields may be null

- `fact_player_events`
  - one row per death event
  - includes player, fight, timestamp, killing blow, and zone context

### Core dimensions

- `dim_encounter`
  - boss/zone reference
  - active tiers only

- `dim_player`
  - canonical player identity
  - guild membership and rank flags

- `dim_guild_member`
  - authoritative Blizzard guild roster

### Major gold products

- `gold_player_attendance`
- `gold_weekly_activity`
- `gold_player_performance_summary`
- `gold_boss_kill_roster`
- `gold_player_boss_performance`
- `gold_boss_progression`
- `gold_raid_summary`
- `gold_progression_timeline`
- `gold_best_kills`
- `gold_boss_wipe_analysis`
- `gold_boss_mechanics`
- `gold_player_survivability`
- `gold_boss_ability_deaths`
- `gold_encounter_catalog`
- `gold_guild_roster`
- `gold_live_raid_roster`
- `gold_guild_zone_ranks`
- `gold_player_character_media`
- `gold_player_character_equipment`
- `gold_player_raid_achievements`
- `gold_player_utility_by_pull`
- `gold_wipe_survival_events`
- `gold_wipe_cooldown_utilization`
- `gold_raid_team`
- `gold_player_mplus_summary`
- `gold_player_mplus_score_history`
- `gold_player_mplus_run_history`
- `gold_player_mplus_weekly_activity`
- `gold_player_mplus_dungeon_breakdown`

Do not assume this list is exhaustive. Inspect actual code if exact availability matters.

---

## Key data gotchas

### Throughput

`throughput_per_second` comes from WarcraftLogs rankings amount.

It is already DPS/HPS-style per-second throughput.

Do not recompute it from playerDetails.

### Parse data

WCL parse rankings can be incomplete shortly after report upload.

`rank_percent`, `bracket_percent`, and related fields may be null.

Do not bucket null ranks into zero.

### Raider.IO history

Raider.IO score history starts from the first successful nightly ingestion.

It is not true season-start history unless ingestion began at season start.

Trend charts should be clear that they show captured snapshots only.

### Preparation page

The live Preparation page is current-tier-only.

It does not primarily use historical all-time consumable/combat stat aggregates.

Current preparation inputs include:

- `gold_raid_summary`
- `gold_boss_kill_roster`
- `live_raid_roster`
- `gold_raid_team`
- `preparation_overrides.csv`

Preparation scoring:

- food buff coverage
- flask/phial coverage
- weapon enhancement coverage
- combat potion usage
- attendance/current-team scope
- combat potion usage is displayed for all roles
- combat potion usage contributes to readiness scoring only for DPS

Do not redesign this page into an all-time historical preparation page unless requested.

### Mythic+ page

Mythic+ data comes from Raider.IO-derived products.

Important inputs:

- `gold_player_mplus_summary`
- `gold_player_mplus_score_history`
- `gold_player_mplus_weekly_activity`
- `gold_player_mplus_dungeon_breakdown`

Current intended behaviour:

- KPI strip is scoped to active raid-team members with non-zero current Raider.IO score.
- Main panels may toggle between Guild and Raid Team scope.
- Dungeon coverage heatmap should scale against the visible filtered key range.
- Score trajectory may use a relative y-axis, but absolute score values must remain visible.

### Wipe Analysis page

Wipe Analysis is the main surface for wipe diagnosis and survival utility review.

It should answer:

- where are we wiping?
- why are we wiping?
- who dies most or earliest?
- are we improving?
- what utility/cooldown capacity is unused?

Important notes:

- wipe death metrics are logged death events on wipe pulls
- they are not simple wipe participation counts
- survival scores should be presence-normalised where designed
- personal defensive capacity can be spec-gated or talent-gated
- be conservative when counting cooldowns as available
- raid/external cooldown panels use unused cast capacity, not death-window checks

---

## Guild rank mapping

Blizzard guild rank IDs are 0-indexed.

Current mapping:

| Rank ID | Label | Category | Raid Team |
| --- | --- | --- | --- |
| 0 | Guild Master | GM | yes |
| 1 | GM Alt | GM | yes |
| 2 | Officer | Officer | yes |
| 3 | Officer Alt | Officer | yes |
| 4 | Officer Alt | Officer | yes |
| 5 | Raider | Raider | yes |
| 6 | Raider Alt | Raider Alt | no |
| 7 | Bestie | Bestie | no |
| 8 | Trial | Trial | yes |
| 9 | Social | Social | no |

Raid team ranks:

    0, 1, 2, 3, 4, 5, 8

---

## WoW class ID mapping

Blizzard class IDs:

| ID | Class |
| --- | --- |
| 1 | Warrior |
| 2 | Paladin |
| 3 | Hunter |
| 4 | Rogue |
| 5 | Priest |
| 6 | Death Knight |
| 7 | Shaman |
| 8 | Mage |
| 9 | Warlock |
| 10 | Monk |
| 11 | Druid |
| 12 | Demon Hunter |
| 13 | Evoker |

---

## Ingestion rules

Do not make ingestion less robust for speed.

Important ingestion behaviours:

- WarcraftLogs uses OAuth2 client credentials.
- Blizzard Profile API uses OAuth2 client credentials.
- Raider.IO character profile endpoint is public.
- 404s from Raider.IO per character are non-fatal.
- WarcraftLogs archived reports are skipped and marked.
- WCL rate limiting should respect `Retry-After`.
- 5xx errors should use bounded exponential backoff.
- Token refresh should be proactive where implemented.
- Death ingestion should fetch deaths one fight at a time to avoid long multi-fight truncation.

Do not remove retry/rate-limit/skip-marker behaviour without a specific reason.

---

## Pipeline rules

DLT/Lakeflow pipeline priorities:

1. stable schemas
2. explicit transformations
3. safe deduplication
4. gold products designed for frontend use
5. clear lineage from source → bronze → silver → gold

Do not:

- switch Auto Loader bronze tables to schema inference casually
- collapse schemas to avoid object count without understanding quota implications
- remove expectations or data quality checks without replacing them
- change table grains without updating downstream users
- silently alter meaning of existing gold columns

If a gold table grain changes, update docs and frontend assumptions.

---

## Publishing/export rules

Recommended production path:

    scripts/publish_dashboard_assets.py

Legacy/local fallback:

    scripts/dev/export_gold_tables.py

Be careful when editing export/publishing logic:

- maintain manifest compatibility
- do not break frontend runtime fetch
- do not assume CSV is still the primary production path
- keep local/dev fallback behaviour if it still exists
- be explicit about any rerun or publish step required

---

## Testing and checks

Use relevant checks only.

Frontend checks may include:

    cd frontend
    npm run lint
    npm run typecheck
    npm run build
    npm test

Only run commands that exist in the repo.

Python checks may include:

    ruff check .
    mypy ingestion/
    pytest ingestion/tests/

Databricks deployment checks may include:

    databricks bundle validate
    databricks bundle deploy

Do not invent successful test results.

If checks are not run, say so.

---

## Manual responsive QA checklist

For frontend layout/responsive work, manually reason or test against:

- 360px mobile
- 390px mobile
- 768px tablet
- 1024px laptop
- 1440px desktop
- 1920px wide
- 2560px ultra-wide

Verify:

- navigation is usable
- desktop sidebar does not dominate mobile
- no unexpected horizontal page overflow
- cards align consistently
- dense tables scroll rather than crush content
- chart labels/tooltips remain readable
- filters are usable on touch screens
- sticky/fixed elements do not cover content
- text remains readable
- ultra-wide panels do not stretch into unreadable line lengths
- player/profile pages do not become cramped or excessively wide

---

## Planning mode

When the user asks for a plan:

- do not edit files
- inspect only enough code to ground the plan
- identify likely files to change
- separate global fixes from page-specific fixes
- give PR-sized implementation phases
- include acceptance criteria
- call out uncertainty

A good plan is executable by another coding agent without rereading the whole repo.

---

## Implementation mode

When the user asks to implement:

- edit only the files required
- keep the change scoped
- prefer existing patterns
- do not opportunistically refactor unrelated code
- do not introduce new libraries unless necessary
- do not change data semantics without explicit approval
- run relevant checks if practical

After implementation, summarise the diff and behaviour.

---

## Refactor mode

Refactors must preserve behaviour unless the user explicitly asks for behaviour changes.

Before a refactor:

- identify the target scope
- identify risky dependencies
- avoid moving data-shape logic unless needed
- keep commits/changes small

Do not combine a refactor with a visual redesign unless asked.

---

## Documentation mode

When updating docs:

- remove stale claims
- prefer concise operational truth over marketing language
- keep data-source and table-grain descriptions accurate
- update README/docs when behaviour changes materially
- do not paste huge data dictionaries into unrelated docs

---

## Common anti-patterns to avoid

Avoid:

- broad repo exploration before making a small change
- rewriting a whole page to fix a small bug
- inventing fields or API responses
- turning null analytics values into zero
- making all text smaller to fit dense layouts
- fixing mobile by hiding important content without a deliberate pattern
- fixing ultra-wide by randomly adding max-widths to individual cards
- adding new abstractions that only wrap one component
- changing pipeline table grains without updating frontend/docs
- treating legacy CSV export as the only source of frontend data
- overusing animations in dense dashboards
- using generic SaaS dashboard filler instead of actual analytics

---

## Helpful reference docs

Read only when relevant:

    docs/architecture/overview.md
    docs/architecture/dashboard_data_publishing.md
    docs/data_dictionary/README.md
    docs/runbooks/README.md
    docs/runbooks/migrate-bronze-landing.md
    docs/runbooks/rerun-ingestion.md
    docs/adr/README.md

If these docs conflict with code, trust code first and note the documentation mismatch.

---

## Final principle

This project is valuable because it connects a real data pipeline to a real analytics frontend.

Preserve that.

Make changes that improve correctness, clarity, maintainability, and usability without flattening the project into a generic demo dashboard.
