# Architecture Overview

## System Design

This project follows a **medallion architecture** on Databricks, with a static-export pattern for the frontend to avoid the need for any backend server.

```
┌──────────────────────────────────────────────────────────────────┐
│                        Databricks Workspace                       │
│                                                                    │
│  ┌─────────────┐    ┌──────────────────────────────────────────┐  │
│  │  Ingestion  │    │         DLT Pipeline (Lakeflow)          │  │
│  │    Jobs     │───>│  Bronze ──> Silver ──> Gold              │  │
│  │             │    │                                          │  │
│  └─────────────┘    └───────────────────┬──────────────────────┘  │
│                                         │                          │
└─────────────────────────────────────────┼──────────────────────────┘
                                          │ Nightly export job
                                          ▼
                                  ┌───────────────┐
                                  │  Static JSON  │
                                  │  (committed   │
                                  │   to repo)    │
                                  └───────┬───────┘
                                          │ GitHub Actions deploy
                                          ▼
                                  ┌───────────────┐
                                  │    React      │
                                  │  Frontend     │
                                  │  (Vercel /    │
                                  │  GH Pages)    │
                                  └───────────────┘
```

## Layer Responsibilities

### Ingestion
- Fetches data from the external API using the pluggable adapter pattern
- Writes raw records to Delta tables with metadata columns (`_ingested_at`, `_source`, `_endpoint`)
- Runs as a scheduled Databricks Job (not part of the DLT pipeline)

### Bronze
- Near-verbatim copy of the raw ingestion tables
- Applies schema enforcement only
- Drops clearly invalid rows using `@dlt.expect_or_drop`
- Preserves all original fields — never modifies source values

### Silver
- Cleans, normalises, and deduplicates Bronze data
- Casts types, trims strings, resolves foreign keys
- Applies the full set of data quality expectations
- Is the source of truth for downstream Gold tables

### Gold
- Business-ready data products consumed by the frontend
- Aggregated, enriched, and named for business users
- Exported nightly to static JSON by `scripts/export_gold_tables.py`

## Key Design Decisions

See the [ADR index](../adr/README.md) for documented decisions including:
- Why DLT over notebooks + jobs
- Why static JSON export over a live backend
- Why the adapter pattern for ingestion

## Data Flow Sequence

1. Ingestion job runs (scheduled, or manually triggered)
2. Raw records land in `bronze_*` tables
3. DLT pipeline is triggered (scheduled or continuous)
4. Bronze → Silver → Gold transformations execute
5. Nightly export job queries Gold tables via SQL warehouse
6. JSON files are committed to `data/exports/`
7. GitHub Actions rebuilds and deploys the frontend
