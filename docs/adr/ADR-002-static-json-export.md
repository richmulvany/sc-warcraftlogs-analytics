# ADR-002: Static JSON Export for Frontend Data

**Date:** 2025-01-01
**Status:** Accepted

## Context

The React frontend needs to display data from the Gold Delta tables. Options for
serving this data include a live backend API, Databricks SQL direct queries from
the frontend, and pre-generated static files.

## Decision

Export Gold tables to static JSON files nightly via a GitHub Actions workflow.
The frontend fetches these files directly with no backend server.

## Rationale

- **No backend to maintain** — eliminates a server, a deployment, and ongoing costs
- **Fast frontend** — static JSON served from a CDN is faster than a live database query
- **Free hosting** — Vercel, Netlify, and GitHub Pages serve static files for free, with no spin-down
- **Data is not real-time** — guild raid data updates at most once per day, so nightly export is sufficient
- **Simplicity** — the export script is 60 lines of Python; a backend API would be 10x that

## Alternatives Considered

**FastAPI / Express proxy server:** Requires a always-on server (cost) or accepts cold-start
latency (bad UX). Adds a deployment to maintain. Overkill for read-only, low-frequency data.

**Databricks SQL direct from frontend:** Exposes Databricks credentials in the browser.
Not acceptable from a security standpoint.

**Databricks Lakeview / Genie:** Locks the frontend into Databricks' BI tooling and
eliminates the custom React dashboard.

## Consequences

- Data is always up to 24 hours stale
- The nightly export job must succeed for the frontend to reflect new data
- If the Gold schema changes, the frontend API layer must be updated simultaneously
