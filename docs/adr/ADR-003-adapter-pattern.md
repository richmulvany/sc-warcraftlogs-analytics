# ADR-003: Adapter Pattern for Ingestion

**Date:** 2025-01-01
**Status:** Accepted

## Context

This project ingests several source APIs. The ingestion layer must keep
source-specific API logic isolated so job orchestration and downstream tables do
not depend on each client's transport details.

## Decision

Implement a `BaseAdapter` abstract class with three required methods: `authenticate()`,
`fetch()`, and `validate()`. All ingestion logic is encapsulated inside adapters.

## Rationale

- **Extensibility** — adding a source requires a focused adapter/client instead
  of spreading API-specific HTTP logic through job scripts
- **Testability** — adapters can be unit-tested in isolation with HTTP mocking
- **Separation of concerns** — the Bronze/Silver/Gold layers are completely decoupled from
  the specifics of any API

## Alternatives Considered

**Direct API calls in job scripts:** Simpler initially but tightly couples
orchestration to source-specific transport details.

**Airbyte / Fivetran connectors:** Production-grade but introduces an external
dependency and does not fit all of the WarcraftLogs, Blizzard, Raider.IO, and
Google Sheets source-specific logic in this repo.

## Consequences

- All API-specific logic must live inside an adapter — no direct HTTP calls in job scripts
- New adapters must implement all three interface methods
- The adapter pattern adds a small amount of abstraction overhead for simple cases
