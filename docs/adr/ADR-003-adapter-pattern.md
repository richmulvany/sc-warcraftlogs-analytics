# ADR-003: Adapter Pattern for Ingestion

**Date:** 2025-01-01
**Status:** Accepted

## Context

This project is designed as a reusable template. The ingestion layer must be easy
to swap when the template is applied to a different data source.

## Decision

Implement a `BaseAdapter` abstract class with three required methods: `authenticate()`,
`fetch()`, and `validate()`. All ingestion logic is encapsulated inside adapters.

## Rationale

- **Reusability** — cloning the template and replacing one adapter directory is all that's
  needed to point the entire pipeline at a new data source
- **Testability** — adapters can be unit-tested in isolation with HTTP mocking
- **Separation of concerns** — the Bronze/Silver/Gold layers are completely decoupled from
  the specifics of any API

## Alternatives Considered

**Direct API calls in job scripts:** Simpler initially but tightly couples the pipeline
to one data source. Reusing the template requires significant refactoring.

**Airbyte / Fivetran connectors:** Production-grade but introduces an external dependency
and is overkill for a portfolio project with a single data source.

## Consequences

- All API-specific logic must live inside an adapter — no direct HTTP calls in job scripts
- New adapters must implement all three interface methods
- The adapter pattern adds a small amount of abstraction overhead for simple cases
