# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for this project.

ADRs document significant technical decisions: what was decided, why, and what alternatives were considered.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-001](ADR-001-dlt-over-notebooks.md) | Use DLT instead of notebooks + jobs | Accepted |
| [ADR-002](ADR-002-static-json-export.md) | Static JSON export for frontend data | Accepted |
| [ADR-003](ADR-003-adapter-pattern.md) | Adapter pattern for ingestion | Accepted |

## Creating a new ADR

Copy the template below into a new file named `ADR-NNN-short-title.md`.

```markdown
# ADR-NNN: Title

**Date:** 2026-04-11
**Status:** Proposed | Accepted | Deprecated | Superseded by ADR-NNN

## Context
What is the problem or situation this decision addresses?

## Decision
What was decided?

## Rationale
Why was this chosen over alternatives?

## Alternatives Considered
What other approaches were evaluated and why were they rejected?

## Consequences
What are the trade-offs or follow-on implications?
```
