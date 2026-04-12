# Data Contract: gold_entity_summary

| Field | Value |
|-------|-------|
| **Status** | Active |
| **Version** | 1.0.0 |
| **Effective date** | 2026-04-11 |
| **Producer** | DLT pipeline (`pipeline/gold/entity_summary.py`) |
| **Consumer** | Frontend dashboard (`frontend/src/api/index.ts`) |
| **Owner** | richmulvany |
| **Spec** | [datacontract.com v0.9](https://datacontract.com) |

---

## Purpose

Provides a per-category summary of entities for display in the frontend dashboard.
This is the primary data product powering the Overview and Breakdown sections.

---

## Schema

All columns below are guaranteed to be present. The consumer must not depend on
any column not listed here.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `category` | STRING | No | Entity category name. Unique per row. |
| `total_count` | LONG | No | Total number of entity records in this category. Always ≥ 1. |
| `unique_count` | LONG | No | Count of distinct entity IDs. Always ≤ `total_count`. |
| `latest_created_at` | TIMESTAMP | No | Most recent entity creation timestamp in this category. |
| `earliest_created_at` | TIMESTAMP | No | Oldest entity creation timestamp in this category. |
| `_gold_generated_at` | TIMESTAMP | No | Pipeline run timestamp. For provenance only — do not use in business logic. |

### Row guarantee

One row per `category`. The set of categories is not guaranteed to be stable —
new categories may appear and existing ones may disappear as source data changes.
The consumer must handle an unknown category gracefully.

---

## Freshness SLA

| Commitment | Value |
|------------|-------|
| Target refresh time | 03:00 UTC daily |
| Maximum acceptable staleness | 25 hours |
| Staleness detection | `_gold_generated_at` is exposed in the JSON export manifest |

The frontend displays `exported_at` from the manifest to inform users of data age.
If the pipeline fails and the export is older than 25 hours, the consumer should
surface a visible staleness warning.

---

## Quality Guarantees

The following DLT expectations are enforced at pipeline runtime.
Rows violating these rules are dropped before reaching this table.

| Rule | Expression | Action on failure |
|------|------------|-------------------|
| No null IDs | `id IS NOT NULL` | Row dropped at Silver |
| No null categories | `category IS NOT NULL` | Row dropped at Silver |
| Non-negative counts | `total_count >= 0` | Pipeline fails |

In addition, the export script validates that the JSON file is non-empty before
committing it to the repository. A failed export does not overwrite the previous
valid export.

---

## Breaking vs Non-Breaking Changes

### Non-breaking (no consumer coordination required)
- Adding a new `_metadata` column (prefix `_` is reserved for pipeline internals)
- Adding a new category value
- Increasing the precision of an existing numeric column

### Breaking (requires consumer update and version bump)
- Renaming or removing any column in the schema table above
- Changing the type of any column
- Changing the granularity (e.g. moving from per-category to per-category-per-day)
- Removing a previously guaranteed category

---

## Change Management

1. **Propose** — open a GitHub issue describing the change and whether it is breaking
2. **Notify** — if breaking, update the consumer code in the same PR as the producer change
3. **Version** — increment the contract version (`MAJOR` for breaking, `MINOR` for additive, `PATCH` for corrections to this document)
4. **Document** — update this file and add an entry to `CHANGELOG.md`

For a solo project, steps 1–2 can be a single PR with a clear commit message.
The discipline still matters: it builds the habit and keeps the git history self-documenting.

---

## Known Limitations

- Soft-deleted entities are not excluded. If the source API introduces a `deleted_at`
  field in future, `total_count` may decrease unexpectedly. This will be treated as a
  breaking change at that point.
- The `category` field is passed through from the source API without normalisation.
  Case sensitivity is not guaranteed across API versions.

---

## Version History

| Version | Date | Change |
|---------|------|--------|
| 1.0.0 | 2026-04-11 | Initial contract |
