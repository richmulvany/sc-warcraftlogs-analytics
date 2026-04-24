# Data Contract: preparation_identity_overrides

| Field | Value |
|-------|-------|
| **Status** | Draft |
| **Version** | 0.1.0 |
| **Effective date** | 2026-04-24 |
| **Producer** | Admin-managed Unity Catalog table (`00_governance.warcraftlogs_admin.preparation_identity_overrides`) |
| **Consumer** | Frontend dashboard (`frontend/src/pages/Preparation.tsx`) via static export (`scripts/export_gold_tables.py`) |
| **Owner** | richmulvany |
| **Spec** | [datacontract.com v0.9](https://datacontract.com) |

---

## Purpose

Provides manual identity overrides for the Preparation page when a single raider
plays multiple characters in the current tier.

This table is intentionally outside the DLT pipeline schema:
- it is operational metadata, not pipeline-derived fact data
- it must survive pipeline reruns untouched
- it avoids adding more objects to the already constrained DLT schema

The table supports two cases:
- `replace`: one roster character should inherit another character's current-tier preparation data
- `pool`: multiple characters should be treated as one raid identity for attendance and readiness

---

## Schema

All columns below are guaranteed to be present in the exported CSV consumed by the frontend.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | STRING | No | Stable override identifier. Primary key. |
| `mode` | STRING | No | One of `replace` or `pool`. |
| `source_character` | STRING | Yes | Source roster character for `replace`. Null for `pool`. |
| `target_character` | STRING | Yes | Target tracked character for `replace`. Null for `pool`. |
| `characters` | STRING | Yes | Pipe-delimited character list for `pool`. Null for `replace`. |
| `display_name` | STRING | Yes | Canonical raider label shown in the UI. |
| `enabled` | BOOLEAN | No | Whether the override is active. |
| `notes` | STRING | Yes | Optional admin notes. |
| `updated_by` | STRING | Yes | Authenticated actor who last changed the row. |
| `updated_at` | TIMESTAMP | Yes | Timestamp of the last change. |

### Row guarantees

- `id` is unique
- `mode = 'replace'` requires both `source_character` and `target_character`
- `mode = 'pool'` requires `characters` with at least two pipe-delimited names
- disabled rows remain exportable for auditability, but the consumer ignores them

---

## Freshness SLA

| Commitment | Value |
|------------|-------|
| Target refresh time | On-demand after an admin change |
| Maximum acceptable staleness | Undefined; this is manual state |
| Staleness detection | Compare exported CSV timestamp in git or file metadata |

This is operator-managed state, not scheduled fact data. The important guarantee
is correctness after an explicit admin change, not a daily refresh cadence.

---

## Quality Guarantees

- Export preserves every row from the UC table
- Frontend ignores malformed or disabled rows rather than crashing
- Consumer logic must tolerate additive metadata columns

Validation should be enforced at write time by the admin workflow, not by the
frontend parser.

---

## Breaking vs Non-Breaking Changes

### Non-breaking
- adding nullable metadata columns such as `created_at`
- changing note text or display names
- adding new rows

### Breaking
- renaming or removing any of the schema columns above
- changing `characters` away from pipe-delimited string encoding without a coordinated consumer update
- changing the meaning of `replace` or `pool`

---

## Change Management

1. Update the UC table definition and this contract in the same change.
2. Update the governed export in `scripts/export_gold_tables.py` if the exported column set changes.
3. Update the Preparation page parser if a breaking change is introduced.

---

## Version History

| Version | Date | Change |
|---------|------|--------|
| 0.1.0 | 2026-04-24 | Initial draft |
