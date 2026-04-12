# Data Contracts

This directory contains data contracts for all Gold layer data products.

A data contract is a formal agreement between the pipeline (producer) and the
frontend dashboard (consumer) that defines schema guarantees, freshness SLAs,
quality commitments, and the change management policy.

This is distinct from the [data dictionary](../data_dictionary/README.md), which
is purely descriptive reference material. A contract is a *promise*.

---

## Contracts

| Contract | Table | Status |
|----------|-------|--------|
| [gold_entity_summary](gold_entity_summary.md) | `gold_entity_summary` | Active |

---

## Contract Lifecycle

| Status | Meaning |
|--------|---------|
| `Draft` | Under discussion, not yet enforced |
| `Active` | In force — both producer and consumer rely on it |
| `Deprecated` | Will be removed; replacement contract noted |
| `Retired` | No longer in use |

---

## Adding a Contract

When you add a new gold table, create a corresponding contract here.
Copy `gold_entity_summary.md` as a starting template and fill in all sections.

The contract should be created alongside the table — not after the fact.
It forces the right questions to be asked before the table is built:
what does the consumer actually need, and what can the producer reliably guarantee?
