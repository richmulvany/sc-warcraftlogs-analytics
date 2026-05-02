# Data Contracts

Data contracts are executable producer/consumer promises for governed data
products. They are distinct from the data dictionary: the dictionary explains
fields, while contracts define what the pipeline and dashboard may rely on.

## Contract Layers

This project uses two contract layers:

- **Gold product contracts** live in `pipeline/contracts/gold/`. They describe
  Gold tables as producer-owned data products, including grain, primary key,
  metric semantics, allowed nulls, and quality rules.
- **Dashboard asset contracts** live in `pipeline/contracts/dashboard_assets/`.
  They describe the static JSON datasets consumed by the frontend after any
  projection or presentation-safe coercion in the publisher.

The catalog file `pipeline/contracts/data_products.yml` ties those contracts
together and carries the project-level `contract_set_version` that is published
in `manifest.json`.

Final-state policy:

- Every table in `03_gold.sc_analytics` has a Gold product contract.
- Every exported dashboard JSON dataset has a dashboard asset contract.
- Silver tables are not contracted in this system; Silver remains the cleaned
  integration layer, while Gold is the governed semantic product layer.
- Operational governance tables outside Gold may have dashboard asset contracts
  when they are exported as frontend JSON, but they are not Gold products.

## Field Semantics

Every field listed in a contract must exist in every row. Value validity is
separate:

- `required`: kept for Data Contract Specification compatibility and to signal
  that the field is part of the contract surface.
- `nullable`: whether `null` is a valid value.
- `allowEmpty`: whether an empty string is a valid value.

Do not mark a field `required: false` just because `""` is valid. Use
`required: true`, `nullable: false`, and `allowEmpty: true` for intentional empty
string sentinels such as `override_label`.

Unique keys should normally use fields with `nullable: false` and
`allowEmpty: false`.

## Validation Flow

During dashboard asset publishing:

1. Exported source projections are validated against the matching Gold product
   contract when one exists. Because dashboard assets intentionally publish
   narrow projections rather than `SELECT *`, this validation checks selected
   Gold fields and only the key/rules whose fields are present.
2. Exported JSON rows are validated against the matching dashboard asset
   contract when one exists.
3. Contract failures abort before `latest/` is swapped.
4. Datasets without contracts are allowed by default but logged as warnings.

Set `DASHBOARD_CONTRACT_STRICT=true` to fail publishing when a frontend-exported
dataset lacks either a dashboard asset contract or, for Gold-backed exports, a
Gold product contract.

## Manifest Metadata

The publisher writes contract metadata into `manifest.json`:

- `contract_set_version`
- per-dataset `contract_id` and `contract_version`
- per-dataset `source_contract_id` and `source_contract_version`

The frontend sidebar displays the contract set version and data timestamp. Per
dataset contract versions stay in the manifest for debugging and future “About
data” views.

## Current Enforced Contracts

All Gold product contracts are listed in
`pipeline/contracts/data_products.yml`. The same catalog records whether a Gold
product is exposed to the dashboard, chatbot, internal downstream jobs, or
monitoring. Dashboard asset contracts are required for every dataset exported by
`scripts/publish_dashboard_assets.py`, including operator-managed governance
assets such as `preparation_overrides`.

## Contract Lifecycle

| Status | Meaning |
|--------|---------|
| `draft` | Under discussion, not yet enforced |
| `active` | In force and validated |
| `deprecated` | Still available, replacement planned |
| `retired` | No longer used |

Version contract files manually. Increment a contract version for any schema,
grain, semantic, or validation-rule change that downstream consumers need to
understand.
