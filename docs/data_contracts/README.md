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

1. Source rows are validated against the matching Gold product contract when one
   exists.
2. Exported JSON rows are validated against the matching dashboard asset
   contract when one exists.
3. Contract failures abort before `latest/` is swapped.
4. Datasets without contracts are allowed by default but logged as warnings.

Set `DASHBOARD_CONTRACT_STRICT=true` to fail publishing when a frontend-exported
dataset lacks either a dashboard asset contract or a Gold product contract.

## Manifest Metadata

The publisher writes contract metadata into `manifest.json`:

- `contract_set_version`
- per-dataset `contract_id` and `contract_version`
- per-dataset `source_contract_id` and `source_contract_version`

The frontend sidebar displays the contract set version and data timestamp. Per
dataset contract versions stay in the manifest for debugging and future “About
data” views.

## Current Enforced Contracts

Dashboard-critical Gold/dashboard products are contracted first. Internal-only
Gold tables may remain uncontracted until they become shared or frontend
consumed.

| Product | Gold Contract | Dashboard Asset Contract |
|---------|---------------|--------------------------|
| Preparation readiness | `pipeline/contracts/gold/gold_preparation_readiness.yml` | `pipeline/contracts/dashboard_assets/preparation_readiness.yml` |
| Wipe survival discipline | `pipeline/contracts/gold/gold_wipe_survival_discipline.yml` | `pipeline/contracts/dashboard_assets/wipe_survival_discipline.yml` |
| Player survivability rankings | `pipeline/contracts/gold/gold_player_survivability_rankings.yml` | `pipeline/contracts/dashboard_assets/player_survivability_rankings.yml` |
| Player Mythic+ summary | `pipeline/contracts/gold/gold_player_mplus_summary.yml` | `pipeline/contracts/dashboard_assets/player_mplus_summary.yml` |
| Boss kill roster | `pipeline/contracts/gold/gold_boss_kill_roster.yml` | `pipeline/contracts/dashboard_assets/boss_kill_roster.yml` |
| Raid summary | `pipeline/contracts/gold/gold_raid_summary.yml` | `pipeline/contracts/dashboard_assets/raid_summary.yml` |
| Player attendance | `pipeline/contracts/gold/gold_player_attendance.yml` | `pipeline/contracts/dashboard_assets/player_attendance.yml` |
| Player performance summary | `pipeline/contracts/gold/gold_player_performance_summary.yml` | `pipeline/contracts/dashboard_assets/player_performance_summary.yml` |
| Boss progression | `pipeline/contracts/gold/gold_boss_progression.yml` | `pipeline/contracts/dashboard_assets/boss_progression.yml` |
| Best kills | `pipeline/contracts/gold/gold_best_kills.yml` | `pipeline/contracts/dashboard_assets/best_kills.yml` |

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
