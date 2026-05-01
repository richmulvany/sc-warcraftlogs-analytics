# Dashboard Data Publishing

## Why this exists

The dashboard used to depend on generated CSV files being written into
`frontend/public/data` and committed back to GitHub. That made deploys noisy,
mixed generated artifacts with source code, and tied frontend hosting to the
repository checkout.

The project now uses a static data publishing flow instead:

Databricks gold tables -> UC Volume JSON assets -> GitHub Actions -> Cloudflare R2 -> static frontend

This keeps the frontend statically hostable while removing generated dashboard
data from the repo. The active GitHub Actions deployment path publishes data to
Cloudflare R2.

## Why GitHub Actions sits in the middle

This repo runs on Databricks Free Edition. The implementation does **not**
assume Databricks can upload directly to Cloudflare R2.

Instead:

1. Databricks writes dashboard JSON assets to a Unity Catalog volume.
2. GitHub Actions downloads those files using the Databricks CLI.
3. GitHub Actions uploads them to Cloudflare R2 using S3-compatible credentials.
4. The frontend fetches them from a public R2 URL at runtime.

## Databricks-side assets

Publisher script:

- [scripts/publish_dashboard_assets.py](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/scripts/publish_dashboard_assets.py)

Databricks job wrapper:

- [ingestion/jobs/publish_dashboard_assets.py](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/ingestion/jobs/publish_dashboard_assets.py)

Default output path:

- `/Volumes/03_gold/sc_analytics/dashboard_exports`

Output layout:

```text
/Volumes/03_gold/sc_analytics/dashboard_exports/
  latest/
    manifest.json
    raid_summary.json
    player_attendance.json
    ...
  snapshots/
    2026-04-25T02-15-00Z/
      manifest.json
      raid_summary.json
      player_attendance.json
      ...
```

The publisher writes:

- `manifest.json`
- one JSON file per exported dataset
- a timestamped snapshot folder
- a mirrored `latest/` folder for the frontend

`manifest.json` also includes data contract metadata:

- `contract_set_version` from `pipeline/contracts/data_products.yml`
- per-dataset dashboard asset `contract_id` and `contract_version`
- per-dataset source Gold `source_contract_id` and `source_contract_version`

The frontend sidebar displays the contract set version and manifest
`generated_at` timestamp. The per-dataset metadata is retained in the manifest
for diagnostics and future data-about views.

## GitHub Actions workflow

Workflow:

- `sc-analytics-publish-dashboard-data`
  ([.github/workflows/publish-dashboard-data.yml](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/.github/workflows/publish-dashboard-data.yml))

It:

1. installs the Databricks CLI
2. downloads `dbfs:/Volumes/03_gold/sc_analytics/dashboard_exports/latest`
3. validates `manifest.json`
4. prints a file size summary and fails if any single JSON file exceeds 25 MB
5. fails if the downloaded `latest/` folder exceeds 125 MB
6. uploads only `latest/*` to Cloudflare R2
7. uploads the current snapshot to `snapshots/<snapshot_id>/`

## Required GitHub secrets

Databricks:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

Cloudflare R2:

- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- optional `R2_PREFIX`
`R2_PREFIX` is optional and should be a parent folder only. For a public URL like
`https://data.sc-analytics.org/latest`, leave `R2_PREFIX` empty. Do not set it
to `latest`, or you will end up publishing to `latest/latest/...`.

## Required Cloudflare R2 setup

You need:

- an R2 bucket
- an access key / secret with write access to that bucket
- a public read URL for the bucket contents
- a CORS policy that allows the frontend's origin

Recommended:

- attach a custom domain such as `https://data.your-domain.com/latest`
- point the frontend at that public URL with `VITE_DASHBOARD_DATA_BASE_URL`
- add an R2 lifecycle rule that deletes `snapshots/` objects after 14 days
- configure a Cloudflare Budget Alert at the lowest available threshold, ideally £0.01 or £1

### CORS

The frontend fetches `manifest.json` and dataset JSON files cross-origin from the
data subdomain. The browser will refuse to read those responses unless R2 returns
an `Access-Control-Allow-Origin` header for the frontend's origin.

The CORS policy lives at
[infra/cloudflare/r2-cors.json](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/infra/cloudflare/r2-cors.json)
as a reference policy, but it is **not** applied by the GitHub Action. CORS is
bucket configuration, not data publishing, and the R2 access key used by CI only
needs object read/write permissions. Any new frontend origin (custom domain,
preview domain, additional localhost port) must be added to the bucket's CORS
policy manually in Cloudflare or with a separately-privileged admin key.

Apply it manually once with an admin-capable key:

```bash
aws s3api put-bucket-cors \
  --bucket "$R2_BUCKET" \
  --cors-configuration "file://infra/cloudflare/r2-cors.json" \
  --endpoint-url "https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
```

To verify CORS for an origin:

```bash
curl -sv -H "Origin: https://your.frontend" \
  https://data.sc-analytics.org/latest/manifest.json -o /dev/null 2>&1 \
  | grep -i access-control-allow-origin
```

A missing header means the origin is not in the allowlist.

## Frontend runtime config

Environment variable:

- `VITE_DASHBOARD_DATA_BASE_URL`

Example:

```bash
VITE_DASHBOARD_DATA_BASE_URL=https://data.sc-analytics.org/latest
```

When it is set:

- the frontend fetches `manifest.json`
- datasets are loaded from the manifest-driven JSON paths
- the header shows `generated_at` from the manifest

When it is **not** set:

- the frontend falls back to the old local static CSV path (`/data`)
- this preserves local development and rollback options
- in production builds without `frontend/public/data/` committed, those CSV
  fetches will 404 — the manifest URL is the load-bearing config

`useManifest` surfaces fetch failures via its `error` field and logs them to the
browser console, so a missing manifest no longer fails silently.

## Export guardrails

The Databricks publisher enforces these defaults:

- `MAX_DATASET_ROWS = 100_000`
- `MAX_DATASET_BYTES = 25 MB`
- `MAX_TOTAL_EXPORT_BYTES = 175 MB`

They can be overridden with:

- `DASHBOARD_EXPORT_MAX_DATASET_ROWS`
- `DASHBOARD_EXPORT_MAX_DATASET_BYTES`
- `DASHBOARD_EXPORT_PLAYER_DEATH_EVENTS_MAX_DATASET_BYTES`
- `DASHBOARD_EXPORT_MAX_TOTAL_EXPORT_BYTES`

If any dataset or the overall export exceeds the configured limits, the publish fails loudly.
High-volume event-level datasets may define narrower per-dataset overrides rather than raising
the global asset limit for every product. `player_death_events` currently defaults to 40 MB
because it is a row-level event feed used by Wipe Analysis and player detail views.

## Contract Validation

Dashboard publishing validates rows before writing the snapshot:

1. source rows are validated against a matching Gold contract from
   `pipeline/contracts/gold/`
2. exported JSON rows are validated against a matching dashboard asset contract
   from `pipeline/contracts/dashboard_assets/`
3. only a fully valid snapshot is copied into `latest/`

Datasets without contracts publish with warnings by default. Set
`DASHBOARD_CONTRACT_STRICT=true` to fail when any exported dataset is missing a
Gold or dashboard asset contract.

## Manual runbook

### 1. Publish assets in Databricks

Run the asset-write stage job (writes JSON to the UC Volume only):

```bash
databricks bundle run write_dashboard_assets
```

### 2. Push assets to R2

Either trigger the publish stage from Databricks (which dispatches the
GitHub Actions workflow and waits for completion):

```bash
databricks bundle run publish_post_write
```

…or run the GitHub Actions workflow manually with `workflow_dispatch`:

- `Publish Dashboard Data`

### 3. Run the full daily flow on demand

```bash
databricks bundle run daily_orchestrator
```

### 3. Verify

Check:

- `manifest.json` exists in R2
- dataset JSON files exist under the configured public URL
- the frontend header shows a recent “updated” time

## Rollback

If the hosted JSON path is unavailable, remove `VITE_DASHBOARD_DATA_BASE_URL` from
the frontend deployment environment and the app will fall back to local/static CSV
loading.

That gives a clean migration path without forcing a big-bang frontend rewrite.
