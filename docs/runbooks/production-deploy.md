# Runbook: Production Deploy

## Goal

Deploy the Databricks bundle to the production target and keep the daily orchestrator scheduled only there.

## CI path

- feature branches run `databricks bundle validate --target development` and `--target production`
- `main` runs `databricks bundle deploy --target production`
- Free Edition production deploys run as `developer_user` because service
  principals are not available in this workspace setup.
- CI sets `developer_user` from the GitHub `production` environment secret
  `DATABRICKS_USER_NAME`.

Workflow:

- [.github/workflows/databricks-deploy.yml](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/.github/workflows/databricks-deploy.yml)

## Target behavior

- `development`
  - workspace path is per-user under `/Workspace/Users/<developer>/sc-analytics/development`
  - no orchestrator schedule
  - intended for on-demand runs only
- `production`
  - workspace path is `/Workspace/sc-analytics/production`
  - bundle runs as `developer_user`
  - `daily_orchestrator` is scheduled and `UNPAUSED`

## Manual validation

```bash
BUNDLE_VAR_developer_user="<databricks-user-email>" \
databricks bundle validate --target development
BUNDLE_VAR_developer_user="<databricks-user-email>" \
databricks bundle validate --target production
```

## Manual deploy

```bash
BUNDLE_VAR_developer_user="<databricks-user-email>" \
databricks bundle deploy --target production
```

## Post-deploy checks

1. Confirm the `daily_orchestrator` exists only on the production target as a scheduled job.
2. Confirm `warcraftlogs_pipeline` resolves with `development: false` on production.
3. Run one on-demand `daily_orchestrator` execution if you need an immediate refresh.
