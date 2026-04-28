# Runbooks

Operational guides for common tasks.

| Runbook | Description |
|---------|-------------|
| [Production deploy](production-deploy.md) | Validate and deploy the Databricks bundle to the production target |
| [WCL parse-null backfill](wcl-parse-null-backfill.md) | Diagnose and rerun incomplete WCL rankings payloads |
| [Blizzard 403 / auth failures](blizzard-403.md) | Recover Blizzard guild/profile ingestion after credential or permission failures |
| [Migrate bronze landing volumes](migrate-bronze-landing.md) | Archive-only record of the completed landing-volume migration |
| [Re-run failed ingestion](rerun-ingestion.md) | Incremental reruns, targeted report backfills, and death-data recovery after WCL truncation |
| [Add a gold table](add-gold-table.md) | How to add a new Gold layer data product |
| [Rotate API credentials](rotate-credentials.md) | Updating secrets without downtime |
