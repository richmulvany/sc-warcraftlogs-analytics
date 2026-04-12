# Runbook: Re-run Failed Ingestion

## Symptoms

- GitHub Actions `Export Data` workflow shows no new data
- Gold tables have a stale `_gold_generated_at` timestamp
- Databricks Job run shows `FAILED` status

## Steps

### 1. Identify the failure

In the Databricks UI: Workflows → Jobs → Nightly Ingestion → View run history

Check the error in the failed task's output. Common causes:
- API rate limit hit (HTTP 429)
- Authentication expired (HTTP 401)
- Network timeout

### 2. For rate limit errors (HTTP 429)

Wait 15 minutes, then manually trigger the job:

```bash
databricks jobs run-now --job-id <job-id>
```

Or via the UI: Workflows → Jobs → Nightly Ingestion → Run now.

### 3. For authentication errors (HTTP 401)

Rotate the API key:
1. Generate a new key in your API provider's dashboard
2. Update the Databricks secret: `databricks secrets put-secret pipeline-secrets api-key --string-value "new-key"`
3. Re-run the job

### 4. Verify recovery

After re-running:
- Check the job shows `SUCCEEDED`
- Verify row counts in the bronze table increased: `SELECT COUNT(*) FROM main.pipeline_prod.bronze_entities`
- Manually trigger the Export Data workflow if data is urgently needed

### 5. Check for data gaps

If the job failed for multiple days, run the ingestion job once per missed day
with the appropriate date parameters (if your adapter supports backfill).
