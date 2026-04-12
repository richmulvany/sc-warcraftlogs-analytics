# Runbook: Rotate API Credentials

## Steps

### 1. Generate new credentials

Obtain the new API key from your data source provider.

### 2. Update the Databricks secret

```bash
databricks secrets put-secret pipeline-secrets api-key --string-value "your-new-key"
```

No restart is required — secrets are read at job runtime.

### 3. Verify

Trigger a test ingestion run:

```bash
databricks jobs run-now --job-id <job-id>
```

Check that it completes successfully.

### 4. Update GitHub Actions secret (if used)

If `SOURCE_API_KEY` is also stored as a GitHub Actions secret (e.g. for the export job),
update it in: Repository Settings → Secrets and variables → Actions.

### 5. Revoke the old key

Revoke the old key in your API provider's dashboard once you've confirmed the new one works.
