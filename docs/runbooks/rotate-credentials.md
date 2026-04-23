# Runbook: Rotate API Credentials

## Scope

The ingestion job currently reads secrets from the Databricks secret scope `warcraftlogs`.

Required keys:
- `client_id`
- `client_secret`
- `blizzard_client_id`
- `blizzard_client_secret`

Raider.IO does not require credentials.

## Rotate WarcraftLogs credentials

```bash
databricks secrets put-secret warcraftlogs client_id --string-value "your-new-client-id"
databricks secrets put-secret warcraftlogs client_secret --string-value "your-new-client-secret"
```

## Rotate Blizzard credentials

```bash
databricks secrets put-secret warcraftlogs blizzard_client_id --string-value "your-new-client-id"
databricks secrets put-secret warcraftlogs blizzard_client_secret --string-value "your-new-client-secret"
```

No pipeline redeploy is required. Secrets are read at job runtime.

## Verify

1. Run the ingestion job.
2. Confirm authentication succeeds for the affected source.
3. Confirm fresh landing files appear in the expected directories.

Example:

```bash
databricks bundle run nightly_ingestion
```

## After rotation

- rerun ingestion if the prior run failed with `401` or auth errors
- run the DLT update if new data landed successfully
- rerun exports if downstream CSVs need to be refreshed
