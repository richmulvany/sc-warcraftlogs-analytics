# Runbook: Rotate API Credentials

## Scope

Two Databricks secret scopes are used.

`warcraftlogs` — read by the ingestion job:
- `client_id`
- `client_secret`
- `blizzard_client_id`
- `blizzard_client_secret`

`github` — read by `sc-analytics-publish-post-write` to dispatch the
GitHub Actions workflow that uploads dashboard assets to Cloudflare R2:
- `github_token` — fine-grained PAT with `Actions: Read & Write` on this repo
- `github_repo` — `owner/repo` (only required if the job parameter is left blank)

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
databricks bundle run ingestion_daily
```

## Rotate GitHub trigger token

```bash
databricks secrets put-secret github github_token
databricks secrets put-secret github github_repo  # owner/repo
```

To verify the token works without running the full pipeline, dispatch the
publish stage on its own:

```bash
databricks bundle run publish_post_write
```

## After rotation

- rerun ingestion if the prior run failed with `401` or auth errors
- run the DLT update if new data landed successfully
- rerun exports if downstream CSVs need to be refreshed
