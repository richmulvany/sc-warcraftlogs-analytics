# Deploying the chatbot backend to Azure

The Dockerfile at [`backend/Dockerfile`](Dockerfile) produces a single
container image that runs the FastAPI app under `uvicorn`. The image is
self-contained: the semantic registry is rebuilt from
`pipeline/contracts/gold/*.yml` during the build so what runs in Azure exactly
matches what's checked in.

## Local sanity check

```bash
# Build with the repo root as context (Dockerfile lives in backend/).
docker build -f backend/Dockerfile -t sc-analytics-chatbot .

# Run with your existing root .env (loaded by backend.app.config).
docker run --rm -p 8000:8000 --env-file .env sc-analytics-chatbot

# In another terminal:
curl -s localhost:8000/healthz
curl -s localhost:8000/chat -H 'Content-Type: application/json' \
  -d '{"question":"Which bosses are we wiping on most?"}' | jq
```

## Azure target — recommendation

Pick **Azure Container Apps** unless you already use App Service. Container
Apps is simpler for stateless HTTP services, autoscales to zero, has built-in
managed-identity Databricks-friendly auth options, and binds secrets as env
vars without any wrappers.

If you already have an App Service plan, **Azure App Service for Containers**
on Linux works with the same image — just point it at the registry image and
expose `WEBSITES_PORT=8000`.

## One-time prerequisites

1. An Azure Container Registry (ACR), e.g. `scanalyticsacr`.
2. A resource group, e.g. `sc-analytics-rg`.
3. A Container Apps environment, e.g. `sc-analytics-env`, in the same region.
4. A dedicated Databricks **service principal** with `SELECT` only on
   `03_gold.sc_analytics`. Generate a personal access token for it; do not
   reuse your personal token in production.

## Build and push

```bash
az acr login --name scanalyticsacr
docker build -f backend/Dockerfile \
    -t scanalyticsacr.azurecr.io/sc-analytics-chatbot:$(git rev-parse --short HEAD) \
    -t scanalyticsacr.azurecr.io/sc-analytics-chatbot:latest .
docker push scanalyticsacr.azurecr.io/sc-analytics-chatbot --all-tags
```

## Create the Container App

```bash
az containerapp create \
  --name sc-analytics-chatbot \
  --resource-group sc-analytics-rg \
  --environment sc-analytics-env \
  --image scanalyticsacr.azurecr.io/sc-analytics-chatbot:latest \
  --registry-server scanalyticsacr.azurecr.io \
  --target-port 8000 \
  --ingress external \
  --min-replicas 0 \
  --max-replicas 2 \
  --cpu 0.5 --memory 1.0Gi \
  --secrets \
      openai-key=$OPENAI_API_KEY \
      databricks-token=$DATABRICKS_TOKEN \
      chatbot-api-key=$(openssl rand -hex 32) \
  --env-vars \
      OPENAI_API_KEY=secretref:openai-key \
      DATABRICKS_TOKEN=secretref:databricks-token \
      CHATBOT_BACKEND_API_KEY=secretref:chatbot-api-key \
      CHATBOT_CORS_ORIGINS=https://sc-analytics.org \
      DATABRICKS_HOST=adb-XXXX.X.azuredatabricks.net \
      DATABRICKS_WAREHOUSE_ID=YOUR_WAREHOUSE_ID \
      CHATBOT_DATABRICKS_CATALOG=03_gold \
      CHATBOT_DATABRICKS_SCHEMA=sc_analytics \
      OPENAI_MODEL=gpt-5.4-nano \
      SQL_ROW_LIMIT=500
```

Then read the public hostname:

```bash
az containerapp show --name sc-analytics-chatbot \
    --resource-group sc-analytics-rg \
    --query properties.configuration.ingress.fqdn -o tsv
```

Set the following in the **frontend** deploy configuration (Cloudflare Pages
→ Environment Variables, Static Web Apps configuration, etc.) and redeploy:

- `VITE_CHATBOT_API_URL` = `https://<that-hostname>` (Vite only inlines names
  starting with `VITE_`; calling it `CHATBOT_BACKEND_URL` will silently leave
  the bundle empty.)
- `VITE_CHATBOT_API_KEY` = the same value you put in `CHATBOT_BACKEND_API_KEY`
  on the backend. The frontend sends it as the `X-API-Key` header.

For local dev, `frontend/.env` already sets `VITE_CHATBOT_API_URL=http://localhost:8000`.
Add a matching `VITE_CHATBOT_API_KEY` line if you also set `CHATBOT_BACKEND_API_KEY`
in your local backend env (it's optional locally — when unset, auth is skipped).

## Production hardening still to do

The shipped backend is a working portfolio integration. Before exposing it
publicly you should add:

1. **CORS allowlist.** Set `CHATBOT_CORS_ORIGINS` to your frontend origin(s),
   comma-separated. Default `*` is fine for local but unsafe in production.
2. **Auth on `/chat`.** Set `CHATBOT_BACKEND_API_KEY` to a strong random value
   (`openssl rand -hex 32`) and the matching `VITE_CHATBOT_API_KEY` on the
   frontend. With the env var set, the backend rejects unauthenticated calls
   with 401. For multi-tenant production, replace this with OAuth (Cloudflare
   Access, Discord, etc.) — the shared-secret model is fine for a guild site.
3. **Rate limiting.** `slowapi` adds per-IP limits in ~10 lines; cap at
   something like 30 requests / 5 minutes / IP. Each call costs an OpenAI
   request and a Databricks query — without a limit, anyone with the URL can
   rack up charges.
4. **Service-principal Databricks token.** Not your personal PAT. Grant
   `SELECT` only on `03_gold.sc_analytics` and a 30s statement timeout in the
   warehouse settings.
5. **OpenAI billing alert.** Set a hard monthly cap in the OpenAI dashboard so
   a buggy retry loop or abuse can't spiral.
6. **Log redaction.** Make sure uvicorn access logs don't include the question
   body or the rendered SQL — both can contain player names that count as
   guild-internal data.

## CI / deploy automation

The repo has a GitHub Actions workflow at
`.github/workflows/chatbot-backend-deploy.yml`.

It runs on pushes that touch:

- `backend/**`
- `pipeline/contracts/**`
- `scripts/build_semantic_registry.py`
- `scripts/dashboard_asset_contracts.py`
- the workflow file itself

Feature branches build the image as validation. Pushes to `main` build the
image, push both `${GITHUB_SHA}` and `latest` tags to ACR, then update the
Azure Container App to the SHA-tagged image.

Configure these GitHub **production environment secrets** for OIDC login:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`

Configure these GitHub **production environment variables**:

- `ACR_NAME`, e.g. `scanalyticsacr`
- `ACR_LOGIN_SERVER`, e.g. `scanalyticsacr.azurecr.io`
- `AZURE_RESOURCE_GROUP`, e.g. `sc-analytics-rg`
- `AZURE_CONTAINER_APP_NAME`, e.g. `sc-analytics-chatbot`

The Azure identity used by GitHub needs:

- `AcrPush` on the Azure Container Registry.
- permission to update the Container App, for example `Contributor` scoped to
  the resource group or a narrower custom role.

Runtime secrets such as `OPENAI_API_KEY`, `DATABRICKS_TOKEN`, and
`CHATBOT_BACKEND_API_KEY` remain configured on the Container App itself. The
workflow only rolls the image; it does not overwrite runtime environment
settings.

## Query memory storage

By default, the chatbot reads seed query-memory examples from
`backend/app/query_memory.json`. That file is baked into the container image,
so runtime feedback is not durable across container revisions unless you
configure a live store.

For production, use Cloudflare R2 via its S3-compatible API:

```bash
az containerapp secret set \
  --name sc-analytics-chatbot \
  --resource-group sc-analytics-rg \
  --secrets \
      query-memory-r2-access-key-id="$QUERY_MEMORY_R2_ACCESS_KEY_ID" \
      query-memory-r2-secret-access-key="$QUERY_MEMORY_R2_SECRET_ACCESS_KEY"

az containerapp update \
  --name sc-analytics-chatbot \
  --resource-group sc-analytics-rg \
  --set-env-vars \
      QUERY_MEMORY_BACKEND=r2 \
      QUERY_MEMORY_R2_ACCOUNT_ID=YOUR_CLOUDFLARE_ACCOUNT_ID \
      QUERY_MEMORY_R2_BUCKET="$R2_BUCKET" \
      QUERY_MEMORY_R2_PREFIX=sc-analytics-data \
      QUERY_MEMORY_R2_ACCESS_KEY_ID=secretref:query-memory-r2-access-key-id \
      QUERY_MEMORY_R2_SECRET_ACCESS_KEY=secretref:query-memory-r2-secret-access-key
```

Create the R2 API token with object **read AND write** access to only that
bucket. A token scoped to read-only will let `_read_raw` succeed but every
feedback write will return a 502 from `/chat/feedback`. With those variables
set, good/bad answer feedback is written to R2 and shared by all backend
revisions. Without them, feedback is written only to the container filesystem
and should be treated as ephemeral.

You can reuse the same bucket as the rest of the published dashboard data.
If your existing dashboard objects are under `sc-analytics-data/latest/` and
`sc-analytics-data/snapshots/`, set:

- `QUERY_MEMORY_R2_BUCKET` to the bucket name, which should match `R2_BUCKET`.
- `QUERY_MEMORY_R2_PREFIX=sc-analytics-data`.

With that configuration, the chatbot stores memory at:

`s3://<R2_BUCKET>/sc-analytics-data/query_memory/query_memory.json`

If `sc-analytics-data` is the bucket name itself rather than an object prefix,
leave `QUERY_MEMORY_R2_PREFIX` unset and set
`QUERY_MEMORY_R2_BUCKET=sc-analytics-data`; the object key will be
`query_memory/query_memory.json`.

You can also override the exact object path with
`QUERY_MEMORY_R2_OBJECT_KEY=sc-analytics-data/query_memory/query_memory.json`.
Do not include the bucket name in `QUERY_MEMORY_R2_OBJECT_KEY`; it should only
be the object key inside the bucket.

### Verify the deployment

```bash
# 1. Confirm the new revision actually picked up the env vars and is serving
#    100% of traffic (az containerapp update creates a new revision; an old
#    one keeps serving until traffic shifts).
az containerapp revision list \
  --name sc-analytics-chatbot \
  --resource-group sc-analytics-rg \
  -o table

# 2. Hit the health endpoint to confirm the container can talk to R2.
curl -s https://<app-host>/chat/memory/health | jq
# Expected: {"backend":"r2","read_ok":true,"entry_count":N,"bucket":"...","key":"..."}

# 3. Submit a thumbs-up in the UI, then re-check — entry_count should rise.
```

If `read_ok` is `false`, the `read_error` field tells you whether the bucket
is missing, the credentials are wrong, or the endpoint URL is malformed.
