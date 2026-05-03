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
  --env-vars \
      OPENAI_API_KEY=secretref:openai-key \
      DATABRICKS_TOKEN=secretref:databricks-token \
      DATABRICKS_HOST=adb-XXXX.X.azuredatabricks.net \
      DATABRICKS_WAREHOUSE_ID=YOUR_WAREHOUSE_ID \
      CHATBOT_DATABRICKS_CATALOG=03_gold \
      CHATBOT_DATABRICKS_SCHEMA=sc_analytics \
      OPENAI_MODEL=gpt-4o \
      SQL_ROW_LIMIT=500
```

Then read the public hostname:

```bash
az containerapp show --name sc-analytics-chatbot \
    --resource-group sc-analytics-rg \
    --query properties.configuration.ingress.fqdn -o tsv
```

Set that hostname (with `https://` prefix) as `VITE_CHATBOT_API_URL` in the
frontend's deploy configuration (Cloudflare Pages → Environment Variables) and
redeploy the frontend so the production bundle points at it.

## Production hardening still to do

The shipped backend is a working portfolio integration. Before exposing it
publicly you should add:

1. **CORS allowlist.** [`backend/app/main.py`](app/main.py) currently sets
   `allow_origins=["*"]`. Replace with `["https://sc-analytics.org"]` and any
   preview hostnames you use.
2. **Auth on `/chat`.** At minimum a shared secret check (header → 401 if
   missing). For real production, gate via your existing auth (Cloudflare
   Access, Discord OAuth, etc.) or add a simple JWT issued at the frontend.
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

## CI / deploy automation (when ready)

The repo already has `.github/workflows/databricks-deploy.yml` for the
pipeline. Add a parallel `chatbot-deploy.yml` that:

- builds the image on every push to `main` that touches `backend/**` or
  `pipeline/contracts/**`,
- pushes to ACR,
- runs `az containerapp update --image ...` to roll out a new revision.

Use OIDC federated credentials so no Azure secret needs to live in GitHub
Actions.
