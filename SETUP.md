# Setup Guide

This guide walks through the complete first-time setup of this template for a new project.

## Step 1: Create your Databricks workspace

1. Sign up at [databricks.com/try-databricks](https://www.databricks.com/try-databricks) (Free Edition)
2. Create a workspace and note your **workspace URL** (e.g. `https://adb-xxxx.azuredatabricks.net`)
3. Generate a **Personal Access Token**: User Settings → Developer → Access Tokens

## Step 2: Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your values:

```
DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=databricks_pipeline
SOURCE_API_BASE_URL=https://your-api.example.com
SOURCE_API_KEY=your-api-key
EXPORT_STORAGE_PATH=/dbfs/mnt/exports
```

## Step 3: Configure the Databricks CLI

```bash
databricks configure --token
# Enter your host and token when prompted
databricks auth test  # Verify connection
```

## Step 4: Run first-time setup

```bash
make init
```

## Step 5: Configure secrets in Databricks

Never store secrets in code. Upload them to Databricks Secret Scopes:

```bash
make setup-secrets
```

This runs `scripts/bootstrap_secrets.sh`, which prompts you for values and creates the secret scope.

Alternatively, manually:

```bash
databricks secrets create-scope pipeline-secrets
databricks secrets put-secret pipeline-secrets api-key --string-value "your-value"
databricks secrets put-secret pipeline-secrets api-base-url --string-value "your-value"
```

Reference secrets in your pipeline code:
```python
api_key = dbutils.secrets.get(scope="pipeline-secrets", key="api-key")
```

## Step 6: Implement your adapter

1. Copy the example adapter:
   ```bash
   cp -r ingestion/src/adapters/example_adapter ingestion/src/adapters/my_source
   ```
2. Implement the three methods in `adapter.py` — see [Adapter Guide](docs/architecture/adapter_guide.md)
3. Update `ingestion/config/source_config.yml` with your API details

## Step 7: Deploy the DLT pipeline

```bash
make deploy-pipeline
```

Then in Databricks UI:
1. Go to **Workflows → Delta Live Tables**
2. Find your pipeline (named from `databricks.yml`)
3. Click **Start** to run in Development Mode

## Step 8: Set up the frontend

```bash
cd frontend
cp .env.example .env.local
npm install
npm run dev   # Local development
```

Edit `frontend/.env.local`:
```
VITE_DATA_BASE_URL=https://richmulvany.github.io/databricks-pipeline-template/data
```

## Step 9: Deploy the frontend

### Option A: Vercel (recommended)

```bash
npm install -g vercel
cd frontend && vercel
```

Or connect your GitHub repo at [vercel.com](https://vercel.com) for automatic deploys.

### Option B: Netlify

```bash
npm install -g netlify-cli
cd frontend && netlify deploy --prod
```

### Option C: GitHub Pages

The included GitHub Actions workflow (`.github/workflows/frontend-deploy.yml`) deploys automatically on push to `main`.

Enable GitHub Pages in your repo settings: Settings → Pages → Source: GitHub Actions.

## Step 10: Set up nightly data export

The gold tables are exported as static JSON nightly, so the frontend has no backend dependency.

In your Databricks workspace, create a scheduled job:
- Script: `scripts/export_gold_tables.py`
- Schedule: Daily at 02:00 UTC
- Cluster: Use the default free-tier cluster

The export pushes JSON files to `data/exports/`, which are then committed by the CI pipeline and served statically.

---

## Verifying the Setup

```bash
make verify       # Runs a full health check
```

Expected output:
```
[OK] Databricks CLI connected
[OK] Secret scope exists
[OK] Pipeline deployed
[OK] Sample data present
[OK] Frontend builds successfully
```
