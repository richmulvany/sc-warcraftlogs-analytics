# 🏗️ Databricks Medallion Pipeline Template

[![CI](https://github.com/richmulvany/databricks-pipeline-template/actions/workflows/ci.yml/badge.svg)](https://github.com/richmulvany/databricks-pipeline-template/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-grade, reusable template for building **Databricks DLT (Lakeflow Declarative Pipelines)** projects with a Bronze → Silver → Gold medallion architecture, a pluggable API ingestion layer, and a React frontend dashboard.

Built to run on the **Databricks Free Edition**.

---

## Architecture

```
External API
     │
     ▼
┌─────────────┐
│  Ingestion  │  Pluggable adapter pattern — swap the API source easily
└──────┬──────┘
       │ Raw Delta tables
       ▼
┌─────────────┐
│   Bronze    │  Raw landing — schema enforcement, metadata columns
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Silver    │  Cleaned, normalised, deduplicated
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Gold     │  Business-ready data products
└──────┬──────┘
       │ Nightly JSON export
       ▼
┌─────────────┐
│  Frontend   │  React dashboard (Vercel / Netlify / GitHub Pages)
└─────────────┘
```

## Repository Structure

```
.
├── .github/
│   ├── workflows/          # CI, Databricks deploy, frontend deploy
│   └── ISSUE_TEMPLATE/     # Bug report, feature request templates
├── ingestion/
│   ├── src/
│   │   ├── adapters/       # Pluggable API adapter pattern
│   │   └── endpoints/      # Per-domain API modules
│   ├── jobs/               # Databricks job definitions
│   └── config/             # Rate limits, API config
├── pipeline/
│   ├── bronze/             # DLT bronze layer tables
│   ├── silver/             # DLT silver layer tables
│   ├── gold/               # DLT gold layer data products
│   └── expectations/       # Data quality rules
├── frontend/               # React + Vite + TypeScript dashboard
├── infra/                  # Databricks asset bundle config
├── docs/
│   ├── adr/                # Architecture Decision Records
│   ├── runbooks/           # Operational runbooks
│   ├── data_dictionary/    # Gold layer table definitions
│   └── data_contracts/     # Producer/consumer schema agreements
├── notebooks/              # Exploratory analysis
├── scripts/                # Utility scripts
├── data/samples/           # Mock API responses for testing
├── Makefile
├── pyproject.toml
└── CHANGELOG.md
```

## Quick Start

### Prerequisites

- [Databricks Free Edition](https://www.databricks.com/try-databricks) account
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and configured
- Python 3.11+
- Node.js 18+

### 1. Use this template

Click **"Use this template"** on GitHub, or:

```bash
pip install cookiecutter
cookiecutter gh:richmulvany/databricks-pipeline-template
```

### 2. First-time setup

```bash
make init
```

This will:
- Install Python dependencies
- Copy `.env.example` to `.env`
- Verify Databricks CLI connection
- Set up pre-commit hooks

### 3. Configure your data source

Copy `ingestion/src/adapters/example_adapter/` and implement the three required methods.
See [Adapter Guide](docs/architecture/adapter_guide.md).

### 4. Deploy the pipeline

```bash
make deploy-pipeline
```

### 5. Deploy the frontend

```bash
make deploy-frontend
```

See [SETUP.md](SETUP.md) for the full step-by-step guide.

---

## Swapping the Data Source

This template uses an **adapter pattern** for ingestion. To use a different API:

1. Copy `ingestion/src/adapters/example_adapter/` to `ingestion/src/adapters/your_source/`
2. Implement `BaseAdapter` methods: `authenticate()`, `fetch()`, `validate()`
3. Update `ingestion/config/source_config.yml`
4. Update gold layer table definitions in `pipeline/gold/`

See [docs/architecture/adapter_guide.md](docs/architecture/adapter_guide.md).

---

## Testing

```bash
make test          # Run all tests
make test-unit     # Unit tests only
make test-lint     # Linting only
```

---

## Documentation

- [Setup Guide](SETUP.md)
- [Architecture Overview](docs/architecture/overview.md)
- [Adapter Guide](docs/architecture/adapter_guide.md)
- [ADR Index](docs/adr/README.md)
- [Data Dictionary](docs/data_dictionary/README.md)
- [Data Contracts](docs/data_contracts/README.md)
- [Runbooks](docs/runbooks/README.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

---

## Built With Enterprise Patterns

| Pattern | Implementation |
|---|---|
| Medallion architecture | Databricks DLT (Lakeflow) |
| Infrastructure as Code | Databricks Asset Bundles |
| Data quality | DLT Expectations |
| CI/CD | GitHub Actions |
| Adapter pattern | Pluggable ingestion layer |
| ADRs | Architecture Decision Records in `docs/adr/` |
| Data contracts | Producer/consumer schema agreements in `docs/data_contracts/` |
| Secret management | Databricks Secret Scopes |
| Code quality | ruff + mypy + pre-commit |

