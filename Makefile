.PHONY: help init verify test test-unit test-lint deploy-pipeline deploy-frontend \
        setup-secrets export-data clean format

PYTHON := python3
PIP    := pip3
NPM    := npm

# Default target
help:
	@echo ""
	@echo "  Databricks Medallion Pipeline Template"
	@echo ""
	@echo "  Setup"
	@echo "    make init              First-time project setup"
	@echo "    make verify            Health check — confirms everything is configured"
	@echo "    make setup-secrets     Upload secrets to Databricks Secret Scope"
	@echo ""
	@echo "  Development"
	@echo "    make test              Run all tests"
	@echo "    make test-unit         Run unit tests only"
	@echo "    make test-lint         Run linting and type checks"
	@echo "    make format            Auto-format code with ruff"
	@echo ""
	@echo "  Deployment"
	@echo "    make deploy-pipeline   Deploy DLT pipeline via Databricks Asset Bundles"
	@echo "    make deploy-frontend   Build and deploy the React frontend"
	@echo "    make export-data       Run gold table export to static JSON"
	@echo ""
	@echo "  Maintenance"
	@echo "    make clean             Remove build artefacts"
	@echo ""

init:
	@echo ">> Installing Python dependencies..."
	$(PIP) install -e ".[dev]"
	@echo ">> Setting up pre-commit hooks..."
	pre-commit install
	@echo ">> Copying .env.example to .env (if not present)..."
	@[ -f .env ] || cp .env.example .env && echo "   Created .env — please fill in your values." || echo "   .env already exists, skipping."
	@echo ">> Verifying Databricks CLI..."
	@databricks auth test 2>/dev/null && echo "   [OK] Databricks CLI connected" || echo "   [WARN] Databricks CLI not configured — run: databricks configure --token"
	@echo ">> Installing frontend dependencies..."
	cd frontend && $(NPM) install
	@echo ""
	@echo "Setup complete. Next: edit .env, then run 'make deploy-pipeline'."

verify:
	@echo ">> Running health checks..."
	$(PYTHON) scripts/verify_setup.py

setup-secrets:
	@echo ">> Uploading secrets to Databricks Secret Scope..."
	bash scripts/bootstrap_secrets.sh

test:
	@echo ">> Running all tests..."
	pytest ingestion/tests pipeline/tests --tb=short -q

test-unit:
	pytest ingestion/tests/unit pipeline/tests/unit --tb=short -q

test-lint:
	@echo ">> Linting with ruff..."
	ruff check ingestion/ pipeline/ scripts/
	@echo ">> Type checking with mypy..."
	mypy ingestion/src pipeline/

format:
	ruff format ingestion/ pipeline/ scripts/
	ruff check --fix ingestion/ pipeline/ scripts/

deploy-pipeline:
	@echo ">> Deploying DLT pipeline via Databricks Asset Bundles..."
	databricks bundle deploy
	@echo "Pipeline deployed. Start it in the Databricks UI under Workflows -> Delta Live Tables."

deploy-frontend:
	@echo ">> Building frontend..."
	cd frontend && $(NPM) run build
	@echo ">> Deploying to Vercel..."
	cd frontend && vercel --prod

export-data:
	@echo ">> Exporting gold tables to static JSON..."
	$(PYTHON) scripts/export_gold_tables.py

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache"   -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache"   -exec rm -rf {} + 2>/dev/null || true
	rm -rf frontend/dist frontend/.vercel 2>/dev/null || true
	@echo "Cleaned."
