# Contributing

## Development setup

```bash
git clone https://github.com/richmulvany/databricks-pipeline-template
cd databricks-pipeline-template
make init
```

## Branching

- `main` — stable, deployed
- `develop` — integration branch
- `feature/short-description` — feature work
- `fix/short-description` — bug fixes

## Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add player performance silver table
fix: handle null boss_id in bronze ingestion
docs: update adapter guide with authentication example
chore: bump ruff to 0.5.0
```

## Before submitting a PR

```bash
make format      # Auto-format
make test-lint   # Lint + type check
make test        # Full test suite
```

All CI checks must pass. PRs require at least one approving review.

## Adding a new gold table

1. Create `pipeline/gold/your_table.py` with the DLT definition
2. Add expectations to `pipeline/expectations/your_table_expectations.py`
3. Add an entry to `docs/data_dictionary/README.md`
4. Update `scripts/export_gold_tables.py` to include the new table
5. Add a corresponding API function in `frontend/src/api/`

## Adding a new API endpoint

1. Create `ingestion/src/endpoints/your_endpoint.py`
2. Add a corresponding ingestion job in `ingestion/jobs/`
3. Add mock data to `data/samples/`
4. Write tests in `ingestion/tests/`
