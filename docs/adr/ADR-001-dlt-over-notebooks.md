# ADR-001: Use DLT Instead of Notebooks + Jobs

**Date:** 2025-01-01
**Status:** Accepted

## Context

The pipeline needs to transform data through Bronze, Silver, and Gold layers with
data quality checks, dependency management, and automatic retries.

The two main options are:
1. Standard notebooks wired together with Databricks Workflows (Jobs)
2. Delta Live Tables (DLT / Lakeflow Declarative Pipelines)

## Decision

Use DLT for all pipeline transformations (Bronze → Silver → Gold).

## Rationale

DLT provides several features that would require significant boilerplate to implement manually:

- **Automatic dependency resolution** — DLT infers table dependencies from `dlt.read()` calls
- **Built-in data quality** — `@dlt.expect` decorators replace manual assertion notebooks
- **Lineage tracking** — the DLT UI shows a live DAG with row counts and error rates
- **Development mode** — run and iterate without affecting production tables
- **Simplified orchestration** — no need to manage task order in a Workflow

DLT is available on the Databricks Free Edition in Development Mode, making it
suitable for this project without requiring a paid tier.

## Alternatives Considered

**Notebooks + Jobs:** More flexible but requires manual dependency management,
custom error handling, and separate data quality tooling. More boilerplate for the same result.

**Apache Airflow:** Powerful but introduces significant operational overhead (running an
Airflow server) that is not justified for a single-project pipeline.

## Consequences

- Pipeline code must be written as DLT notebooks (Python or SQL with `dlt.*` decorators)
- Cannot use standard PySpark `spark.write` — must use DLT materialisation
- Production mode requires a paid Databricks tier (Development Mode is sufficient for this project)
