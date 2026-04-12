"""
Export gold layer Delta tables to static JSON files.

Run nightly via GitHub Actions (see .github/workflows/export-data.yml).
The frontend fetches these static files — no backend server required.

Usage:
    python scripts/export_gold_tables.py
"""

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG = os.environ.get("DATABRICKS_CATALOG", "main")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "pipeline_prod")
OUTPUT_DIR = Path(os.environ.get("EXPORT_OUTPUT_DIR", "data/exports"))

# Add gold tables here: output filename -> SQL query
GOLD_TABLE_EXPORTS: dict[str, str] = {
    "entity_summary": f"SELECT * FROM {CATALOG}.{SCHEMA}.gold_entity_summary",
    "boss_progression": f"SELECT * FROM {CATALOG}.{SCHEMA}.gold_boss_progression",
    "raid_summary": f"SELECT * FROM {CATALOG}.{SCHEMA}.gold_raid_summary",
    "progression_timeline": f"SELECT * FROM {CATALOG}.{SCHEMA}.gold_progression_timeline",
}


def export_table(client: WorkspaceClient, name: str, query: str) -> int:
    """Execute a SQL query and write results to JSON. Returns record count."""
    logger.info("Exporting: %s", name)

    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses found. Create one in the Databricks UI.")
    warehouse_id = warehouses[0].id

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="60s",
    )

    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Query failed for {name}: {response.status.error}")

    columns = [col.name for col in response.manifest.schema.columns]
    rows = response.result.data_array or []
    records = [dict(zip(columns, row, strict=True)) for row in rows]

    output_path = OUTPUT_DIR / f"{name}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "exported_at": datetime.now(UTC).isoformat(),
        "record_count": len(records),
        "data": records,
    }

    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    logger.info("  Wrote %d records to %s", len(records), output_path)
    return len(records)


def main() -> None:
    client = WorkspaceClient()
    total = 0

    for name, query in GOLD_TABLE_EXPORTS.items():
        total += export_table(client, name, query)

    manifest = {
        "exported_at": datetime.now(UTC).isoformat(),
        "tables": list(GOLD_TABLE_EXPORTS.keys()),
        "total_records": total,
    }
    with open(OUTPUT_DIR / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info("Export complete. %d total records.", total)


if __name__ == "__main__":
    main()
