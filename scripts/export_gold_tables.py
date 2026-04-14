"""
Export frontend gold tables from Databricks SQL to static CSV files.

The React app reads CSVs from `frontend/public/data`, so this script exports the
same files the frontend consumes rather than the old JSON sample format.

Usage:
    python scripts/export_gold_tables.py
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG = os.environ.get("DATABRICKS_CATALOG", "04_sdp")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "warcraftlogs")
OUTPUT_DIR = Path(os.environ.get("EXPORT_OUTPUT_DIR", "frontend/public/data"))
POLL_INTERVAL_SECONDS = float(os.environ.get("EXPORT_POLL_INTERVAL_SECONDS", "2"))
POLL_TIMEOUT_SECONDS = int(os.environ.get("EXPORT_POLL_TIMEOUT_SECONDS", "300"))

FRONTEND_TABLES: dict[str, str] = {
    "gold_raid_summary.csv": "gold_raid_summary",
    "gold_player_performance_summary.csv": "gold_player_performance_summary",
    "gold_boss_progression.csv": "gold_boss_progression",
    "gold_boss_kill_roster.csv": "gold_boss_kill_roster",
    "gold_player_attendance.csv": "gold_player_attendance",
    "gold_guild_roster.csv": "gold_guild_roster",
    "gold_weekly_activity.csv": "gold_weekly_activity",
    "gold_boss_wipe_analysis.csv": "gold_boss_wipe_analysis",
    "gold_player_survivability.csv": "gold_player_survivability",
    "gold_progression_timeline.csv": "gold_progression_timeline",
    "gold_raid_team.csv": "gold_raid_team",
    "gold_best_kills.csv": "gold_best_kills",
    "gold_boss_mechanics.csv": "gold_boss_mechanics",
    "gold_player_boss_performance.csv": "gold_player_boss_performance",
}


def _first_warehouse_id(client: WorkspaceClient) -> str:
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses found. Create or start one in Databricks.")
    warehouse_id = warehouses[0].id
    if not warehouse_id:
        raise RuntimeError("First SQL warehouse had no id.")
    return warehouse_id


def _wait_for_success(
    client: WorkspaceClient,
    response: Any,
) -> Any:
    statement_id = getattr(response, "statement_id", None)
    if not statement_id:
        return response

    deadline = time.time() + POLL_TIMEOUT_SECONDS
    current = response
    while time.time() < deadline:
        state = getattr(getattr(current, "status", None), "state", None)
        if state == sql.StatementState.SUCCEEDED:
            return current
        if state in {
            sql.StatementState.CANCELED,
            sql.StatementState.CLOSED,
            sql.StatementState.FAILED,
        }:
            raise RuntimeError(
                f"Statement {statement_id} ended in state {state}: {getattr(getattr(current, 'status', None), 'error', None)}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)
        current = client.statement_execution.get_statement(statement_id)

    raise TimeoutError(
        f"Timed out waiting for statement {statement_id} to finish after {POLL_TIMEOUT_SECONDS}s"
    )


def _iter_external_links(result_chunk: Any) -> list[str]:
    links = getattr(result_chunk, "external_links", None) or []
    urls: list[str] = []
    for link in links:
        url = getattr(link, "external_link", None) or getattr(link, "url", None)
        if url:
            urls.append(url)
    return urls


def _download_chunk(url: str) -> bytes:
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        response = client.get(url, headers={})
        response.raise_for_status()
        return response.content


def _write_csv_from_statement(
    client: WorkspaceClient, statement_response: Any, output_path: Path
) -> int:
    statement_id = getattr(statement_response, "statement_id", None)
    if not statement_id:
        raise RuntimeError("Statement response did not include a statement_id.")

    manifest = getattr(statement_response, "manifest", None)
    chunks = getattr(manifest, "chunks", None) or []
    chunk_count = len(chunks) if chunks else 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0

    with output_path.open("wb") as fh:
        for chunk_index in range(chunk_count):
            chunk = (
                getattr(statement_response, "result", None)
                if chunk_index == 0
                else client.statement_execution.get_statement_result_chunk_n(
                    statement_id, chunk_index
                )
            )

            urls = _iter_external_links(chunk)
            if not urls:
                raise RuntimeError(
                    f"No external CSV links returned for statement {statement_id} chunk {chunk_index}."
                )

            for url in urls:
                payload = _download_chunk(url)
                if chunk_index > 0 and payload.startswith(b"\xef\xbb\xbf"):
                    payload = payload[3:]
                fh.write(payload)

            row_count += int(getattr(chunks[chunk_index], "row_count", 0) or 0)

    return row_count


def export_table(client: WorkspaceClient, warehouse_id: str, filename: str, table_name: str) -> int:
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    logger.info("Exporting %s -> %s", full_name, filename)

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"SELECT * FROM {full_name}",
        disposition=sql.Disposition.EXTERNAL_LINKS,
        format=sql.Format.CSV,
        wait_timeout="10s",
        on_wait_timeout=sql.ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )

    response = _wait_for_success(client, response)
    output_path = OUTPUT_DIR / filename
    row_count = _write_csv_from_statement(client, response, output_path)
    logger.info("  wrote %s rows to %s", row_count or "unknown", output_path)
    return row_count


def main() -> None:
    client = WorkspaceClient()
    warehouse_id = _first_warehouse_id(client)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for filename, table_name in FRONTEND_TABLES.items():
        total_rows += export_table(client, warehouse_id, filename, table_name)

    logger.info(
        "Export complete. %s total rows across %s files.",
        total_rows or "unknown",
        len(FRONTEND_TABLES),
    )


if __name__ == "__main__":
    main()
