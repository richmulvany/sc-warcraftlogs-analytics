"""Read-only Databricks SQL Warehouse connection.

Connections are short-lived; we open per-query rather than pooling because
warehouses cold-start anyway and the chatbot is low-QPS. The Databricks user
or service principal backing the token must have ``SELECT`` only on the gold
schema — defence in depth on top of :mod:`sql_guard`.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from .config import Settings, get_settings


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


@lru_cache(maxsize=1)
def _resolve_warehouse_id(host: str, token: str) -> str:
    """Auto-discover the first running SQL warehouse if not configured.

    Mirrors the pattern in ``scripts/export_gold_tables.py``. Cached so the
    warehouse list is only fetched once per process.
    """

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sdk is required for warehouse auto-discovery; "
            "install it or set DATABRICKS_WAREHOUSE_ID explicitly."
        ) from exc

    client = WorkspaceClient(host=host, token=token)
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses are visible to this Databricks token.")
    warehouse_id = warehouses[0].id or ""
    if not warehouse_id:
        raise RuntimeError("Databricks returned a warehouse without an id.")
    return warehouse_id


def execute_select(sql: str, settings: Settings | None = None) -> QueryResult:
    """Execute ``sql`` against the configured warehouse and return rows.

    Raises :class:`RuntimeError` if Databricks credentials are missing or the
    SDK is unavailable. The caller is responsible for having validated ``sql``
    via :func:`backend.app.sql_guard.guard_sql` first.
    """

    settings = settings or get_settings()
    if not (settings.databricks_host and settings.databricks_token):
        raise RuntimeError(
            "Databricks credentials are not configured. "
            "Set DATABRICKS_HOST and DATABRICKS_TOKEN."
        )

    warehouse_id = settings.databricks_warehouse_id or _resolve_warehouse_id(
        settings.databricks_host, settings.databricks_token
    )

    try:
        from databricks import sql as dbsql
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sql-connector is not installed. Run: pip install databricks-sql-connector"
        ) from exc

    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    with (
        dbsql.connect(
            server_hostname=settings.databricks_host,
            http_path=http_path,
            access_token=settings.databricks_token,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(sql)
        columns = tuple(desc[0] for desc in (cur.description or ()))
        rows = tuple(tuple(row) for row in cur.fetchall())
        return QueryResult(columns=columns, rows=rows)
