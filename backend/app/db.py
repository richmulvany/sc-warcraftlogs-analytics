"""Read-only Databricks SQL Warehouse connection.

Connections are short-lived; we open per-query rather than pooling because
warehouses cold-start anyway and the chatbot is low-QPS. The Databricks user
or service principal backing the token must have ``SELECT`` only on the gold
schema — defence in depth on top of :mod:`sql_guard`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import Settings, get_settings


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


def execute_select(sql: str, settings: Settings | None = None) -> QueryResult:
    """Execute ``sql`` against the configured warehouse and return rows.

    Raises :class:`RuntimeError` if Databricks credentials are missing or the
    SDK is unavailable. The caller is responsible for having validated ``sql``
    via :func:`backend.app.sql_guard.guard_sql` first.
    """

    settings = settings or get_settings()
    if not (
        settings.databricks_host and settings.databricks_token and settings.databricks_warehouse_id
    ):
        raise RuntimeError(
            "Databricks credentials are not configured. "
            "Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID."
        )

    try:
        from databricks import sql as dbsql
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sql-connector is not installed. Run: pip install databricks-sql-connector"
        ) from exc

    http_path = f"/sql/1.0/warehouses/{settings.databricks_warehouse_id}"
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
