"""Validate LLM-generated SQL before it ever reaches Databricks.

Rules:
- Single statement only.
- ``SELECT`` (or top-level ``WITH ... SELECT``) only — no DML/DDL/admin.
- Every table reference must be on the registry allowlist.
- A ``LIMIT`` is appended if absent.

Uses ``sqlglot`` for parsing so we do not rely on regex for safety-critical
checks. Raises :class:`SqlGuardError` for any rejection; the exception message
is safe to surface to the user.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp


class SqlGuardError(ValueError):
    """Raised when generated SQL is not safe to execute."""


_FORBIDDEN_NODE_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Drop,
    exp.Alter,
    exp.Create,
    exp.TruncateTable,
    exp.Command,  # catches OPTIMIZE/VACUUM/USE/etc. that sqlglot parses as Command
)


@dataclass(frozen=True)
class GuardedSql:
    sql: str
    tables_used: tuple[str, ...]


def guard_sql(sql: str, allowlist: set[str], *, default_limit: int = 500) -> GuardedSql:
    """Parse, validate, and rewrite ``sql`` for safe execution.

    ``allowlist`` is the set of fully-qualified table names the chatbot may
    read. Comparisons are case-insensitive. The function returns the rewritten
    SQL and the set of tables it references.
    """

    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        raise SqlGuardError("Empty SQL.")

    # Reject anything that smells like multi-statement injection.
    if ";" in cleaned:
        raise SqlGuardError("Only a single statement is allowed.")

    try:
        statements = sqlglot.parse(cleaned, read="databricks")
    except sqlglot.errors.ParseError as exc:
        raise SqlGuardError(f"Could not parse SQL: {exc}") from exc

    if len(statements) != 1 or statements[0] is None:
        raise SqlGuardError("Exactly one statement is required.")

    statement = statements[0]

    if not isinstance(statement, exp.Select | exp.Subquery | exp.With | exp.Union):
        raise SqlGuardError(
            f"Only SELECT/WITH queries are allowed, got {type(statement).__name__}."
        )

    for node in statement.walk():
        if isinstance(node, _FORBIDDEN_NODE_TYPES):
            raise SqlGuardError(f"Statement contains a forbidden operation: {type(node).__name__}.")

    tables_used: list[str] = []
    allowlist_lower = {name.lower() for name in allowlist}
    for table in statement.find_all(exp.Table):
        # Skip subquery aliases and CTE references that resolve to expressions, not real tables.
        name = _qualified_name(table)
        if not name:
            continue
        if _is_cte_reference(table, statement):
            continue
        if name.lower() not in allowlist_lower:
            raise SqlGuardError(f"Table {name!r} is not on the chatbot allowlist.")
        tables_used.append(name)

    if not tables_used:
        raise SqlGuardError("Query references no allowlisted tables.")

    limited = _ensure_limit(statement, default_limit)
    rendered = limited.sql(dialect="databricks")
    return GuardedSql(sql=rendered, tables_used=tuple(dict.fromkeys(tables_used)))


def _qualified_name(table: exp.Table) -> str:
    parts = [
        p for p in (table.args.get("catalog"), table.args.get("db"), table.this) if p is not None
    ]
    return ".".join(_strip_identifier(part) for part in parts)


def _strip_identifier(node: exp.Expression) -> str:
    if isinstance(node, exp.Identifier):
        return node.name
    return str(node)


def _is_cte_reference(table: exp.Table, root: exp.Expression) -> bool:
    parts = _qualified_name(table).split(".")
    if len(parts) != 1:
        return False
    name = parts[0].lower()
    for cte in root.find_all(exp.CTE):
        alias = cte.args.get("alias")
        if alias and isinstance(alias, exp.TableAlias):
            cte_name = _strip_identifier(alias.this) if alias.this else ""
            if cte_name.lower() == name:
                return True
    return False


def _ensure_limit(statement: exp.Expression, default_limit: int) -> exp.Expression:
    select = statement
    if isinstance(statement, exp.With):
        select = statement.this
    if isinstance(select, exp.Select) and select.args.get("limit") is None:
        select.set("limit", exp.Limit(expression=exp.Literal.number(default_limit)))
    return statement
