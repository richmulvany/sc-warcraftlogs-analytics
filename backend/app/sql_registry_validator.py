"""Registry-backed SQL semantic validation.

``sql_guard`` handles safety: only SELECT, allowlisted tables, LIMIT, no DDL.
This module handles a different problem: whether the SQL references columns on
the table aliases that actually own those columns according to the semantic
registry. That catches failures such as ``fpe.boss_name`` before the query
spends time in Databricks.
"""

from __future__ import annotations

import difflib

import sqlglot
from sqlglot import exp

from .semantic_registry import Registry, TableInfo
from .sql_guard import SqlGuardError


def validate_sql_columns(sql: str, registry: Registry) -> None:
    """Raise ``SqlGuardError`` when a qualified column is not on its alias table."""

    try:
        statements = sqlglot.parse(sql.strip().rstrip(";"), read="databricks")
    except sqlglot.errors.ParseError as exc:
        raise SqlGuardError(f"Could not parse SQL for column validation: {exc}") from exc
    if len(statements) != 1 or statements[0] is None:
        raise SqlGuardError("Exactly one statement is required for column validation.")

    statement = statements[0]
    alias_to_table = _table_aliases(statement, registry)
    for column in statement.find_all(exp.Column):
        qualifier = _column_qualifier(column)
        if not qualifier:
            continue
        info = alias_to_table.get(qualifier.lower())
        # CTEs/subquery aliases are not validated here because their columns are
        # derived expressions. Real table aliases are validated strictly.
        if info is None:
            continue
        column_name = column.name
        known_columns = {str(c["name"]).lower(): str(c["name"]) for c in info.columns}
        if column_name.lower() in known_columns:
            continue
        suggestion = _suggest_column(column_name, alias_to_table)
        raise SqlGuardError(
            f"Column `{qualifier}.{column_name}` does not exist on `{info.model}`. "
            f"`{info.model}` columns include: {', '.join(sorted(known_columns.values())[:30])}."
            + (f" {suggestion}" if suggestion else "")
        )


def _table_aliases(statement: exp.Expression, registry: Registry) -> dict[str, TableInfo]:
    by_name = _registry_lookup(registry)
    aliases: dict[str, TableInfo] = {}
    cte_names = _cte_names(statement)
    for table in statement.find_all(exp.Table):
        qualified = _qualified_name(table)
        if not qualified or qualified.lower() in cte_names:
            continue
        info = by_name.get(qualified.lower())
        if info is None:
            continue
        aliases[_table_alias(table).lower()] = info
        aliases[info.model.lower()] = info
        aliases[qualified.lower()] = info
    return aliases


def _registry_lookup(registry: Registry) -> dict[str, TableInfo]:
    out: dict[str, TableInfo] = {}
    for info in registry.tables.values():
        out[info.model.lower()] = info
        out[info.table.lower()] = info
        out[f"{registry.catalog}.{registry.schema}.{info.model}".lower()] = info
    return out


def _cte_names(statement: exp.Expression) -> set[str]:
    names: set[str] = set()
    for cte in statement.find_all(exp.CTE):
        alias = cte.args.get("alias")
        if alias and isinstance(alias, exp.TableAlias) and alias.this:
            names.add(_strip_identifier(alias.this).lower())
    return names


def _qualified_name(table: exp.Table) -> str:
    parts = [
        p for p in (table.args.get("catalog"), table.args.get("db"), table.this) if p is not None
    ]
    return ".".join(_strip_identifier(part) for part in parts)


def _strip_identifier(node: exp.Expression) -> str:
    if isinstance(node, exp.Identifier):
        return node.name
    return str(node)


def _table_alias(table: exp.Table) -> str:
    alias = table.args.get("alias")
    if alias and isinstance(alias, exp.TableAlias) and alias.this:
        return _strip_identifier(alias.this)
    return table.name


def _column_qualifier(column: exp.Column) -> str:
    table = column.args.get("table")
    if table is None:
        return ""
    return _strip_identifier(table)


def _suggest_column(column_name: str, aliases: dict[str, TableInfo]) -> str:
    owners: list[str] = []
    for alias, info in aliases.items():
        if any(str(c["name"]).lower() == column_name.lower() for c in info.columns):
            owners.append(f"`{alias}.{column_name}`")
    if owners:
        return f"That column exists on: {', '.join(sorted(set(owners)))}."

    all_columns = sorted({str(c["name"]) for info in aliases.values() for c in info.columns})
    close = difflib.get_close_matches(column_name, all_columns, n=3)
    if close:
        return f"Did you mean one of: {', '.join(f'`{c}`' for c in close)}?"
    return ""
