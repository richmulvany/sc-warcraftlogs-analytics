"""Tests for backend.app.sql_guard."""

from __future__ import annotations

import pytest

from backend.app.sql_guard import SqlGuardError, guard_sql

ALLOWLIST = {
    "03_gold.sc_analytics.gold_player_mplus_summary",
    "03_gold.sc_analytics.fact_player_fight_performance",
    "03_gold.sc_analytics.dim_player",
    # Bare model names too — the chatbot may emit either form.
    "gold_player_mplus_summary",
    "fact_player_fight_performance",
    "dim_player",
}


def test_simple_select_passes() -> None:
    result = guard_sql(
        "SELECT player_name FROM 03_gold.sc_analytics.dim_player",
        allowlist=ALLOWLIST,
    )
    assert "dim_player" in result.tables_used[0]
    assert "LIMIT" in result.sql.upper()


def test_select_with_existing_limit_is_preserved() -> None:
    result = guard_sql(
        "SELECT player_name FROM 03_gold.sc_analytics.dim_player LIMIT 10",
        allowlist=ALLOWLIST,
    )
    assert "LIMIT 10" in result.sql.upper()


def test_with_cte_passes_and_skips_cte_alias() -> None:
    sql = "WITH p AS (SELECT player_name FROM 03_gold.sc_analytics.dim_player) " "SELECT * FROM p"
    result = guard_sql(sql, allowlist=ALLOWLIST)
    # Only the real table is reported, not the CTE alias.
    assert all("dim_player" in t for t in result.tables_used)


def test_join_across_two_allowlisted_tables() -> None:
    sql = (
        "SELECT d.player_name, f.rank_percent "
        "FROM 03_gold.sc_analytics.dim_player d "
        "JOIN 03_gold.sc_analytics.fact_player_fight_performance f "
        "ON f.player_name = d.player_name"
    )
    result = guard_sql(sql, allowlist=ALLOWLIST)
    assert len(result.tables_used) == 2


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE 03_gold.sc_analytics.gold_player_mplus_summary",
        "DELETE FROM 03_gold.sc_analytics.dim_player WHERE 1=1",
        "INSERT INTO 03_gold.sc_analytics.dim_player VALUES ('x')",
        "UPDATE 03_gold.sc_analytics.dim_player SET realm = 'x'",
        "MERGE INTO 03_gold.sc_analytics.dim_player USING dim_player ON 1=1 "
        "WHEN MATCHED THEN DELETE",
        "ALTER TABLE 03_gold.sc_analytics.dim_player ADD COLUMN x STRING",
        "CREATE TABLE x AS SELECT * FROM 03_gold.sc_analytics.dim_player",
        "TRUNCATE TABLE 03_gold.sc_analytics.dim_player",
    ],
)
def test_forbidden_statements_blocked(sql: str) -> None:
    with pytest.raises(SqlGuardError):
        guard_sql(sql, allowlist=ALLOWLIST)


def test_unallowlisted_table_blocked() -> None:
    with pytest.raises(SqlGuardError, match="not on the chatbot allowlist"):
        guard_sql(
            "SELECT * FROM 02_silver.sc_analytics.silver_player_deaths",
            allowlist=ALLOWLIST,
        )


def test_multi_statement_blocked() -> None:
    with pytest.raises(SqlGuardError, match="single statement"):
        guard_sql(
            "SELECT 1 FROM 03_gold.sc_analytics.dim_player; "
            "SELECT 2 FROM 03_gold.sc_analytics.dim_player",
            allowlist=ALLOWLIST,
        )


def test_empty_blocked() -> None:
    with pytest.raises(SqlGuardError):
        guard_sql("   ", allowlist=ALLOWLIST)


def test_unparseable_blocked() -> None:
    with pytest.raises(SqlGuardError):
        guard_sql("not sql at all !!!", allowlist=ALLOWLIST)


def test_optimize_command_blocked() -> None:
    with pytest.raises(SqlGuardError):
        guard_sql(
            "OPTIMIZE 03_gold.sc_analytics.dim_player",
            allowlist=ALLOWLIST,
        )
