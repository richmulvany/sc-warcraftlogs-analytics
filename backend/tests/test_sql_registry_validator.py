from __future__ import annotations

import pytest

from backend.app.semantic_registry import load_registry
from backend.app.sql_guard import SqlGuardError
from backend.app.sql_registry_validator import validate_sql_columns


def test_rejects_column_on_wrong_alias() -> None:
    registry = load_registry()
    sql = """
SELECT fpe.boss_name
FROM 03_gold.sc_analytics.fact_player_events AS fpe
JOIN 03_gold.sc_analytics.fact_player_fight_performance AS fpf
  ON fpe.report_code = fpf.report_code
"""

    with pytest.raises(SqlGuardError, match=r"fpe\.boss_name.*fact_player_events"):
        validate_sql_columns(sql, registry)


def test_accepts_enriched_death_event_columns() -> None:
    registry = load_registry()
    sql = """
SELECT gpde.boss_name, gpde.player_name, COUNT(*) AS death_count
FROM 03_gold.sc_analytics.gold_player_death_events AS gpde
GROUP BY gpde.boss_name, gpde.player_name
"""

    validate_sql_columns(sql, registry)
