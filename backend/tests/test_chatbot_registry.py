from __future__ import annotations

from backend.app.chatbot import _select_relevant_tables
from backend.app.semantic_registry import load_registry


def test_boss_scoped_deaths_prefer_enriched_death_table() -> None:
    registry = load_registry()

    relevant = _select_relevant_tables("Who dies most often on each boss?", registry)

    assert relevant[0].model == "gold_player_death_events"
    assert any(column["name"] == "boss_name" for column in relevant[0].columns)
