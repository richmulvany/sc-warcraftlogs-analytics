from __future__ import annotations

from backend.app.chatbot import _query_examples_block, _select_relevant_tables, answer_question
from backend.app.db import QueryResult
from backend.app.semantic_registry import load_registry


def test_boss_scoped_deaths_prefer_enriched_death_table() -> None:
    registry = load_registry()

    relevant = _select_relevant_tables("Who dies most often on each boss?", registry)

    assert relevant[0].model == "gold_player_death_events"
    assert any(column["name"] == "boss_name" for column in relevant[0].columns)


def test_boss_death_leader_question_gets_approved_prompt_example() -> None:
    registry = load_registry()

    examples = _query_examples_block("Who dies most often on each boss?", registry)

    assert "Approved SQL examples from query memory" in examples
    assert "gold_player_death_events" in examples
    assert "fact_player_events" not in examples
    assert "ROW_NUMBER() OVER" in examples
    assert "death_count DESC" in examples


def test_boss_death_leader_still_uses_llm_sql_generation(monkeypatch) -> None:
    registry = load_registry()
    calls: list[tuple[str, str]] = []

    def fake_execute_select(sql, settings=None):
        assert "gold_player_death_events" in sql
        assert "fact_player_events" not in sql
        return QueryResult(
            columns=(
                "zone_name",
                "boss_name",
                "difficulty_label",
                "player_name",
                "player_class",
                "death_count",
            ),
            rows=(
                (
                    "Manaforge Omega",
                    "Plexus Sentinel",
                    "Mythic",
                    "Cherven",
                    "Mage",
                    7,
                ),
            ),
        )

    class FakeLLM:
        def call(self, system: str, user: str) -> str:
            calls.append((system, user))
            if len(calls) == 1:
                assert "Approved SQL examples from query memory" in system
                return """
WITH death_counts AS (
  SELECT
    boss_name,
    difficulty_label,
    player_name,
    player_class,
    COUNT(*) AS death_count
  FROM 03_gold.sc_analytics.gold_player_death_events
  WHERE boss_name IS NOT NULL
  GROUP BY boss_name, difficulty_label, player_name, player_class
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY boss_name, difficulty_label
      ORDER BY death_count DESC, player_name ASC
    ) AS death_rank
  FROM death_counts
)
SELECT
  boss_name,
  difficulty_label,
  player_name,
  player_class,
  death_count
FROM ranked
WHERE death_rank = 1
ORDER BY boss_name ASC
LIMIT 500
""".strip()
            return "Plexus Sentinel: Cherven has 7 deaths on Mythic."

    monkeypatch.setattr("backend.app.chatbot.execute_select", fake_execute_select)
    monkeypatch.setattr(
        "backend.app.chatbot.record_candidate", lambda question, sql, tables_used: "candidate-id"
    )

    response = answer_question(
        "Who dies most often on each boss?",
        registry=registry,
        llm=FakeLLM(),
    )

    assert len(calls) == 2
    assert response.response_id == "candidate-id"
    assert response.from_memory is False
    assert response.rows[0]["boss_name"] == "Plexus Sentinel"
    assert response.answer == "Plexus Sentinel: Cherven has 7 deaths on Mythic."
