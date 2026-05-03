from __future__ import annotations

import json

from backend.app.query_memory import (
    apply_feedback,
    direct_reuse_entry,
    record_candidate,
    record_rejected_query,
    rejected_examples,
)
from backend.app.semantic_registry import load_registry


def test_feedback_promotes_candidate_for_exact_reuse(tmp_path, monkeypatch) -> None:
    memory_path = tmp_path / "query_memory.json"
    memory_path.write_text(json.dumps({"version": 1, "entries": []}), encoding="utf-8")
    monkeypatch.setattr("backend.app.query_memory.DEFAULT_MEMORY_PATH", memory_path)
    registry = load_registry()

    sql = "SELECT player_name FROM 03_gold.sc_analytics.dim_player LIMIT 10"
    response_id = record_candidate(
        "Who is on the roster?", sql, ["03_gold.sc_analytics.dim_player"]
    )

    assert direct_reuse_entry("Who is on the roster?", registry) is None

    approved = apply_feedback(response_id=response_id, effective=True)
    reuse = direct_reuse_entry("Who is on the roster?", registry)

    assert approved.status == "approved"
    assert approved.use_for_prompt is True
    assert approved.allow_direct_reuse is True
    assert reuse is not None
    assert reuse.sql == sql


def test_negative_feedback_blocks_reuse(tmp_path, monkeypatch) -> None:
    memory_path = tmp_path / "query_memory.json"
    memory_path.write_text(json.dumps({"version": 1, "entries": []}), encoding="utf-8")
    monkeypatch.setattr("backend.app.query_memory.DEFAULT_MEMORY_PATH", memory_path)
    registry = load_registry()

    sql = "SELECT player_name FROM 03_gold.sc_analytics.dim_player LIMIT 10"
    response_id = record_candidate(
        "Who is on the roster?", sql, ["03_gold.sc_analytics.dim_player"]
    )

    rejected = apply_feedback(response_id=response_id, effective=False)

    assert rejected.status == "rejected"
    assert rejected.use_for_prompt is False
    assert rejected.allow_direct_reuse is False
    assert direct_reuse_entry("Who is on the roster?", registry) is None


def test_failed_query_is_available_as_rejected_example(tmp_path, monkeypatch) -> None:
    memory_path = tmp_path / "query_memory.json"
    memory_path.write_text(json.dumps({"version": 1, "entries": []}), encoding="utf-8")
    monkeypatch.setattr("backend.app.query_memory.DEFAULT_MEMORY_PATH", memory_path)
    registry = load_registry()

    sql = "SELECT fpe.boss_name " "FROM 03_gold.sc_analytics.fact_player_events AS fpe LIMIT 10"
    response_id = record_rejected_query(
        "Who dies most often on each boss?",
        sql,
        error="databricks: fpe.boss_name cannot be resolved",
        tables_used=["03_gold.sc_analytics.fact_player_events"],
    )

    examples = rejected_examples("Who dies most often on each boss?", registry)

    assert response_id.startswith("mem_")
    assert examples
    assert examples[0].status == "rejected"
    assert "fpe.boss_name" in examples[0].sql
