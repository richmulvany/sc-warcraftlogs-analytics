from __future__ import annotations

import json

from backend.app.query_memory import (
    apply_feedback,
    direct_reuse_entry,
    prompt_examples,
    query_memory_health,
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


def test_r2_memory_defaults_to_existing_dashboard_prefix(monkeypatch) -> None:
    monkeypatch.setenv("QUERY_MEMORY_BACKEND", "r2")
    monkeypatch.setenv("R2_BUCKET", "guild-dashboard-assets")
    monkeypatch.setenv("R2_PREFIX", "sc-analytics-data")
    monkeypatch.setenv("QUERY_MEMORY_R2_ACCOUNT_ID", "account")
    monkeypatch.setattr(
        "backend.app.query_memory._read_r2_raw",
        lambda: {"version": 1, "entries": []},
    )

    health = query_memory_health()

    assert health["bucket"] == "guild-dashboard-assets"
    assert health["key"] == "sc-analytics-data/query_memory/query_memory.json"
    assert health["read_ok"] is True


def test_r2_memory_read_failure_falls_back_to_local_seed(tmp_path, monkeypatch) -> None:
    memory_path = tmp_path / "query_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "version": 1,
                "entries": [
                    {
                        "id": "seed",
                        "question": "Who dies most often on each boss?",
                        "normalized_question": "who dies most often on each boss",
                        "sql": "SELECT player_name FROM 03_gold.sc_analytics.dim_player LIMIT 10",
                        "status": "approved",
                        "source": "seed",
                        "use_for_prompt": True,
                        "allow_direct_reuse": False,
                        "positive_feedback": 1,
                        "negative_feedback": 0,
                        "notes": "",
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("QUERY_MEMORY_BACKEND", "r2")
    monkeypatch.setattr("backend.app.query_memory.DEFAULT_MEMORY_PATH", memory_path)

    def fail_read() -> dict:
        raise RuntimeError("R2 unavailable")

    monkeypatch.setattr("backend.app.query_memory._read_r2_raw", fail_read)
    registry = load_registry()

    examples = prompt_examples("Who dies most often on each boss?", registry)
    assert examples
    assert examples[0].id == "seed"


def test_record_candidate_does_not_raise_when_r2_write_fails(monkeypatch) -> None:
    monkeypatch.setenv("QUERY_MEMORY_BACKEND", "r2")
    monkeypatch.setattr(
        "backend.app.query_memory._read_r2_raw",
        lambda: {"version": 1, "entries": []},
    )

    def fail_write(raw: dict) -> None:
        raise RuntimeError("R2 write denied")

    monkeypatch.setattr("backend.app.query_memory._write_r2_raw", fail_write)

    response_id = record_candidate(
        "Who is on the roster?",
        "SELECT player_name FROM 03_gold.sc_analytics.dim_player LIMIT 10",
        ["03_gold.sc_analytics.dim_player"],
    )

    assert response_id.startswith("mem_")
