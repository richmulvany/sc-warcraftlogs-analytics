"""Feedback-backed SQL memory for the chatbot.

The memory has two jobs:

* prompt memory: approved examples are inserted into the SQL-generation prompt
  so the LLM learns reliable patterns without bypassing reasoning;
* reuse memory: only SQL that a user has explicitly marked effective can be
  reused directly for the same normalized question.

This intentionally avoids silently caching every generated query as truth.
Unreviewed successful queries are stored as candidates; negative feedback keeps
them out of both prompt examples and direct reuse.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .semantic_registry import Registry
from .sql_guard import SqlGuardError, guard_sql

DEFAULT_MEMORY_PATH = Path(__file__).with_name("query_memory.json")


@dataclass(frozen=True)
class QueryMemoryEntry:
    id: str
    question: str
    normalized_question: str
    sql: str
    status: str
    source: str
    use_for_prompt: bool
    allow_direct_reuse: bool
    positive_feedback: int
    negative_feedback: int
    notes: str
    created_at: str
    updated_at: str


def normalize_question(question: str) -> str:
    return re.sub(r"\s+", " ", question.lower().strip().rstrip("?!."))


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _entry_id(question: str, sql: str) -> str:
    digest = hashlib.sha256(f"{normalize_question(question)}\n{sql}".encode()).hexdigest()
    return f"mem_{digest[:16]}"


def _read_raw(path: Path | None = None) -> dict[str, Any]:
    target = path or DEFAULT_MEMORY_PATH
    if not target.exists():
        return {"version": 1, "entries": []}
    return json.loads(target.read_text(encoding="utf-8"))


def _write_raw(raw: dict[str, Any], path: Path | None = None) -> None:
    target = path or DEFAULT_MEMORY_PATH
    target.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")


def load_query_memory(path: Path | None = None) -> list[QueryMemoryEntry]:
    raw = _read_raw(path)
    entries: list[QueryMemoryEntry] = []
    for item in raw.get("entries", []):
        entries.append(
            QueryMemoryEntry(
                id=item["id"],
                question=item["question"],
                normalized_question=item.get("normalized_question")
                or normalize_question(item["question"]),
                sql=item["sql"],
                status=item.get("status", "candidate"),
                source=item.get("source", "unknown"),
                use_for_prompt=bool(item.get("use_for_prompt", False)),
                allow_direct_reuse=bool(item.get("allow_direct_reuse", False)),
                positive_feedback=int(item.get("positive_feedback", 0)),
                negative_feedback=int(item.get("negative_feedback", 0)),
                notes=item.get("notes", ""),
                created_at=item.get("created_at", ""),
                updated_at=item.get("updated_at", ""),
            )
        )
    return entries


def _token_score(question: str, entry: QueryMemoryEntry) -> int:
    stop = {"what", "which", "with", "from", "that", "this", "most", "often"}
    q_tokens = {
        t
        for t in re.findall(r"[a-z0-9_]+", normalize_question(question))
        if len(t) > 2 and t not in stop
    }
    e_tokens = {
        t
        for t in re.findall(r"[a-z0-9_]+", entry.normalized_question)
        if len(t) > 2 and t not in stop
    }
    return len(q_tokens & e_tokens)


def _valid_entry_sql(entry: QueryMemoryEntry, registry: Registry) -> bool:
    try:
        guard_sql(entry.sql, allowlist=registry.allowlist())
    except SqlGuardError:
        return False
    return True


def prompt_examples(question: str, registry: Registry, limit: int = 3) -> list[QueryMemoryEntry]:
    candidates = [
        entry
        for entry in load_query_memory()
        if entry.status == "approved" and entry.use_for_prompt and _valid_entry_sql(entry, registry)
    ]
    candidates.sort(
        key=lambda entry: (
            entry.normalized_question != normalize_question(question),
            -_token_score(question, entry),
            entry.question,
        )
    )
    return candidates[:limit]


def rejected_examples(question: str, registry: Registry, limit: int = 3) -> list[QueryMemoryEntry]:
    candidates = [
        entry
        for entry in load_query_memory()
        if entry.status == "rejected" and _valid_entry_sql(entry, registry)
    ]
    candidates.sort(
        key=lambda entry: (
            entry.normalized_question != normalize_question(question),
            -_token_score(question, entry),
            entry.question,
        )
    )
    return candidates[:limit]


def direct_reuse_entry(question: str, registry: Registry) -> QueryMemoryEntry | None:
    normalized = normalize_question(question)
    for entry in load_query_memory():
        if (
            entry.normalized_question == normalized
            and entry.status == "approved"
            and entry.allow_direct_reuse
            and _valid_entry_sql(entry, registry)
        ):
            return entry
    return None


def record_candidate(question: str, sql: str, tables_used: list[str]) -> str:
    raw = _read_raw()
    entries = raw.setdefault("entries", [])
    entry_id = _entry_id(question, sql)
    now = _now_iso()
    for entry in entries:
        if entry.get("id") == entry_id:
            entry["updated_at"] = now
            return entry_id
    entries.append(
        {
            "id": entry_id,
            "question": question,
            "normalized_question": normalize_question(question),
            "sql": sql,
            "status": "candidate",
            "source": "runtime",
            "use_for_prompt": False,
            "allow_direct_reuse": False,
            "positive_feedback": 0,
            "negative_feedback": 0,
            "tables_used": tables_used,
            "notes": "Captured after a successful chatbot response; requires user feedback before reuse.",
            "created_at": now,
            "updated_at": now,
        }
    )
    _write_raw(raw)
    return entry_id


def record_rejected_query(
    question: str,
    sql: str,
    *,
    error: str,
    tables_used: list[str] | None = None,
) -> str:
    raw = _read_raw()
    entries = raw.setdefault("entries", [])
    entry_id = _entry_id(question, sql)
    now = _now_iso()
    for entry in entries:
        if entry.get("id") == entry_id:
            entry["status"] = "rejected"
            entry["use_for_prompt"] = False
            entry["allow_direct_reuse"] = False
            entry["negative_feedback"] = int(entry.get("negative_feedback", 0)) + 1
            entry["last_error"] = error
            entry["notes"] = "Automatically rejected after SQL validation or execution failed."
            entry["updated_at"] = now
            _write_raw(raw)
            return entry_id
    entries.append(
        {
            "id": entry_id,
            "question": question,
            "normalized_question": normalize_question(question),
            "sql": sql,
            "status": "rejected",
            "source": "runtime_error",
            "use_for_prompt": False,
            "allow_direct_reuse": False,
            "positive_feedback": 0,
            "negative_feedback": 1,
            "tables_used": tables_used or [],
            "last_error": error,
            "notes": "Automatically rejected after SQL validation or execution failed.",
            "created_at": now,
            "updated_at": now,
        }
    )
    _write_raw(raw)
    return entry_id


def apply_feedback(
    *,
    response_id: str,
    effective: bool,
    question: str | None = None,
    sql: str | None = None,
) -> QueryMemoryEntry:
    raw = _read_raw()
    entries = raw.setdefault("entries", [])
    now = _now_iso()
    target: dict[str, Any] | None = None
    for entry in entries:
        if entry.get("id") == response_id:
            target = entry
            break
    if target is None:
        if not question or not sql:
            raise KeyError(response_id)
        target = {
            "id": response_id,
            "question": question,
            "normalized_question": normalize_question(question),
            "sql": sql,
            "status": "candidate",
            "source": "feedback",
            "use_for_prompt": False,
            "allow_direct_reuse": False,
            "positive_feedback": 0,
            "negative_feedback": 0,
            "notes": "",
            "created_at": now,
            "updated_at": now,
        }
        entries.append(target)

    if effective:
        target["positive_feedback"] = int(target.get("positive_feedback", 0)) + 1
        target["status"] = "approved"
        target["use_for_prompt"] = True
        target["allow_direct_reuse"] = True
        target["notes"] = (
            "Approved by user feedback; eligible for prompt examples and exact-question reuse."
        )
    else:
        target["negative_feedback"] = int(target.get("negative_feedback", 0)) + 1
        target["status"] = "rejected"
        target["use_for_prompt"] = False
        target["allow_direct_reuse"] = False
        target["notes"] = (
            "Rejected by user feedback; excluded from prompt examples and direct reuse."
        )
    target["updated_at"] = now
    _write_raw(raw)

    return QueryMemoryEntry(
        id=target["id"],
        question=target["question"],
        normalized_question=target.get("normalized_question")
        or normalize_question(target["question"]),
        sql=target["sql"],
        status=target.get("status", "candidate"),
        source=target.get("source", "unknown"),
        use_for_prompt=bool(target.get("use_for_prompt", False)),
        allow_direct_reuse=bool(target.get("allow_direct_reuse", False)),
        positive_feedback=int(target.get("positive_feedback", 0)),
        negative_feedback=int(target.get("negative_feedback", 0)),
        notes=target.get("notes", ""),
        created_at=target.get("created_at", ""),
        updated_at=target.get("updated_at", ""),
    )
