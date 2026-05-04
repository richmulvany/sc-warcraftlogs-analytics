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
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .semantic_registry import Registry
from .sql_guard import SqlGuardError, guard_sql

DEFAULT_MEMORY_PATH = Path(__file__).with_name("query_memory.json")
DEFAULT_R2_OBJECT_KEY = "chatbot/query_memory.json"


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
    if path is None and _r2_enabled():
        return _read_r2_raw()
    target = path or DEFAULT_MEMORY_PATH
    if not target.exists():
        return {"version": 1, "entries": []}
    return json.loads(target.read_text(encoding="utf-8"))


def _write_raw(raw: dict[str, Any], path: Path | None = None) -> None:
    if path is None and _r2_enabled():
        _write_r2_raw(raw)
        return
    target = path or DEFAULT_MEMORY_PATH
    target.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")


def _r2_enabled() -> bool:
    return os.getenv("QUERY_MEMORY_BACKEND", "").lower() in {"r2", "s3"}


def _r2_client():
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for QUERY_MEMORY_BACKEND=r2; install backend requirements."
        ) from exc

    endpoint_url = os.getenv("QUERY_MEMORY_R2_ENDPOINT_URL")
    account_id = os.getenv("QUERY_MEMORY_R2_ACCOUNT_ID")
    if not endpoint_url and account_id:
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint_url:
        raise RuntimeError(
            "QUERY_MEMORY_R2_ENDPOINT_URL or QUERY_MEMORY_R2_ACCOUNT_ID is required for R2 query memory."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("QUERY_MEMORY_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("QUERY_MEMORY_R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("QUERY_MEMORY_R2_REGION", "auto"),
    )


def _r2_location() -> tuple[str, str]:
    bucket = os.getenv("QUERY_MEMORY_R2_BUCKET", "")
    key = os.getenv("QUERY_MEMORY_R2_OBJECT_KEY", DEFAULT_R2_OBJECT_KEY)
    if not bucket:
        raise RuntimeError("QUERY_MEMORY_R2_BUCKET is required for R2 query memory.")
    return bucket, key


def _read_r2_raw() -> dict[str, Any]:
    bucket, key = _r2_location()
    client = _r2_client()
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404", "NoSuchBucket"}:
            return {"version": 1, "entries": []}
        raise
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def _write_r2_raw(raw: dict[str, Any]) -> None:
    bucket, key = _r2_location()
    _r2_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=(json.dumps(raw, indent=2) + "\n").encode("utf-8"),
        ContentType="application/json",
    )


def query_memory_health() -> dict[str, Any]:
    """Diagnostic snapshot of the query-memory backend.

    Used by the /chat/memory/health endpoint so operators can confirm R2 is
    reachable and how many entries are stored without shelling into the
    container. Performs a read only — no writes — to avoid creating noise
    when credentials are read-only.
    """

    backend = "r2" if _r2_enabled() else "local"
    info: dict[str, Any] = {
        "backend": backend,
        "read_ok": False,
        "read_error": None,
        "entry_count": 0,
    }
    if backend == "r2":
        try:
            bucket, key = _r2_location()
            info["bucket"] = bucket
            info["key"] = key
            info["endpoint_url"] = os.getenv("QUERY_MEMORY_R2_ENDPOINT_URL") or (
                f"https://{os.getenv('QUERY_MEMORY_R2_ACCOUNT_ID', '')}" ".r2.cloudflarestorage.com"
                if os.getenv("QUERY_MEMORY_R2_ACCOUNT_ID")
                else None
            )
        except RuntimeError as exc:
            info["read_error"] = str(exc)
            return info
    else:
        info["path"] = str(DEFAULT_MEMORY_PATH)
    try:
        raw = _read_raw()
        info["read_ok"] = True
        info["entry_count"] = len(raw.get("entries", []))
    except Exception as exc:
        info["read_error"] = f"{type(exc).__name__}: {exc}"
    return info


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
