"""LangChain pipeline: natural-language question → grounded SQL → answer.

The flow is intentionally boring: build a system prompt from the semantic
registry, ask the LLM for SQL, validate via :mod:`sql_guard`, execute via
:mod:`db`, ask the LLM to write a short answer grounded in the result, and
return everything (including the SQL and tables-used) so the UI can show it.

If the LLM fails to produce valid SQL twice, the chatbot returns a structured
"I cannot answer this from the governed dataset" response listing the closest
example questions from the registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import Settings, get_settings
from .db import QueryResult, execute_select
from .schemas import ChatResponse
from .semantic_registry import Registry, TableInfo, load_registry
from .sql_guard import SqlGuardError, guard_sql

MAX_SQL_ATTEMPTS = 2
MAX_PROMPT_TABLES = 12


def _registry_overview(registry: Registry) -> str:
    """Compact bullet list of primary-tier tables for the system prompt."""

    lines: list[str] = []
    for info in registry.primary():
        summary = info.ai_summary or info.description or info.grain or ""
        lines.append(
            f"- `{info.table}` — {summary or '(no summary; rely on column names)'} "
            f"Grain: {info.grain or 'unspecified'}."
        )
        if info.example_questions:
            lines.append(f"    e.g. {info.example_questions[0]}")
    return "\n".join(lines)


def _table_columns_block(infos: list[TableInfo]) -> str:
    blocks: list[str] = []
    for info in infos:
        cols = "\n".join(
            f"  - `{c['name']}` ({c['type']}): {c.get('description') or 'no description'}"
            for c in info.columns
        )
        blocks.append(
            f"### {info.table}\n"
            f"Grain: {info.grain or 'unspecified'}.\n"
            f"Primary key: {', '.join(info.primary_key) or 'none'}.\n"
            f"Summary: {info.ai_summary or info.description or '—'}\n"
            f"Columns:\n{cols}"
        )
    return "\n\n".join(blocks)


def _build_system_prompt(registry: Registry) -> str:
    return (
        "You are a SQL assistant for the SC Analytics WarcraftLogs dashboard. "
        "Answer using ONLY the listed tables. Output a single SELECT statement, "
        "no commentary. Use Databricks SQL syntax. Always qualify tables as "
        f"`{registry.catalog}.{registry.schema}.<table>`.\n\n"
        f"Available tables:\n{_registry_overview(registry)}\n\n"
        "Guardrails:\n"
        "- Never write INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/CREATE.\n"
        "- Never query tables outside the list above.\n"
        "- If you cannot answer with the listed tables, reply with the literal "
        "string CANNOT_ANSWER.\n"
    )


def _select_relevant_tables(question: str, registry: Registry) -> list[TableInfo]:
    """Cheap keyword overlap to keep the prompt compact.

    Replace with retrieval/embeddings for production. Returns at most
    ``MAX_PROMPT_TABLES`` tables, primary first.
    """

    q = question.lower()
    scored: list[tuple[int, TableInfo]] = []
    for info in registry.tables.values():
        haystack = " ".join(
            [info.model, info.ai_summary, info.description, info.grain]
            + [str(q) for q in info.example_questions]
            + [c.get("name", "") for c in info.columns]
        ).lower()
        score = sum(1 for word in q.split() if len(word) > 3 and word in haystack)
        if info.chatbot_tier == "primary":
            score += 1
        if score:
            scored.append((score, info))
    scored.sort(key=lambda pair: (-pair[0], pair[1].chatbot_tier != "primary"))
    if not scored:
        return registry.primary()[:MAX_PROMPT_TABLES]
    return [info for _, info in scored[:MAX_PROMPT_TABLES]]


@dataclass
class _LLM:
    """Thin wrapper so tests can substitute a fake."""

    settings: Settings

    def call(self, system: str, user: str) -> str:  # pragma: no cover - network
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError("openai package not installed") from exc
        client = OpenAI(api_key=self.settings.openai_api_key)
        rsp = client.chat.completions.create(
            model=self.settings.openai_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0,
        )
        return (rsp.choices[0].message.content or "").strip()


def _collect_caveats(tables: list[TableInfo], columns: tuple[str, ...]) -> list[str]:
    out: list[str] = []
    column_set = {c.lower() for c in columns}
    for info in tables:
        for col in info.columns:
            unit = (col.get("unit") or "").lower()
            name = col.get("name", "").lower()
            if name in column_set and col.get("classification") in {"guild_internal", "pii"}:
                out.append(f"`{name}` is {col['classification']}; handle with care.")
            if name in {"rank_percent", "bracket_percent"} and name in column_set:
                out.append(
                    "Parse percentiles can be null on recent reports until WCL "
                    "rankings finalise."
                )
            if "raiderio" in (info.ai_summary or "").lower() and unit == "score":
                out.append(
                    "Raider.IO score history starts at first ingestion, not at "
                    "season start — older trends are not available."
                )
    return list(dict.fromkeys(out))


def _rows_as_dicts(result: QueryResult) -> list[dict[str, Any]]:
    return [dict(zip(result.columns, row, strict=False)) for row in result.rows]


def answer_question(
    question: str,
    *,
    registry: Registry | None = None,
    settings: Settings | None = None,
    llm: _LLM | None = None,
) -> ChatResponse:
    settings = settings or get_settings()
    registry = registry or load_registry()
    llm = llm or _LLM(settings=settings)

    relevant = _select_relevant_tables(question, registry)
    system_prompt = _build_system_prompt(registry) + "\n\n" + _table_columns_block(relevant)

    last_error: str | None = None
    for attempt in range(MAX_SQL_ATTEMPTS):
        prompt_user = (
            question
            if attempt == 0
            else (
                f"{question}\n\nYour previous answer was rejected: {last_error}. "
                "Return a corrected single SELECT statement."
            )
        )
        raw = llm.call(system_prompt, prompt_user)
        if "CANNOT_ANSWER" in raw.upper():
            return _cannot_answer(registry)
        sql = _strip_code_fence(raw)
        try:
            guarded = guard_sql(
                sql,
                allowlist=registry.allowlist(),
                default_limit=settings.sql_row_limit,
            )
            break
        except SqlGuardError as exc:
            last_error = str(exc)
            continue
    else:
        return ChatResponse(
            answer="I could not produce a safe SQL query for that question.",
            error=last_error,
            caveats=["The model was asked to retry but produced an invalid query twice."],
        )

    try:
        result = execute_select(guarded.sql, settings=settings)
    except Exception as exc:  # pragma: no cover - depends on warehouse
        return ChatResponse(
            answer="The query failed to execute against Databricks.",
            sql=guarded.sql,
            tables_used=list(guarded.tables_used),
            error=str(exc),
        )

    rows = _rows_as_dicts(result)
    answer_prompt = (
        "Given this user question and the resulting rows from a Databricks SQL "
        "query, write a short (1–3 sentence) plain-English answer. Cite numeric "
        "values directly from the rows. If the rows are empty, say so."
    )
    answer_text = llm.call(
        answer_prompt,
        f"Question: {question}\nRows (first 20): {rows[:20]}\nSQL: {guarded.sql}",
    )

    caveats = _collect_caveats(relevant, result.columns)
    return ChatResponse(
        answer=answer_text,
        sql=guarded.sql,
        tables_used=list(guarded.tables_used),
        rows=rows,
        caveats=caveats,
    )


def _strip_code_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        # remove leading ```sql / ``` and trailing ```
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def _cannot_answer(registry: Registry) -> ChatResponse:
    samples: list[str] = []
    for info in registry.primary():
        samples.extend(info.example_questions[:1])
    return ChatResponse(
        answer="I can't answer that from the governed analytics dataset.",
        caveats=["The chatbot is restricted to the gold layer."]
        + ([f"Try one of: {'; '.join(samples[:5])}"] if samples else []),
    )
