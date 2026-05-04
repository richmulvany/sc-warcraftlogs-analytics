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

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from .config import Settings, get_settings
from .db import QueryResult, execute_select
from .query_memory import (
    direct_reuse_entry,
    prompt_examples,
    record_candidate,
    record_rejected_query,
    rejected_examples,
)
from .schemas import ChatResponse
from .semantic_registry import Registry, TableInfo, load_registry
from .sql_guard import SqlGuardError, guard_sql
from .sql_registry_validator import validate_sql_columns

MAX_SQL_ATTEMPTS = 3
MAX_PROMPT_TABLES = 12


def _is_boss_death_leader_question(question: str) -> bool:
    """Detect "who dies most on each boss" style questions.

    Broadly matched on purpose — the cost of a false positive is "we route the
    question to gold_player_death_events even though gold_player_survivability
    might also have worked", which is fine. The cost of a false negative is the
    LLM picks fact_player_events and produces SQL that fails column validation.
    """

    q = question.lower()
    asks_deaths = any(word in q for word in ("death", "deaths", "die", "dies", "died", "dying"))
    asks_most = any(phrase in q for phrase in ("most", "highest", "top", "leading", "leader"))
    asks_boss_scope = "boss" in q or "encounter" in q
    return asks_deaths and asks_most and asks_boss_scope


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


def _query_examples_block(question: str, registry: Registry) -> str:
    examples = prompt_examples(question, registry)
    if not examples:
        return ""
    blocks: list[str] = [
        "Approved SQL examples from query memory. Use these as patterns when "
        "they match the user's intent, but still write the final SQL yourself "
        "using the current table/column contracts."
    ]
    for idx, example in enumerate(examples, start=1):
        blocks.append(f"Example {idx}\n" f"Question: {example.question}\n" f"SQL:\n{example.sql}")
    return "\n\n".join(blocks)


def _rejected_examples_block(question: str, registry: Registry) -> str:
    examples = rejected_examples(question, registry)
    if not examples:
        return ""
    blocks: list[str] = [
        "Known bad SQL patterns from previous failed attempts. Do NOT repeat "
        "these mistakes. Pay special attention to table aliases and column "
        "ownership."
    ]
    for idx, example in enumerate(examples, start=1):
        blocks.append(
            f"Bad example {idx}\n" f"Question: {example.question}\n" f"Do not write:\n{example.sql}"
        )
    return "\n\n".join(blocks)


def _table_columns_block(infos: list[TableInfo]) -> str:
    blocks: list[str] = []
    for info in infos:
        cols = "\n".join(
            f"  - `{c['name']}` ({c['type']}): {c.get('description') or 'no description'}"
            + (f" Allowed: {', '.join(map(str, c['enum']))}." if c.get("enum") else "")
            + (f" Unit: {c['unit']}." if c.get("unit") else "")
            for c in info.columns
        )
        joins = ""
        if info.join_keys:
            bullets = "\n".join(
                f"  - `{j.get('column')}` -> {j.get('joinsTo')}"
                + (f" ({j['cardinality']})" if j.get("cardinality") else "")
                for j in info.join_keys
            )
            joins = (
                "\nCanonical joins (use ONLY these columns to join — do not add"
                " extra equality conditions on columns not listed here):\n" + bullets
            )
        avoid = ""
        if info.not_recommended_for:
            bullets = "\n".join(f"  - {item}" for item in info.not_recommended_for)
            avoid = f"\nAvoid:\n{bullets}"
        blocks.append(
            f"### {info.table}\n"
            f"Grain: {info.grain or 'unspecified'}.\n"
            f"Primary key: {', '.join(info.primary_key) or 'none'}.\n"
            f"Summary: {info.ai_summary or info.description or '—'}\n"
            f"Columns:\n{cols}"
            f"{joins}"
            f"{avoid}"
        )
    return "\n\n".join(blocks)


def _build_system_prompt(registry: Registry, relevant: list[TableInfo]) -> str:
    # Query memory examples are added later because they are question-specific.
    return (
        "You are a SQL assistant for the SC Analytics WarcraftLogs dashboard. "
        "Answer using ONLY the tables described below. Output a single SELECT "
        "statement, no commentary, no markdown fences. Use Databricks SQL "
        f"syntax. Always qualify tables as `{registry.catalog}.{registry.schema}.<table>`.\n\n"
        "Tables you may query (full column list per table):\n\n"
        f"{_table_columns_block(relevant)}\n\n"
        "Other primary tables (summary only — request not in scope unless one of these obviously fits):\n"
        f"{_registry_overview(registry)}\n\n"
        "Hard rules:\n"
        "- Never write INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/CREATE/TRUNCATE.\n"
        "- Never reference tables outside the lists above.\n"
        "- Prefer the tables in the first (full-column) list when possible.\n"
        "- Use column names EXACTLY as listed under each table — do not invent "
        "or pluralise (e.g. it is `boss_kills`, not `total_kills`).\n"
        "- Each column you reference must appear under the column list of the "
        "table you are querying it from. Never copy a column from one table's "
        "list into a SELECT/JOIN against a different table — the column lists "
        "are authoritative and the tables are NOT interchangeable even when "
        "they look related (e.g. gold_boss_wipe_analysis and gold_boss_mechanics "
        "are separate tables; phase/duration buckets live only on gold_boss_mechanics).\n"
        "- Before joining two tables, confirm BOTH tables actually list the join "
        "column. Do not assume player-keyed tables carry zone_id, encounter_id, "
        "report_code, or fight_id unless the column appears in that table's list.\n"
        "- Prefer joining via the canonical fact tables (fact_player_events, "
        "fact_player_fight_performance) when one side is a player rollup that "
        "does not carry zone/encounter columns.\n"
        "- For boss-scoped death questions such as `who dies most often on "
        "each boss`, prefer `gold_player_death_events` because it already has "
        "`boss_name`, `player_name`, and one row per death. Do not select "
        "`boss_name` from `fact_player_events`; that column is not present "
        "there. The expected shape is: aggregate `COUNT(*) AS death_count` by "
        "`boss_name`, `difficulty_label`, `player_name`, and `player_class`, "
        "then use `ROW_NUMBER() OVER (PARTITION BY boss_name, difficulty_label "
        "ORDER BY death_count DESC, player_name ASC)` and keep `death_rank = 1`.\n"
        "- Honour each table's Avoid: list — those are anti-patterns from the contract.\n"
        "- For `who is worst at X` style questions, prefer ORDER BY <metric> ASC LIMIT N "
        "rather than a hard threshold filter; thresholds can return zero rows.\n"
        "- For `most common <thing>` questions, exclude null values of that field "
        "(or label them `unknown`) when the contract says null means missing/unknown.\n"
        "- If a question is genuinely unanswerable from these tables (e.g. "
        "weather, real-life identity), reply with the single literal token "
        "CANNOT_ANSWER and nothing else. Do not output CANNOT_ANSWER if any "
        "listed table can plausibly answer the question.\n"
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
        if (
            info.model == "gold_player_death_events"
            and any(word in q for word in ("death", "deaths", "die", "dies", "died"))
            and any(word in q for word in ("boss", "encounter"))
        ):
            score += 4
        if score:
            scored.append((score, info))
    scored.sort(key=lambda pair: (-pair[0], pair[1].chatbot_tier != "primary"))
    if not scored:
        return registry.primary()[:MAX_PROMPT_TABLES]
    selected = [info for _, info in scored[:MAX_PROMPT_TABLES]]
    if _is_boss_death_leader_question(question):
        death_events = registry.tables.get("gold_player_death_events")
        if death_events:
            # Hard-route: this is the only table the LLM should see for this
            # question class. Keeping fact_player_events in the prompt was
            # letting the model pick it as a "more fundamental" peer and
            # produce SQL referencing fpe.boss_name (which doesn't exist).
            selected = [death_events]
    return selected[:MAX_PROMPT_TABLES]


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


def _shrink_tables(
    current: list[TableInfo],
    tables_used: list[str] | None,
    error: str,
) -> list[TableInfo]:
    """Drop tables that produced a column-not-found error so the next attempt
    can't repeat the same mistake.

    For column-resolution errors (the dominant failure mode for this codebase
    — hallucinated columns like fpe.boss_name), we trim the table that was in
    the failing SQL out of the prompt entirely. We only shrink when at least
    one table would remain; otherwise we keep the current set so the LLM still
    has something to work with on the next attempt.
    """

    if not tables_used:
        return current
    looks_like_column_error = any(
        token in error.lower()
        for token in ("cannot be resolved", "does not exist", "unresolved", "column")
    )
    if not looks_like_column_error:
        return current
    used_set = {t.split(".")[-1] for t in tables_used}
    remaining = [info for info in current if info.model not in used_set]
    if not remaining:
        return current
    return remaining


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


def _write_answer(
    *,
    question: str,
    rows: list[dict[str, Any]],
    sql: str,
    llm: _LLM,
) -> str:
    answer_prompt = (
        "Given this user question and the resulting rows from a Databricks SQL "
        "query, write a concise answer grounded only in the returned rows. "
        "When the rows represent a leaderboard or one row per group, prefer a "
        "compact markdown bullet list or table-like list rather than summarising "
        "only the first row. Cite numeric values directly from the rows. If the "
        "rows are empty, say so."
    )
    return llm.call(
        answer_prompt,
        f"Question: {question}\nRows (first 50): {rows[:50]}\nSQL: {sql}",
    )


def _answer_from_guarded_sql(
    *,
    question: str,
    guarded_sql: str,
    tables_used: list[str],
    relevant: list[TableInfo],
    settings: Settings,
    llm: _LLM,
    from_memory: bool = False,
    response_id: str | None = None,
) -> ChatResponse:
    result = execute_select(guarded_sql, settings=settings)
    rows = _rows_as_dicts(result)
    answer_text = _write_answer(question=question, rows=rows, sql=guarded_sql, llm=llm)
    caveats = _collect_caveats(relevant, result.columns)
    if from_memory:
        caveats = ["Reused SQL from approved query memory for this exact question."] + caveats
    return ChatResponse(
        answer=answer_text,
        sql=guarded_sql,
        tables_used=tables_used,
        rows=rows,
        caveats=caveats,
        response_id=response_id,
        from_memory=from_memory,
    )


def answer_question(
    question: str,
    *,
    registry: Registry | None = None,
    settings: Settings | None = None,
    llm: _LLM | None = None,
) -> ChatResponse:
    """Blocking variant kept for the existing /chat endpoint and eval harness.

    Drains :func:`answer_question_stream` and returns only the final response.
    """

    final: ChatResponse | None = None
    for event in answer_question_stream(question, registry=registry, settings=settings, llm=llm):
        if event.get("type") == "final":
            final = event["response"]  # type: ignore[assignment]
    assert final is not None
    return final


def answer_question_stream(
    question: str,
    *,
    registry: Registry | None = None,
    settings: Settings | None = None,
    llm: _LLM | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield progress events as the orchestrator runs.

    Event shapes (all dicts):

    - ``{"type": "step", "phase": <str>, "status": "running"|"done"|"error",
        "attempt"?: int, "detail"?: any, "error"?: str}``
    - ``{"type": "final", "response": ChatResponse}``

    The phases are ``selecting_tables``, ``writing_sql``, ``executing_sql``,
    ``writing_answer``. The frontend renders these as a Claude-Code-style
    live tool stream.
    """

    settings = settings or get_settings()
    registry = registry or load_registry()
    llm = llm or _LLM(settings=settings)

    yield {"type": "step", "phase": "selecting_tables", "status": "running"}
    relevant = _select_relevant_tables(question, registry)
    yield {
        "type": "step",
        "phase": "selecting_tables",
        "status": "done",
        "detail": [info.model for info in relevant],
    }

    reuse = direct_reuse_entry(question, registry)
    if reuse is not None:
        yield {
            "type": "step",
            "phase": "memory_reuse",
            "status": "done",
            "detail": reuse.id,
        }
        guarded = guard_sql(
            reuse.sql,
            allowlist=registry.allowlist(),
            default_limit=settings.sql_row_limit,
        )
        yield {"type": "step", "phase": "executing_sql", "status": "running"}
        try:
            response = _answer_from_guarded_sql(
                question=question,
                guarded_sql=guarded.sql,
                tables_used=list(guarded.tables_used),
                relevant=relevant,
                settings=settings,
                llm=llm,
                from_memory=True,
                response_id=reuse.id,
            )
            yield {"type": "step", "phase": "executing_sql", "status": "done"}
        except Exception as exc:
            yield {
                "type": "step",
                "phase": "executing_sql",
                "status": "error",
                "error": str(exc),
            }
            raise
        yield {"type": "final", "response": response}
        return

    examples = _query_examples_block(question, registry)
    rejected = _rejected_examples_block(question, registry)

    def _build_full_prompt(tables: list[TableInfo]) -> str:
        prompt = _build_system_prompt(registry, tables)
        if examples:
            prompt = f"{prompt}\n\n{examples}\n"
        if rejected:
            prompt = f"{prompt}\n\n{rejected}\n"
        return prompt

    # Combined retry loop: the LLM is given a chance to fix both sql_guard
    # rejections and Databricks execution errors. The latter handle the common
    # case of hallucinated column names (e.g. `total_kills` when the column is
    # actually `boss_kills`); we feed the database error back so it can correct.
    # On column-not-found failures we also drop the offending table from the
    # prompt for the next attempt — otherwise the LLM tends to retry the same
    # wrong table with cosmetic changes.
    last_error: str | None = None
    last_bad_sql: str | None = None
    last_response_id: str | None = None
    guarded = None
    result = None
    current_tables = relevant
    for attempt in range(MAX_SQL_ATTEMPTS):
        system_prompt = _build_full_prompt(current_tables)
        prompt_user = (
            question
            if attempt == 0
            else (
                f"{question}\n\nYour previous SQL was rejected with this error: "
                f"{last_error}\n"
                f"Previous SQL to avoid:\n{last_bad_sql or '(not available)'}\n"
                "Return a corrected single SELECT statement using only columns "
                "that appear in the table column lists above."
            )
        )
        yield {
            "type": "step",
            "phase": "writing_sql",
            "status": "running",
            "attempt": attempt + 1,
        }
        raw = llm.call(system_prompt, prompt_user)
        if "CANNOT_ANSWER" in raw.upper():
            yield {
                "type": "step",
                "phase": "writing_sql",
                "status": "done",
                "attempt": attempt + 1,
                "detail": "CANNOT_ANSWER",
            }
            yield {"type": "final", "response": _cannot_answer(registry)}
            return
        sql = _strip_code_fence(raw)
        yield {
            "type": "step",
            "phase": "writing_sql",
            "status": "done",
            "attempt": attempt + 1,
            "detail": sql,
        }
        try:
            guarded = guard_sql(
                sql,
                allowlist=registry.allowlist(),
                default_limit=settings.sql_row_limit,
            )
            validate_sql_columns(guarded.sql, registry)
        except SqlGuardError as exc:
            last_error = f"sql_guard: {exc}"
            last_bad_sql = guarded.sql if guarded else sql
            tables_used = list(guarded.tables_used) if guarded else None
            last_response_id = record_rejected_query(
                question,
                last_bad_sql,
                error=last_error,
                tables_used=tables_used,
            )
            current_tables = _shrink_tables(current_tables, tables_used, last_error)
            yield {
                "type": "step",
                "phase": "executing_sql",
                "status": "error",
                "attempt": attempt + 1,
                "error": last_error,
            }
            continue
        yield {
            "type": "step",
            "phase": "executing_sql",
            "status": "running",
            "attempt": attempt + 1,
        }
        try:
            result = execute_select(guarded.sql, settings=settings)
            last_error = None
            yield {
                "type": "step",
                "phase": "executing_sql",
                "status": "done",
                "attempt": attempt + 1,
                "detail": len(result.rows),
            }
            break
        except Exception as exc:
            last_error = f"databricks: {exc}"
            last_bad_sql = guarded.sql
            tables_used = list(guarded.tables_used)
            last_response_id = record_rejected_query(
                question,
                guarded.sql,
                error=last_error,
                tables_used=tables_used,
            )
            current_tables = _shrink_tables(current_tables, tables_used, last_error)
            yield {
                "type": "step",
                "phase": "executing_sql",
                "status": "error",
                "attempt": attempt + 1,
                "error": last_error,
            }
            continue
    else:
        yield {
            "type": "final",
            "response": ChatResponse(
                answer="I could not produce a working query for that question.",
                sql=guarded.sql if guarded else None,
                tables_used=list(guarded.tables_used) if guarded else [],
                error=last_error,
                caveats=["The model retried but the query still failed."],
                response_id=last_response_id,
            ),
        }
        return

    yield {"type": "step", "phase": "writing_answer", "status": "running"}
    rows = _rows_as_dicts(result)
    answer_text = _write_answer(question=question, rows=rows, sql=guarded.sql, llm=llm)
    caveats = _collect_caveats(relevant, result.columns)
    response_id = None
    if guarded and not last_error and not getattr(result, "error", None):
        response_id = record_candidate(question, guarded.sql, list(guarded.tables_used))
    yield {"type": "step", "phase": "writing_answer", "status": "done"}
    yield {
        "type": "final",
        "response": ChatResponse(
            answer=answer_text,
            sql=guarded.sql,
            tables_used=list(guarded.tables_used),
            rows=rows,
            caveats=caveats,
            response_id=response_id,
            from_memory=False,
        ),
    }


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
