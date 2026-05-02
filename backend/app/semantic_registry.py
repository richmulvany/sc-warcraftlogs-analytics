"""Load the semantic registry produced by ``scripts/build_semantic_registry.py``.

The registry is the chatbot's view of the gold layer: only tables it is
permitted to query and only the metadata it needs to write grounded SQL. The
registry is loaded once at startup; restart the process after regenerating it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from .config import get_settings


@dataclass(frozen=True)
class TableInfo:
    table: str
    model: str
    chatbot_tier: str
    grain: str
    primary_key: tuple[str, ...]
    description: str
    ai_summary: str
    example_questions: tuple[str, ...]
    metrics: tuple[dict[str, Any], ...]
    join_keys: tuple[dict[str, Any], ...]
    not_recommended_for: tuple[str, ...]
    columns: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class Registry:
    version: int
    catalog: str
    schema: str
    tables: dict[str, TableInfo]

    def allowlist(self) -> set[str]:
        """Return the set of fully-qualified table names the chatbot may query."""

        return {f"{self.catalog}.{self.schema}.{t.model}" for t in self.tables.values()} | {
            t.table for t in self.tables.values()
        }

    def primary(self) -> list[TableInfo]:
        return [t for t in self.tables.values() if t.chatbot_tier == "primary"]

    def secondary(self) -> list[TableInfo]:
        return [t for t in self.tables.values() if t.chatbot_tier == "secondary"]


@lru_cache(maxsize=1)
def load_registry(path: Path | None = None) -> Registry:
    settings = get_settings()
    target = path or settings.semantic_registry_path
    raw = json.loads(target.read_text(encoding="utf-8"))
    tables: dict[str, TableInfo] = {}
    for spec in raw.get("tables", []):
        info = TableInfo(
            table=spec["table"],
            model=spec["model"],
            chatbot_tier=spec.get("chatbot_tier", "secondary"),
            grain=spec.get("grain", ""),
            primary_key=tuple(spec.get("primary_key", ())),
            description=spec.get("description", ""),
            ai_summary=spec.get("ai_summary", ""),
            example_questions=tuple(spec.get("example_questions", ())),
            metrics=tuple(spec.get("metrics", ())),
            join_keys=tuple(spec.get("join_keys", ())),
            not_recommended_for=tuple(spec.get("not_recommended_for", ())),
            columns=tuple(spec.get("columns", ())),
        )
        tables[info.model] = info
    return Registry(
        version=int(raw.get("version", 1)),
        catalog=raw.get("catalog", "03_gold"),
        schema=raw.get("schema", "sc_analytics"),
        tables=tables,
    )
