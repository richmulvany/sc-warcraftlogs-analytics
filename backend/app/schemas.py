"""Pydantic schemas for the chatbot HTTP API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)


class ChatResponse(BaseModel):
    answer: str
    sql: str | None = None
    tables_used: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    error: str | None = None
    response_id: str | None = None
    from_memory: bool = False


class ChatMetaResponse(BaseModel):
    model: str


class ChatFeedbackRequest(BaseModel):
    response_id: str = Field(..., min_length=1, max_length=200)
    effective: bool
    question: str | None = Field(default=None, max_length=2000)
    sql: str | None = Field(default=None, max_length=50000)


class ChatFeedbackResponse(BaseModel):
    response_id: str
    status: str
    use_for_prompt: bool
    allow_direct_reuse: bool
