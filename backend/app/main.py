"""FastAPI entrypoint for the SC Analytics chatbot."""

from __future__ import annotations

import hmac
import json
import logging

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .chatbot import answer_question, answer_question_stream
from .config import get_settings
from .query_memory import apply_feedback, query_memory_health
from .schemas import (
    ChatFeedbackRequest,
    ChatFeedbackResponse,
    ChatMemoryHealthResponse,
    ChatMetaResponse,
    ChatRequest,
    ChatResponse,
)
from .semantic_registry import load_registry

logger = logging.getLogger(__name__)

app = FastAPI(title="SC Analytics Chatbot", version="0.1.0")

_settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(_settings.cors_allow_origins) or ["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key"],
)


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Reject requests when CHATBOT_BACKEND_API_KEY is set and the header is wrong.

    When no key is configured (local dev), the dependency is a no-op.
    Comparison uses ``hmac.compare_digest`` to avoid timing leaks.
    """

    expected = _settings.api_key
    if not expected:
        return  # auth disabled
    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key.")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    registry = load_registry()
    return {
        "status": "ok",
        "registry_tables": str(len(registry.tables)),
        "primary_tier": str(len(registry.primary())),
        "auth_required": "true" if _settings.api_key else "false",
    }


@app.post("/chat", response_model=ChatResponse, dependencies=[Depends(require_api_key)])
def chat(request: ChatRequest) -> ChatResponse:
    try:
        return answer_question(request.question)
    except RuntimeError as exc:
        # Misconfiguration (missing creds / SDK) — surface a 503 rather than 500.
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/chat/stream", dependencies=[Depends(require_api_key)])
def chat_stream(request: ChatRequest) -> StreamingResponse:
    """Server-sent events variant of /chat that streams per-step progress.

    Emits ``event: step`` frames as the orchestrator runs (table selection,
    SQL writing, execution, answer writing) and a final ``event: final``
    frame containing the same JSON shape as ChatResponse. The frontend
    renders the steps inline so users can see what the bot is doing.
    """

    def event_stream():
        try:
            for event in answer_question_stream(request.question):
                if event.get("type") == "final":
                    response: ChatResponse = event["response"]
                    payload = json.dumps(response.model_dump(mode="json"))
                    yield f"event: final\ndata: {payload}\n\n"
                else:
                    yield f"event: step\ndata: {json.dumps(event)}\n\n"
        except RuntimeError as exc:
            yield f"event: error\ndata: {json.dumps({'detail': str(exc)})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable proxy buffering on Azure/nginx
        },
    )


@app.get("/chat/meta", response_model=ChatMetaResponse)
def chat_meta() -> ChatMetaResponse:
    # Always return a non-empty model string. Falls back to the Settings default
    # ("gpt-5.4-nano") so the frontend's "Powered by …" strip never silently
    # collapses to a placeholder when the env var is missing.
    return ChatMetaResponse(model=_settings.openai_model or "gpt-5.4-nano")


@app.get("/chat/memory/health", response_model=ChatMemoryHealthResponse)
def chat_memory_health() -> ChatMemoryHealthResponse:
    return ChatMemoryHealthResponse(**query_memory_health())


@app.post(
    "/chat/feedback", response_model=ChatFeedbackResponse, dependencies=[Depends(require_api_key)]
)
def chat_feedback(request: ChatFeedbackRequest) -> ChatFeedbackResponse:
    try:
        entry = apply_feedback(
            response_id=request.response_id,
            effective=request.effective,
            question=request.question,
            sql=request.sql,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Unknown response_id.") from exc
    except Exception as exc:
        # Persistence failures (R2 down, bad credentials, etc.) used to bubble
        # up as opaque 500s the frontend swallowed silently. Surface them as a
        # 502 with the underlying message so the UI can show a retry hint.
        logger.exception("apply_feedback failed for response_id=%s", request.response_id)
        raise HTTPException(status_code=502, detail=f"Failed to persist feedback: {exc}") from exc
    return ChatFeedbackResponse(
        response_id=entry.id,
        status=entry.status,
        use_for_prompt=entry.use_for_prompt,
        allow_direct_reuse=entry.allow_direct_reuse,
    )
