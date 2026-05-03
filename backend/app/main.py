"""FastAPI entrypoint for the SC Analytics chatbot."""

from __future__ import annotations

import hmac

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .chatbot import answer_question
from .config import get_settings
from .query_memory import apply_feedback
from .schemas import ChatFeedbackRequest, ChatFeedbackResponse, ChatRequest, ChatResponse
from .semantic_registry import load_registry

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
    return ChatFeedbackResponse(
        response_id=entry.id,
        status=entry.status,
        use_for_prompt=entry.use_for_prompt,
        allow_direct_reuse=entry.allow_direct_reuse,
    )
