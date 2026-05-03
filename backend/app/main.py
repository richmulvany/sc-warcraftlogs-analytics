"""FastAPI entrypoint for the SC Analytics chatbot."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .chatbot import answer_question
from .schemas import ChatRequest, ChatResponse
from .semantic_registry import load_registry

app = FastAPI(title="SC Analytics Chatbot", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    registry = load_registry()
    return {
        "status": "ok",
        "registry_tables": str(len(registry.tables)),
        "primary_tier": str(len(registry.primary())),
    }


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    try:
        return answer_question(request.question)
    except RuntimeError as exc:
        # Misconfiguration (missing creds / SDK) — surface a 503 rather than 500.
        raise HTTPException(status_code=503, detail=str(exc)) from exc
