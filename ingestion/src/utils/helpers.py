"""Shared utility functions for ingestion jobs."""

import asyncio
import time
from collections import deque
from typing import Any

import structlog

log = structlog.get_logger(__name__)


class RateLimiter:
    """
    Token-bucket rate limiter for API calls.

    Usage:
        limiter = RateLimiter(requests_per_minute=30)
        async with limiter:
            response = await client.fetch(...)
    """

    def __init__(self, requests_per_minute: int) -> None:
        self._rpm = requests_per_minute
        self._window = 60.0
        self._calls: deque[float] = deque()

    async def __aenter__(self) -> "RateLimiter":
        now = time.monotonic()
        cutoff = now - self._window

        # Prune old calls outside the window
        while self._calls and self._calls[0] < cutoff:
            self._calls.popleft()

        if len(self._calls) >= self._rpm:
            sleep_for = self._window - (now - self._calls[0])
            log.debug("rate_limiter.sleeping", seconds=round(sleep_for, 2))
            await asyncio.sleep(sleep_for)

        self._calls.append(time.monotonic())
        return self

    async def __aexit__(self, *_: Any) -> None:
        pass


def add_ingestion_metadata(record: dict[str, Any], source: str) -> dict[str, Any]:
    """
    Attach standard ingestion metadata to a raw record.

    Adds:
        _source:       Source adapter slug
        _ingested_at:  Unix timestamp (ms) at ingestion time
    """
    import time

    return {
        **record,
        "_source": source,
        "_ingested_at": int(time.time() * 1000),
    }
