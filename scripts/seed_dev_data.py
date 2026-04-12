"""
Generate synthetic sample data for local development and testing.
Writes JSON files to data/samples/ so tests and notebooks can run
without hitting the real API.

Usage:
    python scripts/seed_dev_data.py
"""

import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path("data/samples")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

rng = random.Random(42)  # Deterministic seed for reproducibility


def random_timestamp(days_ago_max: int = 90) -> str:
    delta = timedelta(days=rng.randint(0, days_ago_max), seconds=rng.randint(0, 86400))
    return (datetime.now(UTC) - delta).isoformat()


def generate_entities(n: int = 50) -> list[dict]:
    categories = ["alpha", "beta", "gamma", "delta"]
    return [
        {
            "id": i + 1,
            "name": f"Entity {i + 1}",
            "category": rng.choice(categories),
            "score": round(rng.uniform(0, 100), 2),
            "active": rng.choice([True, False]),
            "created_at": random_timestamp(),
        }
        for i in range(n)
    ]


def generate_events(n: int = 200) -> list[dict]:
    event_types = ["created", "updated", "deleted", "viewed"]
    return [
        {
            "id": i + 1,
            "entity_id": rng.randint(1, 50),
            "event_type": rng.choice(event_types),
            "occurred_at": random_timestamp(),
            "metadata": {"source": "api", "version": "1.0"},
        }
        for i in range(n)
    ]


def wrap_paginated(data: list[dict], page_size: int = 100) -> dict:
    return {
        "data": data[:page_size],
        "total": len(data),
        "page": 1,
        "has_more": len(data) > page_size,
    }


samples = {
    "entities.json": wrap_paginated(generate_entities()),
    "events.json": wrap_paginated(generate_events()),
    "metadata.json": {
        "data": [{"key": "schema_version", "value": "1.0"}],
        "total": 1,
        "page": 1,
        "has_more": False,
    },
}

for filename, payload in samples.items():
    path = OUTPUT_DIR / filename
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[OK] Wrote {path}  ({payload.get('total', '?')} records)")

print(f"\nSample data written to {OUTPUT_DIR}/")
