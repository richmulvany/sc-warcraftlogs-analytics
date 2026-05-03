# SC Analytics Chatbot Backend

Read-only natural-language interface to the gold layer in Databricks. The
backend only ever queries tables listed in
`backend/app/semantic_registry.json` (generated from
`pipeline/contracts/gold/*.yml`). All generated SQL is parsed with sqlglot
before execution; non-`SELECT` statements and unallowlisted tables are
rejected.

## Layout

```
backend/
  app/
    config.py              # env vars (pydantic-style dataclass)
    db.py                  # databricks-sql-connector wrapper
    schemas.py             # ChatRequest / ChatResponse
    sql_guard.py           # sqlglot-based safety check
    semantic_registry.py   # loads semantic_registry.json
    chatbot.py             # LangChain pipeline
    main.py                # FastAPI: /healthz, /chat
    semantic_registry.json # generated; do not edit
  tests/
    test_sql_guard.py
  requirements.txt
  .env.example
```

## Running locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r backend/requirements.txt
cp backend/.env.example backend/.env  # fill in your values

# Regenerate the semantic registry from gold contracts:
python scripts/build_semantic_registry.py

uvicorn backend.app.main:app --reload
```

POST a question:

```bash
curl -s localhost:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"Who died most often on Mythic Gallywix?"}' | jq
```

## Tests

```bash
pip install pytest sqlglot
pytest backend/tests/
```

`test_sql_guard.py` covers:

- single-`SELECT` and `WITH ... SELECT` allowed
- existing `LIMIT` preserved, otherwise appended
- joins across allowlisted tables
- DROP / DELETE / INSERT / UPDATE / MERGE / ALTER / CREATE / TRUNCATE / OPTIMIZE rejected
- multi-statement payloads rejected
- unallowlisted tables (e.g. silver) rejected

## Updating tables the chatbot can see

Edit `pipeline/contracts/gold/<table>.yml`, set
`x-sc-analytics.chatbotTier: primary` (or `secondary`), then run:

```bash
python scripts/check_contract_drift.py --update
python scripts/build_semantic_registry.py
```

Restart the backend so it picks up the new registry.

## Safety notes

- Tokens come from env only. `.env` is gitignored.
- The Databricks token must belong to a user/SP with `SELECT` only on
  `03_gold.sc_analytics`. The SQL guard is defence in depth, not the only
  safety layer.
- A `LIMIT` is always enforced (default 500, env-configurable).
- The chatbot returns `CANNOT_ANSWER` and a structured response when no
  primary-tier table can answer the question — it never falls back to a
  free-form guess.
