# Adapter Guide

The ingestion layer uses an **adapter pattern** so the rest of the pipeline
(Bronze, Silver, Gold, frontend) is decoupled from any specific data source.
Adding a data source means implementing one class in one directory and wiring
the source into the ingestion job that owns the required ordering.

## Adapter Interface

All adapters implement `BaseAdapter` from `ingestion/src/adapters/base.py`:

```python
class BaseAdapter(ABC):
    def authenticate(self) -> None: ...
    def fetch(self, endpoint: str, params: dict | None = None) -> FetchResult: ...
    def validate(self, result: FetchResult) -> bool: ...
```

## Creating a New Adapter

### 1. Implement the three methods

```python
# ingestion/src/adapters/my_source/adapter.py

class MySourceAdapter(BaseAdapter):

    def authenticate(self) -> None:
        # Create your API client, store on self
        self._client = MyApiClient(api_key=self.config.api_key)

    def fetch(self, endpoint: str, params=None) -> FetchResult:
        # Call the API, return a FetchResult
        data = self._client.get(endpoint, params=params)
        return FetchResult(
            source="my_source",
            endpoint=endpoint,
            records=data["items"],
            total_records=data["total"],
            has_more=data["has_more"],
        )

    def validate(self, result: FetchResult) -> bool:
        return len(result.records) > 0
```

### 2. Update config

Edit `ingestion/config/source_config.yml` to match your API's endpoints and rate limits.

### 3. Update the ingestion job

Edit `ingestion/jobs/ingest_primary.py` to import and use your adapter:

```python
from ingestion.src.adapters.my_source.adapter import MySourceAdapter
```

### 4. Write tests

Add tests to `ingestion/tests/unit/test_my_source_adapter.py`.
Mock HTTP calls at the client boundary and cover retry/error handling for the
source-specific API.

## Current Project Adapters

- `ingestion/src/adapters/wcl/client.py` — WarcraftLogs GraphQL OAuth2 adapter with query-specific fetch helpers and archived-report handling.
- `ingestion/src/adapters/blizzard/client.py` — Blizzard OAuth2 REST adapter for guild roster ingestion.
- `ingestion/src/adapters/raiderio/client.py` — Raider.IO public REST adapter for current-season Mythic+ character profiles. It intentionally uses a typed manual retry helper instead of an untyped decorator so `mypy` pre-commit checks pass.
