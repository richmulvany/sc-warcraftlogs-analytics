# Adapter Guide

The ingestion layer uses an **adapter pattern** so the rest of the pipeline
(Bronze, Silver, Gold, frontend) is decoupled from any specific data source.
Swapping the data source means implementing one class in one directory.

## Adapter Interface

All adapters implement `BaseAdapter` from `ingestion/src/adapters/base.py`:

```python
class BaseAdapter(ABC):
    def authenticate(self) -> None: ...
    def fetch(self, endpoint: str, params: dict | None = None) -> FetchResult: ...
    def validate(self, result: FetchResult) -> bool: ...
```

## Creating a New Adapter

### 1. Copy the example adapter

```bash
cp -r ingestion/src/adapters/example_adapter ingestion/src/adapters/my_source
```

### 2. Implement the three methods

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

### 3. Update config

Edit `ingestion/config/source_config.yml` to match your API's endpoints and rate limits.

### 4. Update the ingestion job

Edit `ingestion/jobs/ingest_primary.py` to import and use your adapter:

```python
from ingestion.src.adapters.my_source.adapter import MySourceAdapter
```

### 5. Write tests

Add tests to `ingestion/tests/unit/test_my_source_adapter.py`.
Use `responses` to mock HTTP calls — see `test_example_adapter.py` for a template.

## WarcraftLogs Adapter (Reference Implementation)

For a complete real-world example using the WarcraftLogs GraphQL API v2,
see the project this template was derived from:
[github.com/richmulvany/wcl-guild-analytics](https://github.com/richmulvany/wcl-guild-analytics)
