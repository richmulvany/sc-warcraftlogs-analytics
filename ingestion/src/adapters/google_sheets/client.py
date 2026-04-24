"""Google Sheets CSV-export adapter.

Wraps the public ``/export?format=csv&gid=`` endpoint that does not require
auth for sheets shared with "anyone with the link".
"""

import httpx
import structlog

from ingestion.src.adapters.base import FetchResult

log = structlog.get_logger(__name__)


class GoogleSheetsAdapter:
    """Minimal HTTP adapter for downloading public Google Sheets as CSV text."""

    def __init__(self, timeout: float = 120.0) -> None:
        self._http: httpx.Client | None = None
        self._timeout = timeout

    def authenticate(self) -> None:
        """No auth required for public sheets — just open the HTTP client."""
        self._http = httpx.Client(follow_redirects=True, timeout=self._timeout)
        log.info("google_sheets.client_ready")

    def close(self) -> None:
        if self._http:
            self._http.close()
            self._http = None

    def fetch_sheet_csv(self, sheet_id: str, gid: str | int = 0) -> FetchResult:
        """
        Download one sheet/gid of a Google Sheet as CSV text.

        Returns a FetchResult with one record containing ``sheet_id``, ``gid``,
        and ``csv_text``. The silver layer parses the CSV with an explicit
        StructType; bronze stores the raw text opaquely.
        """
        if self._http is None:
            raise RuntimeError("Call authenticate() before making requests.")
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export"
        response = self._http.get(url, params={"format": "csv", "gid": str(gid)})
        response.raise_for_status()
        csv_text = response.text
        log.info(
            "google_sheets.fetched",
            sheet_id=sheet_id,
            gid=str(gid),
            bytes=len(csv_text),
        )
        return FetchResult(
            source="google_sheets",
            endpoint="sheet_csv",
            records=[{"sheet_id": sheet_id, "gid": str(gid), "csv_text": csv_text}],
            total_records=1,
            has_more=False,
        )
