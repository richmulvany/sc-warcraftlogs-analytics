from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.publish_dashboard_assets import (
    MAX_DATASET_BYTES,
    MAX_DATASET_ROWS,
    MAX_TOTAL_EXPORT_BYTES,
    DatasetResult,
    _validate_dataset_size,
    _validate_total_export_size,
    normalise_row_for_json,
    write_manifest,
)


def test_normalise_row_for_json_handles_nested_types() -> None:
    value = {
        "ts": datetime(2026, 4, 25, 2, 15, tzinfo=UTC),
        "day": date(2026, 4, 25),
        "amount": Decimal("12.50"),
        "nested": {"items": [Decimal("1.5"), None]},
    }

    result = normalise_row_for_json(value)

    assert result == {
        "ts": "2026-04-25T02:15:00Z",
        "day": "2026-04-25",
        "amount": 12.5,
        "nested": {"items": [1.5, None]},
    }


def test_write_manifest_writes_expected_payload(tmp_path: Path) -> None:
    manifest = write_manifest(
        tmp_path,
        generated_at="2026-04-25T02:15:00Z",
        snapshot_id="2026-04-25T02-15-00Z",
        datasets=[
            DatasetResult(
                dataset_name="raid_summary",
                source_table="03_gold.sc_analytics.gold_raid_summary",
                row_count=123,
                path="raid_summary.json",
                byte_size=4096,
            )
        ],
    )

    assert manifest["generated_at"] == "2026-04-25T02:15:00Z"
    assert manifest["datasets"]["raid_summary"]["row_count"] == 123
    assert manifest["datasets"]["raid_summary"]["byte_size"] == 4096

    payload = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert payload == manifest


def test_validate_dataset_size_rejects_rows_over_limit() -> None:
    with pytest.raises(RuntimeError, match="exceeded row limit"):
        _validate_dataset_size(
            "raid_summary",
            row_count=MAX_DATASET_ROWS + 1,
            byte_size=1024,
        )


def test_validate_dataset_size_rejects_bytes_over_limit() -> None:
    with pytest.raises(RuntimeError, match="exceeded byte limit"):
        _validate_dataset_size(
            "raid_summary",
            row_count=1,
            byte_size=MAX_DATASET_BYTES + 1,
        )


def test_validate_total_export_size_rejects_total_over_limit() -> None:
    with pytest.raises(RuntimeError, match="Total export exceeded byte limit"):
        _validate_total_export_size(MAX_TOTAL_EXPORT_BYTES + 1)
