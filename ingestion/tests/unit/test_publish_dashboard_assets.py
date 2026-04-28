from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

import scripts.publish_dashboard_assets as publish_dashboard_assets
from scripts.publish_dashboard_assets import (
    MAX_DATASET_BYTES,
    MAX_DATASET_ROWS,
    MAX_TOTAL_EXPORT_BYTES,
    DatasetResult,
    _publish_local_tree,
    _validate_dataset_size,
    _validate_manifest_datasets,
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


def test_validate_manifest_datasets_rejects_missing_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        publish_dashboard_assets,
        "EXPORT_TABLES",
        {"raid_summary": "03_gold.sc_analytics.gold_raid_summary"},
    )
    monkeypatch.setattr(
        publish_dashboard_assets,
        "QUERY_EXPORTS",
        {"boss_progression": ("03_gold.sc_analytics.gold_boss_progression", "SELECT 1")},
    )

    with pytest.raises(RuntimeError, match="missing=boss_progression"):
        _validate_manifest_datasets(
            {
                "format_version": 1,
                "generated_at": "2026-04-25T02:15:00Z",
                "snapshot_id": "2026-04-25T02-15-00Z",
                "datasets": {
                    "raid_summary": {
                        "path": "raid_summary.json",
                        "row_count": 1,
                        "byte_size": 2,
                        "source_table": "03_gold.sc_analytics.gold_raid_summary",
                    }
                },
            }
        )


def test_publish_local_tree_failure_leaves_latest_intact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        publish_dashboard_assets,
        "EXPORT_TABLES",
        {"raid_summary": "03_gold.sc_analytics.gold_raid_summary"},
    )
    monkeypatch.setattr(publish_dashboard_assets, "QUERY_EXPORTS", {})

    staging_root = tmp_path / "staging"
    latest_source = staging_root / "latest"
    snapshot_source = staging_root / "snapshots" / "2026-04-25T02-15-00Z"
    latest_source.mkdir(parents=True)
    snapshot_source.mkdir(parents=True)

    dataset = DatasetResult(
        dataset_name="raid_summary",
        source_table="03_gold.sc_analytics.gold_raid_summary",
        row_count=2,
        path="raid_summary.json",
        byte_size=16,
    )
    (latest_source / "raid_summary.json").write_text('{"version":"new"}', encoding="utf-8")
    (snapshot_source / "raid_summary.json").write_text('{"version":"new"}', encoding="utf-8")
    write_manifest(
        latest_source,
        generated_at="2026-04-25T02:15:00Z",
        snapshot_id="2026-04-25T02-15-00Z",
        datasets=[dataset],
    )
    write_manifest(
        snapshot_source,
        generated_at="2026-04-25T02:15:00Z",
        snapshot_id="2026-04-25T02-15-00Z",
        datasets=[dataset],
    )

    output_root = tmp_path / "output"
    latest_target = output_root / "latest"
    latest_target.mkdir(parents=True)
    (latest_target / "raid_summary.json").write_text('{"version":"old"}', encoding="utf-8")
    write_manifest(
        latest_target,
        generated_at="2026-04-24T02:15:00Z",
        snapshot_id="2026-04-24T02-15-00Z",
        datasets=[
            DatasetResult(
                dataset_name="raid_summary",
                source_table="03_gold.sc_analytics.gold_raid_summary",
                row_count=1,
                path="raid_summary.json",
                byte_size=16,
            )
        ],
    )

    original_copy_tree_dir = publish_dashboard_assets._copy_tree_dir

    def flaky_copy_tree_dir(source: Path, destination: Path) -> None:
        if destination.name != "latest.new":
            original_copy_tree_dir(source, destination)
            return

        destination.mkdir(parents=True, exist_ok=False)
        entries = sorted(source.iterdir())
        halfway = max(1, len(entries) // 2)
        for index, entry in enumerate(entries):
            target = destination / entry.name
            if entry.is_dir():
                shutil.copytree(entry, target)
            else:
                shutil.copy2(entry, target)
            if index + 1 >= halfway:
                raise RuntimeError("simulated mid-publish failure")

    monkeypatch.setattr(publish_dashboard_assets, "_copy_tree_dir", flaky_copy_tree_dir)

    with pytest.raises(RuntimeError, match="simulated mid-publish failure"):
        _publish_local_tree(staging_root, str(output_root))

    assert not (output_root / "latest.new").exists()
    assert (output_root / "latest" / "raid_summary.json").read_text(
        encoding="utf-8"
    ) == '{"version":"old"}'
    old_manifest = json.loads(
        (output_root / "latest" / "manifest.json").read_text(encoding="utf-8")
    )
    assert old_manifest["snapshot_id"] == "2026-04-24T02-15-00Z"
