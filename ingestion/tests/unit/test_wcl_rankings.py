"""Tests for WarcraftLogs rankings completeness helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ingestion.src.utils.wcl_rankings import rankings_completeness


def _write_rankings_file(
    tmp_path: Path,
    *,
    dps_payload: dict[str, Any],
    hps_payload: dict[str, Any] | None,
) -> str:
    path = tmp_path / "rankings.jsonl"
    record = {"rankings_json": json.dumps(dps_payload)}
    if hps_payload is not None:
        record["rankings_hps_json"] = json.dumps(hps_payload)
    path.write_text(json.dumps(record) + "\n")
    return str(path)


def test_rankings_completeness_all_null_payload(tmp_path: Path) -> None:
    dps_payload = {
        "data": [
            {
                "fightID": 101,
                "roles": {
                    "tanks": {"characters": [{"rankPercent": None}]},
                    "dps": {"characters": [{"rankPercent": None}, {"rankPercent": None}]},
                },
            }
        ]
    }
    hps_payload = {
        "data": [{"fightID": 101, "roles": {"healers": {"characters": [{"rankPercent": None}]}}}]
    }

    assert rankings_completeness(
        _write_rankings_file(tmp_path, dps_payload=dps_payload, hps_payload=hps_payload)
    ) == (1, 1, 4, 4)


def test_rankings_completeness_partial_null_payload(tmp_path: Path) -> None:
    dps_payload = {
        "data": [
            {
                "fightID": 102,
                "roles": {
                    "tanks": {"characters": [{"rankPercent": 88.0}]},
                    "dps": {
                        "characters": [
                            {"rankPercent": 97.0},
                            {"rankPercent": 91.0},
                            {"rankPercent": None},
                        ]
                    },
                },
            }
        ]
    }
    hps_payload = {
        "data": [{"fightID": 102, "roles": {"healers": {"characters": [{"rankPercent": 83.0}]}}}]
    }

    assert rankings_completeness(
        _write_rankings_file(tmp_path, dps_payload=dps_payload, hps_payload=hps_payload)
    ) == (1, 1, 1, 5)


def test_rankings_completeness_legacy_no_hps_payload(tmp_path: Path) -> None:
    dps_payload = {
        "data": [
            {"fightID": 201, "roles": {"tanks": {"characters": []}, "dps": {"characters": []}}},
            {"fightID": 202, "roles": {"tanks": {"characters": []}, "dps": {"characters": []}}},
        ]
    }

    assert rankings_completeness(
        _write_rankings_file(tmp_path, dps_payload=dps_payload, hps_payload=None)
    ) == (2, 2, 0, 0)


def test_rankings_completeness_fully_populated_payload(tmp_path: Path) -> None:
    dps_payload = {
        "data": [
            {
                "fightID": 301,
                "roles": {
                    "tanks": {"characters": [{"rankPercent": 72.0}]},
                    "dps": {"characters": [{"rankPercent": 98.0}, {"rankPercent": 87.0}]},
                },
            }
        ]
    }
    hps_payload = {
        "data": [{"fightID": 301, "roles": {"healers": {"characters": [{"rankPercent": 93.0}]}}}]
    }

    assert rankings_completeness(
        _write_rankings_file(tmp_path, dps_payload=dps_payload, hps_payload=hps_payload)
    ) == (0, 1, 0, 4)
