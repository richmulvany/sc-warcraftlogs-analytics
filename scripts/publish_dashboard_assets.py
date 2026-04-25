"""
Publish dashboard-ready JSON assets to a Unity Catalog volume.

This coexists with ``scripts/export_gold_tables.py`` during migration. The old
CSV export remains available for local fallback, while this script produces the
runtime JSON assets and manifest consumed by the new static publishing flow:

Databricks gold tables -> UC volume JSON assets -> GitHub Actions -> R2
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GOLD_CATALOG = os.environ.get("DASHBOARD_EXPORT_GOLD_CATALOG", "03_gold")
GOLD_SCHEMA = os.environ.get("DASHBOARD_EXPORT_GOLD_SCHEMA", "sc_analytics")
OVERRIDES_CATALOG = os.environ.get("OVERRIDES_DATABRICKS_CATALOG", "00_governance")
OVERRIDES_SCHEMA = os.environ.get("OVERRIDES_DATABRICKS_SCHEMA", "warcraftlogs_admin")
OVERRIDES_TABLE = os.environ.get("OVERRIDES_DATABRICKS_TABLE", "preparation_identity_overrides")
DEFAULT_OUTPUT_PATH = os.environ.get(
    "DASHBOARD_EXPORT_VOLUME_PATH",
    "/Volumes/03_gold/sc_analytics/dashboard_exports",
)
MAX_DATASET_ROWS = int(os.environ.get("DASHBOARD_EXPORT_MAX_DATASET_ROWS", "100000"))
MAX_DATASET_BYTES = int(os.environ.get("DASHBOARD_EXPORT_MAX_DATASET_BYTES", str(25 * 1024 * 1024)))
MAX_TOTAL_EXPORT_BYTES = int(
    os.environ.get("DASHBOARD_EXPORT_MAX_TOTAL_EXPORT_BYTES", str(100 * 1024 * 1024))
)
POLL_INTERVAL_SECONDS = float(os.environ.get("EXPORT_POLL_INTERVAL_SECONDS", "2"))
POLL_TIMEOUT_SECONDS = int(os.environ.get("EXPORT_POLL_TIMEOUT_SECONDS", "300"))


def _gold(name: str) -> str:
    return f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{name}"


EXPORT_TABLES: dict[str, str] = {
    "raid_summary": _gold("gold_raid_summary"),
    "player_attendance": _gold("gold_player_attendance"),
    "boss_wipe_analysis": _gold("gold_boss_wipe_analysis"),
    "player_survivability": _gold("gold_player_survivability"),
    "boss_progression": _gold("gold_boss_progression"),
    "best_kills": _gold("gold_best_kills"),
    "player_performance_summary": _gold("gold_player_performance_summary"),
    "boss_kill_roster": _gold("gold_boss_kill_roster"),
    "raid_team": _gold("gold_raid_team"),
    "live_raid_roster": _gold("gold_live_raid_roster"),
    "player_mplus_summary": _gold("gold_player_mplus_summary"),
    "weekly_activity": _gold("gold_weekly_activity"),
    "guild_roster": _gold("gold_guild_roster"),
    "boss_mechanics": _gold("gold_boss_mechanics"),
    "player_boss_performance": _gold("gold_player_boss_performance"),
    "progression_timeline": _gold("gold_progression_timeline"),
    "guild_zone_ranks": _gold("gold_guild_zone_ranks"),
    "player_character_media": _gold("gold_player_character_media"),
    "player_character_equipment": _gold("gold_player_character_equipment"),
    "player_raid_achievements": _gold("gold_player_raid_achievements"),
    "player_mplus_score_history": _gold("gold_player_mplus_score_history"),
    "player_mplus_run_history": _gold("gold_player_mplus_run_history"),
    "player_mplus_weekly_activity": _gold("gold_player_mplus_weekly_activity"),
    "player_mplus_dungeon_breakdown": _gold("gold_player_mplus_dungeon_breakdown"),
    "player_utility_by_pull": _gold("gold_player_utility_by_pull"),
    "wipe_survival_events": _gold("gold_wipe_survival_events"),
    "wipe_cooldown_utilization": _gold("gold_wipe_cooldown_utilization"),
    "boss_progress_history": _gold("gold_boss_progress_history"),
    "boss_pull_history": _gold("gold_boss_pull_history"),
    "player_death_events": _gold("gold_player_death_events"),
    "encounter_catalog": _gold("gold_encounter_catalog"),
}

QUERY_EXPORTS: dict[str, str] = {
    "preparation_overrides": (
        f"SELECT * FROM {OVERRIDES_CATALOG}.{OVERRIDES_SCHEMA}.{OVERRIDES_TABLE}"
    )
}


@dataclass(frozen=True)
class DatasetResult:
    dataset_name: str
    source_table: str
    row_count: int
    path: str
    byte_size: int


def iso_utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_snapshot_id(when: datetime | None = None) -> str:
    ts = when or datetime.now(UTC)
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")


def normalise_row_for_json(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {
            key: normalise_row_for_json(item)
            for key, item in cast(dict[str, Any], value.asDict(recursive=True)).items()
        }
    if isinstance(value, dict):
        return {str(key): normalise_row_for_json(item) for key, item in value.items()}
    if isinstance(value, (list | tuple)):
        return [normalise_row_for_json(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def write_json_file(path: Path, data: Any) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    path.write_text(payload, encoding="utf-8")
    return len(payload.encode("utf-8"))


def _validate_dataset_size(dataset_name: str, *, row_count: int, byte_size: int) -> None:
    if row_count > MAX_DATASET_ROWS:
        raise RuntimeError(
            f'Dataset "{dataset_name}" exceeded row limit: {row_count} > {MAX_DATASET_ROWS}'
        )
    if byte_size > MAX_DATASET_BYTES:
        raise RuntimeError(
            f'Dataset "{dataset_name}" exceeded byte limit: {byte_size} > {MAX_DATASET_BYTES}'
        )


def _validate_total_export_size(total_byte_size: int) -> None:
    if total_byte_size > MAX_TOTAL_EXPORT_BYTES:
        raise RuntimeError(
            f"Total export exceeded byte limit: {total_byte_size} > {MAX_TOTAL_EXPORT_BYTES}"
        )


def _first_warehouse_id(client: WorkspaceClient) -> str:
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses found. Create or start one in Databricks.")
    warehouse_id = warehouses[0].id
    if not warehouse_id:
        raise RuntimeError("First SQL warehouse had no id.")
    return warehouse_id


def _wait_for_success(client: WorkspaceClient, response: Any) -> Any:
    statement_id = getattr(response, "statement_id", None)
    if not statement_id:
        return response

    deadline = time.time() + POLL_TIMEOUT_SECONDS
    current = response
    while time.time() < deadline:
        state = getattr(getattr(current, "status", None), "state", None)
        if state == sql.StatementState.SUCCEEDED:
            return current
        if state in {
            sql.StatementState.CANCELED,
            sql.StatementState.CLOSED,
            sql.StatementState.FAILED,
        }:
            raise RuntimeError(
                f"Statement {statement_id} ended in state {state}: "
                f"{getattr(getattr(current, 'status', None), 'error', None)}"
            )
        time.sleep(POLL_INTERVAL_SECONDS)
        current = client.statement_execution.get_statement(statement_id)

    raise TimeoutError(
        f"Timed out waiting for statement {statement_id} to finish after {POLL_TIMEOUT_SECONDS}s"
    )


def _statement_rows_to_dicts(response: Any) -> list[dict[str, Any]]:
    result = getattr(response, "result", None)
    data_array = getattr(result, "data_array", None) or []
    manifest = getattr(response, "manifest", None)
    schema = getattr(manifest, "schema", None)
    columns = getattr(schema, "columns", None) or []
    column_names = [str(getattr(column, "name", "")) for column in columns]
    if not column_names:
        raise RuntimeError("Statement response did not include column metadata.")
    return [
        {
            column_names[index]: normalise_row_for_json(value)
            for index, value in enumerate(row)
            if index < len(column_names)
        }
        for row in data_array
    ]


def _get_spark_session() -> Any | None:
    spark_session = globals().get("spark")
    return spark_session


def _parse_volume_path(path: str) -> tuple[str, str, str] | None:
    parts = Path(path).parts
    if len(parts) < 5 or parts[1] != "Volumes":
        return None
    return parts[2], parts[3], parts[4]


def _ensure_output_volume(client: WorkspaceClient | None, output_path: str) -> None:
    volume_parts = _parse_volume_path(output_path)
    if volume_parts is None:
        return
    catalog, schema, volume = volume_parts
    statement = f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`"
    spark_session = _get_spark_session()
    if spark_session is not None:
        spark_session.sql(statement)
        return
    if client is None:
        return
    warehouse_id = _first_warehouse_id(client)
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    _wait_for_success(client, response)


def _local_writeable_path(output_path: str) -> bool:
    return not output_path.startswith("/Volumes/")


def _databricks_cp(local_path: Path, remote_path: str) -> None:
    target = f"dbfs:{remote_path}" if remote_path.startswith("/Volumes/") else remote_path
    subprocess.run(
        ["databricks", "fs", "cp", str(local_path), target, "--overwrite"],
        check=True,
    )


def _databricks_cp_dir(local_path: Path, remote_path: str) -> None:
    target = f"dbfs:{remote_path}" if remote_path.startswith("/Volumes/") else remote_path
    subprocess.run(
        ["databricks", "fs", "cp", str(local_path), target, "-r"],
        check=True,
    )


def _databricks_rm(remote_path: str) -> None:
    target = f"dbfs:{remote_path}" if remote_path.startswith("/Volumes/") else remote_path
    subprocess.run(
        ["databricks", "fs", "rm", target, "-r"],
        check=False,
    )


def _query_rows(
    spark_session: Any | None,
    client: WorkspaceClient | None,
    warehouse_id: str | None,
    *,
    table_name: str | None = None,
    sql_text: str | None = None,
) -> list[dict[str, Any]]:
    if spark_session is not None:
        dataframe = spark_session.table(table_name) if table_name else spark_session.sql(sql_text)
        # Small static dashboard export only. For very large datasets this should
        # be rewritten to stream/partition output rather than collecting to driver.
        return [normalise_row_for_json(row) for row in dataframe.collect()]

    if client is None or warehouse_id is None:
        raise RuntimeError("No Spark session or Databricks SQL client available for export.")

    statement = sql_text or f"SELECT * FROM {table_name}"
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    response = _wait_for_success(client, response)
    return _statement_rows_to_dicts(response)


def export_dataset(
    spark_session: Any | None,
    client: WorkspaceClient | None,
    warehouse_id: str | None,
    dataset_name: str,
    table_name: str,
    output_dir: Path,
) -> DatasetResult:
    rows = _query_rows(
        spark_session,
        client,
        warehouse_id,
        table_name=table_name,
    )
    output_file = output_dir / f"{dataset_name}.json"
    byte_size = write_json_file(output_file, rows)
    _validate_dataset_size(dataset_name, row_count=len(rows), byte_size=byte_size)
    logger.info(
        "Exported %s -> %s (%d rows, %d bytes)", table_name, output_file, len(rows), byte_size
    )
    return DatasetResult(
        dataset_name=dataset_name,
        source_table=table_name,
        row_count=len(rows),
        path=output_file.name,
        byte_size=byte_size,
    )


def export_query_dataset(
    spark_session: Any | None,
    client: WorkspaceClient | None,
    warehouse_id: str | None,
    dataset_name: str,
    source_table: str,
    sql_text: str,
    output_dir: Path,
) -> DatasetResult:
    rows = _query_rows(
        spark_session,
        client,
        warehouse_id,
        sql_text=sql_text,
    )
    output_file = output_dir / f"{dataset_name}.json"
    byte_size = write_json_file(output_file, rows)
    _validate_dataset_size(dataset_name, row_count=len(rows), byte_size=byte_size)
    logger.info(
        "Exported %s -> %s (%d rows, %d bytes)", source_table, output_file, len(rows), byte_size
    )
    return DatasetResult(
        dataset_name=dataset_name,
        source_table=source_table,
        row_count=len(rows),
        path=output_file.name,
        byte_size=byte_size,
    )


def write_manifest(
    manifest_dir: Path,
    *,
    generated_at: str,
    snapshot_id: str,
    datasets: list[DatasetResult],
) -> dict[str, Any]:
    payload = {
        "generated_at": generated_at,
        "snapshot_id": snapshot_id,
        "format_version": 1,
        "datasets": {
            dataset.dataset_name: {
                "path": dataset.path,
                "row_count": dataset.row_count,
                "byte_size": dataset.byte_size,
                "source_table": dataset.source_table,
            }
            for dataset in datasets
        },
    }
    write_json_file(manifest_dir / "manifest.json", payload)
    return payload


def _publish_local_tree(staging_root: Path, output_path: str) -> None:
    target_root = Path(output_path)
    target_root.mkdir(parents=True, exist_ok=True)
    latest_dir = target_root / "latest"
    snapshots_dir = target_root / "snapshots"
    shutil.rmtree(latest_dir, ignore_errors=True)
    shutil.copytree(staging_root / "latest", latest_dir)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    snapshot_id = next((staging_root / "snapshots").iterdir()).name
    shutil.copytree(
        staging_root / "snapshots" / snapshot_id,
        snapshots_dir / snapshot_id,
        dirs_exist_ok=True,
    )


def _publish_remote_tree(staging_root: Path, output_path: str) -> None:
    latest_remote = f"{output_path}/latest"
    snapshot_id = next((staging_root / "snapshots").iterdir()).name
    snapshot_remote = f"{output_path}/snapshots/{snapshot_id}"
    _databricks_rm(latest_remote)
    _databricks_rm(snapshot_remote)
    _databricks_cp_dir(staging_root / "latest", latest_remote)
    _databricks_cp_dir(staging_root / "snapshots" / snapshot_id, snapshot_remote)


def _publish_tree(staging_root: Path, output_path: str) -> None:
    if _local_writeable_path(output_path):
        _publish_local_tree(staging_root, output_path)
    else:
        _publish_remote_tree(staging_root, output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH)
    args, unknown = parser.parse_known_args()
    if unknown:
        logger.info("Ignoring unknown CLI args: %s", " ".join(unknown))
    return args


def main() -> None:
    args = parse_args()
    output_path = args.output_path

    spark_session = _get_spark_session()
    client = None if spark_session is not None else WorkspaceClient()
    warehouse_id = None if client is None else _first_warehouse_id(client)
    _ensure_output_volume(client, output_path)

    generated_at = iso_utc_now()
    snapshot_id = make_snapshot_id()

    with tempfile.TemporaryDirectory(prefix="dashboard_assets_") as tmp_dir:
        staging_root = Path(tmp_dir)
        latest_dir = staging_root / "latest"
        snapshot_dir = staging_root / "snapshots" / snapshot_id
        latest_dir.mkdir(parents=True, exist_ok=True)
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        dataset_results: list[DatasetResult] = []
        for dataset_name, table_name in EXPORT_TABLES.items():
            result = export_dataset(
                spark_session,
                client,
                warehouse_id,
                dataset_name,
                table_name,
                snapshot_dir,
            )
            dataset_results.append(result)
        for dataset_name, query in QUERY_EXPORTS.items():
            source_table = f"{OVERRIDES_CATALOG}.{OVERRIDES_SCHEMA}.{OVERRIDES_TABLE}"
            result = export_query_dataset(
                spark_session,
                client,
                warehouse_id,
                dataset_name,
                source_table,
                query,
                snapshot_dir,
            )
            dataset_results.append(result)

        manifest = write_manifest(
            snapshot_dir,
            generated_at=generated_at,
            snapshot_id=snapshot_id,
            datasets=dataset_results,
        )
        manifest_byte_size = (snapshot_dir / "manifest.json").stat().st_size
        total_export_bytes = (
            sum(dataset.byte_size for dataset in dataset_results) + manifest_byte_size
        )
        _validate_total_export_size(total_export_bytes)

        shutil.copytree(snapshot_dir, latest_dir, dirs_exist_ok=True)
        _publish_tree(staging_root, output_path)

        logger.info(
            "Published dashboard assets to %s (snapshot=%s datasets=%d total_bytes=%d)",
            output_path,
            snapshot_id,
            len(manifest["datasets"]),
            total_export_bytes,
        )


if __name__ == "__main__":
    main()
