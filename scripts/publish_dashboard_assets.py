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
    os.environ.get("DASHBOARD_EXPORT_MAX_TOTAL_EXPORT_BYTES", str(125 * 1024 * 1024))
)
POLL_INTERVAL_SECONDS = float(os.environ.get("EXPORT_POLL_INTERVAL_SECONDS", "2"))
POLL_TIMEOUT_SECONDS = int(os.environ.get("EXPORT_POLL_TIMEOUT_SECONDS", "300"))


def _gold(name: str) -> str:
    return f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{name}"


def _projection(table_name: str, columns: list[str]) -> str:
    return f"SELECT {', '.join(columns)} FROM {table_name}"


EXPORT_TABLES: dict[str, str] = {}

QUERY_EXPORTS: dict[str, tuple[str, str]] = {
    "raid_summary": (
        _gold("gold_raid_summary"),
        _projection(
            _gold("gold_raid_summary"),
            [
                "report_code",
                "report_title",
                "start_time_utc",
                "end_time_utc",
                "zone_id",
                "zone_name",
                "raid_night_date",
                "primary_difficulty",
                "total_pulls",
                "boss_kills",
                "total_wipes",
                "total_fight_seconds",
                "unique_bosses_engaged",
                "unique_bosses_killed",
            ],
        ),
    ),
    "player_attendance": (
        _gold("gold_player_attendance"),
        _projection(
            _gold("gold_player_attendance"),
            [
                "player_name",
                "player_class",
                "total_raids_tracked",
                "raids_present",
                "raids_benched",
                "raids_absent",
                "last_raid_date",
                "first_raid_date",
                "zones_attended",
                "attendance_rate_pct",
            ],
        ),
    ),
    "boss_wipe_analysis": (
        _gold("gold_boss_wipe_analysis"),
        _projection(
            _gold("gold_boss_wipe_analysis"),
            [
                "encounter_id",
                "boss_name",
                "zone_name",
                "difficulty",
                "difficulty_label",
                "total_wipes",
                "best_wipe_pct",
                "avg_wipe_pct",
                "avg_last_phase",
                "max_phase_reached",
                "avg_wipe_duration_seconds",
                "longest_wipe_seconds",
                "first_wipe_date",
                "latest_wipe_date",
                "raid_nights_attempted",
                "avg_wipe_pct_rounded",
            ],
        ),
    ),
    "player_survivability": (
        _gold("gold_player_survivability"),
        _projection(
            _gold("gold_player_survivability"),
            [
                "player_name",
                "player_class",
                "total_deaths",
                "kills_tracked",
                "deaths_per_kill",
                "most_common_killing_blow",
                "most_common_killing_blow_count",
                "top_killing_blows_json",
                "zones_died_in",
                "last_death_timestamp_ms",
            ],
        ),
    ),
    "boss_progression": (
        _gold("gold_boss_progression"),
        _projection(
            _gold("gold_boss_progression"),
            [
                "encounter_id",
                "boss_name",
                "zone_id",
                "zone_name",
                "difficulty",
                "difficulty_label",
                "total_pulls",
                "total_kills",
                "total_wipes",
                "best_kill_seconds",
                "avg_pull_duration_seconds",
                "is_killed",
                "first_kill_date",
                "last_attempt_date",
                "wipe_to_kill_ratio",
            ],
        ),
    ),
    "best_kills": (
        _gold("gold_best_kills"),
        _projection(
            _gold("gold_best_kills"),
            [
                "encounter_id",
                "boss_name",
                "zone_name",
                "difficulty",
                "difficulty_label",
                "best_kill_seconds",
                "avg_kill_seconds",
                "total_kills",
                "first_kill_date",
                "latest_kill_date",
                "best_kill_mm_ss",
            ],
        ),
    ),
    "player_performance_summary": (
        _gold("gold_player_performance_summary"),
        _projection(
            _gold("gold_player_performance_summary"),
            [
                "player_name",
                "player_class",
                "realm",
                "role",
                "primary_spec",
                "kills_tracked",
                "avg_throughput_per_second",
                "best_throughput_per_second",
                "avg_rank_percent",
                "best_rank_percent",
                "avg_item_level",
                "last_seen_date",
            ],
        ),
    ),
    "boss_kill_roster": (
        _gold("gold_boss_kill_roster"),
        (
            f"SELECT report_code, fight_id, boss_name, encounter_id, difficulty, difficulty_label, "
            f"zone_name, raid_night_date, duration_seconds, player_name, player_class, role, spec, "
            f"avg_item_level, potion_use, combat_potion_names, has_food_buff, food_buff_names, "
            f"has_flask_or_phial_buff, flask_or_phial_names, has_weapon_enhancement, "
            f"weapon_enhancement_names, throughput_per_second, rank_percent "
            f"FROM {_gold('gold_boss_kill_roster')}"
        ),
    ),
    "raid_team": (
        _gold("gold_raid_team"),
        _projection(
            _gold("gold_raid_team"),
            [
                "name",
                "player_class",
                "realm",
                "rank",
                "rank_label",
                "rank_category",
                "is_active",
                "total_raids_tracked",
                "raids_present",
                "attendance_rate_pct",
                "last_raid_date",
                "first_raid_date",
                "possible_main",
                "has_possible_alt_in_logs",
            ],
        ),
    ),
    "live_raid_roster": (
        _gold("gold_live_raid_roster"),
        _projection(
            _gold("gold_live_raid_roster"),
            [
                "name",
                "roster_rank",
                "player_class",
                "race",
                "note",
                "source_refreshed_at",
            ],
        ),
    ),
    "player_mplus_summary": (
        _gold("gold_player_mplus_summary"),
        _projection(
            _gold("gold_player_mplus_summary"),
            [
                "player_name",
                "realm_slug",
                "region",
                "profile_url",
                "season",
                "snapshot_at",
                "score_all",
                "score_dps",
                "score_healer",
                "score_tank",
                "world_rank",
                "region_rank",
                "realm_rank",
                "total_runs",
                "timed_runs",
                "untimed_runs",
                "highest_timed_level",
                "highest_untimed_level",
                "most_common_key_level",
                "most_common_key_count",
                "best_run_dungeon",
                "best_run_short_name",
                "best_run_level",
                "best_run_score",
                "best_run_timed",
                "best_run_completed_at",
                "best_run_url",
            ],
        ),
    ),
    "weekly_activity": (
        _gold("gold_weekly_activity"),
        _projection(
            _gold("gold_weekly_activity"),
            [
                "week_start",
                "raid_nights",
                "total_boss_kills",
                "total_wipes",
                "total_pulls",
                "total_raid_seconds",
                "zones_raided",
            ],
        ),
    ),
    "guild_roster": (
        _gold("gold_guild_roster"),
        _projection(
            _gold("gold_guild_roster"),
            [
                "name",
                "player_class",
                "realm",
                "rank",
                "rank_label",
                "rank_category",
                "is_raid_team",
                "is_active",
                "total_raids_tracked",
                "raids_present",
                "attendance_rate_pct",
                "last_raid_date",
                "first_raid_date",
            ],
        ),
    ),
    "boss_mechanics": (
        _gold("gold_boss_mechanics"),
        _projection(
            _gold("gold_boss_mechanics"),
            [
                "encounter_id",
                "boss_name",
                "zone_name",
                "difficulty",
                "difficulty_label",
                "total_wipes",
                "avg_boss_pct",
                "pct_wipes_phase_1",
                "pct_wipes_phase_2",
                "pct_wipes_phase_3_plus",
                "wipes_lt_1min",
                "wipes_1_3min",
                "wipes_3_5min",
                "wipes_5plus_min",
                "last_week_avg_boss_pct",
                "progress_trend",
            ],
        ),
    ),
    "progression_timeline": (
        _gold("gold_progression_timeline"),
        _projection(
            _gold("gold_progression_timeline"),
            [
                "encounter_id",
                "boss_name",
                "zone_name",
                "difficulty",
                "difficulty_label",
                "raid_night_date",
                "cumulative_kills",
            ],
        ),
    ),
    "guild_zone_ranks": (
        _gold("gold_guild_zone_ranks"),
        _projection(
            _gold("gold_guild_zone_ranks"),
            [
                "zone_id",
                "zone_name",
                "world_rank",
                "region_rank",
                "server_rank",
            ],
        ),
    ),
    "player_character_media": (
        _gold("gold_player_character_media"),
        _projection(
            _gold("gold_player_character_media"),
            [
                "player_name",
                "realm_slug",
                "avatar_url",
                "inset_url",
                "main_url",
                "main_raw_url",
            ],
        ),
    ),
    "player_character_equipment": (
        _gold("gold_player_character_equipment"),
        (
            f"SELECT player_name, realm_slug, slot_type, slot_name, item_id, item_name, icon_url, "
            f"quality, item_level, inventory_type, item_subclass, binding, transmog_name, "
            f"enchantments_json, sockets_json, stats_json, spells_json "
            f"FROM {_gold('gold_player_character_equipment')}"
        ),
    ),
    "player_raid_achievements": (
        _gold("gold_player_raid_achievements"),
        _projection(
            _gold("gold_player_raid_achievements"),
            [
                "player_name",
                "realm_slug",
                "achievement_id",
                "achievement_name",
                "completed_timestamp",
            ],
        ),
    ),
    "player_mplus_score_history": (
        _gold("gold_player_mplus_score_history"),
        (
            f"SELECT player_name, realm_slug, region, profile_url, season, snapshot_at, "
            f"snapshot_date, score_all, score_dps, score_healer, score_tank, world_rank, "
            f"region_rank, realm_rank "
            f"FROM {_gold('gold_player_mplus_score_history')}"
        ),
    ),
    "player_mplus_run_history": (
        _gold("gold_player_mplus_run_history"),
        (
            f"SELECT player_name, realm_slug, region, season, dungeon, short_name, mythic_level, "
            f"score, completed_at, completed_date, clear_time_ms, par_time_ms, timed, url "
            f"FROM {_gold('gold_player_mplus_run_history')}"
        ),
    ),
    "player_mplus_weekly_activity": (
        _gold("gold_player_mplus_weekly_activity"),
        _projection(
            _gold("gold_player_mplus_weekly_activity"),
            [
                "player_name",
                "realm_slug",
                "region",
                "season",
                "week_start",
                "total_runs",
                "timed_runs",
                "untimed_runs",
                "highest_key_level",
                "unique_dungeons",
                "most_common_key_level",
            ],
        ),
    ),
    "player_mplus_dungeon_breakdown": (
        _gold("gold_player_mplus_dungeon_breakdown"),
        (
            f"SELECT player_name, realm_slug, region, season, dungeon, best_short_name, "
            f"highest_key_level, highest_timed_level, total_runs, timed_runs, untimed_runs, "
            f"latest_completed_at, best_key_level, best_score, best_timed, best_clear_time_ms, "
            f"best_par_time_ms, best_completed_at, best_run_url "
            f"FROM {_gold('gold_player_mplus_dungeon_breakdown')}"
        ),
    ),
    "boss_progress_history": (
        _gold("gold_boss_progress_history"),
        _projection(
            _gold("gold_boss_progress_history"),
            [
                "encounter_id",
                "boss_name",
                "zone_name",
                "difficulty",
                "difficulty_label",
                "raid_night_date",
                "report_code",
                "report_title",
                "start_time_utc",
                "end_time_utc",
                "pulls_on_night",
                "kills_on_night",
                "wipes_on_night",
                "best_wipe_pct_on_night",
                "avg_wipe_pct_on_night",
                "best_boss_hp_remaining",
                "is_kill_on_night",
                "kill_duration_seconds",
                "longest_pull_seconds",
            ],
        ),
    ),
    "boss_pull_history": (
        _gold("gold_boss_pull_history"),
        _projection(
            _gold("gold_boss_pull_history"),
            [
                "encounter_id",
                "boss_name",
                "zone_name",
                "difficulty",
                "difficulty_label",
                "raid_night_date",
                "report_code",
                "report_title",
                "start_time_utc",
                "end_time_utc",
                "fight_id",
                "is_kill",
                "boss_percentage",
                "boss_hp_remaining",
                "duration_seconds",
                "last_phase",
            ],
        ),
    ),
    "encounter_catalog": (
        _gold("gold_encounter_catalog"),
        _projection(
            _gold("gold_encounter_catalog"),
            [
                "zone_id",
                "zone_name",
                "encounter_id",
                "encounter_name",
                "difficulty_names",
            ],
        ),
    ),
    "player_utility_by_pull": (
        _gold("gold_player_utility_by_pull"),
        (
            f"SELECT DISTINCT report_code, fight_id, boss_name, zone_name, difficulty_label, "
            f"is_kill, player_name, player_class "
            f"FROM {_gold('gold_player_utility_by_pull')}"
        ),
    ),
    "wipe_survival_events": (
        _gold("gold_wipe_survival_events"),
        (
            f"SELECT boss_name, zone_name, difficulty_label, player_name, player_class, "
            f"killing_blow_name, healthstone_before_death, health_potion_before_death "
            f"FROM {_gold('gold_wipe_survival_events')}"
        ),
    ),
    "wipe_cooldown_utilization": (
        _gold("gold_wipe_cooldown_utilization"),
        (
            f"SELECT boss_name, zone_name, difficulty_label, cooldown_category, player_name, "
            f"player_class, ability_id, ability_name, "
            f"SUM(possible_casts) AS possible_casts, "
            f"SUM(actual_casts) AS actual_casts, "
            f"SUM(missed_casts) AS missed_casts "
            f"FROM {_gold('gold_wipe_cooldown_utilization')} "
            f"GROUP BY boss_name, zone_name, difficulty_label, cooldown_category, "
            f"player_name, player_class, ability_id, ability_name"
        ),
    ),
    "player_boss_performance": (
        _gold("gold_player_boss_performance"),
        (
            f"SELECT player_name, encounter_id, boss_name, zone_name, difficulty, difficulty_label, "
            f"kills_on_boss, avg_throughput_per_second, best_throughput_per_second, "
            f"avg_rank_percent, best_rank_percent "
            f"FROM {_gold('gold_player_boss_performance')}"
        ),
    ),
    "player_death_events": (
        _gold("gold_player_death_events"),
        (
            f"SELECT report_code, fight_id, boss_name, zone_name, difficulty_label, raid_night_date, "
            f"is_kill, player_name, player_class, death_timestamp_ms, fight_start_ms, killing_blow_name "
            f"FROM {_gold('gold_player_death_events')}"
        ),
    ),
    "preparation_overrides": (
        f"{OVERRIDES_CATALOG}.{OVERRIDES_SCHEMA}.{OVERRIDES_TABLE}",
        f"SELECT * FROM {OVERRIDES_CATALOG}.{OVERRIDES_SCHEMA}.{OVERRIDES_TABLE}",
    ),
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
    if spark_session is not None:
        return spark_session
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.getActiveSession()


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


def _volume_path_is_locally_writeable(output_path: str) -> bool:
    return output_path.startswith("/Volumes/") and Path("/Volumes").exists()


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
    if _local_writeable_path(output_path) or _volume_path_is_locally_writeable(output_path):
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
        for dataset_name, query_spec in QUERY_EXPORTS.items():
            source_table, query = query_spec
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
