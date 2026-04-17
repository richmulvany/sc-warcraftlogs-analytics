"""
Export frontend gold tables from Databricks SQL to static CSV files.

The React app reads CSVs from `frontend/public/data`, so this script exports the
same files the frontend consumes rather than the old JSON sample format.

Usage:
    python scripts/export_gold_tables.py
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from dotenv import load_dotenv

from ingestion.src.adapters.wcl.client import WarcraftLogsAdapter, WarcraftLogsConfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

CATALOG = os.environ.get("DATABRICKS_CATALOG", "04_sdp")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "warcraftlogs")
_output_dir = Path(os.environ.get("EXPORT_OUTPUT_DIR", "frontend/public/data"))
OUTPUT_DIR = _output_dir if _output_dir.is_absolute() else REPO_ROOT / _output_dir
FRONTEND_PUBLIC_DATA_DIR = REPO_ROOT / "frontend/public/data"
POLL_INTERVAL_SECONDS = float(os.environ.get("EXPORT_POLL_INTERVAL_SECONDS", "2"))
POLL_TIMEOUT_SECONDS = int(os.environ.get("EXPORT_POLL_TIMEOUT_SECONDS", "300"))
LIVE_ROSTER_SHEET_ID = (
    os.environ.get("LIVE_ROSTER_SHEET_ID") or "1fHtbnNTHrLVFqq5e7L7usN4qI4LGd1JKRKMdVuHnmRg"
)
LIVE_ROSTER_SHEET_GID = os.environ.get("LIVE_ROSTER_SHEET_GID") or "0"
LIVE_ROSTER_FILENAME = os.environ.get("LIVE_ROSTER_FILENAME") or "live_raid_roster.csv"
WCL_CLIENT_ID = os.environ.get("WCL_CLIENT_ID") or os.environ.get("WARCRAFTLOGS_CLIENT_ID") or ""
WCL_CLIENT_SECRET = (
    os.environ.get("WCL_CLIENT_SECRET") or os.environ.get("WARCRAFTLOGS_CLIENT_SECRET") or ""
)
WCL_GUILD_NAME = (
    os.environ.get("WCL_GUILD_NAME") or os.environ.get("GUILD_NAME") or "Student Council"
)
WCL_GUILD_SERVER_SLUG = (
    os.environ.get("WCL_GUILD_SERVER_SLUG")
    or os.environ.get("GUILD_SERVER_SLUG")
    or "twisting-nether"
)
WCL_GUILD_SERVER_REGION = (
    os.environ.get("WCL_GUILD_SERVER_REGION") or os.environ.get("GUILD_SERVER_REGION") or "EU"
)
GUILD_ZONE_RANKS_FILENAME = os.environ.get("GUILD_ZONE_RANKS_FILENAME") or "guild_zone_ranks.csv"
BLIZZARD_CLIENT_ID = (
    os.environ.get("BLIZZARD_CLIENT_ID_PROFILE")
    or os.environ.get("BLIZZARD_PROFILE_CLIENT_ID")
    or os.environ.get("BLIZZARD_CLIENT_ID")
    or os.environ.get("BLIZZARD_CLIENT_ID_ROSTER")
    or os.environ.get("BLIZZARD_CLIENTID")
    or os.environ.get("BLIZZARD_API_CLIENT_ID")
    or ""
)
BLIZZARD_CLIENT_SECRET = (
    os.environ.get("BLIZZARD_CLIENT_SECRET_PROFILE")
    or os.environ.get("BLIZZARD_PROFILE_CLIENT_SECRET")
    or os.environ.get("BLIZZARD_CLIENT_SECRET")
    or os.environ.get("BLIZZARD_CLIENT_SECRET_ROSTER")
    or os.environ.get("BLIZZARD_CLIENTSECRET")
    or os.environ.get("BLIZZARD_API_CLIENT_SECRET")
    or ""
)
BLIZZARD_REGION = (
    os.environ.get("BLIZZARD_REGION")
    or os.environ.get("GUILD_SERVER_REGION")
    or WCL_GUILD_SERVER_REGION
    or "EU"
).lower()
BLIZZARD_LOCALE = os.environ.get("BLIZZARD_LOCALE") or "en_GB"
BLIZZARD_PROFILE_EXPORT_CAP = int(os.environ.get("BLIZZARD_PROFILE_EXPORT_CAP", "80"))
PLAYER_CHARACTER_MEDIA_FILENAME = (
    os.environ.get("PLAYER_CHARACTER_MEDIA_FILENAME") or "player_character_media.csv"
)
PLAYER_CHARACTER_EQUIPMENT_FILENAME = (
    os.environ.get("PLAYER_CHARACTER_EQUIPMENT_FILENAME") or "player_character_equipment.csv"
)
PLAYER_RAID_ACHIEVEMENTS_FILENAME = (
    os.environ.get("PLAYER_RAID_ACHIEVEMENTS_FILENAME") or "player_raid_achievements.csv"
)

FRONTEND_TABLES: dict[str, str] = {
    "gold_raid_summary.csv": "gold_raid_summary",
    "gold_player_performance_summary.csv": "gold_player_performance_summary",
    "gold_boss_progression.csv": "gold_boss_progression",
    "gold_boss_kill_roster.csv": "gold_boss_kill_roster",
    "gold_player_attendance.csv": "gold_player_attendance",
    "gold_guild_roster.csv": "gold_guild_roster",
    "gold_weekly_activity.csv": "gold_weekly_activity",
    "gold_boss_wipe_analysis.csv": "gold_boss_wipe_analysis",
    "gold_player_survivability.csv": "gold_player_survivability",
    "gold_progression_timeline.csv": "gold_progression_timeline",
    "gold_raid_team.csv": "gold_raid_team",
    "gold_best_kills.csv": "gold_best_kills",
    "gold_boss_mechanics.csv": "gold_boss_mechanics",
    "gold_player_boss_performance.csv": "gold_player_boss_performance",
    "gold_boss_progress_history.csv": "gold_boss_progress_history",
    "gold_boss_pull_history.csv": "gold_boss_pull_history",
}
EXCLUDED_ZONES = {"Blackrock Depths"}
TABLE_EXPORT_STATEMENTS: dict[str, str] = {
    "gold_weekly_activity": f"""
        SELECT
          DATE_TRUNC('week', CAST(start_time_utc AS TIMESTAMP)) AS week_start,
          COUNT(*) AS raid_nights,
          SUM(COALESCE(boss_kills, 0)) AS total_boss_kills,
          SUM(COALESCE(total_wipes, 0)) AS total_wipes,
          SUM(COALESCE(total_pulls, 0)) AS total_pulls,
          SUM(COALESCE(total_fight_seconds, 0)) AS total_raid_seconds,
          ARRAY_SORT(COLLECT_SET(zone_name)) AS zones_raided
        FROM {CATALOG}.{SCHEMA}.gold_raid_summary
        WHERE zone_name IS NOT NULL
          AND zone_name NOT IN ({", ".join(repr(zone) for zone in sorted(EXCLUDED_ZONES))})
        GROUP BY DATE_TRUNC('week', CAST(start_time_utc AS TIMESTAMP))
        ORDER BY week_start
    """.strip()
}

LIVE_ROSTER_COLUMNS = {
    "name": 0,
    "roster_rank": 3,
    "player_class": 5,
    "race": 119,
    "note": 120,
}

MEDIA_FIELDNAMES = [
    "player_name",
    "realm_slug",
    "avatar_url",
    "inset_url",
    "main_url",
    "main_raw_url",
]
EQUIPMENT_FIELDNAMES = [
    "player_name",
    "realm_slug",
    "slot_type",
    "slot_name",
    "item_id",
    "item_name",
    "icon_url",
    "quality",
    "item_level",
    "inventory_type",
    "item_subclass",
    "binding",
    "transmog_name",
    "enchantments_json",
    "sockets_json",
    "stats_json",
    "spells_json",
    "raw_details_json",
]
RAID_ACHIEVEMENT_FIELDNAMES = [
    "player_name",
    "realm_slug",
    "achievement_id",
    "achievement_name",
    "completed_timestamp",
]

REALM_SLUG_OVERRIDES = {
    "twistingnether": "twisting-nether",
    "twisting-nether": "twisting-nether",
    "defiasbrotherhood": "defias-brotherhood",
    "defias-brotherhood": "defias-brotherhood",
    "argentdawn": "argent-dawn",
    "argent-dawn": "argent-dawn",
}


def _mirror_to_frontend_data(path: Path) -> None:
    if path.parent.resolve() == FRONTEND_PUBLIC_DATA_DIR.resolve():
        return

    mirror_path = FRONTEND_PUBLIC_DATA_DIR / path.name
    mirror_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, mirror_path)
    logger.info("Mirrored %s -> %s", path.name, mirror_path.relative_to(REPO_ROOT))


def _filter_exported_csv(output_path: Path) -> int | None:
    with output_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        if not fieldnames:
            return None

        rows = list(reader)

    original_count = len(rows)
    filtered_rows = rows
    changed = False

    if "zone_name" in fieldnames:
        filtered_rows = [
            row
            for row in filtered_rows
            if (row.get("zone_name") or "").strip() not in EXCLUDED_ZONES
        ]
        changed = changed or len(filtered_rows) != original_count

    if not changed:
        return original_count

    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_rows)

    logger.info(
        "Filtered %s excluded-zone rows from %s",
        original_count - len(filtered_rows),
        output_path.relative_to(REPO_ROOT),
    )
    return len(filtered_rows)


def _first_warehouse_id(client: WorkspaceClient) -> str:
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses found. Create or start one in Databricks.")
    warehouse_id = warehouses[0].id
    if not warehouse_id:
        raise RuntimeError("First SQL warehouse had no id.")
    return warehouse_id


def _wait_for_success(
    client: WorkspaceClient,
    response: Any,
) -> Any:
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
                f"Statement {statement_id} ended in state {state}: {getattr(getattr(current, 'status', None), 'error', None)}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)
        current = client.statement_execution.get_statement(statement_id)

    raise TimeoutError(
        f"Timed out waiting for statement {statement_id} to finish after {POLL_TIMEOUT_SECONDS}s"
    )


def _iter_external_links(result_chunk: Any) -> list[str]:
    links = getattr(result_chunk, "external_links", None) or []
    urls: list[str] = []
    for link in links:
        url = getattr(link, "external_link", None) or getattr(link, "url", None)
        if url:
            urls.append(url)
    return urls


def _download_chunk(url: str) -> bytes:
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        response = client.get(url, headers={})
        response.raise_for_status()
        return response.content


def _download_text(url: str) -> str:
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def export_live_raid_roster(output_dir: Path) -> int:
    if not LIVE_ROSTER_SHEET_ID:
        logger.info("Skipping live raid roster export: no sheet id configured.")
        return 0

    sheet_url = (
        f"https://docs.google.com/spreadsheets/d/{LIVE_ROSTER_SHEET_ID}/export"
        f"?format=csv&gid={LIVE_ROSTER_SHEET_GID}"
    )
    logger.info("Exporting live raid roster -> %s", LIVE_ROSTER_FILENAME)
    payload = _download_text(sheet_url)
    rows = list(csv.reader(io.StringIO(payload)))
    if len(rows) < 3:
        raise RuntimeError("Live roster sheet returned too few rows to parse.")

    refreshed_at = rows[1][1].strip() if len(rows[1]) > 1 else ""
    seen_names: set[str] = set()
    normalised_rows: list[dict[str, str]] = []

    for row in rows[2:]:
        if not row:
            continue

        name = (
            row[LIVE_ROSTER_COLUMNS["name"]].strip()
            if len(row) > LIVE_ROSTER_COLUMNS["name"]
            else ""
        )
        if not name:
            continue

        key = name.casefold()
        if key in seen_names:
            continue
        seen_names.add(key)

        normalised_rows.append(
            {
                "name": name,
                "roster_rank": row[LIVE_ROSTER_COLUMNS["roster_rank"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["roster_rank"]
                else "",
                "player_class": row[LIVE_ROSTER_COLUMNS["player_class"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["player_class"]
                else "",
                "race": row[LIVE_ROSTER_COLUMNS["race"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["race"]
                else "",
                "note": row[LIVE_ROSTER_COLUMNS["note"]].strip()
                if len(row) > LIVE_ROSTER_COLUMNS["note"]
                else "",
                "source_refreshed_at": refreshed_at,
            }
        )

    output_path = output_dir / LIVE_ROSTER_FILENAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "name",
                "roster_rank",
                "player_class",
                "race",
                "note",
                "source_refreshed_at",
            ],
        )
        writer.writeheader()
        writer.writerows(normalised_rows)

    _mirror_to_frontend_data(output_path)
    logger.info("  wrote %s live roster rows to %s", len(normalised_rows), output_path)
    return len(normalised_rows)


def export_guild_zone_ranks(client: WorkspaceClient, warehouse_id: str, output_dir: Path) -> int:
    if not WCL_CLIENT_ID or not WCL_CLIENT_SECRET:
        logger.info("Skipping guild zone ranks export: WCL client credentials not configured.")
        return 0

    logger.info("Exporting guild zone ranks -> %s", GUILD_ZONE_RANKS_FILENAME)
    zone_response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT DISTINCT CAST(zone_id AS STRING) AS zone_id, zone_name "
            f"FROM {CATALOG}.{SCHEMA}.gold_boss_progression "
            "WHERE zone_id IS NOT NULL AND zone_name IS NOT NULL "
            f"AND zone_name NOT IN ({', '.join(repr(zone) for zone in sorted(EXCLUDED_ZONES))})"
        ),
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    zone_response = _wait_for_success(client, zone_response)
    zone_rows = getattr(getattr(zone_response, "result", None), "data_array", None) or []

    adapter = WarcraftLogsAdapter(
        WarcraftLogsConfig(
            client_id=WCL_CLIENT_ID,
            client_secret=WCL_CLIENT_SECRET,
        )
    )
    adapter.authenticate()

    query = """
    query GuildZoneRanks(
      $guildName: String!
      $serverSlug: String!
      $serverRegion: String!
      $zoneId: Int!
    ) {
      guildData {
        guild(name: $guildName, serverSlug: $serverSlug, serverRegion: $serverRegion) {
          zoneRanking(zoneId: $zoneId) {
            progress(size: 20) {
              worldRank { number }
              regionRank { number }
              serverRank { number }
            }
          }
        }
      }
    }
    """

    rows_out: list[dict[str, str | int]] = []
    try:
        for row in zone_rows:
            zone_id = int(row[0])
            zone_name = str(row[1])
            result = adapter.fetch(
                "guild_zone_ranks",
                {
                    "query": query,
                    "variables": {
                        "guildName": WCL_GUILD_NAME,
                        "serverSlug": WCL_GUILD_SERVER_SLUG,
                        "serverRegion": WCL_GUILD_SERVER_REGION,
                        "zoneId": zone_id,
                    },
                },
            )
            data = result.records[0] if result.records else {}
            guild_data = data.get("guildData") or {}
            guild = guild_data.get("guild") or {}
            zone_ranking = guild.get("zoneRanking") or {}
            progress = zone_ranking.get("progress") or {}
            rows_out.append(
                {
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "world_rank": ((progress.get("worldRank") or {}).get("number")) or "",
                    "region_rank": ((progress.get("regionRank") or {}).get("number")) or "",
                    "server_rank": ((progress.get("serverRank") or {}).get("number")) or "",
                }
            )
    finally:
        adapter.close()

    output_path = output_dir / GUILD_ZONE_RANKS_FILENAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["zone_id", "zone_name", "world_rank", "region_rank", "server_rank"],
        )
        writer.writeheader()
        writer.writerows(rows_out)

    _mirror_to_frontend_data(output_path)
    logger.info("  wrote %s guild zone rank rows to %s", len(rows_out), output_path)
    return len(rows_out)


def _realm_to_slug(value: str | None) -> str:
    text = (value or WCL_GUILD_SERVER_SLUG or "").strip()
    if not text:
        return WCL_GUILD_SERVER_SLUG

    cleaned = text.replace("'", "").replace("_", "-").replace(" ", "-")
    cleaned = re.sub(r"(?<=[a-z])(?=[A-Z])", "-", cleaned).lower()
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return REALM_SLUG_OVERRIDES.get(cleaned.replace("-", ""), cleaned)


def _character_slug(value: str) -> str:
    return quote(value.strip().lower(), safe="")


def _blizzard_token() -> str:
    if not BLIZZARD_CLIENT_ID or not BLIZZARD_CLIENT_SECRET:
        raise RuntimeError("Blizzard client credentials are not configured.")

    response = httpx.post(
        "https://oauth.battle.net/token",
        auth=(BLIZZARD_CLIENT_ID, BLIZZARD_CLIENT_SECRET),
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _blizzard_get(
    http: httpx.Client,
    path: str,
    namespace: str | None = None,
) -> dict[str, Any] | None:
    response = http.get(
        f"https://{BLIZZARD_REGION}.api.blizzard.com{path}",
        params={
            "namespace": namespace or f"profile-{BLIZZARD_REGION}",
            "locale": BLIZZARD_LOCALE,
        },
    )
    if response.status_code in {403, 404}:
        return None
    response.raise_for_status()
    return response.json()


def _blizzard_item_icon_url(http: httpx.Client, item_id: Any, cache: dict[str, str]) -> str:
    item_key = str(item_id or "").strip()
    if not item_key:
        return ""
    if item_key in cache:
        return cache[item_key]

    payload = _blizzard_get(
        http,
        f"/data/wow/media/item/{item_key}",
        namespace=f"static-{BLIZZARD_REGION}",
    )
    assets = _asset_map(payload)
    cache[item_key] = assets.get("icon", "")
    return cache[item_key]


def _json_dump(value: Any) -> str:
    if not value:
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _simplify_enchantments(item: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "display_string": enchantment.get("display_string") or "",
            "source_item_name": ((enchantment.get("source_item") or {}).get("name")) or "",
            "enchantment_id": ((enchantment.get("enchantment_id") or enchantment.get("id")) or ""),
        }
        for enchantment in item.get("enchantments", [])
    ]


def _simplify_sockets(item: dict[str, Any]) -> list[dict[str, Any]]:
    sockets = []
    for socket in item.get("sockets", []):
        gem = socket.get("item") or {}
        sockets.append(
            {
                "socket_type": ((socket.get("socket_type") or {}).get("name")) or "",
                "item_id": gem.get("id") or "",
                "item_name": gem.get("name") or "",
                "display_string": socket.get("display_string") or "",
            }
        )
    return sockets


def _simplify_stats(item: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "type": ((stat.get("type") or {}).get("name")) or "",
            "value": stat.get("value") or "",
            "display": stat.get("display", {}).get("display_string")
            if isinstance(stat.get("display"), dict)
            else stat.get("display_string", ""),
            "is_negated": bool(stat.get("is_negated")),
        }
        for stat in item.get("stats", [])
    ]


def _simplify_spells(item: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for spell in item.get("spells", []):
        rows.append(
            {
                "spell_id": ((spell.get("spell") or {}).get("id")) or "",
                "spell_name": ((spell.get("spell") or {}).get("name")) or "",
                "description": spell.get("description") or "",
            }
        )
    return rows


def _asset_map(payload: dict[str, Any] | None) -> dict[str, str]:
    if not payload:
        return {}
    return {
        str(asset.get("key") or ""): str(asset.get("value") or "")
        for asset in payload.get("assets", [])
        if asset.get("key") and asset.get("value")
    }


def _looks_like_raid_feat(name: str) -> bool:
    lowered = name.lower()
    return "cutting edge:" in lowered or "famed slayer" in lowered or "famed bane" in lowered


def _fetch_blizzard_profile_candidates(
    client: WorkspaceClient, warehouse_id: str
) -> list[dict[str, str]]:
    statement = f"""
        SELECT player_name, COALESCE(NULLIF(realm, ''), '{WCL_GUILD_SERVER_SLUG}') AS realm
        FROM {CATALOG}.{SCHEMA}.gold_player_profile
        WHERE player_name IS NOT NULL
          AND player_name != ''
          AND (
            COALESCE(is_raid_team, false) = true
            OR COALESCE(kills_tracked, 0) > 0
          )
        ORDER BY COALESCE(is_raid_team, false) DESC,
                 latest_kill_date DESC NULLS LAST,
                 player_name
        LIMIT {BLIZZARD_PROFILE_EXPORT_CAP}
    """.strip()

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    response = _wait_for_success(client, response)
    rows = getattr(getattr(response, "result", None), "data_array", None) or []

    seen: set[tuple[str, str]] = set()
    candidates: list[dict[str, str]] = []
    for row in rows:
        player_name = str(row[0] or "").strip()
        realm_slug = _realm_to_slug(str(row[1] or ""))
        key = (player_name.casefold(), realm_slug)
        if not player_name or key in seen:
            continue
        seen.add(key)
        candidates.append({"player_name": player_name, "realm_slug": realm_slug})
    return candidates


def export_blizzard_character_profiles(
    client: WorkspaceClient,
    warehouse_id: str,
    output_dir: Path,
) -> int:
    if not BLIZZARD_CLIENT_ID or not BLIZZARD_CLIENT_SECRET:
        logger.info("Skipping Blizzard character profile export: credentials not configured.")
        return 0

    candidates = _fetch_blizzard_profile_candidates(client, warehouse_id)
    logger.info("Exporting Blizzard character profiles for %s characters", len(candidates))
    token = _blizzard_token()

    media_rows: list[dict[str, Any]] = []
    equipment_rows: list[dict[str, Any]] = []
    achievement_rows: list[dict[str, Any]] = []
    item_icon_cache: dict[str, str] = {}

    with httpx.Client(
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=True,
        timeout=30,
    ) as http:
        for candidate in candidates:
            player_name = candidate["player_name"]
            realm_slug = candidate["realm_slug"]
            character_slug = _character_slug(player_name)
            base = f"/profile/wow/character/{realm_slug}/{character_slug}"

            try:
                media = _blizzard_get(http, f"{base}/character-media")
                if media is None:
                    logger.info("Blizzard profile not found for %s-%s", player_name, realm_slug)
                    time.sleep(0.05)
                    continue

                assets = _asset_map(media)
                media_rows.append(
                    {
                        "player_name": player_name,
                        "realm_slug": realm_slug,
                        "avatar_url": assets.get("avatar", ""),
                        "inset_url": assets.get("inset", ""),
                        "main_url": assets.get("main", ""),
                        "main_raw_url": assets.get("main-raw", ""),
                    }
                )

                equipment = _blizzard_get(http, f"{base}/equipment")
                for item in (equipment or {}).get("equipped_items", []):
                    item_id = ((item.get("item") or {}).get("id")) or ""
                    equipment_rows.append(
                        {
                            "player_name": player_name,
                            "realm_slug": realm_slug,
                            "slot_type": ((item.get("slot") or {}).get("type")) or "",
                            "slot_name": ((item.get("slot") or {}).get("name")) or "",
                            "item_id": item_id,
                            "item_name": item.get("name") or "",
                            "icon_url": _blizzard_item_icon_url(http, item_id, item_icon_cache),
                            "quality": ((item.get("quality") or {}).get("name")) or "",
                            "item_level": ((item.get("level") or {}).get("value")) or "",
                            "inventory_type": ((item.get("inventory_type") or {}).get("name"))
                            or "",
                            "item_subclass": ((item.get("item_subclass") or {}).get("name")) or "",
                            "binding": ((item.get("binding") or {}).get("name")) or "",
                            "transmog_name": (
                                ((item.get("transmog") or {}).get("item") or {}).get("name")
                            )
                            or "",
                            "enchantments_json": _json_dump(_simplify_enchantments(item)),
                            "sockets_json": _json_dump(_simplify_sockets(item)),
                            "stats_json": _json_dump(_simplify_stats(item)),
                            "spells_json": _json_dump(_simplify_spells(item)),
                            "raw_details_json": _json_dump(
                                {
                                    "name_description": item.get("name_description") or {},
                                    "requirements": item.get("requirements") or {},
                                    "durability": item.get("durability") or {},
                                    "limit_category": item.get("limit_category") or "",
                                }
                            ),
                        }
                    )

                achievements = _blizzard_get(http, f"{base}/achievements")
                for row in (achievements or {}).get("achievements", []):
                    achievement = row.get("achievement") or {}
                    name = str(achievement.get("name") or "")
                    criteria = row.get("criteria") or {}
                    completed = bool(criteria.get("is_completed")) or bool(
                        row.get("completed_timestamp")
                    )
                    if completed and _looks_like_raid_feat(name):
                        achievement_rows.append(
                            {
                                "player_name": player_name,
                                "realm_slug": realm_slug,
                                "achievement_id": achievement.get("id") or row.get("id") or "",
                                "achievement_name": name,
                                "completed_timestamp": row.get("completed_timestamp") or "",
                            }
                        )
                time.sleep(0.05)
            except Exception as exc:
                logger.warning(
                    "Skipping Blizzard profile for %s-%s: %s", player_name, realm_slug, exc
                )

    outputs = [
        (PLAYER_CHARACTER_MEDIA_FILENAME, MEDIA_FIELDNAMES, media_rows),
        (PLAYER_CHARACTER_EQUIPMENT_FILENAME, EQUIPMENT_FIELDNAMES, equipment_rows),
        (PLAYER_RAID_ACHIEVEMENTS_FILENAME, RAID_ACHIEVEMENT_FIELDNAMES, achievement_rows),
    ]
    for filename, fieldnames, rows in outputs:
        output_path = output_dir / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        _mirror_to_frontend_data(output_path)
        logger.info("  wrote %s rows to %s", len(rows), output_path)

    return len(media_rows) + len(equipment_rows) + len(achievement_rows)


def _write_csv_from_statement(
    client: WorkspaceClient, statement_response: Any, output_path: Path
) -> int:
    statement_id = getattr(statement_response, "statement_id", None)
    if not statement_id:
        raise RuntimeError("Statement response did not include a statement_id.")

    manifest = getattr(statement_response, "manifest", None)
    chunks = getattr(manifest, "chunks", None) or []
    chunk_count = len(chunks) if chunks else 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0

    with output_path.open("wb") as fh:
        for chunk_index in range(chunk_count):
            chunk = (
                getattr(statement_response, "result", None)
                if chunk_index == 0
                else client.statement_execution.get_statement_result_chunk_n(
                    statement_id, chunk_index
                )
            )

            urls = _iter_external_links(chunk)
            if not urls:
                raise RuntimeError(
                    f"No external CSV links returned for statement {statement_id} chunk {chunk_index}."
                )

            for url in urls:
                payload = _download_chunk(url)
                if chunk_index > 0 and payload.startswith(b"\xef\xbb\xbf"):
                    payload = payload[3:]
                fh.write(payload)

            row_count += int(getattr(chunks[chunk_index], "row_count", 0) or 0)

    return row_count


def export_table(client: WorkspaceClient, warehouse_id: str, filename: str, table_name: str) -> int:
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    logger.info("Exporting %s -> %s", full_name, filename)
    statement = TABLE_EXPORT_STATEMENTS.get(table_name, f"SELECT * FROM {full_name}")

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=sql.Disposition.EXTERNAL_LINKS,
        format=sql.Format.CSV,
        wait_timeout="10s",
        on_wait_timeout=sql.ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )

    response = _wait_for_success(client, response)
    output_path = OUTPUT_DIR / filename
    row_count = _write_csv_from_statement(client, response, output_path)
    filtered_count = _filter_exported_csv(output_path)
    if filtered_count is not None:
        row_count = filtered_count
    _mirror_to_frontend_data(output_path)
    logger.info("  wrote %s rows to %s", row_count or "unknown", output_path)
    return row_count


def main() -> None:
    client = WorkspaceClient()
    warehouse_id = _first_warehouse_id(client)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for filename, table_name in FRONTEND_TABLES.items():
        total_rows += export_table(client, warehouse_id, filename, table_name)

    try:
        total_rows += export_live_raid_roster(OUTPUT_DIR)
    except Exception as exc:
        logger.warning("Skipping live raid roster export: %s", exc)

    try:
        total_rows += export_guild_zone_ranks(client, warehouse_id, OUTPUT_DIR)
    except Exception as exc:
        logger.warning("Skipping guild zone ranks export: %s", exc)

    try:
        total_rows += export_blizzard_character_profiles(client, warehouse_id, OUTPUT_DIR)
    except Exception as exc:
        logger.warning("Skipping Blizzard character profile export: %s", exc)

    logger.info(
        "Export complete. %s total rows across %s files.",
        total_rows or "unknown",
        len(FRONTEND_TABLES) + 5,
    )


if __name__ == "__main__":
    main()
