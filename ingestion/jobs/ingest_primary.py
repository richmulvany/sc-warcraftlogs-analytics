# Databricks notebook source
# WarcraftLogs ingestion job — fetches guild reports, fight details, and
# raid attendance from the WCL v2 GraphQL API and lands them as JSON files
# in a Unity Catalog Volume for the DLT pipeline to pick up via Auto Loader.

# COMMAND ----------
# Third-party libraries (httpx, tenacity, structlog, pydantic) are installed
# by Databricks before this notebook runs, via the `libraries` block in the
# job task definition in databricks.yml.  No runtime install step is needed.
# COMMAND ----------
import json
import logging
import os
import sys
from datetime import UTC, datetime

# In Databricks notebooks __file__ is not defined.  Instead, derive the bundle
# root (the "files/" directory that DAB syncs to) from the notebook's own
# workspace path, which is always 3 path segments below the bundle root:
#   .../files/ingestion/jobs/ingest_primary
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_nb_path = _ctx.notebookPath().get()  # e.g. /.../files/ingestion/jobs/ingest_primary
_bundle_root = "/Workspace" + "/".join(_nb_path.split("/")[:-3])  # → .../files
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from ingestion.src.adapters.wcl.client import WarcraftLogsAdapter, WarcraftLogsConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
# Load job parameters (set via databricks.yml base_parameters or widget defaults)
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "04_sdp"  # noqa: F821
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "warcraftlogs"  # noqa: F821
guild_name = (
    dbutils.widgets.get("guild_name") if dbutils.widgets.get("guild_name") else "Student Council"
)  # noqa: F821
server_slug = (
    dbutils.widgets.get("guild_server_slug")
    if dbutils.widgets.get("guild_server_slug")
    else "twisting-nether"
)  # noqa: F821
server_region = (
    dbutils.widgets.get("guild_server_region")
    if dbutils.widgets.get("guild_server_region")
    else "EU"
)  # noqa: F821

logger.info(
    "Ingesting guild=%s server=%s region=%s → %s.%s",
    guild_name,
    server_slug,
    server_region,
    catalog,
    schema,
)

# COMMAND ----------
# Load WCL credentials from Databricks Secret Scope
client_id = dbutils.secrets.get(scope="warcraftlogs", key="client_id")  # noqa: F821
client_secret = dbutils.secrets.get(scope="warcraftlogs", key="client_secret")  # noqa: F821

adapter = WarcraftLogsAdapter(WarcraftLogsConfig(client_id=client_id, client_secret=client_secret))
adapter.authenticate()

# COMMAND ----------
# Ensure the Volume landing directories exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.landing")  # noqa: F821

landing = f"/Volumes/{catalog}/{schema}/landing"
for subdir in ("guild_reports", "report_fights", "raid_attendance"):
    os.makedirs(f"{landing}/{subdir}", exist_ok=True)

# COMMAND ----------
# ── 1. Guild Reports ──────────────────────────────────────────────────────────
# Fetch all pages of guild reports and write each page as a JSONL file.
# Auto Loader processes new files incrementally; the silver layer deduplicates
# on report code so re-runs are safe.

run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
ingested_at = datetime.now(UTC).isoformat()

logger.info("Fetching guild reports …")
page, has_more = 1, True
all_report_codes: list[str] = []

while has_more:
    result = adapter.fetch_guild_reports(guild_name, server_slug, server_region, page=page)
    if not result.records:
        logger.warning("No records returned for guild_reports page %d — stopping", page)
        break

    records = [{**r, "_source": "wcl", "_ingested_at": ingested_at} for r in result.records]
    all_report_codes.extend(r["code"] for r in result.records if r.get("code"))

    out_path = f"{landing}/guild_reports/{run_ts}_p{page}.jsonl"
    with open(out_path, "w") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")

    logger.info("guild_reports page %d → %d records → %s", page, len(records), out_path)
    has_more = result.has_more
    page += 1

logger.info("Total report codes collected: %d", len(all_report_codes))

# COMMAND ----------
# ── 2. Report Fights ──────────────────────────────────────────────────────────
# Fetch fight details for each report. Files are named by report code so we
# skip reports whose fight data was already fetched in a previous run.

logger.info("Fetching fight details for %d reports …", len(all_report_codes))

for report_code in all_report_codes:
    fight_file = f"{landing}/report_fights/{report_code}.jsonl"
    if os.path.exists(fight_file):
        logger.info("report_fights: %s already fetched — skipping", report_code)
        continue

    result = adapter.fetch_report_fights(report_code)
    if not result.records:
        logger.warning("No fight data for report %s — skipping", report_code)
        continue

    report = {**result.records[0], "_source": "wcl", "_ingested_at": ingested_at}
    with open(fight_file, "w") as fh:
        fh.write(json.dumps(report) + "\n")

    logger.info("report_fights: %s → %d fights", report_code, len(report.get("fights") or []))

# COMMAND ----------
# ── 3. Raid Attendance ────────────────────────────────────────────────────────
# Fetch paginated attendance records (which players attended each raid).

logger.info("Fetching raid attendance …")
page, has_more = 1, True

while has_more:
    result = adapter.fetch_raid_attendance(guild_name, server_slug, server_region, page=page)
    if not result.records:
        logger.warning("No records returned for raid_attendance page %d — stopping", page)
        break

    records = [{**r, "_source": "wcl", "_ingested_at": ingested_at} for r in result.records]

    out_path = f"{landing}/raid_attendance/{run_ts}_p{page}.jsonl"
    with open(out_path, "w") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")

    logger.info("raid_attendance page %d → %d records → %s", page, len(records), out_path)
    has_more = result.has_more
    page += 1

logger.info("Ingestion complete.")
