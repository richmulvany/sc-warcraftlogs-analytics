# Databricks notebook source
# Google Sheets-only ingestion wrapper. Reuses ingest_primary with
# stage=google_sheets so the live roster export can rerun independently.

# COMMAND ----------
_CTX = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_NOTEBOOK_PATH = _CTX.notebookPath().get()
_PRIMARY_PATH = "/".join(_NOTEBOOK_PATH.split("/")[:-1] + ["ingest_primary"])

_PARAM_NAMES = [
    "live_roster_sheet_id",
    "live_roster_sheet_gid",
]


def _widget_value(name: str) -> str:
    try:
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return ""


_params = {name: value for name in _PARAM_NAMES if (value := _widget_value(name))}
_params["stage"] = "google_sheets"

dbutils.notebook.exit(dbutils.notebook.run(_PRIMARY_PATH, 0, _params))  # noqa: F821
