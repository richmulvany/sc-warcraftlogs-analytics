# Databricks notebook source
# Blizzard-only ingestion wrapper. Reuses ingest_primary with stage=blizzard
# so guild-member and profile exports can rerun independently.

# COMMAND ----------
_CTX = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_NOTEBOOK_PATH = _CTX.notebookPath().get()
_PRIMARY_PATH = "/".join(_NOTEBOOK_PATH.split("/")[:-1] + ["ingest_primary"])

_PARAM_NAMES = [
    "catalog",
    "schema",
    "guild_name",
    "guild_server_slug",
    "guild_server_region",
    "profile_candidate_catalog",
    "profile_candidate_schema",
    "blizzard_profile_export_enabled",
    "blizzard_profile_export_cap",
    "blizzard_profile_sleep_seconds",
]


def _widget_value(name: str) -> str:
    try:
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return ""


_params = {name: value for name in _PARAM_NAMES if (value := _widget_value(name))}
_params["stage"] = "blizzard"

dbutils.notebook.exit(dbutils.notebook.run(_PRIMARY_PATH, 0, _params))  # noqa: F821
