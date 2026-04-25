# Databricks notebook source
# Runs the dashboard JSON asset publisher on Databricks compute so it can write
# directly to the UC volume path.

# COMMAND ----------
import sys

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_nb_path = _ctx.notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_nb_path.split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from scripts.publish_dashboard_assets import main  # noqa: E402

main()
