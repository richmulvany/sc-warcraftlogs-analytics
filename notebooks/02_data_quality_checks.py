# Databricks notebook source
# Notebook 2: Data Quality Checks
# Run ad-hoc quality checks against any layer.

# COMMAND ----------
catalog = "main"
schema = "pipeline_dev"

# COMMAND ----------
# DLT expectation results — check which rows failed quality rules
# (only available after a DLT pipeline run)
try:
    dlt_events = spark.table(f"{catalog}.{schema}.event_log")
    quality_failures = dlt_events.filter("event_type = 'flow_progress'")
    display(quality_failures.limit(50))
except Exception as e:
    print(f"DLT event log not available: {e}")

# COMMAND ----------
# Manual quality check: duplicate IDs in silver
df_silver = spark.table(f"{catalog}.{schema}.silver_entities")
from pyspark.sql import functions as F

duplicates = (
    df_silver.groupBy("id")
    .count()
    .filter("count > 1")
)
print(f"Duplicate IDs in silver: {duplicates.count()}")
display(duplicates)

# COMMAND ----------
# Freshness check: how recent is the latest record?
latest = df_silver.agg(F.max("created_at").alias("latest_record")).collect()[0][0]
print(f"Latest record timestamp: {latest}")
