# Databricks notebook source
# Verification spike — does @dlt.table(catalog=, schema=) actually publish the
# table to the override target on Free Edition serverless DLT?
#
# How to use:
#   1. Pre-create the target schema in Databricks SQL (one-off):
#        CREATE SCHEMA IF NOT EXISTS 01_bronze.spike_check;
#   2. Add this notebook as the *only* library in a throwaway DLT pipeline
#      (any name, serverless, channel CURRENT, development=true). The pipeline's
#      top-level catalog/schema can be anything — the override should ignore it.
#   3. Run the pipeline. After a successful update, run in SQL:
#        SHOW TABLES IN 01_bronze.spike_check;
#        SELECT * FROM 01_bronze.spike_check.spike_override_probe;
#   4. Pass criterion: the table exists at 01_bronze.spike_check.spike_override_probe
#      and NOT at <pipeline_default_catalog>.<pipeline_default_schema>.spike_override_probe.
#   5. After confirming, drop the throwaway pipeline and the spike schema:
#        DROP SCHEMA 01_bronze.spike_check CASCADE;
#
# If this fails (table lands at the pipeline default, or the kwargs raise),
# stop the refactor and rethink — see plan, "Load-bearing assumption".

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="01_bronze.spike_check.spike_override_probe",
    comment="Throwaway probe for @dlt.table fully-qualified name override support.",
)
def spike_override_probe():
    return (
        spark.range(3)  # noqa: F821
        .withColumn("note", F.lit("override works if this row is in 01_bronze.spike_check"))
    )
