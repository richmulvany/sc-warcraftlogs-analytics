# Databricks notebook source
# Silver layer — cleaned, normalised, deduplicated entities

# COMMAND ----------
import dlt
from pyspark.sql import functions as F


# COMMAND ----------
@dlt.table(
    name="silver_entities",
    comment="Cleaned and normalised entities. Nulls handled, types cast, duplicates removed.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("non_null_id", "id IS NOT NULL")
@dlt.expect_or_drop("non_null_name", "name IS NOT NULL")
@dlt.expect("valid_created_at", "created_at IS NOT NULL")
def silver_entities():
    return (
        dlt.read_stream("bronze_entities")
        # Deduplicate on id, keep latest
        .dropDuplicates(["id"])
        # Normalise string fields
        .withColumn("name", F.trim(F.col("name")))
        # Cast timestamps
        .withColumn("created_at", F.to_timestamp(F.col("created_at")))
        # Drop raw metadata columns not needed downstream
        .drop("_page", "_source")
        # Add silver metadata
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
