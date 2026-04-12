# Databricks notebook source
# Gold layer — entity summary data product
# This is a business-ready table consumed directly by the frontend.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F


# COMMAND ----------
@dlt.table(
    name="gold_entity_summary",
    comment="Business-ready entity summary. Consumed by the frontend dashboard.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_fail("no_null_ids", "id IS NOT NULL")
def gold_entity_summary():
    """
    Aggregate silver_entities into a summary suitable for the frontend.
    Replace with your actual business logic.
    """
    return (
        dlt.read("silver_entities")
        .groupBy("category")
        .agg(
            F.count("id").alias("total_count"),
            F.countDistinct("id").alias("unique_count"),
            F.max("created_at").alias("latest_created_at"),
            F.min("created_at").alias("earliest_created_at"),
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
    )
