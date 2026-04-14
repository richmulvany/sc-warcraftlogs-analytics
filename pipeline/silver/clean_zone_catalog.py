# Databricks notebook source
# Silver layer — zone and encounter reference catalog
#
# silver_zone_catalog — one row per encounter per zone, used to resolve
#                       encounter IDs to boss names and zone context in gold.

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_zone_catalog",
    comment=(
        "WCL zone and encounter reference: one row per encounter per zone. "
        "Used to resolve encounter IDs to boss names across gold tables."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_zone_id", "zone_id IS NOT NULL")
@dlt.expect_or_drop("valid_encounter_id", "encounter_id IS NOT NULL")
def silver_zone_catalog():
    return (
        dlt.read("bronze_zone_catalog")  # batch — zone catalog is a slowly-changing reference
        .filter(F.col("encounters").isNotNull())
        .select(
            F.col("id").alias("zone_id"),
            F.col("name").alias("zone_name"),
            F.col("frozen").alias("zone_frozen"),
            F.explode("encounters").alias("encounter"),
            F.col("difficulties"),
        )
        .select(
            F.col("zone_id"),
            F.col("zone_name"),
            F.col("zone_frozen"),
            F.col("encounter.id").alias("encounter_id"),
            F.col("encounter.name").alias("encounter_name"),
            # Collect difficulty names for this zone (e.g. Normal / Heroic / Mythic)
            F.transform(
                F.col("difficulties"),
                lambda d: d["name"],
            ).alias("difficulty_names"),
        )
        .dropDuplicates(["zone_id", "encounter_id"])
    )
