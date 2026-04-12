# Databricks notebook source
# Notebook 1: Explore Raw Data
# Use this notebook to inspect bronze tables and understand the raw data shape.
# Run interactively in Databricks — do not import into pipeline code.

# COMMAND ----------
# Configure catalog and schema
catalog = "main"
schema = "pipeline_dev"

# COMMAND ----------
# List available tables
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))

# COMMAND ----------
# Preview bronze entities
df_bronze = spark.table(f"{catalog}.{schema}.bronze_entities")
print(f"Row count: {df_bronze.count()}")
display(df_bronze.limit(20))

# COMMAND ----------
# Check for nulls in key columns
from pyspark.sql import functions as F

null_counts = df_bronze.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_bronze.columns
])
display(null_counts)

# COMMAND ----------
# Distribution of a categorical field
display(
    df_bronze.groupBy("category")
    .count()
    .orderBy(F.desc("count"))
)
