# Databricks notebook source
# This file runs as a Databricks Job.
# Replace ExampleAdapter with your chosen adapter.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import logging
from datetime import UTC, datetime

from ingestion.src.adapters.example_adapter.adapter import ExampleAdapter
from ingestion.src.adapters.example_adapter.config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
# Load config and authenticate
config = load_config()
adapter = ExampleAdapter(config)
adapter.authenticate()

# COMMAND ----------
# Define which endpoints to ingest
ENDPOINTS = [
    "entities",
    "events",
    "metadata",
]

# COMMAND ----------
# Fetch and write raw data to Bronze Delta tables
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"  # noqa: F821
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "pipeline_dev"  # noqa: F821
ingested_at = datetime.now(UTC).isoformat()

for endpoint in ENDPOINTS:
    logger.info("Ingesting endpoint: %s", endpoint)

    page = 1
    has_more = True

    while has_more:
        result = adapter.fetch(endpoint, params={"page": page, "per_page": 100})

        if not adapter.validate(result):
            logger.warning("Skipping endpoint %s — validation failed", endpoint)
            break

        # Add metadata columns before writing
        records = [
            {
                **record,
                "_source": result.source,
                "_endpoint": result.endpoint,
                "_ingested_at": ingested_at,
                "_page": result.page,
            }
            for record in result.records
        ]

        df = spark.createDataFrame(records)  # noqa: F821
        table_name = f"{catalog}.{schema}.bronze_{endpoint}"

        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )

        logger.info("Wrote %d records to %s", len(records), table_name)

        has_more = result.has_more
        page += 1

logger.info("Ingestion complete.")
