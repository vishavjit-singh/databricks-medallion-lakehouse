# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: schemas and Unity Catalog column masks
# MAGIC Creates the `bronze`, `silver` and `gold` schemas in the target catalog and applies the column masks
# MAGIC generated from `config/pipeline.yaml` (`python -m lakehouse uc-sql`). Run once per workspace/catalog.
# MAGIC
# MAGIC The PII hash salt is read from a secret scope (backed by Azure Key Vault), never from code.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))
from lakehouse.config import load_settings  # noqa: E402
from lakehouse.masking import unity_catalog_sql  # noqa: E402

cfg = load_settings(os.path.abspath("../config/pipeline.yaml"))
for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{layer}")

# COMMAND ----------

# MAGIC %md Column masks can only be attached once `silver.customers` exists, so run this cell after the first
# MAGIC silver load. Statements are generated, so the policy file stays the single source of truth.

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.silver.customers"):
    for stmt in unity_catalog_sql(cfg.pii, catalog=catalog).split(";"):
        body = "\n".join(line for line in stmt.splitlines() if not line.strip().startswith("--")).strip()
        if body:
            spark.sql(body)
