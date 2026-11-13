# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold marts + PII leak scan
# MAGIC Thin wrapper around `lakehouse.spark.jobs.build_gold`. The logic lives in the package so it is unit-tested and
# MAGIC parity-tested in CI; the notebook only wires widgets, secrets and Unity Catalog names.

# COMMAND ----------

dbutils.widgets.text("batch_id", "2026-11-01")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("landing", "/Volumes/main/landing/raw")
dbutils.widgets.text("secret_scope", "lakehouse")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))
from lakehouse.config import load_settings  # noqa: E402
from lakehouse.spark.jobs import Tables, build_gold  # noqa: E402

cfg = load_settings(os.path.abspath("../config/pipeline.yaml"))
cfg.paths["landing"] = dbutils.widgets.get("landing")
os.environ[cfg.pii.salt_env] = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), "pii-hash-salt")
tables = Tables(spark, catalog=dbutils.widgets.get("catalog"))

# COMMAND ----------

result = build_gold(cfg, tables)
print(result)
