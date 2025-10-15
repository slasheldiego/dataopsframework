# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 30_validate
# MAGIC Aplica reglas de calidad y registra resultados.

# COMMAND ----------
# MAGIC %python
import json, uuid
from utils.validation import apply_expectations
from utils.logging import log_event, log_dq

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

cfg = json.loads(spark.read.text(f"/dbfs/FileStore/config/env.{env}.json").collect()[0][0])
run_id = uuid.uuid4().hex

log_event(cfg, run_id, "validate", "STARTED", {"info":"init"})

silver = f"{cfg['database']}.{cfg['silver_table']}"
df = spark.table(silver)

df_valid, results = apply_expectations(df, cfg)
log_dq(cfg, run_id, results)
log_event(cfg, run_id, "validate", "SUCCESS", {"rules": len(results)})

display(df_valid.limit(10))
