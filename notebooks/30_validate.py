# Databricks notebook source
# MAGIC %md
# MAGIC ## 30_validate
# MAGIC Aplica reglas de calidad y registra resultados.

# COMMAND ----------

import json, uuid
from utils.validation2 import apply_expectations
from utils.logging2 import log_event, log_dq

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

cfg_lines = spark.read.text(f"s3a://<utec-s3-name>/config/env.{env}.json").collect()
cfg_json = "\n".join([row[0] for row in cfg_lines])
cfg = json.loads(cfg_json)
run_id = uuid.uuid4().hex

log_event(cfg, run_id, "validate", "STARTED", json.dumps({"info":"init"}))

silver = f"{cfg['database']}.{cfg['silver_table']}"
df = spark.table(silver)

df_valid, results = apply_expectations(df, cfg)
log_dq(cfg, run_id, results)
log_event(cfg, run_id, "validate", "SUCCESS", json.dumps({"rules": len(results)}))

display(df_valid.limit(10))
