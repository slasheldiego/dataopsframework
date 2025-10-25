# Databricks notebook source
# MAGIC %md
# MAGIC ## 40_load
# MAGIC Carga a Gold solo registros validados.

# COMMAND ----------

import json, uuid
from utils.validation2 import apply_expectations
from utils.io2 import write_delta
from utils.logging2 import log_event

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

cfg_lines = spark.read.text(f"s3a://utec-datalake-demo/config/env.{env}.json").collect()
cfg_json = "\n".join([row[0] for row in cfg_lines])
cfg = json.loads(cfg_json)
run_id = uuid.uuid4().hex

log_event(cfg, run_id, "load", "STARTED", json.dumps({"info":"init"}))

silver = f"{cfg['silver_db']}.{cfg['silver_table']}"
df = spark.table(silver)
df_valid, _ = apply_expectations(df, cfg)

gold = f"{cfg['gold_db']}.{cfg['gold_table']}"
write_delta(df_valid, gold, mode="overwrite")

log_event(cfg, run_id, "load", "SUCCESS", json.dumps({"rows": df_valid.count()}))
display(spark.table(gold).limit(10))
