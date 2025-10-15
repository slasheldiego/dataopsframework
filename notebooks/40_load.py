# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 40_load
# MAGIC Carga a Gold solo registros validados.

# COMMAND ----------
# MAGIC %python
import json, uuid
from utils.validation import apply_expectations
from utils.io import write_delta
from utils.logging import log_event

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

cfg = json.loads(spark.read.text(f"/dbfs/FileStore/config/env.{env}.json").collect()[0][0])
run_id = uuid.uuid4().hex

log_event(cfg, run_id, "load", "STARTED", {"info":"init"})

silver = f"{cfg['database']}.{cfg['silver_table']}"
df = spark.table(silver)
df_valid, _ = apply_expectations(df, cfg)

gold = f"{cfg['database']}.{cfg['gold_table']}"
write_delta(df_valid, gold, mode="overwrite")

log_event(cfg, run_id, "load", "SUCCESS", {"rows": df_valid.count()})
display(spark.table(gold).limit(10))
