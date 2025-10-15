# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 10_extract
# MAGIC Lee CSV desde DBFS y lo persiste como tabla Bronze (Delta).

# COMMAND ----------
# MAGIC %python
import json, uuid
from pyspark.sql import functions as F
from utils.io import read_csv_to_df, write_delta
from utils.logging import log_event

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

cfg = json.loads(spark.read.text(f"/dbfs/FileStore/config/env.{env}.json").collect()[0][0])
run_id = uuid.uuid4().hex

log_event(cfg, run_id, "extract", "STARTED", {"info":"init"})

df = read_csv_to_df(cfg["input_csv_path"]).withColumn("ingest_ts", F.current_timestamp())
full_table = f"{cfg['database']}.{cfg['bronze_table']}"
write_delta(df, full_table, mode="overwrite")

log_event(cfg, run_id, "extract", "SUCCESS", {"rows": df.count()})
display(spark.table(full_table).limit(10))
