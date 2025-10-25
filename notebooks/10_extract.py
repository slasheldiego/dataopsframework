# Databricks notebook source
# MAGIC %md
# MAGIC ## 10_extract
# MAGIC Lee CSV desde DBFS y lo persiste como tabla Bronze (Delta).

# COMMAND ----------

import json, uuid
from pyspark.sql import functions as F
from utils.io2 import read_csv_to_df, write_delta
from utils.logging2 import log_event

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

cfg_lines = spark.read.text(f"s3a://utec-datalake-demo/config/env.{env}.json").collect()
cfg_json = "\n".join([row[0] for row in cfg_lines])
cfg = json.loads(cfg_json)
run_id = uuid.uuid4().hex

log_event(cfg, run_id, "extract", "STARTED", json.dumps({"info": "init"}))

df = read_csv_to_df(cfg["input_csv_path"]).withColumn("ingest_ts", F.current_timestamp())
full_table = f"{cfg['bronze_db']}.{cfg['bronze_table']}"
write_delta(df, full_table, mode="overwrite")

log_event(cfg, run_id, "extract", "SUCCESS", json.dumps({"rows": df.count()}))
display(spark.table(full_table).limit(10))
