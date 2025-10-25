# Databricks notebook source
# MAGIC %md
# MAGIC ## 20_transform
# MAGIC Limpia, estandariza y publica Silver.

# COMMAND ----------

import json, uuid
from pyspark.sql import functions as F
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

log_event(cfg, run_id, "transform", "STARTED", json.dumps({"info":"init"}))

bronze = f"{cfg['database']}.{cfg['bronze_table']}"
df = spark.table(bronze).withColumn("name", F.initcap("name")).withColumn("email", F.lower("email"))

silver = f"{cfg['database']}.{cfg['silver_table']}"
write_delta(df, silver, mode="overwrite")

log_event(cfg, run_id, "transform", "SUCCESS", json.dumps({"rows": df.count()}))
display(spark.table(silver).limit(10))
