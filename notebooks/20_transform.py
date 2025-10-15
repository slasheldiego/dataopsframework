# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 20_transform
# MAGIC Limpia, estandariza y publica Silver.

# COMMAND ----------
# MAGIC %python
import json, uuid
from pyspark.sql import functions as F
from utils.io import write_delta
from utils.logging import log_event

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

cfg = json.loads(spark.read.text(f"/dbfs/FileStore/config/env.{env}.json").collect()[0][0])
run_id = uuid.uuid4().hex

log_event(cfg, run_id, "transform", "STARTED", {"info":"init"})

bronze = f"{cfg['database']}.{cfg['bronze_table']}"
df = spark.table(bronze).withColumn("name", F.initcap("name")).withColumn("email", F.lower("email"))

silver = f"{cfg['database']}.{cfg['silver_table']}"
write_delta(df, silver, mode="overwrite")

log_event(cfg, run_id, "transform", "SUCCESS", {"rows": df.count()})
display(spark.table(silver).limit(10))
