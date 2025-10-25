# Databricks notebook source
# MAGIC %md
# MAGIC ## 00_setup
# MAGIC Crea base de datos y tablas de logging.

# COMMAND ----------

import json, os

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

# Leer config desde DBFS
cfg_lines = spark.read.text(f"s3a://utec-datalake-demo/config/env.{env}.json").collect()
cfg_json = "\n".join([row[0] for row in cfg_lines])
cfg = json.loads(cfg_json)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg['operational_db']}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg['operational_db']}.{cfg['event_log_table']} (ts timestamp, run_id string, step string, status string, details string) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg['operational_db']}.{cfg['dq_results_table']} (ts timestamp, run_id string, rule string, failed long) USING DELTA")
display(spark.sql(f"SHOW TABLES IN {cfg['operational_db']}"))
