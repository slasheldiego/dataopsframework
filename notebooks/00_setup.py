# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 00_setup
# MAGIC Crea base de datos y tablas de logging.

# COMMAND ----------
# MAGIC %python
import json, os

try:
    env = dbutils.widgets.get("env")
except:
    env = "dev"

# Leer config desde DBFS
cfg_json = spark.read.text(f"/dbfs/FileStore/config/env.{env}.json").collect()[0][0]
cfg = json.loads(cfg_json)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg['database']}.{cfg['event_log_table']} (ts timestamp, run_id string, step string, status string, details string) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg['database']}.{cfg['dq_results_table']} (ts timestamp, run_id string, rule string, failed long) USING DELTA")
display(spark.sql(f"SHOW TABLES IN {cfg['database']}"))
