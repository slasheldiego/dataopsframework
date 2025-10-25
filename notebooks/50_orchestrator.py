# Databricks notebook source
# MAGIC %md
# MAGIC ## 50_orchestrator
# MAGIC Orquesta el pipeline usando `%run` y parámetros (`env`).

# COMMAND ----------

try:
    dbutils.widgets.text("env", "dev")
except:
    pass

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %run ./10_extract

# COMMAND ----------

# MAGIC %run ./20_transform

# COMMAND ----------

# MAGIC %run ./30_validate

# COMMAND ----------

# MAGIC %run ./40_load

# COMMAND ----------

# MAGIC %md
# MAGIC **Listo.** Revisa las tablas de logging y resultados de calidad:
# MAGIC - `demo.ops_event_log`
# MAGIC - `demo.ops_dq_results`
