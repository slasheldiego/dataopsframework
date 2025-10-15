# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window

def apply_expectations(df, cfg):
    results = []
    df_valid = df
    # Not null
    for col in cfg.get("expectations", {}).get("not_null", []):
        failed = df.filter(F.col(col).isNull()).count()
        results.append({"rule": f"not_null:{col}", "failed": failed})
        df_valid = df_valid.filter(F.col(col).isNotNull())
    # Unique
    for col in cfg.get("expectations", {}).get("unique", []):
        w = Window.partitionBy(col)
        dupes = df.withColumn("__cnt", F.count("*").over(w)).filter(F.col("__cnt")>1).count()
        results.append({"rule": f"unique:{col}", "failed": dupes})
        df_valid = df_valid.dropDuplicates([col])
    # Range
    for col, bounds in cfg.get("expectations", {}).get("range", {}).items():
        min_v, max_v = bounds
        failed = df.filter( (F.col(col) < F.lit(min_v)) | (F.col(col) > F.lit(max_v)) ).count()
        results.append({"rule": f"range:{col}[{min_v},{max_v}]", "failed": failed})
        df_valid = df_valid.filter( (F.col(col) >= F.lit(min_v)) & (F.col(col) <= F.lit(max_v)) )
    # Regex
    for col, pattern in cfg.get("expectations", {}).get("regex", {}).items():
        failed = df.filter(~F.col(col).rlike(pattern)).count()
        results.append({"rule": f"regex:{col}:{pattern}", "failed": failed})
        df_valid = df_valid.filter(F.col(col).rlike(pattern))
    return df_valid, results
