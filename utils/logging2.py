from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder.getOrCreate()

def log_event(cfg, run_id, step, status, extra: dict=None):
    spark = get_spark()
    tbl = f"{cfg['operational_db']}.{cfg['event_log_table']}" if "." not in cfg['event_log_table'] else cfg['event_log_table']
    df = spark.createDataFrame([{"dummy":1}]).select(
        F.current_timestamp().alias("ts"),
        F.lit(run_id).alias("run_id"),
        F.lit(step).alias("step"),
        F.lit(status).alias("status"),
        F.lit((extra or {})).cast("string").alias("details")
    )
    df.write.format("delta").mode("append").saveAsTable(tbl)

def log_dq(cfg, run_id, results: list):
    spark = get_spark()
    tbl = f"{cfg['operational_db']}.{cfg['dq_results_table']}" if "." not in cfg['dq_results_table'] else cfg['dq_results_table']
    if not results:
        results = [{"rule":"-", "failed":0}]
    rows = [{"rule": r["rule"], "failed": int(r["failed"])} for r in results]
    df = spark.createDataFrame(rows).select(
        F.current_timestamp().alias("ts"),
        F.lit(run_id).alias("run_id"),
        "rule",
        "failed"
    )
    df.write.format("delta").mode("append").saveAsTable(tbl)