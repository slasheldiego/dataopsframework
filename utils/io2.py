from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def get_spark():
    return SparkSession.builder.getOrCreate()

def read_csv_to_df(path: str, header=True, inferSchema=True):
    spark = get_spark()
    return spark.read.option("header", header).option("inferSchema", inferSchema).csv(path)

def write_delta(df, full_table_name: str, mode="append"):
    (df.write.format("delta").mode(mode).saveAsTable(full_table_name))

def ensure_database(db: str):
    spark = get_spark()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

def truncate_table(full_table_name: str):
    spark = get_spark()
    spark.sql(f"TRUNCATE TABLE {full_table_name}")

def table_exists(full_table_name: str) -> bool:
    spark = get_spark()
    db, tbl = full_table_name.split(".", 1) if "." in full_table_name else (None, full_table_name)
    return len(spark.sql(f"SHOW TABLES IN {db}" if db else "SHOW TABLES").where(F.col("tableName")==tbl).collect())>0