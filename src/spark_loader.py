import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession


from config.settings import (
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_MASTER
)

def create_spark_session():
    """Create and return a local Spark session with increased memory to prevent OOM errors."""
    # Ensure PySpark workers use the exact same Python executable to avoid "Python worker failed to connect back"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0")
        .getOrCreate()
    )

    print("\n" + "-" * 60)
    print("✅ PySpark Engine Initialized Successfully with the following Configs:")
    print(f" - PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON')}")
    print(f" - Driver Memory : {spark.conf.get('spark.driver.memory')}")
    print(f" - Executor Memory : {spark.conf.get('spark.executor.memory')}")
    print(f" - OffHeap Enabled : {spark.conf.get('spark.memory.offHeap.enabled')}")
    print(f" - Spark UI URL  : {spark.sparkContext.uiWebUrl}")
    print(f" - CSV Escape Char : '\"' (RFC 4180 Standard)")
    print("-" * 60 + "\n")

    return spark

def load_csv_with_spark(file_path: str):
    """
    Load a CSV file using Apache Spark.

    Returns:
        tuple: (spark_session, dataframe)
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    spark = create_spark_session()

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .option("escape", "\"")
        .csv(str(path))
    )

    return spark, df


import time

def load_spark_df_to_raw(df, run_id: str, file_name: str) -> dict:
    """
    Write the Spark DataFrame to MongoDB raw collection in parallel using MongoDB Spark Connector.
    Returns loading statistics.
    """
    from pyspark.sql.functions import struct, lit, current_timestamp, col, date_format
    from config.settings import MONGO_URI, MONGO_DATABASE, RAW_COLLECTION
    
    print("\n" + "=" * 60)
    print("PYSPARK DISTRIBUTED LOAD STARTED (MongoDB Connector)")
    print("=" * 60)
    
    start_time = time.perf_counter()
    
    # 1. Structure the DataFrame to match the raw collection format
    df_structured = df.select(
        struct(
            lit(run_id).alias("run_id"),
            lit(file_name).alias("source_file"),
            lit(None).cast("string").alias("source_row_number"),
            date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ingested_at"),
            lit("pyspark").alias("engine_used")
        ).alias("metadata"),
        struct(*[col(c) for c in df.columns]).alias("source_data")
    )
    
    # 2. Write using MongoDB Spark Connector (v10+)
    (df_structured.write
        .format("mongodb")
        .mode("append")
        .option("spark.mongodb.write.connection.uri", MONGO_URI)
        .option("spark.mongodb.write.database", MONGO_DATABASE)
        .option("spark.mongodb.write.collection", RAW_COLLECTION)
        .save())
    
    elapsed = time.perf_counter() - start_time
    
    # Calculate rows processed
    rows_read = df.count()
    raw_loaded = rows_read
    
    throughput = raw_loaded / elapsed if elapsed > 0 else 0
    
    print("=" * 60)
    print("PYSPARK DISTRIBUTED LOAD COMPLETED")
    print("=" * 60)
    print(f"Rows read       : {rows_read}")
    print(f"Raw loaded      : {raw_loaded}")
    print(f"Elapsed seconds : {elapsed:.3f}")
    print(f"Throughput      : {throughput:.2f} rows/s")
    print("=" * 60)
    
    return {
        "rows_read": rows_read,
        "raw_loaded": raw_loaded,
        "elapsed_seconds": round(elapsed, 3),
        "throughput": round(throughput, 2),
        "engine_used": "pyspark"
    }