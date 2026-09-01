import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession


def create_spark_session():
    """Create and return a local Spark session with increased memory to prevent OOM errors."""
    # Ensure PySpark workers use the exact same Python executable to avoid "Python worker failed to connect back"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("BigData_Spark_Loader")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .getOrCreate()
    )

    print("\n" + "-" * 60)
    print("✅ PySpark Engine Initialized Successfully with the following Configs:")
    print(f" - PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON')}")
    print(f" - Driver Memory : {spark.conf.get('spark.driver.memory')}")
    print(f" - Executor Memory : {spark.conf.get('spark.executor.memory')}")
    print(f" - OffHeap Enabled : {spark.conf.get('spark.memory.offHeap.enabled')}")
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

def _write_partition_to_mongo(partition, run_id: str, file_name: str):
    """Worker function to write a partition of rows to MongoDB in bulk."""
    from pymongo import MongoClient
    from config.settings import MONGO_URI, MONGO_DATABASE, RAW_COLLECTION, BATCH_SIZE
    
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DATABASE][RAW_COLLECTION]
    
    batch = []
    ingested_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
    for row in partition:
        document = {
            "metadata": {
                "run_id": run_id,
                "source_file": file_name,
                "source_row_number": None,  # Not easily available in Spark without window functions
                "ingested_at": ingested_at,
                "engine_used": "pyspark",
            },
            "source_data": row.asDict(),
        }
        batch.append(document)
        
        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch, ordered=False)
            batch.clear()
            
    if batch:
        collection.insert_many(batch, ordered=False)
        
    client.close()

def load_spark_df_to_raw(df, run_id: str, file_name: str) -> dict:
    """
    Write the Spark DataFrame to MongoDB raw collection in parallel using foreachPartition.
    Returns loading statistics.
    """
    print("\n" + "=" * 60)
    print("PYSPARK DISTRIBUTED LOAD STARTED")
    print("=" * 60)
    
    start_time = time.perf_counter()
    
    # Execute the parallel write
    df.foreachPartition(lambda partition: _write_partition_to_mongo(partition, run_id, file_name))
    
    elapsed = time.perf_counter() - start_time
    
    # Since foreachPartition doesn't easily return counts, we'll use df.count()
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