import time
from pathlib import Path
from typing import Any, Dict
from pymongo import MongoClient, UpdateOne

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)
from src.batch_loader import load_csv_to_raw
from src.classification import evaluate_record_classification
from src.file_router import inspect_file, print_router_result
from src.metrics import generate_pipeline_metrics, save_pipeline_reports
from src.mongo_setup import setup_mongodb
from src.quality_rules import clean_record_and_generate_audit
from src.spark_loader import load_csv_with_spark, load_spark_df_to_raw


def process_raw_to_validated_and_quarantine(run_id: str) -> Dict[str, Any]:
    """
    Process raw records loaded in MongoDB orders_raw collection for the current run_id.
    Applies quality rules (Stage 3), classifies records (Stage 4),
    and performs Idempotent Upserts into orders_validated / Inserts into orders_quarantine (Stage 5).

    Returns:
        dict: Counts of valid, corrected, quarantine records and upsert statistics.
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]

    raw_col = db[RAW_COLLECTION]
    val_col = db[VALIDATED_COLLECTION]
    quar_col = db[QUARANTINE_COLLECTION]

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    upsert_inserted = 0
    upsert_modified = 0

    val_bulk_ops = []
    quar_bulk_ops = []

    # Stream raw records for this run_id
    cursor = raw_col.find({"metadata.run_id": run_id})
    total_processed = 0
    batch_start_time = time.perf_counter()

    for raw_doc in cursor:
        total_processed += 1
        source_data = raw_doc.get("source_data", {})
        metadata = raw_doc.get("metadata", {})

        # Stage 3: Clean record and generate audit trail
        cleaned_data, corrections = clean_record_and_generate_audit(source_data)

        # Stage 4: Classification
        status, reasons = evaluate_record_classification(cleaned_data, corrections)

        ingested_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        if status in ["VALID", "CORRECTED"]:
            if status == "VALID":
                valid_count += 1
            else:
                corrected_count += 1

            validated_doc = {
                "order_id": cleaned_data.get("order_id"),
                "customer_id": cleaned_data.get("customer_id"),
                "order_date": cleaned_data.get("order_date"),
                "status": cleaned_data.get("status"),
                "total_amount": cleaned_data.get("total_amount"),
                "currency": cleaned_data.get("currency"),
                "payment_method": cleaned_data.get("payment_method"),
                "payment_status": cleaned_data.get("payment_status"),
                "delivery_type": cleaned_data.get("delivery_type"),
                "delivery_cost": cleaned_data.get("delivery_cost"),
                "payment_amount": cleaned_data.get("payment_amount"),
                "customer_email": cleaned_data.get("customer_email"),
                "customer_phone": cleaned_data.get("customer_phone"),
                "items_json": cleaned_data.get("items_json"),
                "quality_status": status,
                "corrections": corrections,
                "metadata": {
                    "run_id": run_id,
                    "processed_at": ingested_at,
                    "source_file": metadata.get("source_file"),
                    "source_row_number": metadata.get("source_row_number"),
                    "engine_used": metadata.get("engine_used"),
                },
            }

            # Idempotent Upsert operation using order_id as stable business key
            val_bulk_ops.append(
                UpdateOne(
                    {"order_id": cleaned_data.get("order_id")},
                    {"$set": validated_doc},
                    upsert=True,
                )
            )

            if len(val_bulk_ops) >= BATCH_SIZE:
                res = val_col.bulk_write(val_bulk_ops, ordered=False)
                upsert_inserted += res.upserted_count
                upsert_modified += res.modified_count
                val_bulk_ops.clear()

        else:
            # Quarantine Stage
            quarantine_count += 1
            quarantine_doc = {
                "quarantine_reasons": reasons,
                "metadata": {
                    "run_id": run_id,
                    "quarantined_at": ingested_at,
                    "source_file": metadata.get("source_file"),
                    "source_row_number": metadata.get("source_row_number"),
                    "engine_used": metadata.get("engine_used"),
                },
                "raw_record": source_data,
                "cleaned_draft": cleaned_data,
                "corrections": corrections,
            }

            quar_bulk_ops.append(quarantine_doc)

            if len(quar_bulk_ops) >= BATCH_SIZE:
                quar_col.insert_many(quar_bulk_ops, ordered=False)
                quar_bulk_ops.clear()
                
        # Print progress every 10,000 records
        if total_processed % 10000 == 0:
            elapsed = time.perf_counter() - batch_start_time
            rate = 10000 / elapsed if elapsed > 0 else 0
            print(f"Processing | Records: {total_processed:>9} | Time: {elapsed:>7.3f}s | Rate: {rate:>10.2f} rows/s")
            batch_start_time = time.perf_counter()

    # Flush remaining ops
    if val_bulk_ops:
        res = val_col.bulk_write(val_bulk_ops, ordered=False)
        upsert_inserted += res.upserted_count
        upsert_modified += res.modified_count
        val_bulk_ops.clear()

    if quar_bulk_ops:
        quar_col.insert_many(quar_bulk_ops, ordered=False)
        quar_bulk_ops.clear()

    client.close()

    return {
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "upsert_inserted": upsert_inserted,
        "upsert_modified": upsert_modified,
    }


def run_elt_pipeline(file_path: str) -> Dict[str, Any]:
    """
    Execute the complete 6-stage ELT pipeline.
    """
    start_time = time.perf_counter()

    # Ensure MongoDB setup & indexes
    setup_mongodb()

    # Stage 1: File Router & Discovery
    router_res = inspect_file(file_path)
    print_router_result(router_res)

    run_id = router_res["run_id"]
    engine_used = router_res["engine_used"]
    file_name = router_res["file_name"]
    file_size_mb = router_res["file_size_mb"]

    print("\n" + "*" * 60)
    print(f"🚀 STARTING STAGE 2: RAW LOAD USING [{engine_used.upper()}] 🚀")
    print("*" * 60)


    # Stage 2: Raw Load (orders_raw)
    if engine_used == "python_batch":
        load_res = load_csv_to_raw(file_path, run_id, engine_used="python_batch")
        rows_read = load_res["rows_read"]
        raw_loaded = load_res["raw_loaded"]
    else:
        # PySpark Loader (Stage 2)
        spark, df = load_csv_with_spark(file_path)
        # Distributed load to MongoDB via foreachPartition
        load_res = load_spark_df_to_raw(df, run_id, file_name)
        rows_read = load_res["rows_read"]
        raw_loaded = load_res["raw_loaded"]
        spark.stop()

    # Stage 3, 4, 5: Quality, Classification & Idempotent Upsert
    proc_res = process_raw_to_validated_and_quarantine(run_id)

    elapsed_seconds = time.perf_counter() - start_time

    # Stage 6: Metrics & Reports
    metrics = generate_pipeline_metrics(
        run_id=run_id,
        file_name=file_name,
        file_size_mb=file_size_mb,
        engine_used=engine_used,
        rows_read=rows_read,
        raw_loaded=raw_loaded,
        valid_count=proc_res["valid_count"],
        corrected_count=proc_res["corrected_count"],
        quarantine_count=proc_res["quarantine_count"],
        elapsed_seconds=elapsed_seconds,
        upsert_inserted=proc_res["upsert_inserted"],
        upsert_modified=proc_res["upsert_modified"],
    )

    save_pipeline_reports(metrics)

    print("\n" + "=" * 60)
    print("ELT PIPELINE EXECUTION COMPLETED")
    print("=" * 60)
    print(f"Run ID           : {run_id}")
    print(f"Raw Loaded       : {raw_loaded}")
    print(f"Valid            : {proc_res['valid_count']}")
    print(f"Corrected        : {proc_res['corrected_count']}")
    print(f"Quarantine       : {proc_res['quarantine_count']}")
    print(f"Consistency Check: {'PASSED ✅' if metrics['consistency_check']['is_consistent'] else 'FAILED ❌'}")
    print("=" * 60)

    return metrics
