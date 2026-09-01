import csv
import time
from pathlib import Path

from pymongo import MongoClient

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
)


def load_csv_to_raw(
    file_path: str,
    run_id: str,
    engine_used: str = "python_batch",
) -> dict:
    """
    Stream a CSV file and load raw records into MongoDB
    using insert_many in configurable batches.

    MongoDB document structure:

    {
        "_id": ObjectId(...),

        "metadata": {
            "run_id": "...",
            "source_file": "...",
            "source_row_number": 1,
            "ingested_at": "...",
            "engine_used": "python_batch"
        },

        "source_data": {
            "order_id": "...",
            "order_date": "...",
            ...
        }
    }
    """

    input_path = Path(file_path)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DATABASE][RAW_COLLECTION]

    start_time = time.perf_counter()

    rows_read = 0
    raw_loaded = 0
    batch_number = 0

    batch = []

    ingested_at = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ",
        time.gmtime(),
    )

    try:
        with input_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:

            reader = csv.DictReader(file)

            for row in reader:
                rows_read += 1

                document = {
                    "metadata": {
                        "run_id": run_id,
                        "source_file": input_path.name,
                        "source_row_number": rows_read,
                        "ingested_at": ingested_at,
                        "engine_used": engine_used,
                    },

                    "source_data": dict(row),
                }

                batch.append(document)

                if len(batch) >= BATCH_SIZE:
                    batch_number += 1

                    batch_start = time.perf_counter()

                    result = collection.insert_many(
                        batch,
                        ordered=False,
                    )

                    batch_elapsed = (
                        time.perf_counter()
                        - batch_start
                    )

                    inserted = len(result.inserted_ids)
                    raw_loaded += inserted

                    rate = (
                        inserted / batch_elapsed
                        if batch_elapsed > 0
                        else 0
                    )

                    print(
                        f"Batch {batch_number:>4} | "
                        f"Records: {inserted:>5} | "
                        f"Time: {batch_elapsed:>7.3f}s | "
                        f"Rate: {rate:>10.2f} rows/s"
                    )

                    batch.clear()

            # Process remaining records
            if batch:
                batch_number += 1

                batch_start = time.perf_counter()

                result = collection.insert_many(
                    batch,
                    ordered=False,
                )

                batch_elapsed = (
                    time.perf_counter()
                    - batch_start
                )

                inserted = len(result.inserted_ids)
                raw_loaded += inserted

                rate = (
                    inserted / batch_elapsed
                    if batch_elapsed > 0
                    else 0
                )

                print(
                    f"Batch {batch_number:>4} | "
                    f"Records: {inserted:>5} | "
                    f"Time: {batch_elapsed:>7.3f}s | "
                    f"Rate: {rate:>10.2f} rows/s"
                )

                batch.clear()

        elapsed = time.perf_counter() - start_time

        throughput = (
            raw_loaded / elapsed
            if elapsed > 0
            else 0
        )

        result = {
            "rows_read": rows_read,
            "raw_loaded": raw_loaded,
            "batch_count": batch_number,
            "batch_size": BATCH_SIZE,
            "elapsed_seconds": round(elapsed, 3),
            "throughput": round(throughput, 2),
            "engine_used": engine_used,
        }

        print("\n" + "=" * 60)
        print("PYTHON BATCH LOAD COMPLETED")
        print("=" * 60)
        print(f"Rows read       : {rows_read}")
        print(f"Raw loaded      : {raw_loaded}")
        print(f"Batch count     : {batch_number}")
        print(f"Batch size      : {BATCH_SIZE}")
        print(f"Elapsed seconds : {elapsed:.3f}")
        print(f"Throughput      : {throughput:.2f} rows/s")
        print("=" * 60)

        return result

    finally:
        client.close()