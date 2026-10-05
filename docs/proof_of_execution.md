# Proof of Execution & Engineering Documentation

Welcome! This document serves as a practical showcase of the results and evidence proving the successful execution of the Data Pipeline I built. I took these screenshots while the system was running live to clearly demonstrate the implementation of complex engineering concepts-such as distributed parallel processing via PySpark, preventing data duplication (Idempotency), and ensuring strict data quality.

---

## 1. Code Quality & Intelligent File Routing

### 1.1. Automated Testing Success (Pytest)
I wanted to leave absolutely no room for error, so I wrote 13 automated tests to meticulously verify every single cleaning rule, as well as the quarantine logic and the audit trail generation. As you can see in this screenshot, all tests passed successfully and lit up in green!
![Pytest Success](../screenshots/01_pytest_success.png)

### 1.2. The Smart File Router
One of the features I'm most proud of in this system is its ability to evaluate the file size and automatically select the optimal processing engine. Here, we can see how the system read the sample data size (41.77MB) and immediately decided to use the lightweight `python_batch` engine, completely avoiding the heavy overhead of spinning up Spark for a small file.
![Smart File Router](../screenshots/02_sample_routing_start.png)

---

## 2. Data Processing & Idempotency Proof

### 2.1. Successful Sample Execution & Consistency
This screenshot captures the final output after the system processed 100,000 records. The beauty here is the `PASSED ✅` badge, which confirms that the system mathematically verified its own work, ensuring that not a single record was lost during the ETL process.
![Data Consistency](../screenshots/03_sample_execution_passed.png)

### 2.2. The Golden Proof of Idempotency
To prove that my system is smart and does not blindly duplicate data, I executed the exact same file a second time. As clearly shown in the JSON report, the `"inserted_count"` is exactly zero! The system recognized that the data already existed in the database and handled it gracefully via Upsert.
![Idempotency Proof](../screenshots/04_idempotency_zero_inserts.png)

---

## 3. Database Engineering (MongoDB)

### 3.1. Tracking Modifications (Audit Trail)
I didn't just want to clean corrupted data; I wanted to maintain a historical record of exactly what changed. In this database screenshot, you can see the `corrections` array, which accurately logs the transformations applied to the document.
![Audit Trail](../screenshots/05_audit_trail_corrections.png)

### 3.2. Protecting IDs from Duplication
To guarantee the integrity of the database, I enforced a Unique Index on the `order_id` field. This screenshot proves the existence of the `idx_val_order_id_unique` index, which strictly forbids any future ID collisions.
![Unique Index](../screenshots/06_unique_index_order_id.png)

### 3.3. Guarding the Database Structure (Schema Validation)
To ensure that any data entering the target collections complies with our strict standards, I utilized MongoDB's `$jsonSchema`. This shot demonstrates how the database actively rejects any record that is missing mandatory fields or contains out-of-bounds enum values.
![Schema Validation](../screenshots/07_schema_validation_rules.png)

### 3.4. The Quarantine Zone
Here we can see the "hopeless" records that the system couldn't safely repair. They were successfully isolated into the `orders_quarantine` collection, complete with an attached array explaining exactly why they were rejected (`error_codes`), making it extremely easy for data stewards to review them later.
![Quarantine Zone](../screenshots/08_quarantine_error_codes.png)

---

## 4. Conquering Big Data (PySpark)

### 4.1. Intelligent Routing for Big Data (PySpark)
This screenshot proves the File Router in action when dealing with massive datasets. Unlike the earlier 41MB sample, the system detected a dataset size exceeding the threshold and intelligently opted to spin up the `pyspark` engine to handle the load effectively.
![PySpark Routing Start](../screenshots/09_pyspark_routing_start.png)

### 4.2. Handling 30 Million Records (PySpark Load)
This screenshot is the crown jewel of the project. Here, I prove that the system successfully handled a massive dataset containing 30 million records (12.65 GB). Powered by the `pyspark` engine, the data throughput reached an incredible speed of over 85,000 records per second!
![30 Million Records Load](../screenshots/10_pyspark_30m_terminal.png)

### 4.3. Parallel Task Management Dashboard
To prove that I am utilizing true distributed parallel processing, I documented the official Apache Spark Web UI. The screenshot shows my custom application, `OrdersPipeline`, actively running and efficiently distributing tasks across the executors.
![Spark UI Jobs](../screenshots/11_spark_ui_jobs.png)

### 4.4. Execution Stage Partitioning
This final screenshot provides a deep dive into how Spark partitioned the massive 6.0 GiB workload into manageable Stages and micro-tasks. It processed them perfectly in parallel across all CPU cores to guarantee maximum execution speed.
![Spark UI Stages](../screenshots/12_spark_ui_stages.png)

---

## 5. Phase 2: Performance Proofs, Benchmarks & Interactive API

Phase 2 was systematically validated using live MongoDB execution stats, automated test suites, and interactive Swagger UI calls.

### 5.1. Explain Plan Benchmarking Proof (`reports/explain_results.md`)
Using MongoDB's native `.explain("executionStats")`, each query was tested with and without index structures. The empirical evidence demonstrates massive performance gains:

| Query Scenario | Target Index | Execution Stage (Before $\rightarrow$ After) | Docs Examined (Before $\rightarrow$ After) | Reduction (%) | Time Before $\rightarrow$ After |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **1. City & Status (Compound)** | `idx_val_city_status` | `COLLSCAN` $\rightarrow$ `FETCH -> IXSCAN` | $94{,}447 \rightarrow 1{,}592$ | **98.31%** | $59\text{ ms} \rightarrow 4\text{ ms}$ |
| **2. Date Range & Sort** | `idx_val_order_date` | `SORT -> COLLSCAN` $\rightarrow$ `FETCH -> IXSCAN` | In-memory sort eliminated | **Eliminated** | $269\text{ ms} \rightarrow 216\text{ ms}$ |
| **3. Customer History Lookup** | `idx_val_customer_id` | `COLLSCAN` $\rightarrow$ `FETCH -> IXSCAN` | $94{,}447 \rightarrow 1$ | **100.0%** | $56\text{ ms} \rightarrow 1\text{ ms}$ |

*Key Takeaway:* The compound index `(city, status)` reduced collection scanning by **98.31%**, and customer point lookup achieved instantaneous sub-millisecond retrieval (**100% reduction**).

---

### 5.2. Live Analytical Aggregation Output Proof
The 5 aggregation pipelines execute live in-database transformations. For example, `GET /aggregations/top_products` unwinds nested item arrays across nearly 100,000 orders and aggregates product volumes in **142 milliseconds**:

```json
{
  "status": "SUCCESS",
  "aggregation_name": "top_products",
  "returned_count": 7,
  "execution_time_ms": 142.71,
  "results": [
    { "sku": "SKU-1009", "product_name": "شاحن سريع", "total_quantity": 3422, "total_revenue": 27043000, "orders_count": 1714 },
    { "sku": "SKU-1008", "product_name": "محول HDMI", "total_quantity": 3351, "total_revenue": 33188500, "orders_count": 1660 },
    { "sku": "SKU-1005", "product_name": "سماعات رأس", "total_quantity": 3318, "total_revenue": 94460500, "orders_count": 1661 },
    { "sku": "SKU-1010", "product_name": "هاتف سامسونج A54", "total_quantity": 3309, "total_revenue": 728080500, "orders_count": 1634 }
  ]
}
```

---

### 5.3. Materialized Views Incremental Refresh Proof
Executing `POST /refresh-mv?full_refresh=false` verifies the self-contained watermark logic:
- When no new orders are ingested, the system returns immediately in **0ms** without recalculating historical days:
  ```json
  {
    "status": "SUCCESS",
    "refresh_mode": "INCREMENTAL",
    "views": {
      "daily_sales_summary": { "affected_days_count": 0, "message": "لا توجد سجلات مبيعات جديدة؛ العرض المادي محدث بالكامل." },
      "top_products_summary": { "affected_products_count": 0, "message": "لا توجد طلبات جديدة تحتوي منتجات؛ العرض المادي محدث بالكامل." }
    }
  }
  ```
- When new orders are appended, only affected date buckets and product SKUs are merged via `bulk_write` with `upsert=True`.

---

### 5.4. Scheduled Automation & Audit Logging Proof (`job_logs`)
Every execution of the background scheduler or manual trigger via `POST /jobs/{name}/run` persists an immutable audit trail in MongoDB `job_logs`:

```json
{
  "_id": "6ac305855148cef7254f4759",
  "job_name": "refresh_materialized_views",
  "trigger_type": "MANUAL",
  "start_time": "2026-10-05T02:03:49Z",
  "end_time": "2026-10-05T02:03:49Z",
  "elapsed_ms": 363.18,
  "status": "SUCCESS",
  "result": { ... },
  "error_message": null
}
```

---

### 5.5. Unified FastAPI Service Verification (All 10 Endpoints)
The FastAPI evaluation server running on `http://127.0.0.1:8000/docs` was thoroughly evaluated:
- All 10 required endpoints returned **HTTP 200 OK**.
- Interactive dropdown Enums (`QueryName`, `AggregationName`, `MaterializedViewName`, `JobName`) eliminate manual input errors.
- CORS is globally enabled for external client integration.

