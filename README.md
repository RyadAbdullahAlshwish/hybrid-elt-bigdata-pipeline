# Hybrid Big Data ELT Pipeline (PySpark & MongoDB)

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-PySpark-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-NoSQL_Database-47A248?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Architecture](https://img.shields.io/badge/Architecture-8--Stage_ELT-brightgreen)](#-architecture--elt-flow)
[![Data Quality](https://img.shields.io/badge/Data_Quality-Audit_Trail-orange)](#-quality-rules--audit-trail)
[![License](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)

An end-to-end, production-ready Big Data **ELT Data Pipeline** built with Python, PySpark, and MongoDB according to the 8-stage architecture specified for the Big Data Practical Midterm Project.

The pipeline implements an automated **File Router** that dynamically selects the optimal execution engine based on file size, ingests raw records without preliminary data loss, applies in-database and in-memory transformations with full audit trails, enforces database-level schema constraints, and executes strictly **Idempotent Upserts**.

---

## 🚀 Key Features

1. **Intelligent File Router**: Dynamically selects between **Python Batch Loader** ($\le 200\text{ MB}$) for low-overhead streaming ingestion and **PySpark Parallel Engine** ($> 200\text{ MB}$) using the official `mongo-spark-connector`.
2. **Pure ELT Paradigm**: Ingests 100% of untransformed source records into MongoDB `orders_raw` before applying business logic.
3. **9 Automated Quality Rules & Audit Trail**: Cleans numbers (Arabic/Hindi conversion, comma separators), strips textual currencies to standard `YER`, standardizes payment statuses, recalculates totals from item-level data, repairs contacts, and tracks modifications in a dedicated `corrections` array.
4. **Classification & Quarantine Engine**: Segregates records into `VALID`, `CORRECTED`, and `QUARANTINE` using standardized business error codes (`MISSING_ORDER_ID`, `CORRUPTED_JSON`, etc.).
5. **Database-Level Schema Enforcement**: Employs MongoDB `$jsonSchema` validation rules alongside a unique compound index on `order_id` in `orders_validated`.
6. **Strict Idempotency via Bulk Upserts**: Utilizes atomic `bulk_write` operations with upserts to eliminate duplicate records during repeated executions, tracking `inserted_count`, `updated_count`, and `unchanged_count`.
7. **Mathematical Consistency Rule**:
   `run_raw_count` = `run_valid_count` + `run_corrected_count` + `run_quarantine_count`
8. **Automated Error Case Counting**: Dynamically compiles quarantine breakdown metrics inside `reports/results.json` and human-readable `reports/results.md`.
---

## 🏗 Architecture & ELT Flow

```text
[ Raw CSV Input ] 
       │
       ▼
[ 1. File Discovery & Metadata Extraction ]
       │
       ▼
[ 2. File Router ] ─── Size > 200MB? ───► YES ──► PySpark Distributed Loader
       │                                                     │
       NO                                                    │
       ▼                                                     ▼
 Python Batch Loader                                [ MongoDB: orders_raw ]
       │                                                     │
       └──────────────────────────┬──────────────────────────┘
                                  │
                                  ▼
                    [ 3. Pure Raw Load Ingestion ]
                                  │
                                  ▼
                 [ 4. Transform & 8 Quality Rules ] ──► (Generates Audit Trail)
                                  │
                                  ▼
                     [ 5. Record Classification ]
                                  │
                  ┌───────────────┴───────────────┐
                  ▼                               ▼
      [ Valid / Corrected ]             [ Irreparable Defects ]
                  │                               │
                  ▼                               ▼
       [ 6. Idempotent Upsert ]           [ 6. Quarantine Load ]
      (MongoDB $jsonSchema & Index)       (With System Error Codes)
                  │                               │
                  ▼                               ▼
      [ MongoDB: orders_validated ]     [ MongoDB: orders_quarantine ]
                  │                               │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
                 [ 7. Idempotency Check & Metrics ]
                                  │
                                  ▼
                 [ 8. reports/results.json & .md ]
```

### Stage Breakdown
- **Stage 1 (File Discovery)**: Computes dataset byte size, resolves filesystem paths, and provisions a unique `run_id`.
- **Stage 2 (Engine Selection)**: Directs workload to `python_batch` ($\le 200\text{ MB}$) to prevent Spark JVM spin-up overhead on small files, or to `pyspark` ($> 200\text{ MB}$) for distributed core execution and memory safety.
- **Stage 3 (Raw Load)**: Ingests raw documents directly into `orders_raw` without preliminary filtering.
- **Stage 4 (Transform & Quality Rules)**: Standardizes types, formats, and currencies while recording previous and new states in the document's `corrections` list.
- **Stage 5 (Classification)**: Evaluates logical validity and isolates unfixable records.
- **Stage 6 (Final Storage Load)**: Applies MongoDB `$jsonSchema` gatekeeper validation and issues bulk idempotent upserts against target collections.
- **Stage 7 (Idempotency Verification)**: Re-runs input files to confirm zero record inflation (`inserted_count = 0`).
- **Stage 8 (Metrics Reporting)**: Evaluates the mathematical consistency invariant and outputs metrics to `reports/results.json` and `reports/results.md`.

---

## ⚙️ Prerequisites & Setup

### Environment Requirements
- **Python**: 3.10+
- **Java JDK**: Version 11 or 17 (Required for Apache Spark execution)
- **MongoDB**: Community Server running on `localhost:27017`

### Installation
Clone the repository and install dependencies:
```powershell
git clone https://github.com/your-username/hybrid-elt-bigdata-pipeline.git
cd hybrid-elt-bigdata-pipeline
pip install -r requirements.txt
```

---

## 📁 Directory Structure
```text
├── config/
│   ├── settings.py              # Centralized environment configs, thresholds & Spark resources
│   └── orders_schema.json       # MongoDB $jsonSchema validation rules
├── data/
│   ├── raw/                     # Massive raw input datasets (e.g., orders_huge_mixed_quality.csv)
│   └── samples/                 # Sample datasets for lightweight and local runs
├── docs/
│   ├── architecture.md          # Comprehensive architectural specification & design tradeoffs
│   ├── user_guide.md            # User manual and pipeline run instructions
│   ├── data_structure_analysis.md
│   └── execution_flow_report.md
├── notebooks/
│   ├── 01_data_inspection.ipynb # Initial Exploratory Data Analysis (EDA)
│   └── 03_data_analysis.ipynb   # Quality check analysis
├── reports/
│   ├── results.json             # Automated JSON execution report & error_case_counts
│   └── results.md               # Visual Markdown metrics summary
├── run.py                       # Interactive TUI Control Panel (Rich Dashboard)
├── run.bat                      # Windows Launcher for the Control Panel
├── src/
│   ├── batch_loader.py          # Python streaming batch loader (insert_many)
│   ├── spark_loader.py          # Distributed PySpark loader with MongoDB Connector
│   ├── file_router.py           # Engine selector & run_id generator
│   ├── quality_rules.py         # 8 Automated data cleaning rules & audit trail generator
│   ├── classification.py        # Logic categorization (Valid / Corrected / Quarantine)
│   ├── mongo_setup.py           # Schema validator injection & index management
│   ├── metrics.py               # Throughput calculation & automated report generation
│   ├── elt_pipeline.py          # Pipeline orchestrator & bulk idempotent upsert engine
│   ├── create_small_sample.py   # Deterministic sampling utility
│   └── main.py                  # Primary application entry point
└── tests/
    ├── test_cleaning_rules.py   # Pytest suite for data cleansing rules
    └── test_classification.py  # Pytest suite for quarantine categorization
```

---

## 🛠 Data Quality Framework & Quarantine

### 9 Automated Cleaning Rules
1. **Numeric Normalization**: Converts Eastern Arabic and Perso-Arabic numerals to standard Western digits.
2. **Separator Cleansing**: Removes thousand separators and invalid punctuation from numeric fields.
3. **Currency Extraction**: Strips embedded textual currency markers from pricing figures.
4. **Textual Digits Translation**: Translates written Arabic textual numbers into integer representations.
5. **Sign Correction**: Corrects accidental negative pricing and quantity values to absolute figures.
6. **Currency Standardization**: Normalizes diverse regional currency strings to the uniform code `YER`.
7. **Status Mapping**: Standardizes order lifecycle statuses into uniform system states.
8. **Total Amount Recalculation**: Recomputes `total_amount` from `items_json` line items (`unit_price × qty`) plus `delivery_cost`, correcting any mismatch.
9. **Contact Cleansing**: Fixes malformed email syntaxes (extra spaces, double `@`) and normalizes international phone number prefixes.

### Audit Trail Format
Modifications are preserved within the document structure:
```json
{
  "field": "email",
  "old_value": "user@@domain.com",
  "new_value": "user@domain.com",
  "rule_name": "clean_email"
}
```

### Quarantine Error Codes
Records that fail mandatory constraints are routed to `orders_quarantine` tagged with explicit codes:
- `MISSING_ORDER_ID`
- `CORRUPTED_JSON`
- `NEGATIVE_PRICE_REJECTED`
- `UNPARSEABLE_DATE`

---

## 💻 Running the Pipeline

### 🌟 Interactive Control Panel (Recommended)
The most convenient way to operate the pipeline is through the interactive **Rich Terminal Dashboard**. It provides a centralized GUI-like experience in the terminal to launch jobs, check metrics, and run diagnostics without memorizing CLI arguments:
```powershell
run.bat
# Or manually: python run.py
```

### CLI Mode: 1. Extract Sample Dataset
```powershell
python src/create_small_sample.py --input data/raw/orders_huge_mixed_quality.csv --output data/samples/orders_sample_100k.csv --rows 100000
```

### 2. Execute with Small Sample (Routes to Python Batch)
```powershell
python src/main.py --file-path data/samples/orders_sample_100k.csv
```

### 3. Execute with Massive File (Routes to PySpark Parallel Engine)
```powershell
python src/main.py --file-path data/raw/orders_huge_mixed_quality.csv
```

### 4. Verify Idempotency & Zero Duplication
Execute the identical command consecutively:
```powershell
python src/main.py --file-path data/samples/orders_sample_100k.csv
```
Inspect `reports/results.json`:
- `inserted_count`: 0
- `updated_count`: 0
- `unchanged_count`: Equals total valid records
*(No DuplicateKeyError thrown).*

### 5. Run Unit Tests
```powershell
pytest tests/ -v
```

---

## 📊 Output & Metrics Verification

Every execution logs performance and correctness metrics into `reports/results.json`:
```json
{
  "run_id": "9a123126d5124cfdbd8e0d81d54b3c43",
  "engine_used": "pyspark",
  "rows_read": 100000,
  "raw_loaded": 100000,
  "valid_count": 82140,
  "corrected_count": 12860,
  "quarantine_count": 5000,
  "elapsed_seconds": 12.45,
  "throughput": 8032.12,
  "inserted_count": 95000,
  "updated_count": 0,
  "unchanged_count": 0,
  "error_case_counts": {
    "MISSING_ORDER_ID": 1200,
    "CORRUPTED_JSON": 2100,
    "UNPARSEABLE_DATE": 1700
  }
}
```

Mathematical consistency is guaranteed on every run:

$$\mathit{rows\_read} = 100{,}000 = 82{,}140 + 12{,}860 + 5{,}000$$

---

## 📸 Project Showcase & Documentation

For a comprehensive breakdown of the execution flow, architecture, and visual proofs of data quality, please refer to the detailed documentation in the `docs/` directory:

- 📖 [Execution Flow & Visual Proofs](docs/proof_of_execution.md)
- 🏗 [Architecture Specification](docs/architecture.md)
- 🚀 [User Guide](docs/user_guide.md)

*(Example: The pipeline executing a PySpark job on 30M rows)*
![PySpark Execution](screenshots/10_pyspark_30m_terminal.png)

---

# 🌟 Final Project - Phase 2 

This section details the extensions implemented for the final evaluation phase, including practical business queries, compound indexes, execution plan benchmarking, aggregation reports, self-contained materialized views with incremental refresh, scheduled jobs, and a unified evaluation API.

---

## 🚀 Quick Start: Running the Evaluation API

Run the unified FastAPI server with one command:
```powershell
uvicorn src.api:app --host 127.0.0.1 --port 8000 --reload
```

Then open your browser and navigate to the interactive Swagger UI:
👉 **[http://localhost:8000/docs](http://localhost:8000/docs)**

---

## 📡 API Endpoints Specification

All endpoints return well-formed **JSON** and are fully testable via Swagger UI:

| Endpoint | Method | Category | Description |
| :--- | :---: | :---: | :--- |
| **`/health`** | `GET` | System | Checks MongoDB connectivity and displays document counts across all 6 collections. |
| **`/ingest`** | `POST` | Ingestion | Triggers the original ELT pipeline using the existing ingestion gateway. |
| **`/indexes`** | `POST` | Performance | Creates the 3 required indexes on `orders_validated`. |
| **`/indexes/explain`**| `GET` | Performance | Runs `explain("executionStats")` before & after indexes and outputs performance gains. |
| **`/queries`** | `GET` | Queries | Lists the 5 practical business queries and their supported parameters. |
| **`/queries/{name}`** | `GET` | Queries | Executes a specific query by name with dynamic parameters (`limit`, `city`, `status`, etc.). |
| **`/aggregations`** | `GET` | Analytics | Lists the 5 available aggregation reports and their parameters. |
| **`/aggregations/{name}`** | `GET` | Analytics | Executes a specific aggregation report (e.g., `sales_by_city`, `top_products`) and returns live JSON. |
| **`/refresh-mv`** | `POST` | Materialized Views | Executes self-contained incremental refresh for `daily_sales_summary` and `top_products_summary`. |
| **`/views/{name}`** | `GET` | Materialized Views | Reads stored data directly from `daily_sales_summary` or `top_products_summary`. |
| **`/jobs`** | `GET` | Automation | Lists scheduled tasks and displays the database log of their latest execution. |
| **`/jobs/{name}/run`** | `POST` | Automation | Manually triggers any registered task by name and logs results to `job_logs`. |

### 🤖 Quick Automated Evaluation & AI Agent Commands (Curl Cheat Sheet)

If you or an AI Agent are evaluating this repository via terminal or automated scripts:

```powershell
# 1. System Health & Collection Counts
curl.exe -s http://127.0.0.1:8000/health

# 2. Trigger Ingestion (Original ELT Pipeline)
curl.exe -s -X POST http://127.0.0.1:8000/ingest

# 3. Create Required 3 Indexes
curl.exe -s -X POST http://127.0.0.1:8000/indexes

# 4. Run Explain Plan Analysis & Performance Benchmarks
curl.exe -s http://127.0.0.1:8000/indexes/explain

# 5. List Available Queries
curl.exe -s http://127.0.0.1:8000/queries

# 6. Execute Named Query (e.g. City & Status)
curl.exe -s "http://127.0.0.1:8000/queries/orders_by_city_and_status?limit=10"

# 7. List Available Aggregations
curl.exe -s http://127.0.0.1:8000/aggregations

# 8. Execute Named Aggregation (e.g. Top Products)
curl.exe -s http://127.0.0.1:8000/aggregations/top_products

# 9. Execute Incremental Materialized Views Refresh
curl.exe -s -X POST "http://127.0.0.1:8000/refresh-mv?full_refresh=false"

# 10. Read Materialized View Data Directly
curl.exe -s "http://127.0.0.1:8000/views/daily_sales_summary?limit=10"

# 11. List Scheduled Tasks & Latest Execution Logs
curl.exe -s http://127.0.0.1:8000/jobs

# 12. Trigger Manual Job Execution (e.g. refresh_materialized_views)
curl.exe -s -X POST http://127.0.0.1:8000/jobs/refresh_materialized_views/run
```

---


## 🗂️ 1. Queries & Indexes Performance Analysis

### Indexes Created on `orders_validated`:
1. **Compound Index (`idx_val_city_status`)**: Keys: `[("city", 1), ("status", 1)]`
   - *Rationale*: Accelerates localized order filtering, converting costly collection scans (`COLLSCAN`) into direct index lookups (`IXSCAN`).
2. **Chronological Index (`idx_val_order_date`)**: Keys: `[("order_date", -1)]`
   - *Rationale*: Speeds up date-range queries and eliminates in-memory blocking sorts (`SORT -> COLLSCAN`).
3. **Customer Index (`idx_val_customer_id`)**: Keys: `[("customer_id", 1)]`
   - *Rationale*: Enables instant $O(\log N)$ retrieval of purchase histories for specific customers.

### 5 Practical Business Queries Implemented:
1. `orders_by_city_and_status`: Filters orders by geographic location and status.
2. `recent_orders_by_date`: Retrieves recent orders chronologically with date-range filters.
3. `customer_order_history`: Retrieves full order history for a customer ID.
4. `high_value_orders`: Dynamically filters orders exceeding customizable monetary thresholds.
5. `orders_by_payment_method_and_status`: Evaluates orders by payment channels and completion statuses.

### Explain Plan Benchmarking Results:
*Full report available in `reports/explain_results.md` and `reports/explain_results.json`.*

| Query | Target Index | Stage Before | Stage After | Docs Examined (Before $\rightarrow$ After) |
| :--- | :--- | :---: | :---: | :---: |
| **City & Status** | `idx_val_city_status` | `COLLSCAN` | `FETCH -> IXSCAN` | Eliminates full table scan |
| **Date Range & Sort** | `idx_val_order_date` | `SORT -> COLLSCAN` | `FETCH -> IXSCAN` | Eliminates memory sort |
| **Customer Lookup** | `idx_val_customer_id` | `COLLSCAN` | `FETCH -> IXSCAN` | Direct key lookup |

---

## 📊 2. Aggregation Reports (5 Pipelines)

1. **`sales_by_city`**: Aggregates total revenue, order count, and average order value grouped by city.
2. **`top_products`**: Unwinds items array/JSON to determine top-selling SKUs by quantity sold and revenue.
3. **`top_customers`**: Identifies highest-spending VIP customers with total spend and order counts.
4. **`sales_by_period`**: Analyzes daily revenue trends (basis for `daily_sales_summary`).
5. **`orders_by_status`**: Generates a matrix of orders grouped by operational status and payment method.

---

## 🔄 3. Materialized Views & Incremental Refresh

Two self-contained Materialized Views are maintained directly in MongoDB:
1. **`daily_sales_summary`**: Daily rollups of revenue, order volume, and average order size.
2. **`top_products_summary`**: Product-level rollups of total quantity sold, revenue, and order occurrences.

### Incremental Refresh Mechanism:
- Views track their own watermark via a per-document `last_updated_at` field.
- When `POST /refresh-mv` is invoked, only orders processed after the latest watermark (`metadata.processed_at > watermark`) are fetched.
- Atomic `UpdateOne(..., upsert=True)` and `$inc` operators merge new increments without dropping or rebuilding historical data from scratch.

---

## ⏰ 4. Scheduled Jobs & Execution Logging

Two automated background tasks run periodically and can also be triggered on-demand via `POST /jobs/{name}/run`:
1. **`refresh_materialized_views`**: Periodic incremental refresh of materialized views (Runs every 10 min).
2. **`generate_periodic_metrics`**: Compiles periodic executive analytics reports (Runs every 30 min).

### Execution Audit Logging (`job_logs`):
Every execution logs an immutable document in MongoDB `job_logs`:
- `job_name`, `trigger_type` (`SCHEDULED` vs `MANUAL`)
- `start_time`, `end_time`, `elapsed_ms`
- `status` (`SUCCESS` or `FAILED`)
- `result` / `error_message`

---

## 🧪 Verification & Testing Scripts

Run the standalone verification suites in PowerShell:
```powershell
# 1. Test Queries, Indexes, and Explain Plan
python tests/test_indexes_and_queries.py

# 2. Test Aggregations, Materialized Views, and Scheduled Jobs
python tests/test_aggregations_and_views.py

# 3. Run all unit and integration tests with PyTest
pytest tests/ -v
```


