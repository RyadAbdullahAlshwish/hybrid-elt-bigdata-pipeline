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

1. **Intelligent File Router**: Dynamically selects between **Python Batch Loader** ($\le 200$ MB) for low-overhead streaming ingestion and **PySpark Parallel Engine** ($> 200$ MB) using the official `mongo-spark-connector`.
2. **Pure ELT Paradigm**: Ingests 100% of untransformed source records into MongoDB `orders_raw` before applying business logic.
3. **8 Automated Quality Rules & Audit Trail**: Cleans numbers (Arabic/Hindi conversion, comma separators), strips textual currencies to standard `YER`, standardizes payment statuses, repairs contacts, and tracks modifications in a dedicated `corrections` array.
4. **Classification & Quarantine Engine**: Segregates records into `VALID`, `CORRECTED`, and `QUARANTINE` using standardized business error codes (`MISSING_ORDER_ID`, `CORRUPTED_JSON`, etc.).
5. **Database-Level Schema Enforcement**: Employs MongoDB `$jsonSchema` validation rules alongside a unique compound index on `order_id` in `orders_validated`.
6. **Strict Idempotency via Bulk Upserts**: Utilizes atomic `bulk_write` operations with upserts to eliminate duplicate records during repeated executions, tracking `inserted_count`, `updated_count`, and `unchanged_count`.
7. **Mathematical Consistency Rule**:
   $$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$
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

### 8 Automated Cleaning Rules
1. **Numeric Normalization**: Converts Eastern Arabic and Perso-Arabic numerals to standard Western digits.
2. **Separator Cleansing**: Removes thousand separators and invalid punctuation from numeric fields.
3. **Currency Extraction**: Strips embedded textual currency markers from pricing figures.
4. **Textual Digits Translation**: Translates written Arabic textual numbers into integer representations.
5. **Sign Correction**: Corrects accidental negative pricing and quantity values to absolute figures.
6. **Currency Standardization**: Normalizes diverse regional currency strings to the uniform code `YER`.
7. **Status Mapping**: Standardizes order lifecycle statuses into uniform system states.
8. **Contact Cleansing**: Fixes malformed email syntaxes (extra spaces, double `@`) and normalizes international phone number prefixes.

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

### 1. Extract Sample Dataset
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
$$\text{rows\_read} = 100{,}000 = 82{,}140 + 12{,}860 + 5{,}000$$
