# Architecture & Pipeline Design Documentation

## Overview

This project implements a production-ready **ELT Data Pipeline** for Big Data processing (Lecture 5 Homework & Midterm Project).
The pipeline ingests raw, dirty e-commerce order records from CSV files into **MongoDB**, cleans and transforms the data using deterministic rules, logs audit trails for all modifications, and classifies records into validated vs quarantine collections.

---

## 🏗️ 6-Stage Pipeline Architecture

```text
                     [ Dirty CSV Input File ]
                                │
                                ▼
                   [ 1. File Router & Discovery ]
             (Inspects file size and generates unique run_id)
                                │
         ┌──────────────────────┴──────────────────────┐
         ▼                                             ▼
  [ Size <= 200MB ]                             [ Size > 200MB ]
         │                                             │
         ▼                                             ▼
 [ Python Batch Loader ]                       [ PySpark Loader ]
 (Streaming read, batch insert)               (Distributed processing)
         │                                             │
         └──────────────────────┬──────────────────────┘
                                │
                                ▼
                       [ 2. orders_raw ]
          (Stores raw records untouched - Core ELT)
                                │
                                ▼
              [ 3. Quality, Cleaning & Audit Trail ]
          (8 cleaning rules + Audit Trail corrections log)
                                │
                                ▼
                       [ 4. Classification ]
                                │
         ┌──────────────────────┴──────────────────────┐
         ▼                                             ▼
 [ Valid & Corrected Records ]                [ Uncorrectable Records ]
 (Clean or successfully fixed)               (Missing IDs, bad JSON, ???)
         │                                             │
         ▼ (Idempotent Upsert)                         ▼ (Insert)
 [ 5. orders_validated ]                     [ 5. orders_quarantine ]
 (Unique Index on order_id)                  (Includes quarantine_reasons)
                                │
                                ▼
              [ 6. Metrics & reports/results.json ]
  (Performance metrics + Mandatory Mathematical Consistency Rule check)
```

---

## 📋 Data Quality Rules & Audit Trail

| Rule Code | Description | Example Transformation |
| :--- | :--- | :--- |
| `R1_NORMALIZE_DIGITS` | Normalize Eastern Arabic / Persian digits | `'٧٠٦٠٠٠٫٠'` $\rightarrow$ `'706000.0'` |
| `R2_REMOVE_THOUSANDS_SEPARATOR` | Remove commas in numbers | `'135,000.00'` $\rightarrow$ `'135000.00'` |
| `R3_STRIP_CURRENCY_TEXT` | Strip currency suffixes / symbols | `'54000.00 ريال'` $\rightarrow$ `'54000.00'` |
| `R4_ARABIC_WORDS_CONVERSION` | Convert Arabic number words | `'ألفان'` $\rightarrow$ `'2000.0'` |
| `R5_ABS_NEGATIVE_VALUE` | Convert negative monetary amounts | `'-21500.0'` $\rightarrow$ `'21500.0'` |
| `R6_CURRENCY_NORMALIZATION` | Standardize currency codes | `'ريال يمني'` $\rightarrow$ `'YER'` |
| `R7_STATUS_NORMALIZATION` | Standardize status strings | `'مدفوع'` $\rightarrow$ `'تم الدفع'` |
| `R8_CONTACT_FORMAT_CLEANING` | Clean double `@` / double dots | `'user@@example..com'` $\rightarrow$ `'user@example.com'` |

---

## 🔒 Mandatory Mathematical Consistency Rule

Every pipeline run verifies the following formula:

$$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$

The result of this check is exported to `reports/results.json` and `reports/results.md`.

---

# 🌟 Phase 2: Analytics, Indexes, Materialized Views & API Architecture

While Phase 1 guarantees data ingestion purity, cleaning audit trails, and classification, **Phase 2** elevates the system into a high-performance analytical and serving platform. It enables sub-millisecond query execution, live aggregations, self-contained materialized views, scheduled automation, and evaluation via a standardized RESTful API.

```text
========================================================================================================
                                     END-TO-END SYSTEM TOPOLOGY
========================================================================================================

  [ Raw Data Sources (CSV) ]
             │
             ▼
  ┌──────────────────────────────────────────────────────────┐
  │ PHASE 1: Hybrid Ingestion & Transformation Engine        │
  │ • File Router (Python Batch <= 200MB / PySpark > 200MB)  │
  │ • 9 Automated Cleaning Rules & Audit Trail (corrections) │
  │ • Classification & Idempotent Upsert Engine              │
  └────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
  ┌─────────────────────────────────────────────────────────────────────────────────────────┐
  │                         MONGODB MULTI-COLLECTION REPOSITORY                             │
  │                                                                                         │
  │  [ orders_raw ]          [ orders_quarantine ]         [ orders_validated ]             │
  │  (Raw audit copy)        (Unfixable defects)          (Validated & enriched master)     │
  │                                                                │                        │
  │                                           ┌────────────────────┴────────────────────┐   │
  │                                           ▼                                         ▼   │
  │                                [ daily_sales_summary ]                   [ top_products_summary ]│
  │                                (Materialized View #1)                    (Materialized View #2) │
  │                                           │                                         │   │
  │                                           └────────────────────┬────────────────────┘   │
  │                                                                ▼                        │
  │                                                          [ job_logs ]                   │
  │                                                    (Audit log for scheduled jobs)       │
  └────────────────────────────────────────────────────────────────┬────────────────────────┘
                                                                   │
                               ▼───────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────────────────────────────────┐
  │ PHASE 2: Performance, Analytics & Automation Engines                                    │
  │                                                                                         │
  │  1. INDEXING ENGINE               2. AGGREGATIONS               3. MATERIALIZED VIEWS   │
  │  • idx_val_city_status (Compound) • sales_by_city               • Watermark-based       │
  │  • idx_val_order_date             • top_products                  incremental refresh   │
  │  • idx_val_customer_id            • top_customers               • Zero historical       │
  │  • Explain Plan Benchmarking      • sales_by_period               recalculation         │
  │    (98.3% - 100% doc reduction)   • orders_by_status            • Bulk Upsert engine    │
  │                                                                                         │
  │  4. BACKGROUND SCHEDULER                                                                │
  │  • Thread daemon with lifespan management                                               │
  │  • Job 1: refresh_materialized_views (Every 10 min)                                     │
  │  • Job 2: generate_periodic_metrics (Every 30 min)                                      │
  │  • Immutable database logging in job_logs collection                                    │
  └────────────────────────────────────────────────────────────────┬────────────────────────┘
                                                                   │
                                                                   ▼
  ┌─────────────────────────────────────────────────────────────────────────────────────────┐
  │ UNIFIED FASTAPI EVALUATION GATEWAY (Port 8000)                                          │
  │ • 10 REST Endpoints with OpenAPI / Swagger UI (/docs)                                   │
  │ • Full CORS support & Strict Enum Dropdowns for easy evaluation                         │
  │ • Live JSON responses for all queries, aggregations, views, and scheduled jobs          │
  └─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🗄️ 1. Multi-Collection Storage Architecture

The MongoDB repository maintains 6 distinct collections, each with strict isolation of concern:

| Collection Name | Storage Role | Key Indexes | Lifecycle |
| :--- | :--- | :--- | :--- |
| **`orders_raw`** | Raw staging storage | `_id` | Preserved untouched for compliance |
| **`orders_validated`** | Cleaned operational master | `order_id` (Unique), `(city, status)` (Compound), `order_date`, `customer_id` | Continually updated via idempotent upserts |
| **`orders_quarantine`** | Defective records quarantine | `metadata.quarantined_at` | Isolated for review with `error_codes` |
| **`daily_sales_summary`** | Materialized daily rollup | `date` (Unique), `last_updated_at` | Updated incrementally via watermark |
| **`top_products_summary`** | Materialized product totals | `sku` (Unique), `last_updated_at` | Updated incrementally via delta merge |
| **`job_logs`** | Execution audit history | `start_time` (-1), `job_name` | Immutable append-only audit trail |

---

## ⚡ 2. Indexing Engine & Explain Plan Architecture

To ensure linear scalability under Big Data workloads, three specific index structures are engineered on `orders_validated`:

### 1. Compound Index: `idx_val_city_status` `[(city, 1), (status, 1)]`
- **Objective:** Eliminates $O(N)$ full collection scans (`COLLSCAN`) for multi-attribute geographic and operational filtering.
- **Mechanism:** MongoDB navigates an ordered B-Tree index by `city` prefix first, then matches `status`, performing a direct `FETCH -> IXSCAN`.
- **Benchmark:** Reduced examined documents from **94,447** to **1,592** (**98.31% reduction**), dropping response time from 59ms to 4ms.

### 2. Chronological Index: `idx_val_order_date` `[(order_date, -1)]`
- **Objective:** Optimizes date-range scans and eliminates memory-intensive blocking sorts.
- **Mechanism:** Avoids the `SORT` stage in the MongoDB query planner by reading pre-sorted index keys directly.

### 3. Customer History Index: `idx_val_customer_id` `[(customer_id, 1)]`
- **Objective:** Provides instant $O(\log N)$ point lookups for individual customer order histories.
- **Benchmark:** Examined exactly **1 document** instead of 94,447 (**100.0% reduction**), executing in under 1ms.

---

## 📊 3. Analytical Aggregation Pipelines

Five aggregation pipelines run directly within the database engine using native pipeline operators:

1. **`sales_by_city`**: `$match` $\rightarrow$ `$addFields` (type coercion) $\rightarrow$ `$group` by city $\rightarrow$ `$project` $\rightarrow$ `$sort` by revenue.
2. **`top_products`**: `$unwind` on product items $\rightarrow$ `$group` by SKU $\rightarrow$ calculate sum of quantities and revenues $\rightarrow$ `$sort` by quantity sold.
3. **`top_customers`**: Groups by `customer_id`, calculates total lifetime spend, order volume, and identifies VIP customers.
4. **`sales_by_period`**: Grouping by extracted date keys (`$substrBytes`), providing chronological daily trend summaries.
5. **`orders_by_status`**: Multi-dimensional matrix grouping orders by operational `status`, `payment_method`, and `payment_status`.

---

## 🔄 4. Materialized Views: Self-Contained Incremental Refresh

Traditional Materialized Views require full table rebuilds or auxiliary metadata tables. This system implements a **self-contained watermark-based incremental refresh**:

1. **Watermark Extraction:** Each document in the materialized view stores a `last_updated_at` ISO-8601 timestamp. The latest watermark is determined directly:
   $$\text{Watermark} = \max(\text{last\_updated\_at}) \quad \forall \text{ docs in view}$$
2. **Delta Identification:**
   $$\text{New Orders} = \{ d \in \text{orders\_validated} \mid d.\text{metadata.processed\_at} > \text{Watermark} \}$$
3. **Targeted Aggregation:**
   - For `daily_sales_summary`: Only unique dates affected by newly arrived orders are recalculated and updated via bulk upsert (`UpdateOne(..., upsert=True)`).
   - For `top_products_summary`: SKU metrics are incrementally merged using atomic upserts.
4. **Zero Penalty:** If no new orders have been ingested since the last watermark, the refresh returns immediately in 0ms with zero document writes.

---

## ⏱️ 5. Background Scheduler & Job Audit Architecture

A background thread daemon managed by FastAPI's `asynccontextmanager` lifespan oversees periodic maintenance:

- **Thread Daemon:** Executes jobs in a non-blocking background thread pool without impacting HTTP request responsiveness.
- **Audit Logging (`job_logs`):** Every task execution (whether triggered automatically by the schedule or manually via the API) writes an immutable log document containing `run_id`, `trigger_type` (`SCHEDULED` vs `MANUAL`), `start_time`, `end_time`, `elapsed_ms`, `status`, and complete execution payload.

---

## 🌐 6. Unified Evaluation API (FastAPI)

The API layer is built on **FastAPI** and **Uvicorn**, providing:
- **10 Core Endpoints** covering health, ingestion, indexes, explain plan, queries, aggregations, materialized views, and scheduled jobs.
- **OpenAPI 3.1 & Interactive Swagger UI** at `/docs`.
- **CORS Middleware (`CORSMiddleware`)** allowing full cross-origin evaluation from external web clients.
- **Enum Dropdowns** for all path parameters (`QueryName`, `AggregationName`, `MaterializedViewName`, `JobName`), preventing human typos during evaluation.

