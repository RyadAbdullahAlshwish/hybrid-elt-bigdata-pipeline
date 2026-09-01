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
