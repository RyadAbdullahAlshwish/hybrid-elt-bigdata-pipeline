# Big Data Midterm ELT Pipeline Project

An end-to-end Big Data **ELT Data Pipeline** built with Python, PySpark, and MongoDB according to the 6-stage architecture specified for Lecture 5 Homework & Midterm.

---

## 🚀 Key Features

1. **File Router & Discovery**: Automatically selects between **Python Batch Loader** ($\le 200$ MB) and **PySpark Loader** ($> 200$ MB).
2. **Pure ELT Strategy**: Ingests 100% of raw data into MongoDB `orders_raw` without initial data drop.
3. **8 Data Quality Rules & Audit Trail**: Cleans Arabic/Persian digits, thousands separators, currency symbols, spelled-out words, negative values, statuses, and emails/phones while recording full `corrections` history.
4. **Classification & Quarantine**: Routes clean/fixed records to `orders_validated` and defective records to `orders_quarantine` with clear error codes.
5. **Idempotent Upsert**: Employs a Unique Index on `order_id` in `orders_validated` to ensure zero duplication on pipeline re-runs.
6. **Mathematical Consistency Rule Verification**:
   $$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$

---

## 📁 Directory Structure

```text
├── config/
│   └── settings.py          # Configuration parameters & thresholds
├── data/
│   ├── raw/                 # Large datasets
│   └── samples/             # Sample datasets for testing
├── docs/
│   └── architecture.md      # Full architecture & design documentation
├── notebooks/
│   ├── 01_data_inspection.ipynb
│   └── 02_data_profiling.ipynb
├── reports/
│   ├── results.json         # Execution metrics report
│   └── results.md           # Markdown execution summary report
├── src/
│   ├── batch_loader.py      # Python streaming batch loader
│   ├── spark_loader.py      # PySpark loader
│   ├── file_router.py       # Engine router & run_id generator
│   ├── quality_rules.py     # 8 Data quality rules & audit trail
│   ├── classification.py    # Classification into Valid/Corrected vs Quarantine
│   ├── mongo_setup.py       # MongoDB setup & unique index creation
│   ├── metrics.py           # Metrics calculation & report generator
│   ├── elt_pipeline.py      # Master ELT pipeline orchestrator
│   └── main.py              # CLI Entry point
└── tests/
    ├── test_cleaning_rules.py
    └── test_classification.py
```

---

## 💻 Running the Pipeline

### Run Unit Tests
```powershell
pytest tests/ -v
```

### Execute ELT Pipeline
```powershell
python src/main.py --file-path data/samples/orders_sample_10k.csv
```
