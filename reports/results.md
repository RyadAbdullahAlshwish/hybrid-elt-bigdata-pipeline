# Pipeline Execution Summary Report

- **Run ID**: `6012c0b36cb54859b482cd20bde445bd`
- **Timestamp**: `2026-09-07T02:06:09Z`
- **Input File**: `orders_sample_100k.csv` (41.77 MB)
- **Engine Used**: `python_batch`

---

## ⚡ Performance Metrics
- **Elapsed Time**: `34.271 s`
- **Throughput**: `2917.9 rows/s`
- **Batch Size**: `1000`

---

## 📊 Classification Statistics
| Category | Count | Percentage |
| :--- | :--- | :--- |
| **Total Raw** | `100000` | 100% |
| ✅ **Valid** | `79457` | `79.46%` |
| 🛠️ **Corrected** | `15639` | `15.64%` |
| 🚨 **Quarantine** | `4904` | `4.9%` |

---

## 🚫 Error Case Counts (Quarantine Reasons)
```json
{
  "MISSING_CUSTOMER_ID": 1411,
  "CORRUPTED_ITEMS_JSON": 1338,
  "UNKNOWN_PRICE": 674,
  "MULTIPLE_CONFLICTING_ERRORS": 674,
  "UNRESOLVED_CRITICAL_FIELD": 1431,
  "MISSING_ORDER_ID": 721,
  "EMPTY_ITEMS": 677
}
```

---

## 🔄 Idempotent Upsert Statistics
- **Inserted (`inserted_count`)**: `0`
- **Updated (`updated_count`)**: `95,096`
- **Unchanged (`unchanged_count`)**: `0`

---

## 🔒 Consistency Check
- **Status**: `PASSED ✅`
- **Raw Count**: `100,000`
- **Classified Sum**: `100,000`
