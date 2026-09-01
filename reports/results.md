# Pipeline Execution Summary Report

- **Run ID**: `2b970c93488c46818c1e463e8dfbaab2`
- **Timestamp**: `2026-08-30T00:25:29Z`
- **Input File**: `orders_sample_10k.csv` (4.17 MB)
- **Engine Used**: `python_batch`

---

## ⚡ Performance Metrics
- **Elapsed Time**: `4.478 s`
- **Throughput**: `2233.02 rows/s`

---

## 📊 Classification Statistics
| Category | Count | Percentage |
| :--- | :--- | :--- |
| **Raw Loaded (`run_raw_count`)** | `10,000` | 100.0% |
| **Valid (`run_valid_count`)** | `8,133` | `81.33%` |
| **Corrected (`run_corrected_count`)** | `1,359` | `13.59%` |
| **Quarantine (`run_quarantine_count`)** | `508` | `5.08%` |

---

## 🔒 Consistency Check
- **Formula**: `run_raw_count == run_valid_count + run_corrected_count + run_quarantine_count`
- **Status**: `PASSED ✅`
- **Raw Count**: `10,000`
- **Classified Sum**: `10,000`
