import json
import time
from pathlib import Path
from typing import Any, Dict

from config.settings import REPORTS_DIR


def generate_pipeline_metrics(
    run_id: str,
    file_name: str,
    file_size_mb: float,
    engine_used: str,
    rows_read: int,
    raw_loaded: int,
    valid_count: int,
    corrected_count: int,
    quarantine_count: int,
    elapsed_seconds: float,
    upsert_inserted: int = 0,
    upsert_modified: int = 0,
    upsert_unchanged: int = 0,
    error_case_counts: Dict[str, int] = None,
) -> Dict[str, Any]:
    """
    Generate pipeline metrics and verify the mandatory acceptance equation:
    run_raw_count == run_valid_count + run_corrected_count + run_quarantine_count
    """
    throughput = round(raw_loaded / elapsed_seconds, 2) if elapsed_seconds > 0 else 0.0

    # Mandatory Mathematical Consistency Check
    expected_sum = valid_count + corrected_count + quarantine_count
    is_consistent = (raw_loaded == expected_sum)

    metrics = {
        "run_id": run_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input_file": {
            "name": file_name,
            "size_mb": file_size_mb,
            "engine_used": engine_used,
        },
        "performance": {
            "elapsed_seconds": round(elapsed_seconds, 3),
            "throughput_rows_per_sec": throughput,
        },
        "counts": {
            "rows_read": rows_read,
            "run_raw_count": raw_loaded,
            "run_valid_count": valid_count,
            "run_corrected_count": corrected_count,
            "run_quarantine_count": quarantine_count,
        },
        "upsert_stats": {
            "inserted_count": upsert_inserted,
            "updated_count": upsert_modified,
            "unchanged_count": upsert_unchanged,
        },
        "consistency_check": {
            "formula": "run_raw_count == run_valid_count + run_corrected_count + run_quarantine_count",
            "is_consistent": is_consistent,
            "raw_count": raw_loaded,
            "classified_sum": expected_sum,
            "difference": raw_loaded - expected_sum,
        },
        "error_case_counts": error_case_counts or {},
    }

    return metrics


def save_pipeline_reports(metrics: Dict[str, Any]) -> None:
    """Save execution report to reports/results.json and reports/results.md."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. JSON Report
    json_path = REPORTS_DIR / "results.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    # 2. Markdown Report
    md_path = REPORTS_DIR / "results.md"
    md_content = f"""# Pipeline Execution Summary Report

- **Run ID**: `{metrics['run_id']}`
- **Timestamp**: `{metrics['timestamp']}`
- **Input File**: `{metrics['input_file']['name']}` ({metrics['input_file']['size_mb']} MB)
- **Engine Used**: `{metrics['input_file']['engine_used']}`

---

## ⚡ Performance Metrics
- **Elapsed Time**: `{metrics['performance']['elapsed_seconds']} s`
- **Throughput**: `{metrics['performance']['throughput_rows_per_sec']} rows/s`

---

## 📊 Classification Statistics
| Category | Count | Percentage |
| :--- | :--- | :--- |
| **Raw Loaded (`run_raw_count`)** | `{metrics['counts']['run_raw_count']:,}` | 100.0% |
| **Valid (`run_valid_count`)** | `{metrics['counts']['run_valid_count']:,}` | `{metrics['counts']['run_valid_count'] / max(metrics['counts']['run_raw_count'], 1) * 100:.2f}%` |
| **Corrected (`run_corrected_count`)** | `{metrics['counts']['run_corrected_count']:,}` | `{metrics['counts']['run_corrected_count'] / max(metrics['counts']['run_raw_count'], 1) * 100:.2f}%` |
| **Quarantine (`run_quarantine_count`)** | `{metrics['counts']['run_quarantine_count']:,}` | `{metrics['counts']['run_quarantine_count'] / max(metrics['counts']['run_raw_count'], 1) * 100:.2f}%` |

---

## 🚫 Error Case Counts (Quarantine Reasons)
```json
{json.dumps(metrics.get("error_case_counts", {}), indent=2, ensure_ascii=False)}
```

---

## 🔄 Idempotent Upsert Statistics
- **Inserted (`inserted_count`)**: `{metrics['upsert_stats']['inserted_count']:,}`
- **Updated (`updated_count`)**: `{metrics['upsert_stats']['updated_count']:,}`
- **Unchanged (`unchanged_count`)**: `{metrics['upsert_stats']['unchanged_count']:,}`

---

## 🔒 Consistency Check
- **Status**: `{"PASSED ✅" if metrics["consistency_check"]["is_consistent"] else "FAILED ❌"}`
- **Raw Count**: `{metrics['consistency_check']['raw_count']:,}`
- **Classified Sum**: `{metrics['consistency_check']['classified_sum']:,}`
"""
    with md_path.open("w", encoding="utf-8") as f:
        f.write(md_content)

    print("\n" + "=" * 60)
    print(f"Report saved to: {json_path}")
    print(f"Report saved to: {md_path}")
    print("=" * 60)
