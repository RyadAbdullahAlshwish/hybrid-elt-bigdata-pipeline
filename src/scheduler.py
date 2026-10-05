"""
Scheduled Jobs and Task Runner Module.
Phase 2 - Requirements 4: 2 Scheduled Jobs, Manual Execution, and Database Execution Logs.
"""

import time
import threading
from typing import Any, Dict, List, Optional
from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
)
from src.materialized_views import refresh_materialized_views
from src.aggregations import execute_aggregation

JOB_LOGS_COLLECTION = "job_logs"


def get_db(db_instance=None):
    """Return database instance or create client from settings."""
    if db_instance is not None:
        return db_instance
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


def _clean_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to convert ObjectId for clean JSON output."""
    if not doc:
        return doc
    clean = dict(doc)
    if "_id" in clean and not isinstance(clean["_id"], (str, int, float, dict)):
        clean["_id"] = str(clean["_id"])
    return clean


# -----------------------------------------------------------------------------
# Job Implementations
# -----------------------------------------------------------------------------

def _execute_refresh_views_task(db) -> Dict[str, Any]:
    """Task 1: Incrementally refresh both Materialized Views."""
    return refresh_materialized_views(full_refresh=False, db=db)


def _execute_daily_metrics_task(db) -> Dict[str, Any]:
    """
    Task 2: Generate system-wide summary report
    combining sales trends and order status distributions.
    """
    period_res = execute_aggregation("sales_by_period", {"limit": 7}, db=db)
    status_res = execute_aggregation("orders_by_status", {"limit": 10}, db=db)
    
    total_sales_7d = sum(p.get("total_revenue", 0.0) for p in period_res.get("results", []))
    total_orders_7d = sum(p.get("orders_count", 0) for p in period_res.get("results", []))

    return {
        "report_title": "التقرير الإحصائي الدوري للمنظومة",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seven_day_summary": {
            "total_revenue": round(total_sales_7d, 2),
            "total_orders": total_orders_7d
        },
        "recent_daily_trends": period_res.get("results", []),
        "status_distribution": status_res.get("results", [])
    }


# -----------------------------------------------------------------------------
# Registered Jobs Registry
# -----------------------------------------------------------------------------
REGISTERED_JOBS = {
    "refresh_materialized_views": {
        "name": "refresh_materialized_views",
        "title": "مهمة التحديث التزايدي للعروض المادية",
        "description": "تحديث تزايدي تلقائي لجدولي daily_sales_summary و top_products_summary لجلب السجلات الجديدة فقط.",
        "schedule_interval_seconds": 600,  # Runs every 10 minutes
        "schedule_human": "كل 10 دقائق تلقائياً",
        "handler": _execute_refresh_views_task
    },
    "generate_periodic_metrics": {
        "name": "generate_periodic_metrics",
        "title": "مهمة إعداد التقرير الإحصائي الدوري",
        "description": "استخراج وتجميع إحصائيات المبيعات الأسبوعية وتوزيع حالات الطلبات وتحديث التقارير التحليلية.",
        "schedule_interval_seconds": 1800,  # Runs every 30 minutes
        "schedule_human": "كل 30 دقيقة تلقائياً",
        "handler": _execute_daily_metrics_task
    }
}


# -----------------------------------------------------------------------------
# Unified Job Runner with Execution Logging
# -----------------------------------------------------------------------------
def run_job(
    job_name: str,
    trigger_type: str = "MANUAL",
    db=None
) -> Dict[str, Any]:
    """
    Execute a registered job by name and log execution details into job_logs collection.
    
    Args:
        job_name: Identifier of the registered job.
        trigger_type: 'MANUAL' or 'SCHEDULED'.
        db: Optional database instance.
    
    Returns:
        Dict containing execution metrics, logs, and status.
    """
    if job_name not in REGISTERED_JOBS:
        available = list(REGISTERED_JOBS.keys())
        raise ValueError(f"Unknown job '{job_name}'. Available jobs: {available}")

    database = get_db(db)
    logs_col = database[JOB_LOGS_COLLECTION]
    job_meta = REGISTERED_JOBS[job_name]

    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    start_time = time.perf_counter()

    status = "SUCCESS"
    result_data = None
    error_msg = None

    try:
        # Run the job handler
        handler = job_meta["handler"]
        result_data = handler(database)
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        result_data = {"error": str(e)}

    end_time = time.perf_counter()
    end_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    elapsed_ms = round((end_time - start_time) * 1000, 2)

    # Record log document in MongoDB
    log_doc = {
        "job_name": job_name,
        "job_title": job_meta["title"],
        "trigger_type": trigger_type,
        "start_time": start_iso,
        "end_time": end_iso,
        "elapsed_ms": elapsed_ms,
        "status": status,
        "result": result_data,
        "error_message": error_msg
    }

    inserted = logs_col.insert_one(log_doc)
    log_doc["_id"] = str(inserted.inserted_id)

    return {
        "status": status,
        "job_name": job_name,
        "trigger_type": trigger_type,
        "start_time": start_iso,
        "end_time": end_iso,
        "elapsed_ms": elapsed_ms,
        "log_id": log_doc["_id"],
        "result": result_data,
        "error_message": error_msg
    }


# -----------------------------------------------------------------------------
# Jobs Status for GET /jobs
# -----------------------------------------------------------------------------
def get_jobs_status(db=None) -> List[Dict[str, Any]]:
    """
    Retrieve overview of all registered jobs and their latest execution logs.
    Feeds the GET /jobs API endpoint.
    """
    database = get_db(db)
    logs_col = database[JOB_LOGS_COLLECTION]
    jobs_summary = []

    for name, meta in REGISTERED_JOBS.items():
        # Find latest log for this job
        latest_log = logs_col.find_one(
            {"job_name": name},
            sort=[("start_time", -1)]
        )

        job_info = {
            "job_name": name,
            "title": meta["title"],
            "description": meta["description"],
            "schedule": meta["schedule_human"],
            "schedule_interval_seconds": meta["schedule_interval_seconds"],
            "last_execution": _clean_doc(latest_log) if latest_log else None
        }
        jobs_summary.append(job_info)

    return jobs_summary


# -----------------------------------------------------------------------------
# Background Scheduler Engine (Pure Python Daemon)
# -----------------------------------------------------------------------------
class BackgroundTaskScheduler:
    """Lightweight background thread scheduler without external service dependencies."""
    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_run: Dict[str, float] = {}

    def _loop(self):
        while self._running:
            now = time.time()
            for name, meta in REGISTERED_JOBS.items():
                interval = meta["schedule_interval_seconds"]
                last = self._last_run.get(name, 0.0)
                if now - last >= interval:
                    try:
                        self._last_run[name] = now
                        run_job(name, trigger_type="SCHEDULED")
                    except Exception as e:
                        print(f"⚠️ Scheduler error in job '{name}': {e}")
            time.sleep(10)  # Check every 10 seconds

    def start(self):
        """Start the background scheduler thread."""
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._loop, daemon=True, name="BigDataSchedulerThread")
            self._thread.start()
            print("🚀 Background Task Scheduler started successfully.")

    def stop(self):
        """Stop the background scheduler."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        print("🛑 Background Task Scheduler stopped.")


# Global singleton instance
scheduler_instance = BackgroundTaskScheduler()


def start_scheduler():
    """Start global scheduler daemon."""
    scheduler_instance.start()


def stop_scheduler():
    """Stop global scheduler daemon."""
    scheduler_instance.stop()
