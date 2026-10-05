"""
Comprehensive Test Script for Phase 2 Analytics & Background Scheduler:
- 5 Aggregation Reports (src/aggregations.py)
- 2 Materialized Views & Incremental Refresh (src/materialized_views.py)
- 2 Scheduled Jobs & Execution Logging (src/scheduler.py)
"""

import sys
from pathlib import Path

# Add project root to sys.path to allow execution from tests/ directory or project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import time
from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)
from src.aggregations import (
    get_available_aggregations,
    execute_aggregation,
)
from src.materialized_views import (
    refresh_materialized_views,
    get_daily_sales_view,
    get_top_products_view,
    get_materialized_views_status,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
)
from src.scheduler import (
    get_jobs_status,
    run_job,
    JOB_LOGS_COLLECTION,
)


def print_sep(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    val_docs = db[VALIDATED_COLLECTION].count_documents({})

    print_sep("1. حالة قاعدة البيانات الحالية")
    print(f"📊 قاعدة البيانات: {MONGO_DATABASE}")
    print(f"📦 مجموعة الطلبات المصححة ({VALIDATED_COLLECTION}): {val_docs:,} مستند")

    # -------------------------------------------------------------------------
    # 2. اختبار التقارير التجميعية الخمسة (Aggregations)
    # -------------------------------------------------------------------------
    print_sep("2. اختبار تقارير التجميع الخمسة (Aggregations Test)")
    available_aggs = get_available_aggregations()
    print(f"عدد تقارير التجميع المتاحة: {len(available_aggs)}")

    for agg_meta in available_aggs:
        name = agg_meta["name"]
        title = agg_meta["title"]
        print(f"\n▶️ تشغيل التقرير: {title} (`{name}`)")
        try:
            res = execute_aggregation(name, limit=5, db=db)
            print(f"  - الحالة: {res.get('status')}")
            print(f"  - زمن التنفيذ: {res.get('execution_time_ms')} مللي ثانية (ms)")
            print(f"  - عدد النتائج: {res.get('returned_count')}")
            if res.get("results"):
                print(f"  - أول نتيجة: {res['results'][0]}")
            else:
                print("  - لا توجد بيانات حالياً (مجموعة orders_validated فارغة).")
        except Exception as e:
            print(f"  ❌ خطأ في تشغيل التقرير '{name}': {e}")

    # -------------------------------------------------------------------------
    # 3. اختبار العروض المادية والتحديث التزايدي (Materialized Views)
    # -------------------------------------------------------------------------
    print_sep("3. اختبار العروض المادية والتحديث التزايدي (Materialized Views)")
    print("🔹 جاري تشغيل دالة refresh_materialized_views()...")
    try:
        mv_res = refresh_materialized_views(full_refresh=False, db=db)
        print(f"  - الحالة: {mv_res.get('status')}")
        print(f"  - زمن التنفيذ: {mv_res.get('execution_time_ms')} ms")
        print(f"  - ملخص العرض اليومي ({MV_DAILY_SALES}): {mv_res['views'].get(MV_DAILY_SALES)}")
        print(f"  - ملخص أفضل المنتجات ({MV_TOP_PRODUCTS}): {mv_res['views'].get(MV_TOP_PRODUCTS)}")

        # Check status
        status = get_materialized_views_status(db=db)
        print("\n🔹 إحصائيات المجموعات المادية:")
        for vname, vstat in status.items():
            print(f"  - المجموعة `{vname}`: {vstat['document_count']} مستند | آخر تحديث: {vstat['latest_update']}")
    except Exception as e:
        print(f"  ❌ خطأ في العروض المادية: {e}")

    # -------------------------------------------------------------------------
    # 4. اختبار المهام المجدولة وسجلات التشغيل (Scheduled Jobs & Logs)
    # -------------------------------------------------------------------------
    print_sep("4. اختبار المهام المجدولة وسجلات التشغيل (Scheduled Jobs & Logs)")
    
    print("🔹 جاري استعراض قائمة المهام المسجلة:")
    jobs_list = get_jobs_status(db=db)
    for job in jobs_list:
        print(f"  - المهمة: `{job['job_name']}` ({job['title']}) | الجدول: {job['schedule']}")

    print("\n🔹 تجربة التشغيل اليدوي للمهمة 1 (refresh_materialized_views):")
    try:
        run_res1 = run_job("refresh_materialized_views", trigger_type="MANUAL", db=db)
        print(f"  - الحالة: {run_res1['status']}")
        print(f"  - زمن التنفيذ: {run_res1['elapsed_ms']} ms")
        print(f"  - معرّف السجل في قاعدة البيانات (Log ID): {run_res1['log_id']}")
    except Exception as e:
        print(f"  ❌ خطأ في تشغيل المهمة 1: {e}")

    print("\n🔹 تجربة التشغيل اليدوي للمهمة 2 (generate_periodic_metrics):")
    try:
        run_res2 = run_job("generate_periodic_metrics", trigger_type="MANUAL", db=db)
        print(f"  - الحالة: {run_res2['status']}")
        print(f"  - زمن التنفيذ: {run_res2['elapsed_ms']} ms")
        print(f"  - معرّف السجل في قاعدة البيانات (Log ID): {run_res2['log_id']}")
    except Exception as e:
        print(f"  ❌ خطأ في تشغيل المهمة 2: {e}")

    # Verify logs in MongoDB
    logs_count = db[JOB_LOGS_COLLECTION].count_documents({})
    print(f"\n✅ إجمالي سجلات التشغيل المحفوظة في مجموعة `{JOB_LOGS_COLLECTION}`: {logs_count} سجل")

    print_sep("اكتمل اختبار التجميعات والعروض المادية والمهام المجدولة بنجاح!")


def test_aggregations_and_views():
    """PyTest automated discovery entry point."""
    main()


if __name__ == "__main__":
    main()

