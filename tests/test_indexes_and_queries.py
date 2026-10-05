"""
Test Script for Indexes, Practical Queries & Explain Plan Analysis.
Phase 2 - Requirement 1 verification.
"""

import sys
from pathlib import Path

# Add project root to sys.path to allow execution from tests/ directory or project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from pprint import pprint
from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
    REPORTS_DIR,
)
from src.indexes import (
    create_indexes,
    drop_custom_indexes,
    list_indexes,
    run_explain_analysis,
    INDEX_DEFINITIONS,
)
from src.queries import (
    get_available_queries,
    execute_query,
)


def print_separator(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def main():
    print_separator("1. فحص الاتصال بقاعدة البيانات (MongoDB Connectivity)")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.server_info()  # triggers exception if cannot connect
        db = client[MONGO_DATABASE]
        val_col = db[VALIDATED_COLLECTION]
        total_docs = val_col.count_documents({})
        print(f"✅ الاتصال ناجح بـ MongoDB على: {MONGO_URI}")
        print(f"📊 قاعدة البيانات: {MONGO_DATABASE}")
        print(f"📦 مجموعة الطلبات المصححة ({VALIDATED_COLLECTION}): {total_docs:,} مستند")
    except Exception as e:
        print(f"❌ فشل الاتصال بـ MongoDB: {e}")
        print("⚠️ يرجى التأكد من تشغيل خادم MongoDB محلياً (mongod).")
        return

    # -------------------------------------------------------------------------
    # 2. اختبار الفهارس
    # -------------------------------------------------------------------------
    print_separator("2. اختبار إنشاء وفحص الفهارس (Indexes Test)")
    
    print("🔹 جاري إنشاء الفهارس الثلاثة المطلوبة...")
    res_indexes = create_indexes(db)
    print(f"الحالة: {res_indexes['status']}")
    print(f"الرسالة: {res_indexes['message']}")
    print(f"الفهارس المنشأة حديثاً: {res_indexes['created_indexes']}")
    print(f"فهارس كانت موجودة مسبقاً: {res_indexes['already_existing']}")

    print("\n🔹 قائمة الفهارس الفعلية في المجموعة الآن:")
    current_indexes = list_indexes(db)
    for idx in current_indexes:
        print(f"  - الاسم: {idx['name']:<25} | المفاتيح: {idx['keys']} | فريد: {idx['unique']}")

    # -------------------------------------------------------------------------
    # 3. اختبار الاستعلامات الخمسة
    # -------------------------------------------------------------------------
    print_separator("3. اختبار تشغيل الاستعلامات الخمسة (5 Practical Queries)")
    
    available_queries = get_available_queries()
    print(f"عدد الاستعلامات المسجلة: {len(available_queries)}")

    for q_meta in available_queries:
        q_name = q_meta["name"]
        q_title = q_meta["title"]
        print(f"\n▶️ تشغيل الاستعلام: {q_title} (`{q_name}`)")
        
        try:
            res = execute_query(q_name, limit=5, db=db)
            print(f"  - الحالة: {res.get('status')}")
            print(f"  - زمن التنفيذ: {res.get('execution_time_ms')} مللي ثانية (ms)")
            print(f"  - إجمالي المطابق في القاعدة: {res.get('total_matched'):,}")
            print(f"  - عدد النتائج المعروضة: {res.get('returned_count')}")
            
            # Print sample result summary
            if res.get("results"):
                sample = res["results"][0]
                summary = {
                    "order_id": sample.get("order_id"),
                    "city": sample.get("city"),
                    "status": sample.get("status"),
                    "customer_id": sample.get("customer_id"),
                    "order_date": sample.get("order_date"),
                    "total_amount": sample.get("total_amount")
                }
                print(f"  - عينة من أول نتيجة: {summary}")
            else:
                print("  - لا توجد سجلات مطابقة حالياً (المجموعة فارغة أو لا تطابق الشروط).")
        except Exception as e:
            print(f"  ❌ خطأ أثناء تنفيذ الاستعلام: {e}")

    # -------------------------------------------------------------------------
    # 4. اختبار تحليل الأداء (Explain Plan)
    # -------------------------------------------------------------------------
    print_separator("4. اختبار تحليل الأداء (Explain Plan - executionStats)")
    print("🔹 جاري تشغيل explain('executionStats') قبل وبعد الفهارس...")
    
    try:
        explain_report = run_explain_analysis(db=db, save_reports=True)
        print("✅ تم تحليل أداء الاستعلامات وتوليد التقارير بنجاح!\n")
        
        print(f"{'الاستعلام':<35} | {'المرحلة قبل':<12} | {'المرحلة بعد':<12} | {'فحص (قبل)':<10} | {'فحص (بعد)':<10} | {'نسبة التحسن'}")
        print("-" * 105)
        for comp in explain_report["comparisons"]:
            q_name = comp["query_name"][:33]
            b_stage = comp["before"]["stage"][:11]
            a_stage = comp["after"]["stage"][:11]
            b_docs = comp["before"]["total_docs_examined"]
            a_docs = comp["after"]["total_docs_examined"]
            pct = comp["docs_examined_reduction_pct"]
            print(f"{q_name:<35} | {b_stage:<12} | {a_stage:<12} | {b_docs:<10} | {a_docs:<10} | {pct}")
            
        print("\n📁 تم حفظ تقرير النتائج التفصيلي في:")
        print(f"  - [reports/explain_results.json]({REPORTS_DIR / 'explain_results.json'})")
        print(f"  - [reports/explain_results.md]({REPORTS_DIR / 'explain_results.md'})")
    except Exception as e:
        print(f"❌ حدث خطأ أثناء تحليل Explain: {e}")

    print_separator("اكتمل اختبار الفهارس والاستعلامات بنجاح!")


def test_indexes_and_queries():
    """PyTest automated discovery entry point."""
    main()


if __name__ == "__main__":
    main()

