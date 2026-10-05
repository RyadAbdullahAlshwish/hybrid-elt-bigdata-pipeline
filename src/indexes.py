"""
Indexes management and explain plan performance analysis module.
Phase 2 - Requirements 1: Queries, Indexes & Explain Plan.
"""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
    REPORTS_DIR,
)

# -----------------------------------------------------------------------------
# Index Definitions (3 Required Indexes, including 1 Compound Index)
# -----------------------------------------------------------------------------
INDEX_DEFINITIONS = [
    {
        "name": "idx_val_city_status",
        "keys": [("city", 1), ("status", 1)],
        "is_compound": True,
        "description": "فهرس مركب على المدينة وحالة الطلب لتسريع استعلامات الطلبات الجغرافية حسب الحالة",
        "rationale": "يُسرع استعلامات البحث المجمعة بين المدينة وحالة الطلب معاً، وينقل المعالجة من مسح شامل لكافة المستندات (COLLSCAN) إلى مسح موجه ودقيق داخل الفهرس (IXSCAN)."
    },
    {
        "name": "idx_val_order_date",
        "keys": [("order_date", -1)],
        "is_compound": False,
        "description": "فهرس أحادي على تاريخ الطلب تنازلياً لتسريع الفرز والبحث الزمني",
        "rationale": "يُسرع عمليات البحث ضمن نطاقات زمنية محددة ويُغني محرك MongoDB عن عمليات الترتيب البطيئة في الذاكرة (In-Memory Sort) عند جلب أحدث الطلبات."
    },
    {
        "name": "idx_val_customer_id",
        "keys": [("customer_id", 1)],
        "is_compound": False,
        "description": "فهرس أحادي على معرّف العميل لتسريع الوصول لسجل مشتريات العميل",
        "rationale": "يُمكّن محرك قاعدة البيانات من الوصول المباشر والفوري لكافة طلبات عميل محدد بسرعة لوغاريتمية O(log N) بدلاً من الفحص الخطي O(N)."
    }
]


def get_db(db_instance=None):
    """Return database instance or create client from settings."""
    if db_instance is not None:
        return db_instance
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


def create_indexes(db=None) -> Dict[str, Any]:
    """
    Create the 3 required indexes on the orders_validated collection.
    
    Returns:
        Dict containing status, created index names, and index details.
    """
    database = get_db(db)
    collection = database[VALIDATED_COLLECTION]
    
    existing = collection.index_information()
    created = []
    already_existing = []

    for idx_def in INDEX_DEFINITIONS:
        name = idx_def["name"]
        keys = idx_def["keys"]
        if name in existing:
            already_existing.append(name)
        else:
            collection.create_index(keys, name=name)
            created.append(name)

    return {
        "status": "SUCCESS",
        "message": f"تم تجهيز الفهارس بنجاح: {len(created)} تم إنشاؤها، {len(already_existing)} كانت موجودة مسبقاً.",
        "created_indexes": created,
        "already_existing": already_existing,
        "total_required_indexes": len(INDEX_DEFINITIONS),
        "definitions": INDEX_DEFINITIONS
    }


def drop_custom_indexes(db=None) -> Dict[str, Any]:
    """
    Drop the 3 custom indexes from orders_validated collection
    (preserves default _id_ and unique order_id indexes).
    """
    database = get_db(db)
    collection = database[VALIDATED_COLLECTION]
    existing = collection.index_information()
    dropped = []

    for idx_def in INDEX_DEFINITIONS:
        name = idx_def["name"]
        if name in existing:
            collection.drop_index(name)
            dropped.append(name)

    return {
        "status": "SUCCESS",
        "dropped_indexes": dropped,
        "message": f"تم حذف {len(dropped)} فهرس لأغراض الاختبار والمقارنة."
    }


def list_indexes(db=None) -> List[Dict[str, Any]]:
    """List all current indexes on orders_validated with their specifications."""
    database = get_db(db)
    collection = database[VALIDATED_COLLECTION]
    raw_indexes = collection.index_information()
    result = []

    for name, info in raw_indexes.items():
        result.append({
            "name": name,
            "keys": info.get("key", []),
            "unique": info.get("unique", False)
        })
    return result


def _run_explain_query(database, collection, tq: Dict[str, Any]) -> Dict[str, Any]:
    """Execute explain with executionStats verbosity via database command or cursor fallback."""
    find_cmd = {"find": collection.name, "filter": tq["filter"]}
    if tq.get("sort"):
        find_cmd["sort"] = dict(tq["sort"])
    
    try:
        return database.command("explain", find_cmd, verbosity="executionStats")
    except Exception:
        cursor = collection.find(tq["filter"])
        if tq.get("sort"):
            cursor = cursor.sort(tq["sort"])
        return cursor.explain()


def _extract_execution_stats(explain_output: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to extract relevant metrics from MongoDB explain output."""
    exec_stats = explain_output.get("executionStats") or {}
    stages = exec_stats.get("executionStages") or explain_output.get("queryPlanner", {}).get("winningPlan", {})

    stage_name = stages.get("stage", "COLLSCAN")
    input_stage = stages.get("inputStage", {})
    if input_stage:
        inner_stage = input_stage.get("stage")
        if inner_stage:
            stage_name = f"{stage_name} -> {inner_stage}"

    index_name = stages.get("indexName") or input_stage.get("indexName", "None (Full Scan)")

    return {
        "execution_time_millis": exec_stats.get("executionTimeMillis", 0),
        "total_docs_examined": exec_stats.get("totalDocsExamined", 0),
        "total_keys_examined": exec_stats.get("totalKeysExamined", 0),
        "n_returned": exec_stats.get("nReturned", 0),
        "stage": stage_name,
        "index_name": index_name
    }


def run_explain_analysis(db=None, save_reports: bool = True) -> Dict[str, Any]:
    """
    Execute explain('executionStats') on 3 representative queries BEFORE and AFTER
    creating the indexes to scientifically evaluate the performance gain.
    
    Queries tested:
      1. Compound Query: city + status
      2. Chronological Query: order_date range & sort
      3. Customer Query: customer_id lookup
    """
    database = get_db(db)
    collection = database[VALIDATED_COLLECTION]

    # Find sample dynamic values if collection has documents, otherwise use sensible defaults
    sample_doc = collection.find_one() or {}
    sample_city = sample_doc.get("city") or "صنعاء"
    sample_status = sample_doc.get("status") or "مؤكد"
    sample_customer = sample_doc.get("customer_id") or "عميل-0"

    test_queries = [
        {
            "id": "query_1_compound",
            "name": "استعلام المدينة والحالة (Compound Query)",
            "target_index": "idx_val_city_status",
            "filter": {"city": sample_city, "status": sample_status},
            "sort": None,
            "rationale": "يستهدف الفهرس المركب (city, status) لتقليل فحص المستندات لمستوى الصفر تقريباً لغير المطابق."
        },
        {
            "id": "query_2_date_range",
            "name": "استعلام أحدث الطلبات حسب التاريخ (Date Range & Sort)",
            "target_index": "idx_val_order_date",
            "filter": {"order_date": {"$gte": "2025-01-01"}},
            "sort": [("order_date", -1)],
            "rationale": "يستهدف فهرس التاريخ (order_date) لتسريع الفرز واسترجاع الأحدث دون ترتيب مكلف في الذاكرة."
        },
        {
            "id": "query_3_customer",
            "name": "استعلام سجل طلبات عميل محدد (Customer History)",
            "target_index": "idx_val_customer_id",
            "filter": {"customer_id": sample_customer},
            "sort": None,
            "rationale": "يستهدف فهرس العميل (customer_id) للوصول الفوري لسجل مشتريات العميل."
        }
    ]

    # ----------------------------------------------------
    # Step 1: Drop custom indexes to test BEFORE condition
    # ----------------------------------------------------
    drop_custom_indexes(database)

    before_results = {}
    for tq in test_queries:
        explain_raw = _run_explain_query(database, collection, tq)
        before_results[tq["id"]] = _extract_execution_stats(explain_raw)

    # ----------------------------------------------------
    # Step 2: Create custom indexes
    # ----------------------------------------------------
    create_indexes(database)

    # ----------------------------------------------------
    # Step 3: Run queries AFTER creating indexes
    # ----------------------------------------------------
    after_results = {}
    comparisons = []

    for tq in test_queries:
        explain_raw = _run_explain_query(database, collection, tq)
        after_stats = _extract_execution_stats(explain_raw)
        after_results[tq["id"]] = after_stats

        b = before_results[tq["id"]]
        a = after_stats

        docs_diff = b["total_docs_examined"] - a["total_docs_examined"]
        time_diff = b["execution_time_millis"] - a["execution_time_millis"]
        
        reduction_percentage = 0.0
        if b["total_docs_examined"] > 0:
            reduction_percentage = round((docs_diff / b["total_docs_examined"]) * 100, 2)

        comparisons.append({
            "query_id": tq["id"],
            "query_name": tq["name"],
            "target_index": tq["target_index"],
            "rationale": tq["rationale"],
            "filter": tq["filter"],
            "before": b,
            "after": a,
            "docs_examined_reduction": docs_diff,
            "docs_examined_reduction_pct": f"{reduction_percentage}%",
            "time_reduction_ms": time_diff,
            "performance_gain": "استجابة فورية باستخدام الفهرس بدلاً من المسح الشامل"
        })

    report_data = {
        "analysis_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "collection": VALIDATED_COLLECTION,
        "total_queries_analyzed": len(comparisons),
        "comparisons": comparisons
    }

    if save_reports:
        # Save JSON Report
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        json_path = REPORTS_DIR / "explain_results.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)

        # Save Markdown Report
        md_path = REPORTS_DIR / "explain_results.md"
        _generate_explain_markdown(report_data, md_path)

    return report_data


def _generate_explain_markdown(report: Dict[str, Any], output_path: Path):
    """Generate clear markdown documentation of the explain plan results."""
    lines = [
        "# تقرير تحليل أداء الفهارس والاستعلامات (Explain Plan Analysis)",
        "",
        f"**تاريخ التحليل:** `{report['analysis_timestamp']}`  ",
        f"**المجموعة المستهدفة:** `{report['collection']}`  ",
        "",
        "---",
        "",
        "## جدول مقارنة الأداء قبل وبعد الفهارس",
        "",
        "| الاستعلام | الفهرس المستهدف | المرحلة قبل | المرحلة بعد | المستندات المفحوصة (قبل) | المستندات المفحوصة (بعد) | نسبة تقليل الفحص | زمن التنفيذ (قبل / بعد) |",
        "| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |"
    ]

    for item in report["comparisons"]:
        b = item["before"]
        a = item["after"]
        lines.append(
            f"| **{item['query_name']}** | `{item['target_index']}` | `{b['stage']}` | `{a['stage']}` | "
            f"{b['total_docs_examined']:,} | {a['total_docs_examined']:,} | "
            f"**{item['docs_examined_reduction_pct']}** | {b['execution_time_millis']}ms / {a['execution_time_millis']}ms |"
        )

    lines.extend([
        "",
        "---",
        "",
        "## تحليل أسباب اختيار الفهارس وأثرها العملي",
        ""
    ])

    for idx, item in enumerate(report["comparisons"], start=1):
        lines.extend([
            f"### {idx}. {item['query_name']}",
            f"- **الفهرس المعتمد:** `{item['target_index']}`",
            f"- **الفلتر المستخدم:** `{json.dumps(item['filter'], ensure_ascii=False)}`",
            f"- **سبب الاختيار والأثر:** {item['rationale']}",
            f"- **النتيجة المحققة:** تحول المسح من `{item['before']['stage']}` إلى `{item['after']['stage']}` مع تقليل فحص المستندات بمقدار `{item['docs_examined_reduction']:,}` مستنداً ({item['docs_examined_reduction_pct']}).",
            ""
        ])

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
