"""
Practical Business Queries module for orders_validated collection.
Phase 2 - Requirements 1: 5 Practical Queries with Dynamic Parameters.
"""

import time
from typing import Any, Dict, List, Optional
from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)


def get_db(db_instance=None):
    """Return database instance or create client from settings."""
    if db_instance is not None:
        return db_instance
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


def _clean_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to convert ObjectId and non-serializable types for JSON output."""
    if not doc:
        return doc
    clean = dict(doc)
    if "_id" in clean:
        clean["_id"] = str(clean["_id"])
    return clean


# -----------------------------------------------------------------------------
# Metadata for Available Queries (for GET /queries)
# -----------------------------------------------------------------------------
QUERIES_METADATA = [
    {
        "name": "orders_by_city_and_status",
        "title": "الطلبات حسب المدينة وحالة الطلب",
        "description": "استعلام عملي يفلتر الطلبات حسب اسم المدينة وحالة الطلب (مؤكد، قيد الانتظار...) ويستفيد من الفهرس المركب (city, status).",
        "index_used": "idx_val_city_status",
        "supported_params": {
            "city": "اسم المدينة المراد الاستعلام عنها (اختياري - افتراضياً يجلب من البيانات المتاحة)",
            "status": "حالة الطلب المراد تصفيتها (اختياري)",
            "limit": "الحد الأقصى للنتائج المسترجعة (افتراضياً 50)"
        }
    },
    {
        "name": "recent_orders_by_date",
        "title": "أحدث الطلبات حسب الفترة الزمنية",
        "description": "استعلام عملي يستعرض الطلبات الحديثة مرتبة زمنياً تنازلياً مع إمكانية تحديد تاريخ بداية ونهاية، ويستفيد من فهرس التاريخ (order_date).",
        "index_used": "idx_val_order_date",
        "supported_params": {
            "start_date": "تاريخ البداية بصيغة YYYY-MM-DD (اختياري)",
            "end_date": "تاريخ النهاية بصيغة YYYY-MM-DD (اختياري)",
            "limit": "الحد الأقصى للنتائج المسترجعة (افتراضياً 50)"
        }
    },
    {
        "name": "customer_order_history",
        "title": "سجل طلبات عميل محدد",
        "description": "استعلام عملي يسترجع كافة المشتريات والطلبات السابقة لعميل محدد بدلالة معرف العميل، ويستفيد من فهرس العميل (customer_id).",
        "index_used": "idx_val_customer_id",
        "supported_params": {
            "customer_id": "معرف العميل المراد جلب سجل طلباته (مثل: عميل-0)",
            "limit": "الحد الأقصى للنتائج المسترجعة (افتراضياً 50)"
        }
    },
    {
        "name": "high_value_orders",
        "title": "الطلبات ذات القيمة المالية العالية",
        "description": "استعلام عملي لفلترة واستخراج الطلبات الكبرى التي تتجاوز حداً مالياً معيناً لدعم قرارات التسويق والعملاء المميزين.",
        "index_used": "Dynamic Filtering / Sorting",
        "supported_params": {
            "min_amount": "الحد الأدنى لقيمة الطلب (رقم أو نص، افتراضياً 100,000)",
            "limit": "الحد الأقصى للنتائج المسترجعة (افتراضياً 50)"
        }
    },
    {
        "name": "orders_by_payment_method_and_status",
        "title": "الطلبات حسب طريقة وحالة الدفع",
        "description": "استعلام عملي يستعرض الطلبات بحسب وسيلة الدفع (محفظة إلكترونية، بطاقة، نقد عند الاستلام) وحالة السداد (تم الدفع، بانتظار الدفع).",
        "index_used": "idx_val_quality_status / Dynamic",
        "supported_params": {
            "payment_method": "طريقة الدفع (اختياري)",
            "payment_status": "حالة السداد (اختياري)",
            "limit": "الحد الأقصى للنتائج المسترجعة (افتراضياً 50)"
        }
    }
]


def get_available_queries() -> List[Dict[str, Any]]:
    """Return metadata list of all available queries."""
    return QUERIES_METADATA


# -----------------------------------------------------------------------------
# Query Implementation Functions
# -----------------------------------------------------------------------------

def query_orders_by_city_and_status(
    collection,
    city: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """Execute Query 1: Filter by city and order status."""
    query = {}
    if city:
        query["city"] = city
    if status:
        query["status"] = status

    cursor = collection.find(query).limit(limit)
    results = [_clean_doc(d) for d in cursor]
    total_matched = collection.count_documents(query)

    return {
        "query_name": "orders_by_city_and_status",
        "applied_filter": query,
        "total_matched": total_matched,
        "returned_count": len(results),
        "results": results
    }


def query_recent_orders_by_date(
    collection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """Execute Query 2: Retrieve recent orders sorted by order_date descending."""
    date_filter = {}
    if start_date:
        date_filter["$gte"] = start_date
    if end_date:
        date_filter["$lte"] = end_date

    query = {}
    if date_filter:
        query["order_date"] = date_filter

    cursor = collection.find(query).sort("order_date", -1).limit(limit)
    results = [_clean_doc(d) for d in cursor]
    total_matched = collection.count_documents(query)

    return {
        "query_name": "recent_orders_by_date",
        "applied_filter": query,
        "total_matched": total_matched,
        "returned_count": len(results),
        "results": results
    }


def query_customer_order_history(
    collection,
    customer_id: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """Execute Query 3: Retrieve purchase history for a specific customer."""
    if not customer_id:
        # Dynamically discover a customer ID from the data if none was provided
        sample = collection.find_one({"customer_id": {"$exists": True, "$ne": None}})
        customer_id = sample.get("customer_id") if sample else "عميل-0"

    query = {"customer_id": customer_id}
    cursor = collection.find(query).sort("order_date", -1).limit(limit)
    results = [_clean_doc(d) for d in cursor]
    total_matched = collection.count_documents(query)

    return {
        "query_name": "customer_order_history",
        "customer_id": customer_id,
        "applied_filter": query,
        "total_matched": total_matched,
        "returned_count": len(results),
        "results": results
    }


def query_high_value_orders(
    collection,
    min_amount: float = 100000.0,
    limit: int = 50
) -> Dict[str, Any]:
    """Execute Query 4: Filter high-value orders."""
    # Ensure support whether total_amount is stored as numeric or string
    try:
        min_amt_float = float(min_amount)
    except (ValueError, TypeError):
        min_amt_float = 100000.0

    query = {
        "$expr": {
            "$gte": [
                {"$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}},
                min_amt_float
            ]
        }
    }

    cursor = collection.find(query).sort("total_amount", -1).limit(limit)
    results = [_clean_doc(d) for d in cursor]
    total_matched = collection.count_documents(query)

    return {
        "query_name": "high_value_orders",
        "min_amount_threshold": min_amt_float,
        "total_matched": total_matched,
        "returned_count": len(results),
        "results": results
    }


def query_orders_by_payment_method_and_status(
    collection,
    payment_method: Optional[str] = None,
    payment_status: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """Execute Query 5: Filter orders by payment method and payment status."""
    query = {}
    if payment_method:
        query["payment_method"] = payment_method
    if payment_status:
        query["payment_status"] = payment_status

    cursor = collection.find(query).limit(limit)
    results = [_clean_doc(d) for d in cursor]
    total_matched = collection.count_documents(query)

    return {
        "query_name": "orders_by_payment_method_and_status",
        "applied_filter": query,
        "total_matched": total_matched,
        "returned_count": len(results),
        "results": results
    }


# -----------------------------------------------------------------------------
# Dispatcher: execute_query(name, params)
# -----------------------------------------------------------------------------
QUERY_DISPATCHER = {
    "orders_by_city_and_status": query_orders_by_city_and_status,
    "recent_orders_by_date": query_recent_orders_by_date,
    "customer_order_history": query_customer_order_history,
    "high_value_orders": query_high_value_orders,
    "orders_by_payment_method_and_status": query_orders_by_payment_method_and_status,
}


def execute_query(
    name: str,
    params: Optional[Dict[str, Any]] = None,
    db=None,
    **kwargs
) -> Dict[str, Any]:
    """
    Execute a query by its registered identifier.
    
    Args:
        name: Query identifier matching one of the 5 implemented queries.
        params: Optional dictionary of query parameters.
        db: Optional database instance.
        **kwargs: Additional parameters passed directly (e.g. limit=10).
    
    Returns:
        Dict containing query execution result and elapsed time.
    """
    if name not in QUERY_DISPATCHER:
        available = list(QUERY_DISPATCHER.keys())
        raise ValueError(f"Unknown query '{name}'. Available queries: {available}")

    database = get_db(db)
    collection = database[VALIDATED_COLLECTION]
    merged_params = dict(params or {})
    merged_params.update(kwargs)

    start_time = time.perf_counter()
    query_func = QUERY_DISPATCHER[name]
    
    # Execute query function passing collection and params
    result = query_func(collection, **merged_params)
    elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

    result["execution_time_ms"] = elapsed_ms
    result["status"] = "SUCCESS"
    return result
