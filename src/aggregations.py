"""
Aggregation Reports Module for Big Data E-Commerce Analytics.
Phase 2 - Requirements 2: 5 Practical Aggregation Pipelines with JSON Output.
"""

import json
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
    if "_id" in clean and not isinstance(clean["_id"], (str, int, float, dict)):
        clean["_id"] = str(clean["_id"])
    return clean


# -----------------------------------------------------------------------------
# Metadata for Available Aggregations (for GET /aggregations)
# -----------------------------------------------------------------------------
AGGREGATIONS_METADATA = [
    {
        "name": "sales_by_city",
        "title": "تقرير المبيعات حسب المدينة",
        "description": "تجميع إجمالي الإيرادات، وعدد الطلبات، ومتوسط قيمة الطلب لكل مدينة مرتبة تنازلياً حسب الأعلى مبيعاً.",
        "supported_params": {
            "limit": "الحد الأقصى للمدن المسترجعة (افتراضياً 20)",
            "min_orders": "الحد الأدنى لعدد الطلبات في المدينة (اختياري، افتراضياً 1)"
        }
    },
    {
        "name": "top_products",
        "title": "تقرير أفضل المنتجات مبيعاً",
        "description": "تفكيك مصفوفة المنتجات وحساب إجمالي الكميات المباعة وإجمالي الدخل لكل منتج (SKU)، لتحديد المنتجات الأكثر طلباً.",
        "supported_params": {
            "limit": "عدد المنتجات المسترجعة (افتراضياً 20)"
        }
    },
    {
        "name": "top_customers",
        "title": "تقرير أفضل العملاء إنفاقاً (VIP Customers)",
        "description": "تحديد كبار العملاء وترتيبهم حسب إجمالي المبالغ المنفقة، مع إظهار عدد طلباتهم وتاريخ آخر طلب.",
        "supported_params": {
            "limit": "عدد كبار العملاء المسترجعين (افتراضياً 20)"
        }
    },
    {
        "name": "sales_by_period",
        "title": "تقرير المبيعات حسب الفترة الزمنية (اليومي)",
        "description": "تجميع حركة المبيعات والإيرادات زمنياً حسب التاريخ (Daily Trend) لدراسة تطور المبيعات يومياً.",
        "supported_params": {
            "period": "فترة التجميع: 'day' لليومي أو 'month' للشهري (افتراضياً 'day')",
            "limit": "عدد الفترات المسترجعة (افتراضياً 30)",
            "sort_order": "الترتيب: -1 للأحدث أولاً أو 1 للأقدم أولاً (افتراضياً -1)"
        }
    },
    {
        "name": "orders_by_status",
        "title": "تقرير توزيع الطلبات حسب الحالة وطريقة الدفع",
        "description": "تحليل شامل لتوزيع الطلبات عبر مختلف الحالات التشغيلية (مؤكد، قيد الانتظار، ملغي) وطرق وحالات السداد.",
        "supported_params": {
            "limit": "الحد الأقصى للنتائج المسترجعة (افتراضياً 50)"
        }
    }
]


def get_available_aggregations() -> List[Dict[str, Any]]:
    """Return list of available aggregation reports metadata."""
    return AGGREGATIONS_METADATA


# -----------------------------------------------------------------------------
# 1. Sales by City Aggregation Pipeline
# -----------------------------------------------------------------------------
def aggregate_sales_by_city(
    collection,
    limit: int = 20,
    min_orders: int = 1,
    **kwargs
) -> List[Dict[str, Any]]:
    """Aggregate sales, revenue, and average order value by city."""
    pipeline = [
        # Filter valid documents with city specified
        {
            "$match": {
                "city": {"$exists": True, "$ne": None, "$ne": ""}
            }
        },
        # Safe numeric conversion for total_amount
        {
            "$addFields": {
                "numeric_total": {
                    "$convert": {
                        "input": "$total_amount",
                        "to": "double",
                        "onError": 0.0,
                        "onNull": 0.0
                    }
                }
            }
        },
        # Group by city
        {
            "$group": {
                "_id": "$city",
                "total_revenue": {"$sum": "$numeric_total"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$numeric_total"}
            }
        },
        # Filter cities with minimum orders
        {
            "$match": {
                "order_count": {"$gte": int(min_orders)}
            }
        },
        # Clean projection
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_count": "$order_count",
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": int(limit)}
    ]
    return list(collection.aggregate(pipeline))


# -----------------------------------------------------------------------------
# 2. Top Products Aggregation Pipeline
# -----------------------------------------------------------------------------
def aggregate_top_products(
    collection,
    limit: int = 20,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Aggregate top products by total quantity sold and revenue.
    Supports items stored as BSON arrays or JSON strings.
    """
    # First attempt: Native MongoDB aggregation if items is parsed array
    pipeline = [
        {"$match": {"items": {"$type": "array", "$ne": []}}},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": {
                    "sku": {"$ifNull": ["$items.sku", "UNKNOWN"]},
                    "name": {"$ifNull": ["$items.name", "Unknown Product"]}
                },
                "total_quantity": {
                    "$sum": {
                        "$abs": {
                            "$convert": {
                                "input": "$items.qty",
                                "to": "double",
                                "onError": 1.0,
                                "onNull": 1.0
                            }
                        }
                    }
                },
                "total_revenue": {
                    "$sum": {
                        "$abs": {
                            "$convert": {
                                "input": "$items.total",
                                "to": "double",
                                "onError": 0.0,
                                "onNull": 0.0
                            }
                        }
                    }
                },
                "orders_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "sku": "$_id.sku",
                "product_name": "$_id.name",
                "total_quantity": "$total_quantity",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": "$orders_count"
            }
        },
        {"$sort": {"total_quantity": -1}},
        {"$limit": int(limit)}
    ]

    results = list(collection.aggregate(pipeline))
    if results:
        return results

    # Fallback: Process items_json string if array is not directly unwindable
    products_map: Dict[str, Dict[str, Any]] = {}
    cursor = collection.find(
        {"items_json": {"$exists": True, "$ne": None, "$ne": ""}},
        {"items_json": 1}
    ).limit(5000)

    for doc in cursor:
        raw_items = doc.get("items_json")
        if not raw_items:
            continue
        try:
            items_list = json.loads(raw_items) if isinstance(raw_items, str) else raw_items
            if not isinstance(items_list, list):
                continue
            for item in items_list:
                if not isinstance(item, dict):
                    continue
                sku = str(item.get("sku", "UNKNOWN"))
                name = str(item.get("name", "Unknown Product"))
                qty = abs(float(item.get("qty", 1)))
                unit_price = abs(float(item.get("unit_price", item.get("price", 0))))
                total = abs(float(item.get("total", qty * unit_price)))

                if sku not in products_map:
                    products_map[sku] = {
                        "sku": sku,
                        "product_name": name,
                        "total_quantity": 0.0,
                        "total_revenue": 0.0,
                        "orders_count": 0
                    }
                products_map[sku]["total_quantity"] += qty
                products_map[sku]["total_revenue"] += total
                products_map[sku]["orders_count"] += 1
        except Exception:
            continue

    sorted_products = sorted(
        products_map.values(),
        key=lambda p: p["total_quantity"],
        reverse=True
    )[:int(limit)]

    for p in sorted_products:
        p["total_quantity"] = round(p["total_quantity"], 2)
        p["total_revenue"] = round(p["total_revenue"], 2)

    return sorted_products


# -----------------------------------------------------------------------------
# 3. Top Spending Customers Aggregation Pipeline
# -----------------------------------------------------------------------------
def aggregate_top_customers(
    collection,
    limit: int = 20,
    **kwargs
) -> List[Dict[str, Any]]:
    """Aggregate top customers by total expenditure and order count."""
    pipeline = [
        {"$match": {"customer_id": {"$exists": True, "$ne": None, "$ne": ""}}},
        {
            "$addFields": {
                "numeric_total": {
                    "$convert": {
                        "input": "$total_amount",
                        "to": "double",
                        "onError": 0.0,
                        "onNull": 0.0
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$customer_id",
                "customer_name": {"$first": "$customer_name"},
                "total_spent": {"$sum": "$numeric_total"},
                "orders_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$numeric_total"},
                "last_order_date": {"$max": "$order_date"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": {"$ifNull": ["$customer_name", "عميل مجهول"]},
                "total_spent": {"$round": ["$total_spent", 2]},
                "orders_count": "$orders_count",
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "last_order_date": "$last_order_date"
            }
        },
        {"$sort": {"total_spent": -1}},
        {"$limit": int(limit)}
    ]
    return list(collection.aggregate(pipeline))


# -----------------------------------------------------------------------------
# 4. Sales by Period Aggregation Pipeline (Daily / Monthly)
# -----------------------------------------------------------------------------
def aggregate_sales_by_period(
    collection,
    period: str = "day",
    limit: int = 30,
    sort_order: int = -1,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Aggregate revenue and order volume by time period ('day' YYYY-MM-DD or 'month' YYYY-MM).
    Serves as the foundation for the daily_sales_summary Materialized View.
    """
    # Substring length: 10 chars for 'YYYY-MM-DD', 7 chars for 'YYYY-MM'
    sub_len = 7 if str(period).lower().startswith("m") else 10

    pipeline = [
        {"$match": {"order_date": {"$exists": True, "$ne": None, "$ne": ""}}},
        {
            "$addFields": {
                "numeric_total": {
                    "$convert": {
                        "input": "$total_amount",
                        "to": "double",
                        "onError": 0.0,
                        "onNull": 0.0
                    }
                },
                "period_key": {
                    "$substrBytes": ["$order_date", 0, sub_len]
                }
            }
        },
        {
            "$group": {
                "_id": "$period_key",
                "total_revenue": {"$sum": "$numeric_total"},
                "orders_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$numeric_total"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "period": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": "$orders_count",
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        },
        {"$sort": {"period": int(sort_order)}},
        {"$limit": int(limit)}
    ]
    return list(collection.aggregate(pipeline))


# -----------------------------------------------------------------------------
# 5. Orders Distribution by Status & Payment Method Pipeline
# -----------------------------------------------------------------------------
def aggregate_orders_by_status(
    collection,
    limit: int = 50,
    **kwargs
) -> List[Dict[str, Any]]:
    """Aggregate orders grouped by order status, payment method, and payment status."""
    pipeline = [
        {
            "$addFields": {
                "numeric_total": {
                    "$convert": {
                        "input": "$total_amount",
                        "to": "double",
                        "onError": 0.0,
                        "onNull": 0.0
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "status": {"$ifNull": ["$status", "غير محدد"]},
                    "payment_method": {"$ifNull": ["$payment_method", "غير محدد"]},
                    "payment_status": {"$ifNull": ["$payment_status", "غير محدد"]}
                },
                "orders_count": {"$sum": 1},
                "total_amount": {"$sum": "$numeric_total"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "status": "$_id.status",
                "payment_method": "$_id.payment_method",
                "payment_status": "$_id.payment_status",
                "orders_count": "$orders_count",
                "total_amount": {"$round": ["$total_amount", 2]}
            }
        },
        {"$sort": {"orders_count": -1}},
        {"$limit": int(limit)}
    ]
    return list(collection.aggregate(pipeline))


# -----------------------------------------------------------------------------
# Dispatcher: execute_aggregation(name, params)
# -----------------------------------------------------------------------------
AGGREGATION_DISPATCHER = {
    "sales_by_city": aggregate_sales_by_city,
    "top_products": aggregate_top_products,
    "top_customers": aggregate_top_customers,
    "sales_by_period": aggregate_sales_by_period,
    "orders_by_status": aggregate_orders_by_status,
}


def execute_aggregation(
    name: str,
    params: Optional[Dict[str, Any]] = None,
    db=None,
    **kwargs
) -> Dict[str, Any]:
    """
    Execute an aggregation pipeline by identifier.
    
    Args:
        name: Name of the aggregation report.
        params: Optional dictionary of parameters (limit, min_orders, period, etc.).
        db: Optional database instance.
        **kwargs: Additional parameters passed directly.
        
    Returns:
        Dict containing execution status, results, elapsed time, and row count.
    """
    if name not in AGGREGATION_DISPATCHER:
        available = list(AGGREGATION_DISPATCHER.keys())
        raise ValueError(f"Unknown aggregation report '{name}'. Available: {available}")

    database = get_db(db)
    collection = database[VALIDATED_COLLECTION]
    merged_params = dict(params or {})
    merged_params.update(kwargs)

    start_time = time.perf_counter()
    agg_func = AGGREGATION_DISPATCHER[name]
    raw_results = agg_func(collection, **merged_params)
    elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

    cleaned_results = [_clean_doc(d) for d in raw_results]

    return {
        "status": "SUCCESS",
        "aggregation_name": name,
        "returned_count": len(cleaned_results),
        "execution_time_ms": elapsed_ms,
        "applied_params": merged_params,
        "results": cleaned_results
    }
