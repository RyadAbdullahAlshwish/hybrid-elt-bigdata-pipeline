"""
Materialized Views and Incremental Refresh Engine.
Phase 2 - Requirements 3: 2 Materialized Views (daily_sales_summary & top_products_summary)
with Self-Contained Incremental Refresh Mechanism.
"""

import json
import time
from typing import Any, Dict, List, Optional
from pymongo import MongoClient, UpdateOne

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)

# Collection names matching the professor's exact specification
MV_DAILY_SALES = "daily_sales_summary"
MV_TOP_PRODUCTS = "top_products_summary"


def get_db(db_instance=None):
    """Return database instance or create client from settings."""
    if db_instance is not None:
        return db_instance
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


def _clean_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to convert ObjectId to string for JSON serialization."""
    if not doc:
        return doc
    clean = dict(doc)
    if "_id" in clean and not isinstance(clean["_id"], (str, int, float, dict)):
        clean["_id"] = str(clean["_id"])
    return clean


# -----------------------------------------------------------------------------
# Initialization & Index Setup for Materialized Views
# -----------------------------------------------------------------------------
def ensure_materialized_view_indexes(db=None):
    """Ensure unique business key indexes exist on the materialized views."""
    database = get_db(db)
    
    # 1. Unique index on date in daily_sales_summary
    daily_col = database[MV_DAILY_SALES]
    daily_col.create_index([("date", 1)], unique=True, name="idx_mv_daily_date_unique")

    # 2. Unique index on sku in top_products_summary
    prod_col = database[MV_TOP_PRODUCTS]
    prod_col.create_index([("sku", 1)], unique=True, name="idx_mv_top_prod_sku_unique")


# -----------------------------------------------------------------------------
# Watermark Detection for Incremental Refresh
# -----------------------------------------------------------------------------
def _get_latest_watermark(view_collection) -> Optional[str]:
    """
    Find the latest timestamp processed in the materialized view.
    Inspects last_updated_at from the existing documents in the view.
    """
    latest_doc = view_collection.find_one(
        {"last_updated_at": {"$exists": True, "$ne": None}},
        sort=[("last_updated_at", -1)]
    )
    if latest_doc:
        return latest_doc.get("last_updated_at")
    return None


# -----------------------------------------------------------------------------
# Incremental Refresh Logic for daily_sales_summary
# -----------------------------------------------------------------------------
def refresh_daily_sales_summary(
    database,
    full_refresh: bool = False,
    now_iso: str = ""
) -> Dict[str, Any]:
    """
    Incrementally refresh daily_sales_summary.
    If full_refresh is True or view is empty, processes all records.
    Otherwise, only fetches and recalculates dates affected by newly ingested orders.
    """
    val_col = database[VALIDATED_COLLECTION]
    view_col = database[MV_DAILY_SALES]
    
    watermark = None if full_refresh else _get_latest_watermark(view_col)
    
    # Determine the query filter for new/modified orders
    match_filter = {"order_date": {"$exists": True, "$ne": None, "$ne": ""}}
    if watermark:
        match_filter["metadata.processed_at"] = {"$gt": watermark}

    # Identify which dates are affected by new records
    affected_dates_cursor = val_col.distinct(
        "order_date",
        match_filter
    )
    # Extract unique YYYY-MM-DD dates
    affected_days = list({d[:10] for d in affected_dates_cursor if isinstance(d, str) and len(d) >= 10})

    if not affected_days and watermark:
        # Nothing changed
        return {
            "view_name": MV_DAILY_SALES,
            "mode": "INCREMENTAL",
            "affected_days_count": 0,
            "updated_count": 0,
            "message": "لا توجد سجلات مبيعات جديدة؛ العرض المادي محدث بالكامل."
        }

    # Aggregate affected days from orders_validated
    recalc_match = {"order_date": {"$exists": True, "$ne": None, "$ne": ""}}
    if affected_days and watermark:
        # Only aggregate the affected days
        regex_pattern = "^(" + "|".join(affected_days) + ")"
        recalc_match["order_date"] = {"$regex": regex_pattern}

    pipeline = [
        {"$match": recalc_match},
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
                "date_key": {"$substrBytes": ["$order_date", 0, 10]}
            }
        },
        {
            "$group": {
                "_id": "$date_key",
                "total_revenue": {"$sum": "$numeric_total"},
                "orders_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$numeric_total"},
                "latest_order_time": {"$max": "$order_date"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": "$orders_count",
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "latest_order_time": "$latest_order_time"
            }
        }
    ]

    aggregated_records = list(val_col.aggregate(pipeline))

    if not aggregated_records:
        return {
            "view_name": MV_DAILY_SALES,
            "mode": "FULL" if full_refresh or not watermark else "INCREMENTAL",
            "affected_days_count": 0,
            "updated_count": 0,
            "message": "لا توجد بيانات متاحة للتجميع."
        }

    # Bulk Upsert to update or insert days into daily_sales_summary
    bulk_ops = []
    for rec in aggregated_records:
        rec_data = dict(rec)
        rec_data["last_updated_at"] = now_iso
        bulk_ops.append(
            UpdateOne(
                {"date": rec["date"]},
                {"$set": rec_data},
                upsert=True
            )
        )

    upsert_res = view_col.bulk_write(bulk_ops, ordered=False)
    updated_total = upsert_res.upserted_count + upsert_res.modified_count

    return {
        "view_name": MV_DAILY_SALES,
        "mode": "FULL" if full_refresh or not watermark else "INCREMENTAL",
        "affected_days_count": len(aggregated_records),
        "inserted_days": upsert_res.upserted_count,
        "updated_days": upsert_res.modified_count,
        "total_days_in_view": view_col.count_documents({})
    }


# -----------------------------------------------------------------------------
# Incremental Refresh Logic for top_products_summary
# -----------------------------------------------------------------------------
def refresh_top_products_summary(
    database,
    full_refresh: bool = False,
    now_iso: str = ""
) -> Dict[str, Any]:
    """
    Incrementally refresh top_products_summary.
    Aggregates product totals from new/modified orders and merges into top_products_summary.
    """
    val_col = database[VALIDATED_COLLECTION]
    view_col = database[MV_TOP_PRODUCTS]
    
    watermark = None if full_refresh else _get_latest_watermark(view_col)
    
    match_filter = {"items_json": {"$exists": True, "$ne": None, "$ne": ""}}
    if watermark:
        match_filter["metadata.processed_at"] = {"$gt": watermark}

    # Find affected documents
    cursor = val_col.find(match_filter, {"items_json": 1, "metadata": 1})
    
    products_delta: Dict[str, Dict[str, Any]] = {}
    docs_processed = 0

    for doc in cursor:
        docs_processed += 1
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

                if sku not in products_delta:
                    products_delta[sku] = {
                        "sku": sku,
                        "product_name": name,
                        "total_quantity": 0.0,
                        "total_revenue": 0.0,
                        "orders_count": 0
                    }
                products_delta[sku]["total_quantity"] += qty
                products_delta[sku]["total_revenue"] += total
                products_delta[sku]["orders_count"] += 1
        except Exception:
            continue

    if not products_delta:
        return {
            "view_name": MV_TOP_PRODUCTS,
            "mode": "INCREMENTAL" if watermark else "FULL",
            "docs_processed": docs_processed,
            "affected_products_count": 0,
            "updated_count": 0,
            "message": "لا توجد طلبات جديدة تحتوي منتجات؛ العرض المادي محدث بالكامل."
        }

    # If full refresh or initial build, reset the view collection
    if full_refresh or not watermark:
        view_col.delete_many({})

    # Upsert with $inc to maintain incremental accumulation
    bulk_ops = []
    for sku, pdata in products_delta.items():
        if full_refresh or not watermark:
            # Absolute write
            doc_to_save = dict(pdata)
            doc_to_save["last_updated_at"] = now_iso
            bulk_ops.append(
                UpdateOne(
                    {"sku": sku},
                    {"$set": doc_to_save},
                    upsert=True
                )
            )
        else:
            # Incremental accumulation via $inc
            bulk_ops.append(
                UpdateOne(
                    {"sku": sku},
                    {
                        "$inc": {
                            "total_quantity": round(pdata["total_quantity"], 2),
                            "total_revenue": round(pdata["total_revenue"], 2),
                            "orders_count": pdata["orders_count"]
                        },
                        "$set": {
                            "product_name": pdata["product_name"],
                            "last_updated_at": now_iso
                        }
                    },
                    upsert=True
                )
            )

    upsert_res = view_col.bulk_write(bulk_ops, ordered=False)
    updated_total = upsert_res.upserted_count + upsert_res.modified_count

    return {
        "view_name": MV_TOP_PRODUCTS,
        "mode": "FULL" if full_refresh or not watermark else "INCREMENTAL",
        "docs_processed": docs_processed,
        "affected_products_count": len(products_delta),
        "inserted_products": upsert_res.upserted_count,
        "updated_products": upsert_res.modified_count,
        "total_products_in_view": view_col.count_documents({})
    }


# -----------------------------------------------------------------------------
# Unified Refresh Controller (for POST /refresh-mv)
# -----------------------------------------------------------------------------
def refresh_materialized_views(
    full_refresh: bool = False,
    db=None
) -> Dict[str, Any]:
    """
    Execute incremental refresh on both materialized views:
      1. daily_sales_summary
      2. top_products_summary
    
    Returns:
        Dict containing execution metrics, elapsed time, and per-view statistics.
    """
    database = get_db(db)
    ensure_materialized_view_indexes(database)

    start_time = time.perf_counter()
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    daily_stats = refresh_daily_sales_summary(database, full_refresh=full_refresh, now_iso=now_iso)
    prod_stats = refresh_top_products_summary(database, full_refresh=full_refresh, now_iso=now_iso)

    elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

    return {
        "status": "SUCCESS",
        "action": "REFRESH_MATERIALIZED_VIEWS",
        "refresh_mode": "FULL" if full_refresh else "INCREMENTAL",
        "refreshed_at": now_iso,
        "execution_time_ms": elapsed_ms,
        "views": {
            MV_DAILY_SALES: daily_stats,
            MV_TOP_PRODUCTS: prod_stats
        },
        "summary": "تم تنفيذ التحديث التزايدي للعروض المادية بنجاح دون إعادة بناء البيانات من الصفر."
    }


# -----------------------------------------------------------------------------
# Query View Data (Helpers)
# -----------------------------------------------------------------------------
def get_daily_sales_view(limit: int = 30, sort_order: int = -1, db=None) -> List[Dict[str, Any]]:
    """Retrieve summarized records directly from daily_sales_summary materialized view."""
    database = get_db(db)
    view_col = database[MV_DAILY_SALES]
    cursor = view_col.find({}).sort("date", int(sort_order)).limit(int(limit))
    return [_clean_doc(d) for d in cursor]


def get_top_products_view(limit: int = 20, db=None) -> List[Dict[str, Any]]:
    """Retrieve top products directly from top_products_summary materialized view."""
    database = get_db(db)
    view_col = database[MV_TOP_PRODUCTS]
    cursor = view_col.find({}).sort("total_quantity", -1).limit(int(limit))
    return [_clean_doc(d) for d in cursor]


def get_materialized_views_status(db=None) -> Dict[str, Any]:
    """Retrieve the current health, document count, and watermark of materialized views."""
    database = get_db(db)
    daily_col = database[MV_DAILY_SALES]
    prod_col = database[MV_TOP_PRODUCTS]

    return {
        "daily_sales_summary": {
            "collection_name": MV_DAILY_SALES,
            "document_count": daily_col.count_documents({}),
            "latest_update": _get_latest_watermark(daily_col)
        },
        "top_products_summary": {
            "collection_name": MV_TOP_PRODUCTS,
            "document_count": prod_col.count_documents({}),
            "latest_update": _get_latest_watermark(prod_col)
        }
    }
