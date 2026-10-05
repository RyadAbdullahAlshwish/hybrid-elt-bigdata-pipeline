"""
Unified FastAPI Application for Big Data Project Evaluation.
Phase 2 - Requirements 5: 10 Standard Endpoints with JSON Output and Interactive Swagger UI (/docs).
"""

import time
from enum import Enum
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pymongo import MongoClient


# -----------------------------------------------------------------------------
# Enums for Swagger UI Dropdown Selection
# -----------------------------------------------------------------------------
class QueryName(str, Enum):
    orders_by_city_and_status = "orders_by_city_and_status"
    recent_orders_by_date = "recent_orders_by_date"
    customer_order_history = "customer_order_history"
    high_value_orders = "high_value_orders"
    orders_by_payment_method_and_status = "orders_by_payment_method_and_status"


class AggregationName(str, Enum):
    sales_by_city = "sales_by_city"
    top_products = "top_products"
    top_customers = "top_customers"
    sales_by_period = "sales_by_period"
    orders_by_status = "orders_by_status"


class JobName(str, Enum):
    refresh_materialized_views = "refresh_materialized_views"
    generate_periodic_metrics = "generate_periodic_metrics"


class MaterializedViewName(str, Enum):
    daily_sales_summary = "daily_sales_summary"
    top_products_summary = "top_products_summary"

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    SAMPLE_INPUT_FILE,
    LARGE_INPUT_FILE,
)
from src.indexes import (
    create_indexes,
    list_indexes,
    run_explain_analysis,
)
from src.queries import (
    get_available_queries,
    execute_query,
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
    start_scheduler,
    stop_scheduler,
    JOB_LOGS_COLLECTION,
)
from src.elt_pipeline import run_elt_pipeline


# -----------------------------------------------------------------------------
# Lifespan Management (Background Scheduler Startup / Shutdown)
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start scheduled tasks daemon
    start_scheduler()
    yield
    # Shutdown: Stop scheduler daemon cleanly
    stop_scheduler()


# -----------------------------------------------------------------------------
# FastAPI App Definition
# -----------------------------------------------------------------------------
app = FastAPI(
    title="Hybrid ELT Big Data Pipeline API",
    description=(
        "واجهة التشغيل والتقييم الموحدة للمشروع النهائي (Big Data - Phase 2).\n\n"
        "تمكن لجان التقييم والأنظمة الآلية من تشغيل وفحص كافة وظائف خط البيانات "
        "(الاستعلامات، الفهارس، التجميعات، العروض المادية، والمهام المجدولة) بصيغة JSON."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Enable CORS for interactive Swagger UI and external clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


# -----------------------------------------------------------------------------
# Root Endpoint -> Redirect to /docs
# -----------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def root_redirect():
    """Redirect root path to interactive Swagger documentation."""
    return RedirectResponse(url="/docs")


# -----------------------------------------------------------------------------
# 1. GET /health: فحص جاهزية النظام والاتصال
# -----------------------------------------------------------------------------
@app.get(
    "/health",
    tags=["System & Health"],
    summary="فحص حالة النظام والاتصال بقاعدة البيانات"
)
def health_check():
    """فحص جاهزية النظام والاتصال بقاعدة بيانات MongoDB وتعداد السجلات في كافة المجموعات."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.server_info()
        db = client[MONGO_DATABASE]

        collections_stats = {
            RAW_COLLECTION: db[RAW_COLLECTION].count_documents({}),
            VALIDATED_COLLECTION: db[VALIDATED_COLLECTION].count_documents({}),
            QUARANTINE_COLLECTION: db[QUARANTINE_COLLECTION].count_documents({}),
            MV_DAILY_SALES: db[MV_DAILY_SALES].count_documents({}),
            MV_TOP_PRODUCTS: db[MV_TOP_PRODUCTS].count_documents({}),
            JOB_LOGS_COLLECTION: db[JOB_LOGS_COLLECTION].count_documents({}),
        }

        return {
            "status": "HEALTHY",
            "message": "النظام يعمل والاتصال بقاعدة بيانات MongoDB مستقر وجاهز.",
            "database": MONGO_DATABASE,
            "mongodb_uri": MONGO_URI,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "collections_document_counts": collections_stats
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"فشل الاتصال بقاعدة البيانات MongoDB: {str(e)}"
        )


# -----------------------------------------------------------------------------
# 2. POST /ingest: تشغيل مسار إدخال البيانات المعتمد
# -----------------------------------------------------------------------------
@app.post(
    "/ingest",
    tags=["ELT Ingestion"],
    summary="تشغيل خط أنابيب الإدخال والمعالجة الأصلي (ELT Pipeline)"
)
def trigger_ingest(
    payload: Optional[Dict[str, Any]] = Body(
        default=None,
        example={"file_path": "data/samples/orders_sample_100k.csv"}
    )
):
    """
    تشغيل مسار إدخال ومعالجة البيانات المعتمد في المشروع النصفي (ELT Pipeline).
    يقبل مسار أي ملف جديد بصيغة CSV لتنفيذ التحميل الخام ثم التنظيف والتصنيف.
    """
    file_path = None
    if payload and "file_path" in payload:
        file_path = payload["file_path"]

    # If no file specified, auto-discover available sample
    if not file_path:
        sample_100k = Path("data/samples/orders_sample_100k.csv")
        sample_10k = Path(SAMPLE_INPUT_FILE)
        if sample_100k.exists():
            file_path = str(sample_100k)
        elif sample_10k.exists():
            file_path = str(sample_10k)
        else:
            file_path = str(LARGE_INPUT_FILE)

    target_path = Path(file_path)
    if not target_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"الملف المحدد غير موجود على القرص: {file_path}"
        )

    try:
        report = run_elt_pipeline(str(target_path))
        return {
            "status": "SUCCESS",
            "message": "تم اكتمال تشغيل خط المعالجة بنجاح.",
            "ingestion_report": report
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء تشغيل مسار الإدخال: {str(e)}"
        )


# -----------------------------------------------------------------------------
# 3. POST /indexes: إنشاء الفهارس المحددة
# -----------------------------------------------------------------------------
@app.post(
    "/indexes",
    tags=["Indexes & Performance"],
    summary="إنشاء وتجهيز الفهارس الثلاثة المطلوبة"
)
def create_pipeline_indexes():
    """
    إنشاء الفهارس الثلاثة المعتمدة على مجموعة orders_validated:
    1. الفهرس المركب (city, status).
    2. فهرس التاريخ (order_date).
    3. فهرس العميل (customer_id).
    """
    try:
        result = create_indexes()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء إنشاء الفهارس: {str(e)}"
        )


# -----------------------------------------------------------------------------
# Extra: GET /indexes/explain: تنفيذ تحليل الأداء قبل وبعد
# -----------------------------------------------------------------------------
@app.get(
    "/indexes/explain",
    tags=["Indexes & Performance"],
    summary="تشغيل تحليل الأداء (Explain Plan) ومقارنة النتائج قبل وبعد الفهارس"
)
def get_explain_analysis():
    """تشغيل أمر explain('executionStats') على 3 استعلامات قبل وبعد الفهارس وتوليد التقارير."""
    try:
        analysis = run_explain_analysis(save_reports=True)
        return analysis
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء تنفيذ تحليل الأداء: {str(e)}"
        )


# -----------------------------------------------------------------------------
# 4. GET /queries: استرجاع قائمة الاستعلامات المتاحة
# -----------------------------------------------------------------------------
@app.get(
    "/queries",
    tags=["Queries"],
    summary="استرجاع قائمة بأسماء وتفاصيل الاستعلامات المتاحة"
)
def list_available_queries():
    """سرد الاستعلامات الخمسة العملية المتاحة مع وصف كل استعلام والمعاملات المدعومة."""
    return {
        "status": "SUCCESS",
        "total_queries": len(get_available_queries()),
        "queries": get_available_queries()
    }


# -----------------------------------------------------------------------------
# 5. GET /queries/{name}: تشغيل استعلام محدد واسترجاع نتيجته
# -----------------------------------------------------------------------------
@app.get(
    "/queries/{name}",
    tags=["Queries"],
    summary="تشغيل استعلام محدد بالاسم واسترجاع نتائجه بصيغة JSON"
)
def run_named_query(name: QueryName, request: Request):
    """
    تشغيل أحد الاستعلامات الخمسة العملية بالاسم.
    يقبل أي معاملات إضافية كـ Query Parameters (مثل: city, status, limit, customer_id, start_date).
    """
    params = dict(request.query_params)
    # Parse integer limit if passed
    if "limit" in params:
        try:
            params["limit"] = int(params["limit"])
        except ValueError:
            pass

    try:
        query_key = name.value if hasattr(name, "value") else str(name)
        result = execute_query(query_key, params=params)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء تنفيذ الاستعلام '{name}': {str(e)}"
        )


# -----------------------------------------------------------------------------
# 6. GET /aggregations: استرجاع قائمة تقارير التجميع المتاحة
# -----------------------------------------------------------------------------
@app.get(
    "/aggregations",
    tags=["Aggregations"],
    summary="استرجاع قائمة بأسماء تقارير التجميع المتاحة"
)
def list_available_aggregations():
    """سرد تقارير التجميع الخمسة المتاحة مع الشرح والمعاملات المدعومة."""
    return {
        "status": "SUCCESS",
        "total_aggregations": len(get_available_aggregations()),
        "aggregations": get_available_aggregations()
    }


# -----------------------------------------------------------------------------
# 7. GET /aggregations/{name}: تشغيل تقرير تجميع محدد
# -----------------------------------------------------------------------------
@app.get(
    "/aggregations/{name}",
    tags=["Aggregations"],
    summary="تشغيل تقرير تجميع محدد واسترجاع نتائجه بصيغة JSON"
)
def run_named_aggregation(name: AggregationName, request: Request):
    """
    تشغيل أحد تقارير التجميع الخمسة بالاسم (مثل sales_by_city, top_products, top_customers, sales_by_period, orders_by_status).
    يقبل معاملات اختيارية مثل limit و period.
    """
    params = dict(request.query_params)
    if "limit" in params:
        try:
            params["limit"] = int(params["limit"])
        except ValueError:
            pass

    try:
        agg_key = name.value if hasattr(name, "value") else str(name)
        result = execute_aggregation(agg_key, params=params)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء تنفيذ التقرير التجميعي '{name}': {str(e)}"
        )


# -----------------------------------------------------------------------------
# 8. POST /refresh-mv: تنفيذ التحديث التزايدي للعروض المادية
# -----------------------------------------------------------------------------
@app.post(
    "/refresh-mv",
    tags=["Materialized Views"],
    summary="تنفيذ التحديث التزايدي للعروض المادية (Materialized Views)"
)
def trigger_refresh_materialized_views(
    full_refresh: bool = Query(
        default=False,
        description="تفعيل لإجراء إعادة بناء شاملة بدلاً من التحديث التزايدي (افتراضياً False)"
    )
):
    """
    تنفيذ التحديث التزايدي الذاتي لجدولي daily_sales_summary و top_products_summary
    دون إعادة بناء كامل البيانات من البداية في كل مرة.
    """
    try:
        result = refresh_materialized_views(full_refresh=full_refresh)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء تحديث العروض المادية: {str(e)}"
        )


# -----------------------------------------------------------------------------
# Extra: GET /views/{name}: قراءة نتائج العرض المادي مباشرة
# -----------------------------------------------------------------------------
@app.get(
    "/views/{name}",
    tags=["Materialized Views"],
    summary="قراءة محتويات عرض مادي محدد مباشرة"
)
def get_view_data(name: MaterializedViewName, limit: int = Query(default=20, ge=1, le=100)):
    """قراءة البيانات المخزنة مسبقاً داخل daily_sales_summary أو top_products_summary."""
    view_key = name.value if hasattr(name, "value") else str(name)
    if view_key == MV_DAILY_SALES:
        return {
            "view_name": view_key,
            "data": get_daily_sales_view(limit=limit)
        }
    elif view_key == MV_TOP_PRODUCTS:
        return {
            "view_name": view_key,
            "data": get_top_products_view(limit=limit)
        }
    else:
        raise HTTPException(
            status_code=404,
            detail=f"العرض المادي '{name}' غير موجود. المتاح: {[MV_DAILY_SALES, MV_TOP_PRODUCTS]}"
        )


# -----------------------------------------------------------------------------
# 9. GET /jobs: استعراض قائمة المهام المجدولة وسجل آخر تشغيل
# -----------------------------------------------------------------------------
@app.get(
    "/jobs",
    tags=["Scheduled Jobs"],
    summary="استعراض قائمة المهام المجدولة وسجل آخر تنفيذ لكل منها"
)
def list_scheduled_jobs():
    """استعراض المهام المجدولة المسجلة في النظام مع تفاصيل وحالة وتوقيت آخر عملية تشغيل من قاعدة البيانات."""
    try:
        jobs = get_jobs_status()
        return {
            "status": "SUCCESS",
            "total_jobs": len(jobs),
            "jobs": jobs
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء استعراض المهام المجدولة: {str(e)}"
        )


# -----------------------------------------------------------------------------
# 10. POST /jobs/{name}/run: تشغيل مهمة مجدولة يدوياً بالاسم
# -----------------------------------------------------------------------------
@app.post(
    "/jobs/{name}/run",
    tags=["Scheduled Jobs"],
    summary="تشغيل مهمة مجدولة محددة يدوياً واسترجاع نتيجتها"
)
def run_job_manually(name: JobName):
    """
    تشغيل مهمة مجدولة محددة يدوياً أثناء الاختبار أو المناقشة.
    يقوم بتنفيذ المهمة فورياً وتسجيل نتيجة التشغيل ووقت البداية والنهاية في مجموعة job_logs.
    """
    try:
        job_key = name.value if hasattr(name, "value") else str(name)
        result = run_job(job_name=job_key, trigger_type="MANUAL")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء تشغيل المهمة '{name}': {str(e)}"
        )


# -----------------------------------------------------------------------------
# Suppress IDE AI Extensions Probe (GET /v1/models)
# -----------------------------------------------------------------------------
@app.get("/v1/models", include_in_schema=False)
def suppress_v1_models():
    """Silently satisfy IDE background AI probes without cluttering terminal logs."""
    return {"object": "list", "data": []}

