from pathlib import Path


# ============================================================
# Project Paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
SAMPLES_DIR = DATA_DIR / "samples"

REPORTS_DIR = PROJECT_ROOT / "reports"


# ============================================================
# Input Files
# ============================================================

LARGE_INPUT_FILE = RAW_DATA_DIR / "orders_huge_mixed_quality.csv"

SAMPLE_INPUT_FILE = SAMPLES_DIR / "orders_sample_10k.csv"


# ============================================================
# File Router
# ============================================================

# Files <= this size will use Python Batch.
# Files > this size will use PySpark.
SMALL_FILE_THRESHOLD_MB = 200


# ============================================================
# Python Batch
# ============================================================

BATCH_SIZE = 1000


# ============================================================
# MongoDB
# ============================================================

MONGO_URI = "mongodb://localhost:27017"

MONGO_DATABASE = "midterm_data_pipeline"

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"