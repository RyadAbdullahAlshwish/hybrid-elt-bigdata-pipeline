from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def setup_mongodb():
    """
    Initialize MongoDB collections and indexes required for the ELT pipeline.
    Enforces Unique Index on order_id in orders_validated to support Idempotent Upserts.
    """
    client = MongoClient(MONGO_URI)

    try:
        db = client[MONGO_DATABASE]

        # ----------------------------------------------------
        # 1. Raw Collection Indexes
        # ----------------------------------------------------
        raw_col = db[RAW_COLLECTION]
        raw_col.drop() # Clear dirty state from previous runs

        raw_col.create_index([("metadata.run_id", 1)], name="idx_run_id")
        raw_col.create_index([("metadata.source_file", 1)], name="idx_source_file")
        raw_col.create_index([("source_data.order_id", 1)], name="idx_order_id")

        # ----------------------------------------------------
        # 2. Validated Collection Indexes (Unique Index on order_id)
        # ----------------------------------------------------
        val_col = db[VALIDATED_COLLECTION]
        val_col.drop() # Clear dirty state from previous runs
        val_col.create_index([("order_id", 1)], unique=True, name="idx_val_order_id_unique")
        val_col.create_index([("metadata.run_id", 1)], name="idx_val_run_id")
        val_col.create_index([("quality_status", 1)], name="idx_val_quality_status")

        # ----------------------------------------------------
        # 3. Quarantine Collection Indexes
        # ----------------------------------------------------
        quar_col = db[QUARANTINE_COLLECTION]
        quar_col.drop() # Clear dirty state from previous runs

        quar_col.create_index([("metadata.run_id", 1)], name="idx_quar_run_id")
        quar_col.create_index([("quarantine_reasons", 1)], name="idx_quar_reasons")

        print("=" * 60)
        print("MONGODB SETUP COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"URI         : {MONGO_URI}")
        print(f"Database    : {MONGO_DATABASE}")
        print(f"Collections : {RAW_COLLECTION}, {VALIDATED_COLLECTION}, {QUARANTINE_COLLECTION}")
        print("=" * 60)

    finally:
        client.close()


if __name__ == "__main__":
    setup_mongodb()