import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.classification import evaluate_record_classification


def test_classification_valid():
    cleaned_data = {
        "order_id": "ORD-1001",
        "customer_id": "CUST-500",
        "items_json": '[{"item_id": 1, "price": 100}]',
        "total_amount": "100.00",
        "currency": "YER",
        "status": "مؤكد"
    }
    corrections = []

    status, reasons = evaluate_record_classification(cleaned_data, corrections)
    assert status == "VALID"
    assert len(reasons) == 0


def test_classification_corrected():
    cleaned_data = {
        "order_id": "ORD-1002",
        "customer_id": "CUST-500",
        "items_json": '[{"item_id": 1, "price": 100}]',
        "total_amount": "100.00",
        "currency": "YER",
        "status": "تم الدفع"
    }
    corrections = [
        {"field": "status", "original_value": "مدفوع", "corrected_value": "تم الدفع", "rule_code": "R7_STATUS_NORMALIZATION"}
    ]

    status, reasons = evaluate_record_classification(cleaned_data, corrections)
    assert status == "CORRECTED"
    assert len(reasons) == 0


def test_classification_quarantine_missing_order_id():
    cleaned_data = {
        "order_id": "",
        "customer_id": "CUST-500",
        "items_json": '[{"item_id": 1}]',
        "total_amount": "100.00"
    }
    corrections = []

    status, reasons = evaluate_record_classification(cleaned_data, corrections)
    assert status == "QUARANTINE"
    assert "MISSING_ORDER_ID" in reasons


def test_classification_quarantine_corrupted_json():
    cleaned_data = {
        "order_id": "ORD-1003",
        "customer_id": "CUST-500",
        "items_json": "INVALID_JSON_{{",
        "total_amount": "100.00"
    }
    corrections = []

    status, reasons = evaluate_record_classification(cleaned_data, corrections)
    assert status == "QUARANTINE"
    assert "CORRUPTED_ITEMS_JSON" in reasons
