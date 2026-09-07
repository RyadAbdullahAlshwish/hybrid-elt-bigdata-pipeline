import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.quality_rules import (
    clean_record_and_generate_audit,
    normalize_digits_and_separators,
    clean_thousands_separators,
    strip_currency_text,
    convert_arabic_words_to_number,
    clean_negative_values,
    normalize_currency_code,
    normalize_status_string,
    clean_email_and_phone,
)


def test_rule_1_arabic_digits():
    raw = "٧٠٦٠٠٠٫٠"
    res = normalize_digits_and_separators(raw)
    assert res == "706000.0"


def test_rule_2_thousands_separators():
    raw = "135,000.00"
    res = clean_thousands_separators(raw)
    assert res == "135000.00"


def test_rule_3_strip_currency_text():
    raw = "54000.00 ريال"
    res = strip_currency_text(raw)
    assert res == "54000.00"


def test_rule_4_arabic_words():
    raw = "ألفان"
    res = convert_arabic_words_to_number(raw)
    assert res == "2000.0"

    raw2 = "خمسة آلاف"
    res2 = convert_arabic_words_to_number(raw2)
    assert res2 == "5000.0"


def test_rule_5_negative_values():
    raw = "-21500.0"
    res = clean_negative_values(raw)
    assert res == "21500.0"


def test_rule_6_currency_normalization():
    raw = "ريال يمني"
    res = normalize_currency_code(raw)
    assert res == "YER"


def test_rule_7_status_normalization():
    raw = "مدفوع"
    res = normalize_status_string(raw)
    assert res == "تم الدفع"


def test_rule_8_email_and_phone_cleaning():
    email = "user819896@@example.com"
    res_email = clean_email_and_phone("customer_email", email)
    assert res_email == "user819896@example.com"


def test_audit_trail_generation():
    raw_record = {
        "order_id": "ORD-123",
        "delivery_cost": "ألفان",
        "payment_amount": "54000.00 ريال",
        "currency": "ريال يمني",
        "status": "مدفوع",
        "customer_email": "user819896@@example.com"
    }

    cleaned, corrections = clean_record_and_generate_audit(raw_record)

    assert cleaned["delivery_cost"] == "2000.0"
    assert cleaned["payment_amount"] == "54000.00"
    assert cleaned["currency"] == "YER"
    assert cleaned["status"] == "تم الدفع"
    assert cleaned["customer_email"] == "user819896@example.com"

    rule_codes = [c["rule_code"] for c in corrections]
    assert "R4_ARABIC_WORDS_CONVERSION" in rule_codes
    assert "R3_STRIP_CURRENCY_TEXT" in rule_codes
    assert "R6_CURRENCY_NORMALIZATION" in rule_codes
    assert "R7_STATUS_NORMALIZATION" in rule_codes
    assert "R9_CONTACT_FORMAT_CLEANING" in rule_codes


def test_rule_8_recalculate_total_amount():
    """Test R8: Verify total_amount is correctly recalculated from items_json using unit_price."""
    raw_record = {
        "order_id": "ORD-R8-TEST",
        "customer_id": "CUST-1",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"SKU-1","name":"Phone","qty":2,"unit_price":100000.0,"total":200000.0},{"sku":"SKU-2","name":"Case","qty":1,"unit_price":5000.0,"total":5000.0}]',
        "total_amount": "999999",
        "delivery_cost": "3000",
        "currency": "YER",
        "status": "مؤكد",
    }

    cleaned, corrections = clean_record_and_generate_audit(raw_record)

    # items total = (100000 * 2) + (5000 * 1) = 205000
    # expected total = 205000 + 3000 (delivery) = 208000
    assert cleaned["total_amount"] == "208000.0"

    rule_codes = [c["rule_code"] for c in corrections]
    assert "R8_RECALCULATE_TOTAL_AMOUNT" in rule_codes


def test_rule_8_correct_total_unchanged():
    """Test R8: If total_amount is already correct, it should NOT be modified."""
    raw_record = {
        "order_id": "ORD-R8-CORRECT",
        "customer_id": "CUST-1",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"SKU-1","name":"Phone","qty":1,"unit_price":50000.0,"total":50000.0}]',
        "total_amount": "52000",
        "delivery_cost": "2000",
        "currency": "YER",
        "status": "مؤكد",
    }

    cleaned, corrections = clean_record_and_generate_audit(raw_record)

    # items total = 50000, delivery = 2000, expected = 52000 (matches!)
    assert cleaned["total_amount"] == "52000"

    rule_codes = [c["rule_code"] for c in corrections]
    assert "R8_RECALCULATE_TOTAL_AMOUNT" not in rule_codes


def test_rule_8_negative_qty_handled():
    """Test R8: Negative qty inside items_json should be treated as absolute value."""
    raw_record = {
        "order_id": "ORD-R8-NEG",
        "customer_id": "CUST-1",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"SKU-1","name":"Phone","qty":-2,"unit_price":100000.0,"total":200000.0}]',
        "total_amount": "202000",
        "delivery_cost": "2000",
        "currency": "YER",
        "status": "مؤكد",
    }

    cleaned, corrections = clean_record_and_generate_audit(raw_record)

    # abs(-2) * 100000 = 200000 + 2000 delivery = 202000 (matches!)
    assert cleaned["total_amount"] == "202000"

    rule_codes = [c["rule_code"] for c in corrections]
    assert "R8_RECALCULATE_TOTAL_AMOUNT" not in rule_codes
