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
    assert "R8_CONTACT_FORMAT_CLEANING" in rule_codes
