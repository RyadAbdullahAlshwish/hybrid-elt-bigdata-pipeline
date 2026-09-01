import re
import json
from typing import Any, Dict, List, Tuple


# ============================================================
# Mapping Tables & Constants
# ============================================================

ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")

ARABIC_NUMBER_WORDS = {
    "ألف": 1000.0,
    "الف": 1000.0,
    "ألفان": 2000.0,
    "الفان": 2000.0,
    "ألفين": 2000.0,
    "ثلاثة آلاف": 3000.0,
    "ثلاثة الاف": 3000.0,
    "أربعة آلاف": 4000.0,
    "أربعة الاف": 4000.0,
    "خمسة آلاف": 5000.0,
    "خمسة الاف": 5000.0,
    "ستة آلاف": 6000.0,
    "سبعة آلاف": 7000.0,
    "ثمانية آلاف": 8000.0,
    "تسعة آلاف": 9000.0,
    "عشرة آلاف": 10000.0,
}

CURRENCY_MAPPING = {
    "ريال يمني": "YER",
    "ريال": "YER",
    "YER": "YER",
    "yer": "YER",
    "YEMENI RIAL": "YER",
}

STATUS_MAPPING = {
    "تم الدفع": "تم الدفع",
    "مدفوع": "تم الدفع",
    "بانتظار الدفع": "بانتظار الدفع",
    "قيد الدفع": "بانتظار الدفع",
}

CRITICAL_FIELDS = ["order_id", "customer_id", "items_json", "total_amount"]


# ============================================================
# Core Cleaning Rules Functions
# ============================================================

def normalize_digits_and_separators(val: str) -> str:
    """Rule 1: Convert Eastern Arabic/Persian digits to Western digits and decimal points."""
    if not isinstance(val, str):
        return val
    val = val.translate(ARABIC_DIGITS).translate(PERSIAN_DIGITS)
    val = val.replace("٫", ".").replace("٬", ",")
    return val


def clean_thousands_separators(val: str) -> str:
    """Rule 2: Remove thousands separators (commas) from numeric strings."""
    if not isinstance(val, str):
        return val
    # Remove commas if surrounded by digits or numbers
    if re.search(r"\d+,\d+", val):
        val = val.replace(",", "")
    return val


def strip_currency_text(val: str) -> str:
    """Rule 3: Remove currency suffixes/prefixes from numeric string values."""
    if not isinstance(val, str):
        return val
    cleaned = re.sub(r"\s*(ريال|YER|yer|\$|USD)\s*", "", val).strip()
    return cleaned


def convert_arabic_words_to_number(val: str) -> str:
    """Rule 4: Convert Arabic written numbers (e.g. 'ألفان', 'خمسة آلاف') to numeric string."""
    if not isinstance(val, str):
        return val
    stripped = val.strip()
    if stripped in ARABIC_NUMBER_WORDS:
        return str(ARABIC_NUMBER_WORDS[stripped])
    return val


def clean_negative_values(val: str) -> str:
    """Rule 5: Clean negative values in monetary fields by converting to positive string."""
    if not isinstance(val, str):
        return val
    val_str = val.strip()
    if val_str.startswith("-"):
        try:
            num = float(val_str)
            return str(abs(num))
        except ValueError:
            pass
    return val


def normalize_currency_code(val: str) -> str:
    """Rule 6: Standardize currency codes to YER."""
    if not isinstance(val, str):
        return val
    stripped = val.strip()
    if stripped in CURRENCY_MAPPING:
        return CURRENCY_MAPPING[stripped]
    return val


def normalize_status_string(val: str) -> str:
    """Rule 7: Standardize status and payment_status strings."""
    if not isinstance(val, str):
        return val
    stripped = val.strip()
    if stripped in STATUS_MAPPING:
        return STATUS_MAPPING[stripped]
    return val


def clean_email_and_phone(field_name: str, val: str) -> str:
    """Rule 8: Clean email and phone number formats."""
    if not isinstance(val, str):
        return val
    val_str = val.strip()
    
    if field_name == "customer_email":
        # Fix doubled @@ and doubled dots ..
        cleaned = re.sub(r"@{2,}", "@", val_str)
        cleaned = re.sub(r"\.{2,}", ".", cleaned)
        return cleaned
    
    if field_name == "customer_phone":
        # Standardize phone formatting if space-separated
        digits_only = "".join(ch for ch in val_str if ch.isdigit())
        if len(digits_only) >= 9:
            return val_str.strip()
    
    return val_str


# ============================================================
# Master Cleaning & Audit Trail Logic
# ============================================================

def clean_record_and_generate_audit(source_data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Apply the 8 data quality cleaning rules to a raw record dictionary
    and record an Audit Trail for any field that is modified.

    Returns:
        tuple: (cleaned_data_dict, list_of_audit_corrections)
    """
    cleaned_data = dict(source_data)
    corrections = []

    numeric_fields = ["delivery_cost", "payment_amount", "total_amount"]

    for field, raw_val in source_data.items():
        if raw_val is None:
            continue
        
        current_val = str(raw_val).strip()
        orig_val = current_val

        # ----------------------------------------------------
        # Rule 1: Digits & Separator Normalization
        # ----------------------------------------------------
        res = normalize_digits_and_separators(current_val)
        if res != current_val:
            corrections.append({
                "field": field,
                "original_value": current_val,
                "corrected_value": res,
                "rule_code": "R1_NORMALIZE_DIGITS"
            })
            current_val = res

        # ----------------------------------------------------
        # Numeric Field Cleanings (Rules 2, 3, 4, 5)
        # ----------------------------------------------------
        if field in numeric_fields and current_val:
            # Rule 4: Spelled out words
            res = convert_arabic_words_to_number(current_val)
            if res != current_val:
                corrections.append({
                    "field": field,
                    "original_value": current_val,
                    "corrected_value": res,
                    "rule_code": "R4_ARABIC_WORDS_CONVERSION"
                })
                current_val = res

            # Rule 3: Strip currency text
            res = strip_currency_text(current_val)
            if res != current_val:
                corrections.append({
                    "field": field,
                    "original_value": current_val,
                    "corrected_value": res,
                    "rule_code": "R3_STRIP_CURRENCY_TEXT"
                })
                current_val = res

            # Rule 2: Thousands separator
            res = clean_thousands_separators(current_val)
            if res != current_val:
                corrections.append({
                    "field": field,
                    "original_value": current_val,
                    "corrected_value": res,
                    "rule_code": "R2_REMOVE_THOUSANDS_SEPARATOR"
                })
                current_val = res

            # Rule 5: Negative values
            res = clean_negative_values(current_val)
            if res != current_val:
                corrections.append({
                    "field": field,
                    "original_value": current_val,
                    "corrected_value": res,
                    "rule_code": "R5_ABS_NEGATIVE_VALUE"
                })
                current_val = res

        # ----------------------------------------------------
        # Rule 6: Currency Normalization
        # ----------------------------------------------------
        if field == "currency" and current_val:
            res = normalize_currency_code(current_val)
            if res != current_val:
                corrections.append({
                    "field": field,
                    "original_value": current_val,
                    "corrected_value": res,
                    "rule_code": "R6_CURRENCY_NORMALIZATION"
                })
                current_val = res

        # ----------------------------------------------------
        # Rule 7: Status Normalization
        # ----------------------------------------------------
        if field in ["status", "payment_status"] and current_val:
            res = normalize_status_string(current_val)
            if res != current_val:
                corrections.append({
                    "field": field,
                    "original_value": current_val,
                    "corrected_value": res,
                    "rule_code": "R7_STATUS_NORMALIZATION"
                })
                current_val = res

        # ----------------------------------------------------
        # Rule 8: Email & Phone Formatting
        # ----------------------------------------------------
        if field in ["customer_email", "customer_phone"] and current_val:
            res = clean_email_and_phone(field, current_val)
            if res != current_val:
                corrections.append({
                    "field": field,
                    "original_value": current_val,
                    "corrected_value": res,
                    "rule_code": "R8_CONTACT_FORMAT_CLEANING"
                })
                current_val = res

        cleaned_data[field] = current_val

    return cleaned_data, corrections
