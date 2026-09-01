import json
from typing import Any, Dict, List, Tuple


def evaluate_record_classification(
    cleaned_data: Dict[str, Any],
    corrections: List[Dict[str, Any]],
) -> Tuple[str, List[str]]:
    """
    Evaluate whether a cleaned record should be classified as:
    - 'VALID': Record was clean originally.
    - 'CORRECTED': Record was fixed via cleaning rules and is valid.
    - 'QUARANTINE': Record contains unrecoverable defects.

    Returns:
        tuple: (status, list_of_quarantine_reasons)
    """
    reasons = []

    # 1. Missing order_id
    order_id = cleaned_data.get("order_id")
    if not order_id or str(order_id).strip() == "" or str(order_id).strip() == "???":
        reasons.append("MISSING_ORDER_ID")

    # 2. Missing customer_id
    customer_id = cleaned_data.get("customer_id")
    if not customer_id or str(customer_id).strip() == "" or str(customer_id).strip() == "???":
        reasons.append("MISSING_CUSTOMER_ID")

    # 3. Corrupted or Empty items_json
    items_raw = cleaned_data.get("items_json")
    if not items_raw or str(items_raw).strip() == "" or str(items_raw).strip() == "???":
        reasons.append("EMPTY_ITEMS")
    else:
        try:
            parsed_items = json.loads(items_raw)
            if not isinstance(parsed_items, list) or len(parsed_items) == 0:
                reasons.append("EMPTY_ITEMS")
        except (json.JSONDecodeError, TypeError):
            reasons.append("CORRUPTED_ITEMS_JSON")

    # 4. Total Amount Validation
    total_amt = cleaned_data.get("total_amount")
    if not total_amt or str(total_amt).strip() == "???":
        reasons.append("CORRUPTED_TOTAL_AMOUNT")
    else:
        try:
            float(str(total_amt).replace(",", ""))
        except ValueError:
            reasons.append("CORRUPTED_TOTAL_AMOUNT")

    # 5. Check for unresolvable corrupted text in status or currency
    status_val = str(cleaned_data.get("status", "")).strip()
    if status_val == "حالة غير معروفة تمامًا" or status_val == "???":
        reasons.append("UNRESOLVED_CRITICAL_FIELD")

    currency_val = str(cleaned_data.get("currency", "")).strip()
    if currency_val == "عملة غير معروفة" or currency_val == "???":
        reasons.append("UNRESOLVED_CRITICAL_FIELD")

    # If any quarantine reason was triggered, record goes to QUARANTINE
    if reasons:
        return "QUARANTINE", reasons

    # If no quarantine reasons: check if corrections were made
    if corrections:
        return "CORRECTED", []
    
    return "VALID", []
