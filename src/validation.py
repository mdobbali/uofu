# src/validation.py
from datetime import datetime

def validate_row(row: dict) -> bool:
    """
    Validate that a row has a non-empty patient_id
    and a correctly formatted encounter_date (YYYY-MM-DD).
    Returns True if valid, False otherwise.
    """
    # Check patient_id is present and not empty/blank
    patient_id = row.get("patient_id")
    if not patient_id or not str(patient_id).strip():
        return False

    # Check encounter_date format
    date_str = row.get("encounter_date")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return False

    return True