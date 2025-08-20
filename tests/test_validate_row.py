# tests/test_validate_row.py
import pytest
from src.validation import validate_row

def test_validate_row_valid():
    row = {"patient_id": "P12345", "encounter_date": "2025-08-01"}
    assert validate_row(row) is True

def test_validate_row_invalid():
    row = {"patient_id": "", "encounter_date": "08/01/2025"}
    assert validate_row(row) is False