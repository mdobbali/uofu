# Function Details

```
def validate_row(row: Dict) -> bool:
    "this function validates data"
    if not row.get("patient_id") or not str(row["patient_id"]).strip():
        return False
    d = row.get("encounter_date")
    if not d or len(d.split("-")) != 3:  # quick YYYY-MM-DD check
        return False
    return True

```

This is doing this and that: https://dillinger.io/
