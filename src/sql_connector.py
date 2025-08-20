"""
pip install sqlalchemy pyodbc boto3
# macOS: brew install unixodbc
# Driver: ODBC Driver 18 for SQL Server
# ENV needed: MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD
"""

import os, csv, json, io
from typing import Dict, Iterable, List, Tuple

import boto3
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy import event

def validate_row(row: Dict) -> bool:
    "this function validates data"
    if not row.get("patient_id") or not str(row["patient_id"]).strip():
        return False
    d = row.get("encounter_date")
    if not d or len(d.split("-")) != 3:  # quick YYYY-MM-DD check
        return False
    return True

def stream_s3_records(bucket: str, prefix: str, filetype: str = "jsonl") -> Iterable[Dict]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            if filetype.lower() == "jsonl":
                for line in io.BytesIO(body):
                    line = line.decode("utf-8").strip()
                    if line:
                        yield json.loads(line)
            elif filetype.lower() == "csv":
                r = csv.DictReader(io.StringIO(body.decode("utf-8")))
                for row in r:
                    yield row
            else:
                raise ValueError("Use filetype='jsonl' or 'csv'.")

def make_engine() -> Engine:
    driver = "ODBC Driver 18 for SQL Server"
    server = os.environ["MSSQL_SERVER"]      # e.g. "tcp:myserver.db.windows.net,1433"
    database = os.environ["MSSQL_DATABASE"]  # e.g. "claimsdb"
    uid = os.environ.get("MSSQL_USER")
    pwd = os.environ.get("MSSQL_PASSWORD")
    odbc = (
        f"Driver={driver};Server={server};Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
        + (f"UID={uid};PWD={pwd};" if uid and pwd else "")
    )
    from urllib.parse import quote_plus
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}",
                           fast_executemany=True, pool_pre_ping=True)

    @event.listens_for(engine, "before_cursor_execute")
    def _enable_fast_executemany(conn, cursor, statement, params, context, executemany):
        if executemany and hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True
    return engine

def insert_batches(
    engine: Engine,
    rows: Iterable[Dict],
    table: str,
    cols: List[str],
    batch_size: int = 1000,
    invalid_log_path: str = "invalid_rows.csv",
) -> Tuple[int, int]:
    valid_batch, valid_count, invalid_count = [], 0, 0

    # init invalid log
    if not os.path.exists(invalid_log_path):
        with open(invalid_log_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=cols + ["_error"]).writeheader()

    def flush():
        nonlocal valid_batch, valid_count
        if not valid_batch:
            return
        placeholders = ", ".join(f":{c}" for c in cols)
        stmt = text(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})")
        with engine.begin() as conn:  # one transaction per batch
            conn.execute(stmt, valid_batch)
        valid_count += len(valid_batch)
        valid_batch = []

    for row in rows:
        if validate_row(row):
            valid_batch.append({c: row.get(c) for c in cols})
            if len(valid_batch) >= batch_size:
                flush()
        else:
            invalid_count += 1
            with open(invalid_log_path, "a", newline="", encoding="utf-8") as f:
                out = {c: row.get(c) for c in cols}
                out["_error"] = "validation_failed"
                csv.DictWriter(f, fieldnames=cols + ["_error"]).writer.writerow(out)

    flush()
    return valid_count, invalid_count

if __name__ == "__main__":
    # Set via env or use AWS SSO/instance role for S3
    BUCKET = "my-ingest-bucket"
    PREFIX = "claims/2025-08-16/"   # partition or folder
    FILETYPE = "jsonl"              # or "csv"

    TABLE = "dbo.PatientEncounters"
    COLS = ["patient_id", "encounter_date", "claim_amount", "status_code"]

    engine = make_engine()
    rows = stream_s3_records(BUCKET, PREFIX, filetype=FILETYPE)
    inserted, skipped = insert_batches(engine, rows, TABLE, COLS, batch_size=2000)
    print(f"Inserted: {inserted:,} | Skipped (invalid): {skipped:,}")