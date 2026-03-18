"""
lambda/process_upload.py
────────────────────────
AWS Lambda — triggered automatically when a file lands in:
  s3://agentic-erp-artifacts-241030170015/uploads/pending/

What it does:
  1. Downloads the CSV or Excel file from S3
  2. Validates every row (required columns, data types, range checks)
  3. Upserts valid rows into RDS fusion_time_entries
     (INSERT ... ON CONFLICT (id) DO UPDATE — safe to re-upload)
  4. Writes a result JSON to uploads/results/{key}.json
     so the frontend can poll for completion

Expected CSV/Excel columns (order doesn't matter):
  id        integer   — unique row ID (auto-assigned if missing)
  employee  text      — employee name or ID
  date      date      — YYYY-MM-DD
  hours     decimal   — 0 < hours <= 24
  memo      text      — optional, can be blank

Environment variables (set in Lambda config):
  DB_HOST      — RDS endpoint
  DB_PORT      — 5432
  DB_NAME      — agentdb
  DB_USER      — pgadmin
  DB_PASSWORD  — from Secrets Manager (injected by Lambda execution role)
  AWS_REGION   — us-east-1
"""

import io
import json
import logging
import os
import traceback
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import boto3
import psycopg2
import psycopg2.extras

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET         = os.environ.get("S3_ARTIFACTS_BUCKET", "agentic-erp-artifacts-241030170015")
RESULTS_PREFIX = "uploads/results/"

REQUIRED_COLUMNS = {"employee", "date", "hours"}
ALL_COLUMNS      = {"id", "employee", "date", "hours", "memo"}

UPSERT_SQL = """
    INSERT INTO public.fusion_time_entries (id, employee, date, hours, memo)
    VALUES (%(id)s, %(employee)s, %(date)s, %(hours)s, %(memo)s)
    ON CONFLICT (id) DO UPDATE SET
        employee = EXCLUDED.employee,
        date     = EXCLUDED.date,
        hours    = EXCLUDED.hours,
        memo     = EXCLUDED.memo
"""

NEXT_ID_SQL = "SELECT COALESCE(MAX(id), 0) + 1 FROM public.fusion_time_entries"


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

def handler(event, context):
    """Lambda entry point — called by S3 trigger."""
    record     = event["Records"][0]
    bucket     = record["s3"]["bucket"]["name"]
    s3_key     = record["s3"]["object"]["key"]
    result_key = RESULTS_PREFIX + s3_key.split("/")[-1] + ".json"

    logger.info(f"Processing upload: bucket={bucket} key={s3_key}")

    result = {
        "status"        : "error",
        "rows_inserted" : 0,
        "rows_skipped"  : 0,
        "errors"        : [],
        "processed_at"  : datetime.utcnow().isoformat() + "Z",
        "source_key"    : s3_key,
    }

    try:
        # 1. Download file from S3
        rows = _download_and_parse(bucket, s3_key)
        logger.info(f"Parsed {len(rows)} rows from {s3_key}")

        # 2. Validate rows
        valid_rows, row_errors = _validate_rows(rows)
        result["errors"].extend(row_errors)
        result["rows_skipped"] = len(rows) - len(valid_rows)
        logger.info(f"Valid rows: {len(valid_rows)} / {len(rows)}")

        # 3. Upsert into RDS
        if valid_rows:
            inserted = _upsert_rows(valid_rows)
            result["rows_inserted"] = inserted
            result["status"]        = "success"
            logger.info(f"Upserted {inserted} rows into fusion_time_entries")
        else:
            result["status"] = "error" if row_errors else "empty"

    except Exception as e:
        logger.error(f"Fatal error: {e}\n{traceback.format_exc()}")
        result["errors"].append(f"Fatal: {str(e)}")
        result["status"] = "error"

    finally:
        # 4. Write result JSON back to S3 so frontend can poll
        _write_result(result_key, result)
        logger.info(f"Result written to s3://{BUCKET}/{result_key}")

    return result


# ─────────────────────────────────────────────────────────────
# Parse
# ─────────────────────────────────────────────────────────────

def _download_and_parse(bucket: str, key: str) -> list[dict]:
    """Download file from S3 and parse into list of dicts."""
    # Import here so Lambda layer only loads what's needed
    import pandas as pd

    obj      = s3.get_object(Bucket=bucket, Key=key)
    content  = obj["Body"].read()
    filename = key.lower()

    if filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content), dtype=str)
    elif filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(content), dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {filename}. Use .csv or .xlsx")

    # Normalise column names — lowercase, strip whitespace
    df.columns = [c.strip().lower() for c in df.columns]
    df         = df.where(df.notna(), None)   # NaN → None

    return df.to_dict(orient="records")


# ─────────────────────────────────────────────────────────────
# Validate
# ─────────────────────────────────────────────────────────────

def _validate_rows(rows: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Validate each row. Returns (valid_rows, error_messages).

    Rules:
      - employee : required, non-empty string
      - date     : required, parseable as YYYY-MM-DD
      - hours    : required, decimal 0 < hours <= 24
      - id       : optional integer; auto-assigned if missing
      - memo     : optional string
    """
    valid  = []
    errors = []

    # Check required columns exist in file
    if rows:
        file_cols = set(rows[0].keys())
        missing   = REQUIRED_COLUMNS - file_cols
        if missing:
            errors.append(f"Missing required columns: {missing}. Found: {file_cols}")
            return [], errors

    for i, row in enumerate(rows, start=2):   # row 2 = first data row (row 1 = header)
        row_errors = []

        # employee
        employee = (row.get("employee") or "").strip()
        if not employee:
            row_errors.append(f"Row {i}: employee is required")

        # date
        parsed_date = None
        raw_date    = (row.get("date") or "").strip()
        if not raw_date:
            row_errors.append(f"Row {i}: date is required")
        else:
            try:
                parsed_date = date.fromisoformat(raw_date)
            except ValueError:
                row_errors.append(f"Row {i}: date '{raw_date}' is not valid YYYY-MM-DD")

        # hours
        parsed_hours = None
        raw_hours    = str(row.get("hours") or "").strip()
        if not raw_hours:
            row_errors.append(f"Row {i}: hours is required")
        else:
            try:
                parsed_hours = float(raw_hours)
                if not (0 < parsed_hours <= 24):
                    row_errors.append(f"Row {i}: hours={parsed_hours} must be between 0 and 24")
            except ValueError:
                row_errors.append(f"Row {i}: hours '{raw_hours}' is not a valid number")

        # id (optional)
        parsed_id = None
        raw_id    = str(row.get("id") or "").strip()
        if raw_id:
            try:
                parsed_id = int(float(raw_id))
            except ValueError:
                row_errors.append(f"Row {i}: id '{raw_id}' is not a valid integer")

        # memo (optional)
        memo = (row.get("memo") or "").strip() or None

        if row_errors:
            errors.extend(row_errors)
        else:
            valid.append({
                "id"      : parsed_id,    # None = auto-assign in DB step
                "employee": employee,
                "date"    : parsed_date,
                "hours"   : parsed_hours,
                "memo"    : memo,
            })

    return valid, errors


# ─────────────────────────────────────────────────────────────
# Upsert into RDS
# ─────────────────────────────────────────────────────────────

def _get_db_conn():
    """Open a direct psycopg2 connection to RDS."""
    return psycopg2.connect(
        host     = os.environ["DB_HOST"],
        port     = int(os.environ.get("DB_PORT", 5432)),
        dbname   = os.environ.get("DB_NAME", "agentdb"),
        user     = os.environ.get("DB_USER", "pgadmin"),
        password = os.environ["DB_PASSWORD"],
        connect_timeout = 10,
    )


def _upsert_rows(rows: list[dict]) -> int:
    """
    Upsert rows into fusion_time_entries.
    Auto-assigns IDs for rows where id is None.
    Returns number of rows upserted.
    """
    conn = _get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Get starting ID for auto-assignment
                cur.execute(NEXT_ID_SQL)
                next_id = cur.fetchone()[0]

                for row in rows:
                    if row["id"] is None:
                        row["id"] = next_id
                        next_id  += 1

                psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=100)

        return len(rows)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
# Write result
# ─────────────────────────────────────────────────────────────

def _write_result(result_key: str, result: dict):
    """Write processing result JSON to S3 for frontend polling."""
    s3.put_object(
        Bucket      = BUCKET,
        Key         = result_key,
        Body        = json.dumps(result, default=str).encode("utf-8"),
        ContentType = "application/json",
    )
