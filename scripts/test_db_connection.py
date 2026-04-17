"""
scripts/test_db_connection.py
------------------------------
Connectivity test for Oracle Autonomous Database via ORDS REST API.

Checks:
  1. ORDS REST endpoint reachable (HTTPS port 443)
  2. SELECT 1 FROM DUAL succeeds
  3. Lists all expected validation tables and their row counts
  4. Shows last 5 upload_log entries

Usage:
    python scripts/test_db_connection.py

Requires .env file (or environment variables):
    OCI_DB_USER, OCI_DB_PASSWORD
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.oci_db import OrdsDB

EXPECTED_TABLES = [
    "ts_jira_tickets",
    "ts_people",
    "ts_project_mapping",
    "ts_validation_runs",
    "ts_validation_results",
    "upload_log",
]


def main():
    print("=" * 55)
    print("  Oracle ADW Connection Test (ORDS REST)")
    print("=" * 55)

    db = OrdsDB()
    print(f"\nEndpoint: {db._sql_url}")
    print(f"User:     {db.user}")

    # 1. Basic ping
    result = db.health_check()
    if result["status"] == "ok":
        print("\n[1] Connection: OK")
    else:
        print(f"\n[1] Connection: FAILED\n    {result['db']}")
        sys.exit(1)

    # 2. SELECT 1
    rows = db.query("SELECT 1 AS result FROM DUAL")
    print(f"[2] SELECT 1 FROM DUAL: {rows[0]['result']}")

    # 3. Table status
    print(f"\n[3] Table status:")
    print(f"    {'Table':<30} {'Exists':<8} {'Row count':>10}")
    print("    " + "-" * 50)

    all_ok = True
    for tbl in EXPECTED_TABLES:
        try:
            count = db.row_count(tbl)
            print(f"    {tbl:<30} {'YES':<8} {count:>10,}")
        except Exception as e:
            if "table or view does not exist" in str(e).lower() or "ORA-00942" in str(e):
                print(f"    {tbl:<30} {'MISSING':<8} {'-- run create_tables_oci.sql':>10}")
            else:
                print(f"    {tbl:<30} {'ERROR':<8} {str(e)[:40]}")
            all_ok = False

    # 4. Last upload_log entries
    try:
        rows = db.query("""
            SELECT logged_at, table_name, filename, rows_inserted, status
            FROM upload_log
            ORDER BY logged_at DESC
            FETCH FIRST 5 ROWS ONLY
        """)
        if rows:
            print("\n[4] Last 5 upload_log entries:")
            print(f"    {'Logged At':<22} {'Table':<22} {'Inserted':>8} {'Status'}")
            print("    " + "-" * 65)
            for r in rows:
                print(f"    {str(r.get('logged_at',''))[:19]:<22} {str(r.get('table_name','')):<22} {r.get('rows_inserted',0):>8} {r.get('status','')}")
        else:
            print("\n[4] upload_log: empty (no uploads yet)")
    except Exception:
        print("\n[4] upload_log: missing -- run create_tables_oci.sql first")

    print("\n" + "=" * 55)
    if all_ok:
        print("  RESULT: All tables present. DB ready for Step 3.")
    else:
        print("  RESULT: Some tables missing.")
        print("  -> Paste create_tables_oci.sql into Database Actions -> SQL")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()
