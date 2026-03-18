"""
tests/test_db_connection.py
────────────────────────────
Integration tests for db/connection.py

⚠️  REQUIRES SSH TUNNEL TO BE OPEN BEFORE RUNNING:
    ssh -i "C:/Users/PraveenBhaskarani/Downloads/RSA.pem"
        -L 5433:agentic-erp-pgvector.cwb0sigyk8na.us-east-1.rds.amazonaws.com:5432
        ec2-user@98.92.220.36 -N

Run:
    pytest tests\test_db_connection.py -v

What this tests:
  1. Can connect to RDS via SSH tunnel
  2. fusion_time_entries table exists
  3. Table has exactly 119 rows
  4. All 5 expected columns exist
  5. Each of your 5 queries runs and returns correct results
  6. Health check works
  7. Connection pool handles multiple connections
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── Load .env before importing anything ──────────────────────
from dotenv import load_dotenv
load_dotenv(override=True)

from sqlalchemy import text
from app.db.connection import DatabaseManager, get_db
from app.sql.queries import get_query


# ─────────────────────────────────────────────────────────────
# Setup — initialise DB pool once for all tests
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def init_db():
    """
    Initialise the DB connection pool once for the whole test session.
    Reads DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD from .env
    """
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER', 'pgadmin')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5433')}/"
        f"{os.getenv('DB_NAME', 'agentdb')}"
    )
    DatabaseManager.init(db_url=db_url, pool_size=3, echo=False)
    yield
    DatabaseManager.close()


# ─────────────────────────────────────────────────────────────
# Test 1 — Basic connectivity
# ─────────────────────────────────────────────────────────────

def test_can_connect_to_rds():
    """
    Most basic test — can we reach RDS through the SSH tunnel?
    If this fails → SSH tunnel is not open.
    """
    with get_db() as db:
        result = db.execute(text("SELECT 1 AS ping")).fetchone()
        assert result[0] == 1


def test_health_check_returns_ok():
    """Health check used by /health API endpoint."""
    result = DatabaseManager.health_check()
    assert result["status"] == "ok"
    assert result["db"] == "connected"


def test_connected_to_correct_database():
    """Confirm we're on agentdb, not rdsadmin or postgres."""
    with get_db() as db:
        result = db.execute(text("SELECT current_database()")).fetchone()
        assert result[0] == "agentdb"


def test_connected_as_correct_user():
    """Confirm connected as pgadmin."""
    with get_db() as db:
        result = db.execute(text("SELECT current_user")).fetchone()
        assert result[0] == "pgadmin"


# ─────────────────────────────────────────────────────────────
# Test 2 — Table structure
# ─────────────────────────────────────────────────────────────

def test_fusion_time_entries_table_exists():
    """fusion_time_entries must exist in public schema."""
    with get_db() as db:
        result = db.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND   table_name   = 'fusion_time_entries'
        """)).scalar()
        assert result == 1, "Table fusion_time_entries not found!"


def test_all_5_columns_exist():
    """
    Confirm the 5 columns your queries depend on all exist.
    If any column is missing → queries will fail.
    """
    with get_db() as db:
        result = db.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND   table_name   = 'fusion_time_entries'
            ORDER BY ordinal_position
        """)).fetchall()

    columns = {row[0] for row in result}
    assert "id"       in columns, "Column 'id' missing"
    assert "employee" in columns, "Column 'employee' missing"
    assert "date"     in columns, "Column 'date' missing"
    assert "hours"    in columns, "Column 'hours' missing"
    assert "memo"     in columns, "Column 'memo' missing"


# ─────────────────────────────────────────────────────────────
# Test 3 — Row counts
# ─────────────────────────────────────────────────────────────

def test_table_has_rows():
    """
    Table must have at least 119 rows (original load).
    Uses >= so test stays valid when new data is added.
    """
    with get_db() as db:
        count = db.execute(text(
            "SELECT COUNT(*) FROM public.fusion_time_entries"
        )).scalar()
        assert count >= 119, f"Expected at least 119 rows, got {count}"


def test_table_has_rows_with_data():
    """Basic sanity — table is not empty and has real data."""
    with get_db() as db:
        rows = db.execute(text(
            "SELECT * FROM public.fusion_time_entries LIMIT 5"
        )).fetchall()
        assert len(rows) > 0
        assert len(rows[0]) >= 5   # at least 5 columns


# ─────────────────────────────────────────────────────────────
# Test 4 — Run each of your 5 queries against real data
# ─────────────────────────────────────────────────────────────

def test_query_all_entries():
    """all_entries → must return all 119 rows ordered by id."""
    q = get_query("all_entries")
    with get_db() as db:
        rows = db.execute(text(q.sql)).fetchall()
        assert len(rows) == 119, f"Expected 119, got {len(rows)}"
        # Confirm ordered by id ASC — first id <= last id
        first_id = rows[0][0]
        last_id  = rows[-1][0]
        assert first_id <= last_id, "Rows not ordered by id ASC"


def test_query_total_count():
    """total_count → COUNT(*) must equal 119."""
    q = get_query("total_count")
    with get_db() as db:
        result = db.execute(text(q.sql)).fetchone()
        assert result[0] == 119, f"Expected 119, got {result[0]}"


def test_query_blank_memo_returns_rows():
    """
    blank_memo → all returned rows must have memo = None.
    We don't assert an exact count (data may change)
    but every returned row MUST have a null memo.
    """
    q = get_query("blank_memo")
    with get_db() as db:
        rows = db.execute(text(q.sql)).mappings().fetchall()

    assert len(rows) > 0, "Expected at least some blank memo rows"

    for row in rows:
        assert row["memo"] is None, \
            f"Row {row['id']} has memo='{row['memo']}' — should be NULL"


def test_query_blank_memo_columns():
    """blank_memo → must return exactly the 5 expected columns."""
    q = get_query("blank_memo")
    with get_db() as db:
        rows = db.execute(text(q.sql)).mappings().fetchall()

    if rows:
        keys = set(rows[0].keys())
        assert "id"       in keys
        assert "employee" in keys
        assert "date"     in keys
        assert "hours"    in keys
        assert "memo"     in keys


def test_query_last_7_days_date_filter():
    """
    last_7_days → every returned row must have
    date >= today - 7 days.
    """
    q = get_query("last_7_days")
    with get_db() as db:
        rows = db.execute(text(q.sql)).mappings().fetchall()

    # May return 0 rows if no recent data — that's valid
    # But if rows come back, dates must be within range
    for row in rows:
        from datetime import date, timedelta
        cutoff = date.today() - timedelta(days=7)
        assert row["date"] >= cutoff, \
            f"Row {row['id']} has date {row['date']} outside last 7 days"


def test_query_non_erp_memo_filter():
    """
    non_erp_memo → no returned row should have memo starting with 'ERP'.
    Rows with NULL memo are acceptable (NULL NOT LIKE 'ERP%' = NULL in Postgres).
    """
    q = get_query("non_erp_memo")
    with get_db() as db:
        rows = db.execute(text(q.sql)).mappings().fetchall()

    assert len(rows) > 0, "Expected at least some non-ERP memo rows"

    for row in rows:
        memo = row["memo"]
        if memo is not None:
            assert not memo.startswith("ERP"), \
                f"Row {row['id']} has memo '{memo}' — should not start with ERP"


# ─────────────────────────────────────────────────────────────
# Test 5 — Data quality checks
# ─────────────────────────────────────────────────────────────

def test_all_rows_have_employee():
    """Every row must have an employee value — no orphan entries."""
    with get_db() as db:
        nulls = db.execute(text("""
            SELECT COUNT(*)
            FROM public.fusion_time_entries
            WHERE employee IS NULL OR employee = ''
        """)).scalar()
        assert nulls == 0, f"{nulls} rows have no employee"


def test_all_rows_have_date():
    """Every row must have a date."""
    with get_db() as db:
        nulls = db.execute(text("""
            SELECT COUNT(*)
            FROM public.fusion_time_entries
            WHERE date IS NULL
        """)).scalar()
        assert nulls == 0, f"{nulls} rows have no date"


def test_all_rows_have_positive_hours():
    """Hours must be > 0 — zero or negative hours are invalid."""
    with get_db() as db:
        bad = db.execute(text("""
            SELECT COUNT(*)
            FROM public.fusion_time_entries
            WHERE hours <= 0
        """)).scalar()
        assert bad == 0, f"{bad} rows have zero or negative hours"


def test_hours_are_reasonable():
    """No single entry should exceed 24 hours."""
    with get_db() as db:
        bad = db.execute(text("""
            SELECT COUNT(*)
            FROM public.fusion_time_entries
            WHERE hours > 24
        """)).scalar()
        assert bad == 0, f"{bad} rows exceed 24 hours"


# ─────────────────────────────────────────────────────────────
# Test 6 — Connection pool
# ─────────────────────────────────────────────────────────────

def test_multiple_concurrent_queries():
    """
    Run 5 queries back-to-back using the pool.
    Confirms pool handles reuse correctly without errors.
    """
    for i in range(5):
        with get_db() as db:
            result = db.execute(text(
                "SELECT COUNT(*) FROM public.fusion_time_entries"
            )).scalar()
            assert result == 119


def test_connection_returns_to_pool_after_use():
    """
    After using get_db(), the connection must return to pool.
    Confirmed by running 10 queries — pool_size is only 3,
    so connections MUST be reused.
    """
    for i in range(10):
        with get_db() as db:
            db.execute(text("SELECT 1")).fetchone()
    # If pool was broken, this would hang or raise after 3 queries
    assert True
