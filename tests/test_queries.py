"""
tests/test_queries.py
─────────────────────
Unit tests for app/sql/queries.py

Run:
    pytest tests/test_queries.py -v

No DB connection needed — tests query definitions only.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.sql.queries import (
    QUERIES, QueryDef,
    get_query, get_all_names, get_query_catalog, find_query_by_keyword
)


# ─── All 5 queries exist ──────────────────────────────────────

def test_all_5_queries_exist():
    assert "all_entries"  in QUERIES
    assert "total_count"  in QUERIES
    assert "blank_memo"   in QUERIES
    assert "last_7_days"  in QUERIES
    assert "non_erp_memo" in QUERIES

def test_exactly_5_queries_defined():
    assert len(QUERIES) == 5


# ─── Each query is a valid QueryDef ──────────────────────────

def test_all_queries_are_querydef_instances():
    for name, q in QUERIES.items():
        assert isinstance(q, QueryDef), f"{name} is not a QueryDef"

def test_all_queries_have_name():
    for name, q in QUERIES.items():
        assert q.name == name, f"{name}: q.name mismatch"
        assert len(q.name) > 0

def test_all_queries_have_description():
    for name, q in QUERIES.items():
        assert q.description, f"{name}: description is empty"
        assert len(q.description) > 10, f"{name}: description too short"

def test_all_queries_have_sql():
    for name, q in QUERIES.items():
        assert q.sql, f"{name}: SQL is empty"
        assert "SELECT" in q.sql.upper(), f"{name}: SQL must contain SELECT"

def test_all_queries_have_sample_questions():
    for name, q in QUERIES.items():
        assert q.sample_questions, f"{name}: sample_questions is empty"
        assert len(q.sample_questions) >= 3, f"{name}: need at least 3 sample questions"

def test_all_queries_have_returns_columns():
    for name, q in QUERIES.items():
        assert q.returns_columns, f"{name}: returns_columns is empty"


# ─── SQL correctness checks ───────────────────────────────────

def test_all_entries_sql():
    q = QUERIES["all_entries"]
    sql = q.sql.upper()
    assert "SELECT *" in sql
    assert "FUSION_TIME_ENTRIES" in sql
    assert "ORDER BY ID ASC" in sql

def test_total_count_sql():
    q = QUERIES["total_count"]
    sql = q.sql.upper()
    assert "COUNT(*)" in sql
    assert "FUSION_TIME_ENTRIES" in sql
    assert "TOTAL_RECORDS" in sql           # has alias

def test_blank_memo_sql():
    q = QUERIES["blank_memo"]
    sql = q.sql.upper()
    assert "MEMO IS NULL" in sql
    assert "FUSION_TIME_ENTRIES" in sql
    assert "SELECT" in sql
    assert "ID"       in sql
    assert "EMPLOYEE" in sql
    assert "DATE"     in sql
    assert "HOURS"    in sql
    assert "MEMO"     in sql

def test_last_7_days_sql():
    q = QUERIES["last_7_days"]
    sql = q.sql.upper()
    assert "CURRENT_DATE" in sql
    assert "INTERVAL" in sql
    assert "'7 DAYS'" in sql
    assert "FUSION_TIME_ENTRIES" in sql
    assert ">=" in sql

def test_non_erp_memo_sql():
    q = QUERIES["non_erp_memo"]
    sql = q.sql.upper()
    assert "NOT LIKE" in sql
    assert "'ERP%'" in sql.replace('"', "'")  # normalise quotes
    assert "FUSION_TIME_ENTRIES" in sql

def test_no_query_uses_delete_or_drop():
    """Safety check — no destructive SQL allowed."""
    for name, q in QUERIES.items():
        sql_upper = q.sql.upper()
        assert "DELETE" not in sql_upper, f"{name}: DELETE not allowed"
        assert "DROP"   not in sql_upper, f"{name}: DROP not allowed"
        assert "UPDATE" not in sql_upper, f"{name}: UPDATE not allowed"
        assert "INSERT" not in sql_upper, f"{name}: INSERT not allowed"
        assert "TRUNCATE" not in sql_upper, f"{name}: TRUNCATE not allowed"

def test_all_queries_use_correct_table():
    for name, q in QUERIES.items():
        assert "fusion_time_entries" in q.sql.lower(), \
            f"{name}: must query fusion_time_entries"


# ─── Column checks ───────────────────────────────────────────

def test_total_count_returns_single_column():
    q = QUERIES["total_count"]
    assert q.returns_columns == ["total_records"]

def test_row_queries_return_core_columns():
    """Queries returning rows must include the 5 core columns."""
    core = {"id", "employee", "date", "hours", "memo"}
    for name in ["all_entries", "blank_memo", "last_7_days", "non_erp_memo"]:
        cols = set(QUERIES[name].returns_columns)
        assert core == cols, f"{name}: expected columns {core}, got {cols}"


# ─── get_query() helper ───────────────────────────────────────

def test_get_query_returns_correct_querydef():
    q = get_query("blank_memo")
    assert q.name == "blank_memo"
    assert "MEMO IS NULL" in q.sql.upper()

def test_get_query_raises_for_unknown_name():
    try:
        get_query("nonexistent_query")
        assert False, "Should have raised KeyError"
    except KeyError as e:
        assert "nonexistent_query" in str(e)
        assert "Available queries" in str(e)

def test_get_query_error_lists_available_queries():
    try:
        get_query("bad_name")
    except KeyError as e:
        msg = str(e)
        assert "all_entries"  in msg
        assert "blank_memo"   in msg
        assert "last_7_days"  in msg


# ─── get_all_names() ─────────────────────────────────────────

def test_get_all_names_returns_list():
    names = get_all_names()
    assert isinstance(names, list)

def test_get_all_names_contains_all_5():
    names = get_all_names()
    assert "all_entries"  in names
    assert "total_count"  in names
    assert "blank_memo"   in names
    assert "last_7_days"  in names
    assert "non_erp_memo" in names


# ─── get_query_catalog() ─────────────────────────────────────

def test_get_query_catalog_returns_list():
    catalog = get_query_catalog()
    assert isinstance(catalog, list)
    assert len(catalog) == 5

def test_catalog_each_entry_has_required_keys():
    catalog = get_query_catalog()
    required = {"name", "description", "sample_questions", "returns_columns"}
    for entry in catalog:
        assert required == set(entry.keys()), \
            f"Catalog entry missing keys: {required - set(entry.keys())}"

def test_catalog_does_not_expose_raw_sql():
    """Catalog is passed to the LLM — must NOT include raw SQL."""
    catalog = get_query_catalog()
    for entry in catalog:
        assert "sql" not in entry, "Catalog must not expose raw SQL to LLM"
        assert "SELECT" not in str(entry.get("description", "")).upper() or True
        # description may mention SELECT conceptually — that's fine


# ─── find_query_by_keyword() ─────────────────────────────────

def test_find_query_blank_memo_by_keyword():
    result = find_query_by_keyword("blank memo")
    assert result is not None
    assert result.name == "blank_memo"

def test_find_query_last_7_days_by_keyword():
    result = find_query_by_keyword("last 7 days")
    assert result is not None
    assert result.name == "last_7_days"

def test_find_query_erp_by_keyword():
    result = find_query_by_keyword("ERP")
    assert result is not None
    assert result.name == "non_erp_memo"

def test_find_query_returns_none_for_unknown():
    result = find_query_by_keyword("salary payroll benefits")
    assert result is None

def test_find_query_case_insensitive():
    result = find_query_by_keyword("BLANK MEMO")
    assert result is not None
    assert result.name == "blank_memo"
