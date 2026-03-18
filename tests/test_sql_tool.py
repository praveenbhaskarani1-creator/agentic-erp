"""
tests/test_sql_tool.py
───────────────────────
Tests for app/tools/sql_tool.py

UNIT tests    — no DB needed, run anytime
INTEGRATION   — needs SSH tunnel open

Run unit only:
    pytest tests\test_sql_tool.py -v -m unit

Run all (tunnel open):
    pytest tests\test_sql_tool.py -v
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(override=True)

from app.tools.sql_tool import SQLTool, _serialize_row, _success, _empty, _error
from app.sql.queries import get_query


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def tool():
    """Shared SQLTool instance for all tests."""
    return SQLTool()


@pytest.fixture(scope="module", autouse=True)
def init_db_for_integration():
    """Init DB pool once for integration tests."""
    from app.db.connection import DatabaseManager
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER', 'pgadmin')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5433')}/"
        f"{os.getenv('DB_NAME', 'agentdb')}"
    )
    try:
        DatabaseManager.init(db_url=db_url, pool_size=3, echo=False)
    except Exception:
        pass  # Already initialised from another test module
    yield
    # Don't close — other test modules may still need it


# ═════════════════════════════════════════════════════════════
# UNIT TESTS — no DB needed
# ═════════════════════════════════════════════════════════════

class TestSerializeRow:
    """Tests for _serialize_row() — date/decimal conversion."""

    def test_converts_date_to_isoformat(self):
        from datetime import date
        row = {"id": 1, "date": date(2026, 1, 15), "hours": 8.0}
        result = _serialize_row(row)
        assert result["date"] == "2026-01-15"
        assert isinstance(result["date"], str)

    def test_converts_datetime_to_isoformat(self):
        from datetime import datetime
        row = {"created_at": datetime(2026, 1, 15, 10, 30)}
        result = _serialize_row(row)
        assert "2026-01-15" in result["created_at"]
        assert isinstance(result["created_at"], str)

    def test_passes_through_strings(self):
        row = {"employee": "John Smith", "memo": "ERP-001"}
        result = _serialize_row(row)
        assert result["employee"] == "John Smith"
        assert result["memo"] == "ERP-001"

    def test_passes_through_none(self):
        row = {"memo": None, "id": 5}
        result = _serialize_row(row)
        assert result["memo"] is None

    def test_passes_through_integers(self):
        row = {"id": 42, "hours": 8}
        result = _serialize_row(row)
        assert result["id"] == 42


class TestResponseHelpers:
    """Tests for _success(), _empty(), _error() response builders."""

    def test_success_response_structure(self):
        query = get_query("blank_memo")
        rows = [{"id": 1, "employee": "John", "date": "2026-01-15",
                 "hours": 8.0, "memo": None}]
        result = _success(query, rows)
        assert result["status"]      == "success"
        assert result["query_name"]  == "blank_memo"
        assert result["row_count"]   == 1
        assert result["rows"]        == rows
        assert "Found 1 record"      in result["message"]

    def test_success_pluralises_correctly(self):
        query = get_query("blank_memo")
        rows = [{"id": i} for i in range(5)]
        result = _success(query, rows)
        assert "5 records" in result["message"]

    def test_success_singular(self):
        query = get_query("blank_memo")
        result = _success(query, [{"id": 1}])
        assert "1 record" in result["message"]
        assert "records" not in result["message"]

    def test_empty_response_structure(self):
        query = get_query("last_7_days")
        result = _empty(query)
        assert result["status"]     == "empty"
        assert result["query_name"] == "last_7_days"
        assert result["row_count"]  == 0
        assert result["rows"]       == []
        assert "No records" in result["message"]

    def test_error_response_structure(self):
        result = _error("blank_memo", "connection refused")
        assert result["status"]     == "error"
        assert result["query_name"] == "blank_memo"
        assert result["row_count"]  == 0
        assert result["rows"]       == []
        assert "connection refused" in result["message"]


class TestSQLToolUnknownQuery:
    """Tests for error handling on unknown query names."""

    def test_unknown_query_returns_error_status(self, tool):
        result = tool.run("this_query_does_not_exist")
        assert result["status"] == "error"

    def test_unknown_query_error_message_helpful(self, tool):
        result = tool.run("salary_data")
        assert "salary_data" in result["message"]
        assert result["row_count"] == 0
        assert result["rows"] == []

    def test_unknown_query_does_not_raise(self, tool):
        """Tool must never raise — always return a dict."""
        try:
            result = tool.run("nonexistent")
            assert isinstance(result, dict)
        except Exception as e:
            pytest.fail(f"tool.run() raised unexpectedly: {e}")

    def test_empty_string_query_returns_error(self, tool):
        result = tool.run("")
        assert result["status"] == "error"

    def test_result_always_has_required_keys(self, tool):
        """Every result must have these 6 keys — agent depends on them."""
        required = {"status", "query_name", "row_count", "rows", "message"}
        for query_name in ["blank_memo", "nonexistent", ""]:
            result = tool.run(query_name)
            missing = required - set(result.keys())
            assert not missing, f"'{query_name}' result missing keys: {missing}"


# ═════════════════════════════════════════════════════════════
# INTEGRATION TESTS — needs SSH tunnel
# ═════════════════════════════════════════════════════════════

class TestSQLToolIntegration:
    """Runs all 5 queries against your real fusion_time_entries table."""

    # ── all_entries ───────────────────────────────────────────

    def test_all_entries_returns_success(self, tool):
        result = tool.run("all_entries")
        assert result["status"] == "success"

    def test_all_entries_returns_at_least_119_rows(self, tool):
        """Uses >= so test stays valid when new data is added."""
        result = tool.run("all_entries")
        assert result["row_count"] >= 119, \
            f"Expected at least 119 rows, got {result['row_count']}"

    def test_all_entries_rows_are_dicts(self, tool):
        result = tool.run("all_entries")
        assert all(isinstance(r, dict) for r in result["rows"])

    def test_all_entries_rows_have_5_columns(self, tool):
        result = tool.run("all_entries")
        expected = {"id", "employee", "date", "hours", "memo"}
        for row in result["rows"][:3]:   # check first 3
            assert expected.issubset(set(row.keys())), \
                f"Row missing columns: {expected - set(row.keys())}"

    def test_all_entries_dates_are_strings(self, tool):
        """Dates must be serialized to ISO strings, not date objects."""
        result = tool.run("all_entries")
        for row in result["rows"][:5]:
            assert isinstance(row["date"], str), \
                f"date should be str, got {type(row['date'])}"

    # ── total_count ───────────────────────────────────────────

    def test_total_count_returns_success(self, tool):
        result = tool.run("total_count")
        assert result["status"] == "success"

    def test_total_count_returns_at_least_119(self, tool):
        """Uses >= so test stays valid when new data is added."""
        result = tool.run("total_count")
        assert result["rows"][0]["total_records"] >= 119

    def test_total_count_has_1_row(self, tool):
        result = tool.run("total_count")
        assert result["row_count"] == 1

    # ── blank_memo ────────────────────────────────────────────

    def test_blank_memo_returns_success(self, tool):
        result = tool.run("blank_memo")
        assert result["status"] in ("success", "empty")

    def test_blank_memo_all_rows_have_null_memo(self, tool):
        result = tool.run("blank_memo")
        for row in result["rows"]:
            assert row["memo"] is None, \
                f"Row {row['id']} has memo='{row['memo']}' — expected None"

    def test_blank_memo_message_mentions_count(self, tool):
        result = tool.run("blank_memo")
        assert str(result["row_count"]) in result["message"] or \
               "No records" in result["message"]

    # ── last_7_days ───────────────────────────────────────────

    def test_last_7_days_returns_valid_status(self, tool):
        result = tool.run("last_7_days")
        assert result["status"] in ("success", "empty")

    def test_last_7_days_dates_within_range(self, tool):
        from datetime import date, timedelta
        result = tool.run("last_7_days")
        cutoff = date.today() - timedelta(days=7)
        for row in result["rows"]:
            row_date = date.fromisoformat(row["date"])
            assert row_date >= cutoff, \
                f"Row {row['id']} date {row['date']} outside last 7 days"

    # ── non_erp_memo ──────────────────────────────────────────

    def test_non_erp_memo_returns_success(self, tool):
        result = tool.run("non_erp_memo")
        assert result["status"] in ("success", "empty")

    def test_non_erp_memo_no_erp_prefix(self, tool):
        result = tool.run("non_erp_memo")
        for row in result["rows"]:
            if row["memo"] is not None:
                assert not row["memo"].startswith("ERP"), \
                    f"Row {row['id']} memo '{row['memo']}' starts with ERP"

    # ── run_all() ─────────────────────────────────────────────

    def test_run_all_returns_all_5_queries(self, tool):
        results = tool.run_all()
        assert set(results.keys()) == {
            "all_entries", "total_count", "blank_memo",
            "last_7_days", "non_erp_memo"
        }

    def test_run_all_each_result_has_status(self, tool):
        results = tool.run_all()
        for name, result in results.items():
            assert "status" in result, f"{name} missing status"
            assert result["status"] in ("success", "empty", "error")

    # ── summary() ────────────────────────────────────────────

    def test_summary_total_count(self, tool):
        """Checks summary contains a number and 'Total' — not hardcoded to 119."""
        summary = tool.summary("total_count")
        assert "Total" in summary
        assert any(char.isdigit() for char in summary), \
            "Summary should contain a row count number"

    def test_summary_blank_memo(self, tool):
        summary = tool.summary("blank_memo")
        assert isinstance(summary, str)
        assert len(summary) > 0

    def test_summary_unknown_query(self, tool):
        summary = tool.summary("nonexistent")
        assert "Error" in summary
        assert isinstance(summary, str)
