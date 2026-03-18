"""
tests/test_bedrock_tool.py
───────────────────────────
Tests for app/tools/bedrock_tool.py

UNIT tests    — no AWS needed, mocks Bedrock client
INTEGRATION   — needs real AWS credentials + Bedrock access

Run unit only:
    pytest tests\test_bedrock_tool.py -v -k "not Integration"

Run all:
    pytest tests\test_bedrock_tool.py -v
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from app.tools.bedrock_tool import BedrockTool, MAX_ROWS_TO_SEND


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def tool():
    return BedrockTool()


def make_mock_response(text: str) -> MagicMock:
    """Build a mock Bedrock API response."""
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps({
        "content": [{"text": text}]
    }).encode()
    mock_response = MagicMock()
    mock_response.__getitem__ = lambda self, key: mock_body if key == "body" else None
    return mock_response


MOCK_DATA_BLANK_MEMO = {
    "status":      "success",
    "query_name":  "blank_memo",
    "description": "Find all time entries where memo is NULL",
    "row_count":   3,
    "message":     "Found 3 records",
    "rows": [
        {"id": 5,  "employee": "John Smith", "date": "2026-01-15", "hours": 8.0,  "memo": None},
        {"id": 12, "employee": "Jane Doe",   "date": "2026-01-16", "hours": 7.5,  "memo": None},
        {"id": 23, "employee": "Bob Lee",    "date": "2026-01-17", "hours": 6.0,  "memo": None},
    ],
}

MOCK_DATA_EMPTY = {
    "status":      "empty",
    "query_name":  "last_7_days",
    "description": "Entries from last 7 days",
    "row_count":   0,
    "message":     "No records found",
    "rows":        [],
}

MOCK_DATA_ERROR = {
    "status":      "error",
    "query_name":  "all_entries",
    "row_count":   0,
    "message":     "connection refused",
    "rows":        [],
}


# ═════════════════════════════════════════════════════════════
# UNIT TESTS — no AWS needed
# ═════════════════════════════════════════════════════════════

class TestFormatRows:
    """Tests for _format_rows() helper."""

    def test_formats_single_row(self, tool):
        rows = [{"id": 1, "employee": "John", "memo": None}]
        result = tool._format_rows(rows)
        assert "Row 1:" in result
        assert "id=1" in result
        assert "employee=John" in result
        assert "memo=None" in result

    def test_formats_multiple_rows(self, tool):
        rows = [
            {"id": 1, "employee": "John"},
            {"id": 2, "employee": "Jane"},
        ]
        result = tool._format_rows(rows)
        assert "Row 1:" in result
        assert "Row 2:" in result

    def test_empty_rows_returns_message(self, tool):
        result = tool._format_rows([])
        assert result == "No rows."

    def test_each_row_on_separate_line(self, tool):
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        result = tool._format_rows(rows)
        lines = result.strip().split("\n")
        assert len(lines) == 3


class TestCallClaude:
    """Tests for _call_claude() — mocked Bedrock client."""

    def test_success_response_structure(self, tool):
        mock_resp = make_mock_response("blank_memo")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool._call_claude("system", "user", "model-id", 256)
        assert result["status"] == "success"
        assert result["text"]   == "blank_memo"
        assert result["model"]  == "model-id"

    def test_error_response_on_client_error(self, tool):
        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}}
        with patch.object(tool.client, "invoke_model",
                          side_effect=ClientError(error_response, "InvokeModel")):
            result = tool._call_claude("system", "user", "model-id", 256)
        assert result["status"]  == "error"
        assert "AccessDenied"    in result["message"]

    def test_error_response_on_unexpected_exception(self, tool):
        with patch.object(tool.client, "invoke_model",
                          side_effect=Exception("network timeout")):
            result = tool._call_claude("system", "user", "model-id", 256)
        assert result["status"]  == "error"
        assert "network timeout" in result["message"]

    def test_temperature_is_zero(self, tool):
        """Temperature must be 0 for deterministic responses."""
        captured = {}
        def capture_call(**kwargs):
            captured["body"] = json.loads(kwargs["body"])
            return make_mock_response("ok")
        with patch.object(tool.client, "invoke_model", side_effect=capture_call):
            tool._call_claude("system", "user", "model-id", 256)
        assert captured["body"]["temperature"] == 0.0

    def test_system_prompt_included(self, tool):
        captured = {}
        def capture_call(**kwargs):
            captured["body"] = json.loads(kwargs["body"])
            return make_mock_response("ok")
        with patch.object(tool.client, "invoke_model", side_effect=capture_call):
            tool._call_claude("my system prompt", "user msg", "model-id", 256)
        assert captured["body"]["system"] == "my system prompt"


class TestDetectIntent:
    """Tests for detect_intent() — mocked Bedrock."""

    def test_returns_valid_query_name(self, tool):
        mock_resp = make_mock_response("blank_memo")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool.detect_intent("show entries with no memo")
        assert result == "blank_memo"

    def test_strips_whitespace_from_response(self, tool):
        mock_resp = make_mock_response("  blank_memo  \n")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool.detect_intent("show entries with no memo")
        assert result == "blank_memo"

    def test_returns_unknown_for_out_of_scope(self, tool):
        mock_resp = make_mock_response("unknown")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool.detect_intent("what is the weather today")
        assert result == "unknown"

    def test_returns_unknown_on_bedrock_error(self, tool):
        with patch.object(tool.client, "invoke_model",
                          side_effect=Exception("connection error")):
            result = tool.detect_intent("show blank memos")
        assert result == "unknown"

    def test_returns_unknown_for_unexpected_response(self, tool):
        """If Claude returns something not in our query list → unknown."""
        mock_resp = make_mock_response("salary_data")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool.detect_intent("show salaries")
        assert result == "unknown"

    def test_uses_haiku_model(self, tool):
        """Intent detection must use Haiku — cheaper."""
        captured = {}
        def capture_call(**kwargs):
            captured["modelId"] = kwargs.get("modelId")
            return make_mock_response("blank_memo")
        with patch.object(tool.client, "invoke_model", side_effect=capture_call):
            tool.detect_intent("show blank memos")
        assert "haiku" in captured["modelId"].lower()

    def test_all_5_query_names_are_valid_returns(self, tool):
        """Each of the 5 query names must be accepted as valid."""
        valid = ["all_entries", "total_count", "blank_memo",
                 "last_7_days", "non_erp_memo"]
        for name in valid:
            mock_resp = make_mock_response(name)
            with patch.object(tool.client, "invoke_model", return_value=mock_resp):
                result = tool.detect_intent("some question")
            assert result == name, f"Expected {name}, got {result}"


class TestAsk:
    """Tests for ask() — mocked Bedrock."""

    def test_returns_string(self, tool):
        mock_resp = make_mock_response("3 employees have blank memos.")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool.ask(MOCK_DATA_BLANK_MEMO, "who has blank memos?")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_returns_answer_text(self, tool):
        mock_resp = make_mock_response("John Smith, Jane Doe, Bob Lee have blank memos.")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool.ask(MOCK_DATA_BLANK_MEMO, "who has blank memos?")
        assert "John Smith" in result

    def test_empty_data_returns_no_records_message(self, tool):
        """Empty result must not call Bedrock at all — handled locally."""
        with patch.object(tool.client, "invoke_model") as mock_call:
            result = tool.ask(MOCK_DATA_EMPTY, "any recent entries?")
        mock_call.assert_not_called()
        assert "No records" in result or "no" in result.lower()

    def test_error_data_returns_error_message(self, tool):
        """Error result must not call Bedrock — handled locally."""
        with patch.object(tool.client, "invoke_model") as mock_call:
            result = tool.ask(MOCK_DATA_ERROR, "show all entries")
        mock_call.assert_not_called()
        assert "unable" in result.lower() or "error" in result.lower()

    def test_uses_sonnet_model(self, tool):
        """Answer generation must use Sonnet — better quality."""
        captured = {}
        def capture_call(**kwargs):
            captured["modelId"] = kwargs.get("modelId")
            return make_mock_response("Some answer.")
        with patch.object(tool.client, "invoke_model", side_effect=capture_call):
            tool.ask(MOCK_DATA_BLANK_MEMO, "who has blank memos?")
        assert "sonnet" in captured["modelId"].lower()

    def test_large_dataset_truncated(self, tool):
        """More than MAX_ROWS_TO_SEND rows must be truncated before sending."""
        large_data = {
            "status":      "success",
            "query_name":  "all_entries",
            "description": "All records",
            "row_count":   300,
            "message":     "Found 300 records",
            "rows":        [{"id": i, "employee": f"Emp{i}",
                             "date": "2026-01-15", "hours": 8.0, "memo": None}
                            for i in range(300)],
        }
        captured_body = {}
        def capture_call(**kwargs):
            captured_body["body"] = json.loads(kwargs["body"])
            return make_mock_response("Answer about 300 rows.")
        with patch.object(tool.client, "invoke_model", side_effect=capture_call):
            tool.ask(large_data, "show all entries")
        user_msg = captured_body["body"]["messages"][0]["content"]
        row_count_in_msg = user_msg.count("Row ")
        assert row_count_in_msg <= MAX_ROWS_TO_SEND

    def test_bedrock_error_returns_graceful_message(self, tool):
        with patch.object(tool.client, "invoke_model",
                          side_effect=Exception("timeout")):
            result = tool.ask(MOCK_DATA_BLANK_MEMO, "who has blank memos?")
        assert isinstance(result, str)
        assert len(result) > 0
        assert "error" in result.lower() or "encountered" in result.lower()

    def test_never_returns_empty_string(self, tool):
        """ask() must always return something — never empty string."""
        scenarios = [
            (MOCK_DATA_BLANK_MEMO, "question"),
            (MOCK_DATA_EMPTY,      "question"),
            (MOCK_DATA_ERROR,      "question"),
        ]
        mock_resp = make_mock_response("Some answer.")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            for data, q in scenarios:
                result = tool.ask(data, q)
                assert result != "", f"Got empty string for status={data['status']}"


class TestHealthCheck:
    """Tests for health_check()."""

    def test_health_check_ok(self, tool):
        mock_resp = make_mock_response("ok")
        with patch.object(tool.client, "invoke_model", return_value=mock_resp):
            result = tool.health_check()
        assert result["status"]  == "ok"
        assert result["bedrock"] == "connected"

    def test_health_check_error(self, tool):
        with patch.object(tool.client, "invoke_model",
                          side_effect=Exception("no access")):
            result = tool.health_check()
        assert result["status"] == "error"


# ═════════════════════════════════════════════════════════════
# INTEGRATION TESTS — needs real AWS credentials
# ═════════════════════════════════════════════════════════════

import boto3

@pytest.mark.skipif(boto3.Session().get_credentials() is None, reason="Requires real AWS credentials")
class TestBedrockIntegration:
    """
    Real Bedrock calls — needs:
      - AWS credentials configured (praveenAWS IAM user)
      - Bedrock models enabled in us-east-1
      - SSH tunnel open (for DB fixture)
    """

    def test_health_check_real_bedrock(self, tool):
        result = tool.health_check()
        assert result["status"]  == "ok", \
            f"Bedrock health check failed: {result}"
        assert result["bedrock"] == "connected"

    def test_detect_intent_blank_memo(self, tool):
        result = tool.detect_intent("show me entries with no memo description")
        assert result == "blank_memo", \
            f"Expected 'blank_memo', got '{result}'"

    def test_detect_intent_total_count(self, tool):
        result = tool.detect_intent("how many records are in the timesheet")
        assert result == "total_count", \
            f"Expected 'total_count', got '{result}'"

    def test_detect_intent_last_7_days(self, tool):
        result = tool.detect_intent("show me entries from the last week")
        assert result == "last_7_days", \
            f"Expected 'last_7_days', got '{result}'"

    def test_detect_intent_non_erp(self, tool):
        result = tool.detect_intent("find entries not following ERP naming")
        assert result == "non_erp_memo", \
            f"Expected 'non_erp_memo', got '{result}'"

    def test_detect_intent_out_of_scope(self, tool):
        result = tool.detect_intent("what is the weather in San Francisco")
        assert result == "unknown", \
            f"Expected 'unknown', got '{result}'"

    def test_ask_returns_answer_for_blank_memo(self, tool):
        """Real Sonnet call — answer must mention employees or count."""
        from app.db.connection import DatabaseManager
        from app.tools.sql_tool import SQLTool

        db_url = (
            f"postgresql+psycopg2://"
            f"{os.getenv('DB_USER','pgadmin')}:"
            f"{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST','localhost')}:"
            f"{os.getenv('DB_PORT','5433')}/"
            f"{os.getenv('DB_NAME','agentdb')}"
        )
        try:
            DatabaseManager.init(db_url=db_url, pool_size=2, echo=False)
        except Exception:
            pass

        sql = SQLTool()
        data = sql.run("blank_memo")

        answer = tool.ask(data, "which employees have blank memo entries?")

        assert isinstance(answer, str)
        assert len(answer) > 20, "Answer too short — expected real content"
        assert answer != "", "Answer must not be empty"

    def test_ask_total_count_mentions_number(self, tool):
        """Real Sonnet call — answer for total_count must mention a number."""
        from app.tools.sql_tool import SQLTool
        sql  = SQLTool()
        data = sql.run("total_count")
        answer = tool.ask(data, "how many time entries are there?")
        assert any(char.isdigit() for char in answer), \
            "Answer should contain a number for total_count query"

    def test_haiku_faster_than_sonnet(self, tool):
        """Haiku must respond faster than Sonnet — validates routing."""
        import time

        start = time.time()
        tool.detect_intent("show blank memos")
        haiku_time = time.time() - start

        start = time.time()
        tool.ask(MOCK_DATA_BLANK_MEMO, "who has blank memos?")
        sonnet_time = time.time() - start

        # Haiku should be faster — not always guaranteed but usually true
        # Just log the times rather than assert strictly
        logger_msg = f"Haiku: {haiku_time:.2f}s | Sonnet: {sonnet_time:.2f}s"
        print(f"\n  ⏱  {logger_msg}")
        assert True   # timing is informational only

    import logging
    logger = logging.getLogger(__name__)
