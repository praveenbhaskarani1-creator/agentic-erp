"""
tests/test_main.py
───────────────────
Tests for app/main.py — FastAPI endpoints.

UNIT tests    — mocked agent + tools, no AWS/DB needed
INTEGRATION   — needs SSH tunnel + AWS credentials

Run unit only:
    pytest tests\test_main.py -v -k "not Integration"

Run all:
    pytest tests\test_main.py -v
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from fastapi.testclient import TestClient


# ─────────────────────────────────────────────────────────────
# App fixture — initialise once for all tests
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client():
    """
    Create a FastAPI TestClient.
    Mocks DB init so unit tests don't need a real connection.
    """
    with patch("app.main.DatabaseManager") as mock_db:
        mock_db.init.return_value       = None
        mock_db.close.return_value      = None
        mock_db.health_check.return_value = {"db": "connected"}
        from app.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


@pytest.fixture(scope="module")
def live_client():
    """
    Real TestClient with real DB + Bedrock.
    Used for integration tests only.
    """
    from app.main import app
    with TestClient(app) as c:
        yield c


# ═════════════════════════════════════════════════════════════
# UNIT TESTS
# ═════════════════════════════════════════════════════════════

class TestRootEndpoint:

    def test_get_root_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200

    def test_get_root_has_name(self, client):
        data = client.get("/").json()
        assert "name" in data
        assert "Agentic" in data["name"]

    def test_get_root_has_status(self, client):
        data = client.get("/").json()
        assert data["status"] == "running"

    def test_get_root_has_docs_link(self, client):
        data = client.get("/").json()
        assert "docs" in data


class TestQueriesEndpoint:

    def test_get_queries_returns_200(self, client):
        response = client.get("/queries")
        assert response.status_code == 200

    def test_get_queries_returns_5_queries(self, client):
        data = client.get("/queries").json()
        assert data["total"] == 5
        assert len(data["queries"]) == 5

    def test_get_queries_has_required_fields(self, client):
        data    = client.get("/queries").json()
        required = {"name", "description", "sample_questions", "returns_columns"}
        for q in data["queries"]:
            assert required == set(q.keys())

    def test_get_queries_includes_blank_memo(self, client):
        data  = client.get("/queries").json()
        names = [q["name"] for q in data["queries"]]
        assert "blank_memo" in names

    def test_get_queries_all_5_present(self, client):
        data  = client.get("/queries").json()
        names = {q["name"] for q in data["queries"]}
        assert names == {
            "all_entries", "total_count", "blank_memo",
            "last_7_days", "non_erp_memo"
        }


class TestAskEndpoint:

    def test_ask_returns_200(self, client):
        mock_result = {
            "final_answer":     "3 employees have blank memos.",
            "intent_detected":  "blank_memo",
            "intent_source":    "keyword",
            "sql_result":       {"row_count": 3},
            "validation_passed": True,
            "error_message":    None,
        }
        with patch("app.main.run_agent", return_value=mock_result):
            response = client.post("/ask", json={"question": "show blank memos"})
        assert response.status_code == 200

    def test_ask_returns_answer(self, client):
        mock_result = {
            "final_answer":     "3 employees have blank memos.",
            "intent_detected":  "blank_memo",
            "intent_source":    "keyword",
            "sql_result":       {"row_count": 3},
            "validation_passed": True,
            "error_message":    None,
        }
        with patch("app.main.run_agent", return_value=mock_result):
            data = client.post("/ask", json={"question": "show blank memos"}).json()
        assert data["answer"] == "3 employees have blank memos."

    def test_ask_returns_intent(self, client):
        mock_result = {
            "final_answer":     "Answer.",
            "intent_detected":  "blank_memo",
            "intent_source":    "keyword",
            "sql_result":       {"row_count": 3},
            "validation_passed": True,
            "error_message":    None,
        }
        with patch("app.main.run_agent", return_value=mock_result):
            data = client.post("/ask", json={"question": "blank memos"}).json()
        assert data["intent_detected"] == "blank_memo"
        assert data["intent_source"]   == "keyword"

    def test_ask_returns_row_count(self, client):
        mock_result = {
            "final_answer":     "Answer.",
            "intent_detected":  "blank_memo",
            "intent_source":    "keyword",
            "sql_result":       {"row_count": 23},
            "validation_passed": True,
            "error_message":    None,
        }
        with patch("app.main.run_agent", return_value=mock_result):
            data = client.post("/ask", json={"question": "blank memos"}).json()
        assert data["row_count"] == 23

    def test_ask_rejects_empty_question(self, client):
        response = client.post("/ask", json={"question": ""})
        assert response.status_code == 422   # validation error

    def test_ask_rejects_short_question(self, client):
        response = client.post("/ask", json={"question": "hi"})
        assert response.status_code == 422

    def test_ask_rejects_missing_question(self, client):
        response = client.post("/ask", json={})
        assert response.status_code == 422

    def test_ask_accepts_user_id(self, client):
        mock_result = {
            "final_answer": "Answer.", "intent_detected": "blank_memo",
            "intent_source": "keyword", "sql_result": {"row_count": 3},
            "validation_passed": True, "error_message": None,
        }
        with patch("app.main.run_agent", return_value=mock_result) as mock_agent:
            client.post("/ask", json={
                "question": "show blank memos",
                "user_id":  "praveen.b"
            })
        mock_agent.assert_called_once_with(
            question = "show blank memos",
            user_id  = "praveen.b",
        )

    def test_ask_response_has_all_fields(self, client):
        mock_result = {
            "final_answer": "Answer.", "intent_detected": "blank_memo",
            "intent_source": "keyword", "sql_result": {"row_count": 3},
            "validation_passed": True, "error_message": None,
        }
        with patch("app.main.run_agent", return_value=mock_result):
            data = client.post("/ask", json={"question": "show blank memos"}).json()
        expected = {"answer", "intent_detected", "intent_source",
                    "row_count", "query_name", "validation_passed", "data", "error"}
        assert expected == set(data.keys())


class TestHealthEndpoint:

    def test_health_returns_200(self, client):
        with patch("app.main.DatabaseManager") as mock_db, \
             patch("app.main.BedrockTool") as mock_bedrock:
            mock_db.health_check.return_value      = {"db": "connected"}
            mock_bedrock.return_value.health_check.return_value = {"bedrock": "connected"}
            response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok_when_all_good(self, client):
        with patch("app.main.DatabaseManager") as mock_db, \
             patch("app.main.BedrockTool") as mock_bedrock:
            mock_db.health_check.return_value      = {"db": "connected"}
            mock_bedrock.return_value.health_check.return_value = {"bedrock": "connected"}
            data = client.get("/health").json()
        assert data["status"]  == "ok"
        assert data["api"]     == "ok"
        assert data["db"]      == "connected"
        assert data["bedrock"] == "connected"

    def test_health_degraded_when_db_down(self, client):
        with patch("app.main.DatabaseManager") as mock_db, \
             patch("app.main.BedrockTool") as mock_bedrock:
            mock_db.health_check.return_value      = {"db": "error: connection refused"}
            mock_bedrock.return_value.health_check.return_value = {"bedrock": "connected"}
            data = client.get("/health").json()
        assert data["status"] == "degraded"

    def test_health_has_all_fields(self, client):
        with patch("app.main.DatabaseManager") as mock_db, \
             patch("app.main.BedrockTool") as mock_bedrock:
            mock_db.health_check.return_value      = {"db": "connected"}
            mock_bedrock.return_value.health_check.return_value = {"bedrock": "connected"}
            data = client.get("/health").json()
        assert {"status", "api", "db", "bedrock"} == set(data.keys())


class TestExportEndpoint:

    def test_export_returns_200(self, client):
        with patch("app.main.ExportTool") as mock_export:
            mock_export.return_value.export_to_bytes.return_value = (
                b"fake xlsx content", "report.xlsx"
            )
            response = client.get("/export/excel")
        assert response.status_code == 200

    def test_export_returns_xlsx_content_type(self, client):
        with patch("app.main.ExportTool") as mock_export:
            mock_export.return_value.export_to_bytes.return_value = (
                b"fake xlsx content", "report.xlsx"
            )
            response = client.get("/export/excel")
        assert "spreadsheetml" in response.headers["content-type"]

    def test_export_has_content_disposition(self, client):
        with patch("app.main.ExportTool") as mock_export:
            mock_export.return_value.export_to_bytes.return_value = (
                b"fake xlsx content", "report.xlsx"
            )
            response = client.get("/export/excel")
        assert "attachment" in response.headers["content-disposition"]
        assert ".xlsx" in response.headers["content-disposition"]


# ═════════════════════════════════════════════════════════════
# INTEGRATION TESTS — needs SSH tunnel + AWS credentials
# ═════════════════════════════════════════════════════════════

class TestMainIntegration:
    """Real API calls against real DB + Bedrock."""

    def test_root_endpoint(self, live_client):
        response = live_client.get("/")
        assert response.status_code == 200
        assert response.json()["status"] == "running"

    def test_health_all_systems_ok(self, live_client):
        response = live_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["db"]      == "connected", f"DB not connected: {data}"
        assert data["bedrock"] == "connected", f"Bedrock not connected: {data}"
        assert data["status"]  == "ok"

    def test_queries_endpoint(self, live_client):
        response = live_client.get("/queries")
        assert response.status_code == 200
        assert live_client.get("/queries").json()["total"] == 5

    def test_ask_blank_memo(self, live_client):
        response = live_client.post("/ask", json={
            "question": "show me entries with blank memos",
            "user_id":  "praveen.b"
        })
        assert response.status_code == 200
        data = response.json()
        assert data["intent_detected"]   == "blank_memo"
        assert data["validation_passed"] is True
        assert len(data["answer"])       > 20

    def test_ask_total_count(self, live_client):
        response = live_client.post("/ask", json={
            "question": "how many time entries are there"
        })
        assert response.status_code == 200
        data = response.json()
        assert data["intent_detected"] == "total_count"
        assert any(char.isdigit() for char in data["answer"])

    def test_ask_unknown_returns_clarification(self, live_client):
        response = live_client.post("/ask", json={
            "question": "what is the weather today"
        })
        assert response.status_code == 200
        data = response.json()
        assert data["intent_detected"] == "unknown"
        assert len(data["answer"])     > 20

    def test_export_excel_returns_file(self, live_client):
        response = live_client.get("/export/excel")
        assert response.status_code == 200
        assert len(response.content) > 5000
        assert "spreadsheetml" in response.headers["content-type"]

    def test_ask_non_erp_memo(self, live_client):
        response = live_client.post("/ask", json={
            "question": "find entries not following ERP naming"
        })
        assert response.status_code == 200
        data = response.json()
        assert data["intent_detected"] == "non_erp_memo"
