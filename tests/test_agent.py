"""
tests/test_agent.py
────────────────────
Tests for agent nodes, prompts, state, and full graph pipeline.

UNIT tests    — no AWS, no DB needed
INTEGRATION   — needs SSH tunnel + AWS credentials

Run unit only:
    pytest tests\test_agent.py -v -k "not Integration"

Run all:
    pytest tests\test_agent.py -v
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from app.agent.state import AgentState
from app.agent.prompts import (
    intent_system_prompt, intent_user_prompt,
    ANSWER_SYSTEM_PROMPT, answer_user_prompt,
    clarification_message,
)
from app.agent.nodes import (
    intent_node, sql_node, validate_node,
    respond_node, clarify_node,
)
from app.agent.graph import run_agent, route_after_intent


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def make_state(**kwargs) -> AgentState:
    """Build a minimal AgentState with defaults."""
    defaults = {
        "user_question":     "show blank memos",
        "user_id":           "test_user",
        "intent_detected":   None,
        "intent_source":     None,
        "sql_result":        None,
        "sql_error":         None,
        "validation_passed": None,
        "validation_notes":  None,
        "final_answer":      None,
        "error_message":     None,
        "should_clarify":    False,
    }
    return {**defaults, **kwargs}


MOCK_SQL_SUCCESS = {
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

MOCK_SQL_EMPTY = {
    "status": "empty", "query_name": "last_7_days",
    "description": "Last 7 days", "row_count": 0,
    "message": "No records found", "rows": [],
}

MOCK_SQL_ERROR = {
    "status": "error", "query_name": "all_entries",
    "row_count": 0, "message": "connection refused", "rows": [],
}


# ═════════════════════════════════════════════════════════════
# UNIT TESTS
# ═════════════════════════════════════════════════════════════

class TestAgentState:

    def test_state_has_all_required_keys(self):
        state = make_state()
        required = {
            "user_question", "user_id", "intent_detected", "intent_source",
            "sql_result", "sql_error", "validation_passed", "validation_notes",
            "final_answer", "error_message", "should_clarify",
        }
        assert required == set(state.keys())

    def test_state_defaults_are_none(self):
        state = make_state()
        for key in ["intent_detected", "sql_result", "final_answer"]:
            assert state[key] is None

    def test_state_accepts_all_field_types(self):
        state = make_state(
            intent_detected  = "blank_memo",
            sql_result       = MOCK_SQL_SUCCESS,
            validation_passed = True,
            final_answer     = "3 employees have blank memos",
        )
        assert state["intent_detected"]   == "blank_memo"
        assert state["validation_passed"] is True
        assert state["final_answer"]      == "3 employees have blank memos"


class TestPrompts:

    def test_intent_system_prompt_is_string(self):
        prompt = intent_system_prompt()
        assert isinstance(prompt, str)
        assert len(prompt) > 20

    def test_intent_system_prompt_mentions_classifier(self):
        prompt = intent_system_prompt()
        assert "classifier" in prompt.lower() or "intent" in prompt.lower()

    def test_intent_user_prompt_contains_question(self):
        prompt = intent_user_prompt("show blank memos")
        assert "show blank memos" in prompt

    def test_intent_user_prompt_contains_all_queries(self):
        prompt = intent_user_prompt("x")
        for name in ["blank_memo", "all_entries", "total_count",
                     "last_7_days", "non_erp_memo"]:
            assert name in prompt

    def test_answer_system_prompt_is_string(self):
        assert isinstance(ANSWER_SYSTEM_PROMPT, str)
        assert len(ANSWER_SYSTEM_PROMPT) > 20

    def test_answer_system_prompt_mentions_erp(self):
        assert "ERP" in ANSWER_SYSTEM_PROMPT or "Oracle" in ANSWER_SYSTEM_PROMPT

    def test_answer_user_prompt_contains_question(self):
        prompt = answer_user_prompt(MOCK_SQL_SUCCESS, "who has blank memos?")
        assert "who has blank memos?" in prompt

    def test_answer_user_prompt_contains_row_count(self):
        prompt = answer_user_prompt(MOCK_SQL_SUCCESS, "q")
        assert "3" in prompt

    def test_clarification_message_mentions_queries(self):
        msg = clarification_message()
        assert isinstance(msg, str)
        assert len(msg) > 50

    def test_clarification_message_lists_options(self):
        msg = clarification_message()
        assert "•" in msg or "-" in msg or "\n" in msg


class TestIntentNode:

    def test_keyword_match_blank_memo(self):
        state  = make_state(user_question="show me entries with no memo")
        result = intent_node(state)
        assert result["intent_detected"] == "blank_memo"
        assert result["intent_source"]   == "keyword"

    def test_keyword_match_is_fast(self):
        """Keyword match should never call Bedrock."""
        state = make_state(user_question="show me entries with no memo")
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
            result = intent_node(state)
        mock_bedrock.detect_intent.assert_not_called()
        assert result["intent_detected"] == "blank_memo"

    def test_llm_fallback_for_unknown_phrasing(self):
        state = make_state(user_question="which timesheets need descriptions?")
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
            mock_bedrock.detect_intent.return_value = "blank_memo"
            result = intent_node(state)
        mock_bedrock.detect_intent.assert_called_once()
        assert result["intent_detected"] == "blank_memo"
        assert result["intent_source"]   == "llm"

    def test_unknown_intent_sets_should_clarify(self):
        state = make_state(user_question="what is the meaning of life")
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
            mock_bedrock.detect_intent.return_value = "unknown"
            result = intent_node(state)
        assert result["intent_detected"] == "unknown"
        assert result["should_clarify"]  is True

    def test_result_always_has_intent_detected(self):
        for question in ["blank memo", "x y z", "total count"]:
            state = make_state(user_question=question)
            with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
                mock_bedrock.detect_intent.return_value = "unknown"
                result = intent_node(state)
            assert "intent_detected" in result


class TestSQLNode:

    def test_runs_correct_query(self):
        state = make_state(intent_detected="blank_memo")
        with patch("app.agent.nodes._sql_tool") as mock_sql:
            mock_sql.run.return_value = MOCK_SQL_SUCCESS
            result = sql_node(state)
        mock_sql.run.assert_called_once_with("blank_memo")
        assert result["sql_result"] == MOCK_SQL_SUCCESS

    def test_skips_when_no_intent(self):
        state = make_state(intent_detected=None)
        with patch("app.agent.nodes._sql_tool") as mock_sql:
            result = sql_node(state)
        mock_sql.run.assert_not_called()
        assert result["sql_error"] is not None

    def test_skips_when_unknown_intent(self):
        state = make_state(intent_detected="unknown")
        with patch("app.agent.nodes._sql_tool") as mock_sql:
            result = sql_node(state)
        mock_sql.run.assert_not_called()

    def test_handles_sql_error(self):
        state = make_state(intent_detected="blank_memo")
        with patch("app.agent.nodes._sql_tool") as mock_sql:
            mock_sql.run.return_value = MOCK_SQL_ERROR
            result = sql_node(state)
        assert result["sql_error"]          is not None
        assert result["sql_result"]["status"] == "error"


class TestValidateNode:

    def test_passes_valid_result(self):
        state  = make_state(sql_result=MOCK_SQL_SUCCESS)
        result = validate_node(state)
        assert result["validation_passed"] is True

    def test_fails_with_no_result(self):
        state  = make_state(sql_result=None)
        result = validate_node(state)
        assert result["validation_passed"] is False

    def test_fails_on_sql_error(self):
        state  = make_state(sql_result=MOCK_SQL_ERROR)
        result = validate_node(state)
        assert result["validation_passed"] is False

    def test_passes_empty_result(self):
        state  = make_state(sql_result=MOCK_SQL_EMPTY)
        result = validate_node(state)
        assert result["validation_passed"] is True

    def test_warns_on_blank_memo_with_non_null(self):
        bad_result = {
            **MOCK_SQL_SUCCESS,
            "rows": [
                {"id": 1, "employee": "John", "date": "2026-01-15",
                 "hours": 8.0, "memo": "ERP-001"},   # ← has memo, should not be here
            ]
        }
        state  = make_state(sql_result=bad_result)
        result = validate_node(state)
        assert result["validation_passed"] is True  # still passes
        assert result["validation_notes"]  is not None
        assert "Warning" in result["validation_notes"]

    def test_notes_empty_when_all_good(self):
        state  = make_state(sql_result=MOCK_SQL_SUCCESS)
        result = validate_node(state)
        assert result["validation_notes"] is None


class TestRespondNode:

    def test_returns_final_answer(self):
        state = make_state(sql_result=MOCK_SQL_SUCCESS)
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
            mock_bedrock.ask.return_value = "3 employees have blank memos."
            result = respond_node(state)
        assert result["final_answer"] == "3 employees have blank memos."

    def test_handles_no_sql_result(self):
        state  = make_state(sql_result=None)
        result = respond_node(state)
        assert isinstance(result["final_answer"], str)
        assert len(result["final_answer"]) > 0

    def test_appends_validation_warning(self):
        state = make_state(
            sql_result       = MOCK_SQL_SUCCESS,
            validation_notes = "Warning: 2 rows have non-null memo",
        )
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
            mock_bedrock.ask.return_value = "Answer here."
            result = respond_node(state)
        assert "Warning" in result["final_answer"]

    def test_no_warning_appended_when_no_notes(self):
        state = make_state(sql_result=MOCK_SQL_SUCCESS, validation_notes=None)
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
            mock_bedrock.ask.return_value = "Clean answer."
            result = respond_node(state)
        assert result["final_answer"] == "Clean answer."


class TestClarifyNode:

    def test_returns_final_answer(self):
        state  = make_state(should_clarify=True, intent_detected="unknown")
        result = clarify_node(state)
        assert "final_answer" in result
        assert isinstance(result["final_answer"], str)
        assert len(result["final_answer"]) > 20

    def test_clarification_lists_options(self):
        state  = make_state()
        result = clarify_node(state)
        msg    = result["final_answer"]
        assert any(word in msg for word in ["entries", "records", "memo", "count"])


class TestRouting:

    def test_routes_to_clarify_when_unknown(self):
        state = make_state(intent_detected="unknown", should_clarify=True)
        assert route_after_intent(state) == "clarify"

    def test_routes_to_sql_when_known(self):
        state = make_state(intent_detected="blank_memo", should_clarify=False)
        assert route_after_intent(state) == "sql"

    def test_routes_to_clarify_when_should_clarify_true(self):
        state = make_state(intent_detected="blank_memo", should_clarify=True)
        assert route_after_intent(state) == "clarify"

    def test_routes_to_sql_for_all_valid_intents(self):
        for intent in ["all_entries", "total_count", "blank_memo",
                       "last_7_days", "non_erp_memo"]:
            state = make_state(intent_detected=intent, should_clarify=False)
            assert route_after_intent(state) == "sql"


class TestRunAgentUnit:

    def test_returns_dict(self):
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock, \
             patch("app.agent.nodes._sql_tool") as mock_sql:
            mock_bedrock.detect_intent.return_value = "blank_memo"
            mock_bedrock.ask.return_value           = "3 employees have blank memos."
            mock_sql.run.return_value               = MOCK_SQL_SUCCESS
            result = run_agent("show blank memos")
        assert isinstance(result, dict)

    def test_result_has_final_answer(self):
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock, \
             patch("app.agent.nodes._sql_tool") as mock_sql:
            mock_bedrock.detect_intent.return_value = "blank_memo"
            mock_bedrock.ask.return_value           = "Answer here."
            mock_sql.run.return_value               = MOCK_SQL_SUCCESS
            result = run_agent("show blank memos")
        assert "final_answer" in result
        assert result["final_answer"] is not None

    def test_unknown_question_returns_clarification(self):
        with patch("app.agent.nodes._bedrock_tool") as mock_bedrock:
            mock_bedrock.detect_intent.return_value = "unknown"
            result = run_agent("what is the weather today")
        assert result["final_answer"] is not None
        assert result["intent_detected"] == "unknown"

    def test_pipeline_error_handled_gracefully(self):
        with patch("app.agent.graph._graph") as mock_graph:
            mock_graph.invoke.side_effect = Exception("pipeline crashed")
            result = run_agent("show blank memos")
        assert "final_answer"  in result
        assert "error_message" in result
        assert result["final_answer"] is not None


# ═════════════════════════════════════════════════════════════
# INTEGRATION TESTS — needs SSH tunnel + AWS credentials
# ═════════════════════════════════════════════════════════════

class TestAgentIntegration:
    """Full end-to-end pipeline against real RDS + real Bedrock."""

    @pytest.fixture(autouse=True)
    def init_db(self):
        from app.db.connection import DatabaseManager
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

    def test_blank_memo_question(self):
        result = run_agent("show me entries with blank memos")
        assert result["intent_detected"]   == "blank_memo"
        assert result["validation_passed"] is True
        assert result["final_answer"]      is not None
        assert len(result["final_answer"]) > 20

    def test_total_count_question(self):
        result = run_agent("how many time entries are there")
        assert result["intent_detected"] == "total_count"
        assert result["final_answer"]    is not None
        assert any(char.isdigit() for char in result["final_answer"])

    def test_last_7_days_question(self):
        result = run_agent("show me entries from the last week")
        assert result["intent_detected"] in ("last_7_days",)
        assert result["final_answer"]    is not None

    def test_unknown_question_returns_clarification(self):
        result = run_agent("what is the weather in San Francisco")
        assert result["intent_detected"] == "unknown"
        assert result["final_answer"]    is not None
        assert len(result["final_answer"]) > 20

    def test_keyword_match_skips_llm(self):
        """exact sample_question phrase → keyword match, no Haiku call."""
        result = run_agent("show me entries with no memo")
        assert result["intent_source"]   == "keyword"
        assert result["intent_detected"] == "blank_memo"

    def test_state_fully_populated(self):
        result = run_agent("show blank memos")
        assert result["user_question"]    is not None
        assert result["intent_detected"]  is not None
        assert result["sql_result"]       is not None
        assert result["validation_passed"] is not None
        assert result["final_answer"]     is not None
