"""
app/agent/nodes.py
──────────────────
The 5 LangGraph nodes — each does one job and updates state.

Flow:
    intent_node → sql_node → validate_node → respond_node
                ↘ clarify_node (if intent = unknown)

Each node:
  - Takes AgentState as input
  - Returns a dict with ONLY the keys it updated
  - Never raises — always handles errors gracefully
"""

import logging
import re
from app.agent.state import AgentState
from app.agent.prompts import (
    intent_system_prompt, intent_user_prompt,
    ANSWER_SYSTEM_PROMPT, answer_user_prompt,
    GENERAL_KNOWLEDGE_SYSTEM_PROMPT,
    clarification_message,
)
from app.sql.queries import find_query_by_keyword, get_all_names
from app.tools.sql_tool import SQLTool
from app.tools.bedrock_tool import BedrockTool, MODEL_SONNET

logger = logging.getLogger(__name__)

# Shared tool instances — created once, reused across all requests
_sql_tool     = SQLTool()
_bedrock_tool = BedrockTool()

# ─────────────────────────────────────────────────────────────
# SQL Safety Guardrail (Layer 1 — called before execution)
# ─────────────────────────────────────────────────────────────

# All keywords that must never appear in a safe read-only query
_FORBIDDEN_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "REPLACE", "MERGE", "UPSERT",
    "GRANT", "REVOKE", "EXECUTE", "EXEC", "CALL",
]

# Regex: match forbidden keywords as whole words (not inside column/table names)
_FORBIDDEN_PATTERN = re.compile(
    r"\b(" + "|".join(_FORBIDDEN_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

def _validate_sql_safety(sql: str) -> tuple[bool, str]:
    """
    Validate that a SQL string is safe to execute.
    Returns (True, "") if safe, (False, reason) if not.

    Rules:
      1. Must start with SELECT (after stripping whitespace/comments)
      2. No forbidden keywords anywhere in the query
      3. No semicolons mid-query (blocks stacked statements)
      4. No SQL comment sequences that could hide injection
    """
    # Strip leading whitespace
    sql_stripped = sql.strip()

    # Rule 1 — must start with SELECT
    if not sql_stripped.upper().startswith("SELECT"):
        return False, "Query must start with SELECT"

    # Rule 2 — no forbidden keywords (whole word match)
    match = _FORBIDDEN_PATTERN.search(sql_stripped)
    if match:
        return False, f"Forbidden keyword detected: {match.group(0).upper()}"

    # Rule 3 — no semicolons (prevents stacked statements)
    if ";" in sql_stripped:
        return False, "Semicolons are not allowed in queries"

    # Rule 4 — no suspicious comment patterns used for injection
    if "--" in sql_stripped or "/*" in sql_stripped:
        return False, "SQL comments are not allowed in queries"

    return True, ""


# ─────────────────────────────────────────────────────────────
# Node 1 — Intent Detection
# ─────────────────────────────────────────────────────────────

def intent_node(state: AgentState) -> dict:
    """
    Figures out which query to run from the user's question.

    Strategy:
      1. Try keyword match first (free, instant)
      2. If no match → call Haiku to classify (fast, cheap)
      3. If still unknown → set should_clarify = True
    """
    question = state["user_question"]
    logger.info(f"[intent_node] Question: {question[:80]}")

    # ── Step 1: Keyword match ─────────────────────────────────
    keyword_match = find_query_by_keyword(question)
    if keyword_match:
        logger.info(f"[intent_node] Keyword match: {keyword_match.name}")
        return {
            "intent_detected": keyword_match.name,
            "intent_source":   "keyword",
        }

    # ── Step 2: LLM classification via Haiku ─────────────────
    intent = _bedrock_tool.detect_intent(question)
    logger.info(f"[intent_node] LLM intent: {intent}")

    if intent == "unknown":
        return {
            "intent_detected": "unknown",
            "intent_source":   "llm",
            "should_clarify":  False,   # let dynamic_sql_node try first
        }

    return {
        "intent_detected": intent,
        "intent_source":   "llm",
        "should_clarify":  False,
    }


# ─────────────────────────────────────────────────────────────
# Node 2 — SQL Execution
# ─────────────────────────────────────────────────────────────

def sql_node(state: AgentState) -> dict:
    """
    Runs the pre-approved SQL query against RDS.
    Only runs if intent was successfully detected.
    """
    intent = state.get("intent_detected")
    logger.info(f"[sql_node] Running query: {intent}")

    if not intent or intent == "unknown":
        return {"sql_error": "No valid intent detected — skipping SQL"}

    result = _sql_tool.run(intent)

    if result["status"] == "error":
        logger.error(f"[sql_node] Query error: {result['message']}")
        return {
            "sql_result": result,
            "sql_error":  result["message"],
        }

    logger.info(f"[sql_node] Got {result['row_count']} rows")
    return {
        "sql_result": result,
        "sql_error":  None,
    }


# ─────────────────────────────────────────────────────────────
# Node 3 — Validation
# ─────────────────────────────────────────────────────────────

def validate_node(state: AgentState) -> dict:
    """
    Validates the SQL result before passing to Claude.

    Checks:
      - Result has expected structure
      - Row count is reasonable
      - No signs of data corruption

    Never blocks the response — sets validation_notes for warnings.
    """
    sql_result = state.get("sql_result")

    if not sql_result:
        return {
            "validation_passed": False,
            "validation_notes":  "No SQL result to validate",
        }

    if sql_result.get("status") == "error":
        return {
            "validation_passed": False,
            "validation_notes":  f"SQL error: {sql_result.get('message')}",
        }

    notes  = []
    passed = True

    # Check result structure
    if "rows" not in sql_result:
        return {
            "validation_passed": False,
            "validation_notes":  "Result missing rows field",
        }

    rows      = sql_result.get("rows", [])
    row_count = sql_result.get("row_count", 0)

    # Warn if row_count doesn't match actual rows returned
    if len(rows) != row_count and row_count <= 200:
        notes.append(
            f"Row count mismatch: reported {row_count}, got {len(rows)}"
        )

    # Warn if blank_memo query returns rows WITH memos (shouldn't happen)
    if sql_result.get("query_name") == "blank_memo":
        bad = [r for r in rows if r.get("memo") is not None]
        if bad:
            notes.append(
                f"Warning: {len(bad)} rows in blank_memo result have non-null memo"
            )

    # Warn if empty result
    if sql_result.get("status") == "empty":
        notes.append("Query returned no records")

    logger.info(
        f"[validate_node] passed={passed}, "
        f"notes={notes if notes else 'none'}"
    )

    return {
        "validation_passed": passed,
        "validation_notes":  "; ".join(notes) if notes else None,
    }


# ─────────────────────────────────────────────────────────────
# Node 4 — Response Generation
# ─────────────────────────────────────────────────────────────

def respond_node(state: AgentState) -> dict:
    """
    Generates a plain English answer using Claude Sonnet.
    Reads sql_result and user_question from state.
    """
    question   = state["user_question"]
    sql_result = state.get("sql_result")
    notes      = state.get("validation_notes")

    logger.info(f"[respond_node] Generating answer for: {question[:60]}")

    if not sql_result:
        return {
            "final_answer": (
                "I was unable to retrieve data for your question. "
                "Please try again or rephrase your question."
            )
        }

    # Get answer from Sonnet
    answer = _bedrock_tool.ask(
        data     = sql_result,
        question = question,
    )

    # Append validation warning if any
    if notes and "Warning" in notes:
        answer += f"\n\n⚠️ Note: {notes}"

    logger.info(f"[respond_node] Answer generated ({len(answer)} chars)")
    return {"final_answer": answer}


# ─────────────────────────────────────────────────────────────
# Node 5 — Dynamic SQL (LLM writes its own query)
# ─────────────────────────────────────────────────────────────

def dynamic_sql_node(state: AgentState) -> dict:
    """
    When no pre-approved query matches, ask Claude Sonnet to write
    a SELECT query, validate it is safe, execute it, and return results.

    Safety rules enforced here:
      - Query must start with SELECT (case-insensitive)
      - No semicolons mid-query (blocks stacked statements)
      - Forbidden keywords: INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE
    """
    question = state["user_question"]
    logger.info(f"[dynamic_sql_node] Generating SQL for: {question[:80]}")

    # Ask Claude to write the SQL
    result = _bedrock_tool.generate_sql(question)

    if result["status"] == "error":
        logger.warning(f"[dynamic_sql_node] SQL generation failed: {result['message']}")
        return {
            "should_clarify": True,
            "sql_error": result["message"],
        }

    sql = result["sql"].strip()

    # ── Safety guardrail (Layer 1 of 3) ──────────────────────
    safe, reason = _validate_sql_safety(sql)
    if not safe:
        logger.warning(f"[dynamic_sql_node] Unsafe SQL rejected: {reason} | sql={sql[:80]}")
        return {
            "should_clarify": True,
            "sql_error": f"Query rejected by safety guardrail: {reason}",
        }

    logger.info(f"[dynamic_sql_node] Executing dynamic SQL: {sql[:120]}")

    # Execute via SQLTool's raw execution path
    sql_result = _sql_tool.run_raw(sql=sql, description=question)

    logger.info(f"[dynamic_sql_node] Result: status={sql_result.get('status')} rows={sql_result.get('row_count', 0)}")

    return {
        "sql_result":    sql_result,
        "sql_error":     sql_result.get("message") if sql_result.get("status") == "error" else None,
        "is_dynamic_sql": True,
        "intent_source": "dynamic",
    }


# ─────────────────────────────────────────────────────────────
# Node 6 — General Knowledge (Option 4 — Claude general knowledge)
# ─────────────────────────────────────────────────────────────

def general_knowledge_node(state: AgentState) -> dict:
    """
    When dynamic SQL also fails, try answering from Claude's general knowledge.
    Handles HR concepts, Oracle Fusion, ERP terminology, general business topics.
    If Claude cannot answer → sets should_clarify = True for clarify_node.
    """
    question = state["user_question"]
    logger.info(f"[general_knowledge_node] Attempting general answer for: {question[:80]}")

    result = _bedrock_tool._call_claude(
        system_prompt = GENERAL_KNOWLEDGE_SYSTEM_PROMPT,
        user_message  = question,
        model         = MODEL_SONNET,
        max_tokens    = 1024,
    )

    if result.get("status") == "error":
        logger.info("[general_knowledge_node] Bedrock error — routing to clarify")
        return {"should_clarify": True}

    answer = result.get("text", "").strip()

    if not answer or answer == "CANNOT_ANSWER":
        logger.info("[general_knowledge_node] Cannot answer — routing to clarify")
        return {"should_clarify": True}

    logger.info(f"[general_knowledge_node] General answer generated ({len(answer)} chars)")
    return {
        "final_answer":         answer,
        "is_general_knowledge": True,
        "intent_source":        "general_knowledge",
        "should_clarify":       False,   # reset flag set by dynamic_sql_node failure
    }


# ─────────────────────────────────────────────────────────────
# Node 7 — Clarification
# ─────────────────────────────────────────────────────────────

def clarify_node(state: AgentState) -> dict:
    """
    Returns a helpful clarification message when intent is unknown.
    Lists all available queries so user knows what to ask.
    """
    logger.info("[clarify_node] Intent unknown — returning clarification")
    return {"final_answer": clarification_message()}
