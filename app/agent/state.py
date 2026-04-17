"""
app/agent/state.py
──────────────────
AgentState — what the LangGraph agent remembers between nodes.

Think of this as the agent's working memory for a single request.
Every node reads from state and writes back to state.

Flow:
    user_question
        ↓ intent_node
    intent_detected
        ↓ sql_node
    sql_result
        ↓ validate_node
    validation_passed
        ↓ respond_node
    final_answer
"""

from typing import Optional, Any
from typing_extensions import TypedDict


class AgentState(TypedDict):
    """
    Single shared state object passed between all LangGraph nodes.
    Each node reads what it needs and adds its output.
    """

    # ── Input ─────────────────────────────────────────────────
    user_question:      str             # original question from user
    user_id:            Optional[str]   # who asked (for RBAC later)

    # ── Intent detection (intent_node) ────────────────────────
    intent_detected:    Optional[str]   # e.g. "blank_memo"
    intent_source:      Optional[str]   # "keyword" or "llm"

    # ── SQL execution (sql_node) ──────────────────────────────
    sql_result:         Optional[dict]  # full result from sql_tool.run()
    sql_error:          Optional[str]   # error message if query failed

    # ── Validation (validate_node) ────────────────────────────
    validation_passed:  Optional[bool]  # did the result pass validation?
    validation_notes:   Optional[str]   # any warnings or issues found

    # ── Response generation (respond_node) ────────────────────
    final_answer:       Optional[str]   # plain English answer for user

    # ── Routing ───────────────────────────────────────────────
    error_message:      Optional[str]   # set if anything goes wrong
    should_clarify:        Optional[bool]  # True if question was unclear
    is_dynamic_sql:        Optional[bool]  # True if LLM wrote its own SQL
    is_general_knowledge:  Optional[bool]  # True if answered from Claude general knowledge
