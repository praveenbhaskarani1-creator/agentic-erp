"""
app/agent/graph.py
──────────────────
LangGraph StateGraph — wires all nodes into the agent pipeline.

Graph flow:
    START
      ↓
    intent_node
      ↓
    [route_after_intent]
      ├── unknown  → clarify_node → END
      └── known    → sql_node
                        ↓
                    validate_node
                        ↓
                    respond_node
                        ↓
                       END

Usage:
    from app.agent.graph import run_agent

    result = run_agent("show me entries with blank memos")
    print(result["final_answer"])
"""

import logging
from langgraph.graph import StateGraph, END

from app.agent.state import AgentState
from app.agent.nodes import (
    intent_node,
    sql_node,
    dynamic_sql_node,
    general_knowledge_node,
    validate_node,
    respond_node,
    clarify_node,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Routing logic
# ─────────────────────────────────────────────────────────────

def route_after_intent(state: AgentState) -> str:
    """
    After intent_node runs — decide which node comes next.

    Returns:
        "sql"         → known pre-approved query
        "dynamic_sql" → unknown intent, let LLM write its own SELECT
        "clarify"     → dynamic SQL also failed, ask user to rephrase
    """
    if state.get("should_clarify"):
        logger.info("[graph] Routing → clarify_node")
        return "clarify"

    if state.get("intent_detected") == "unknown":
        logger.info("[graph] Routing → dynamic_sql_node (no pre-approved match)")
        return "dynamic_sql"

    logger.info(f"[graph] Routing → sql_node (intent={state.get('intent_detected')})")
    return "sql"


def route_after_dynamic_sql(state: AgentState) -> str:
    """
    After dynamic_sql_node — try general knowledge if SQL failed, otherwise validate.
    """
    if state.get("should_clarify") or state.get("sql_error"):
        logger.info("[graph] Routing → general_knowledge_node (dynamic SQL failed)")
        return "general_knowledge"
    logger.info("[graph] Routing → validate_node (dynamic SQL succeeded)")
    return "validate"


def route_after_general_knowledge(state: AgentState) -> str:
    """
    After general_knowledge_node — clarify if Claude couldn't answer, otherwise END.
    """
    if state.get("should_clarify"):
        logger.info("[graph] Routing → clarify_node (general knowledge exhausted)")
        return "clarify"
    logger.info("[graph] Routing → END (general knowledge answered)")
    return "end"


# ─────────────────────────────────────────────────────────────
# Build the graph
# ─────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    """
    Construct and compile the LangGraph StateGraph.
    Called once at app startup.
    """
    graph = StateGraph(AgentState)

    # ── Add nodes ─────────────────────────────────────────────
    graph.add_node("intent",           intent_node)
    graph.add_node("sql",              sql_node)
    graph.add_node("dynamic_sql",      dynamic_sql_node)
    graph.add_node("general_knowledge", general_knowledge_node)
    graph.add_node("validate",         validate_node)
    graph.add_node("respond",          respond_node)
    graph.add_node("clarify",          clarify_node)

    # ── Entry point ───────────────────────────────────────────
    graph.set_entry_point("intent")

    # ── Routing after intent ───────────────────────────────────
    graph.add_conditional_edges(
        "intent",
        route_after_intent,
        {
            "sql":         "sql",
            "dynamic_sql": "dynamic_sql",
            "clarify":     "clarify",
        }
    )

    # ── Routing after dynamic SQL ──────────────────────────────
    graph.add_conditional_edges(
        "dynamic_sql",
        route_after_dynamic_sql,
        {
            "validate":         "validate",
            "general_knowledge": "general_knowledge",
        }
    )

    # ── Routing after general knowledge ───────────────────────
    graph.add_conditional_edges(
        "general_knowledge",
        route_after_general_knowledge,
        {
            "end":     END,
            "clarify": "clarify",
        }
    )

    # ── Linear flow: sql → validate → respond ─────────────────
    graph.add_edge("sql",      "validate")
    graph.add_edge("validate", "respond")

    # ── Terminal nodes ────────────────────────────────────────
    graph.add_edge("respond", END)
    graph.add_edge("clarify", END)

    return graph.compile()


# ── Compiled graph singleton — built once at import time ──────
_graph = build_graph()


# ─────────────────────────────────────────────────────────────
# Public interface
# ─────────────────────────────────────────────────────────────

def run_agent(question: str, user_id: str = "anonymous") -> dict:
    """
    Run the full agent pipeline for a user question.

    Args:
        question: natural language question from the user
        user_id:  optional user identifier (for RBAC later)

    Returns:
        Full AgentState dict including final_answer.

    Example:
        result = run_agent("show me entries with blank memos")
        print(result["final_answer"])
        # → "23 employees have missing memo entries: ..."
    """
    logger.info(f"[graph] run_agent: '{question[:80]}' (user={user_id})")

    initial_state: AgentState = {
        "user_question":     question,
        "user_id":           user_id,
        "intent_detected":   None,
        "intent_source":     None,
        "sql_result":        None,
        "sql_error":         None,
        "validation_passed": None,
        "validation_notes":  None,
        "final_answer":      None,
        "error_message":     None,
        "should_clarify":       False,
        "is_dynamic_sql":       False,
        "is_general_knowledge": False,
    }

    try:
        result = _graph.invoke(initial_state)
        
        sql_res = result.get("sql_result") or {}
        logger.info(
            f"[graph] Completed — intent={result.get('intent_detected')}, "
            f"rows={sql_res.get('row_count', 0)}"
        )
        return result

    except Exception as e:
        logger.error(f"[graph] Pipeline error: {e}")
        return {
            **initial_state,
            "final_answer":  "An unexpected error occurred. Please try again.",
            "error_message": str(e),
        }
