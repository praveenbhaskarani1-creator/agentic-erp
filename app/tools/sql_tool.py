"""
app/tools/sql_tool.py
─────────────────────
The SQL execution tool for the LangGraph agent.

Takes a query name → runs it against RDS → returns clean Python dicts.

The agent ONLY calls this tool. It cannot run raw SQL.
All queries must exist in app/sql/queries.py first.

Usage:
    from app.tools.sql_tool import SQLTool

    tool = SQLTool()
    result = tool.run("blank_memo")

    # result looks like:
    # {
    #   "status":      "success",
    #   "query_name":  "blank_memo",
    #   "description": "Find all time entries where memo is NULL",
    #   "row_count":   23,
    #   "rows": [
    #     {"id": 5, "employee": "John Smith", "date": "2026-01-15", "hours": 8.0, "memo": None},
    #     ...
    #   ],
    #   "message":     "Found 23 entries with blank memo"
    # }
"""

import logging
import re
from datetime import date, datetime
from typing import Any

from sqlalchemy import text

from app.db.connection import get_db
from app.sql.queries import get_query, get_all_names, QueryDef

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# SQL Safety Guardrail (Layer 2 — defence-in-depth inside tool)
# ─────────────────────────────────────────────────────────────

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|"
    r"MERGE|UPSERT|GRANT|REVOKE|EXECUTE|EXEC|CALL)\b",
    re.IGNORECASE,
)

def _assert_read_only(sql: str) -> None:
    """
    Raises ValueError if sql contains any write/DDL/admin keyword.
    Called inside run_raw() as a second independent safety layer.
    """
    sql = sql.strip()
    if not sql.upper().startswith("SELECT"):
        raise ValueError("Safety violation: query must start with SELECT")
    match = _FORBIDDEN.search(sql)
    if match:
        raise ValueError(f"Safety violation: forbidden keyword '{match.group(0).upper()}' detected")
    if ";" in sql:
        raise ValueError("Safety violation: semicolons not permitted")
    if "--" in sql or "/*" in sql:
        raise ValueError("Safety violation: SQL comments not permitted")


# ─────────────────────────────────────────────────────────────
# Result helpers
# ─────────────────────────────────────────────────────────────

def _serialize_row(row: dict) -> dict:
    """
    Convert a DB row dict to JSON-safe Python types.
    Handles: date → string, datetime → string, Decimal → float
    """
    clean = {}
    for key, value in row.items():
        if isinstance(value, (date, datetime)):
            clean[key] = value.isoformat()
        elif hasattr(value, "__float__"):   # Decimal
            clean[key] = float(value)
        else:
            clean[key] = value
    return clean


def _success(query: QueryDef, rows: list[dict]) -> dict:
    """Build a standard success response."""
    count = len(rows)
    return {
        "status":      "success",
        "query_name":  query.name,
        "description": query.description,
        "row_count":   count,
        "rows":        rows,
        "message":     f"Found {count} record{'s' if count != 1 else ''}",
    }


def _empty(query: QueryDef) -> dict:
    """Build a standard empty-result response."""
    return {
        "status":      "empty",
        "query_name":  query.name,
        "description": query.description,
        "row_count":   0,
        "rows":        [],
        "message":     "No records found matching this query",
    }


def _error(query_name: str, error: str) -> dict:
    """Build a standard error response."""
    return {
        "status":     "error",
        "query_name": query_name,
        "row_count":  0,
        "rows":       [],
        "message":    f"Query failed: {error}",
    }


# ─────────────────────────────────────────────────────────────
# SQL Tool
# ─────────────────────────────────────────────────────────────

class SQLTool:
    """
    Executes pre-approved SQL queries against fusion_time_entries.

    The LangGraph agent uses this tool to get data from RDS.
    All queries are defined in app/sql/queries.py — no raw SQL here.
    """

    def run(self, query_name: str) -> dict:
        """
        Run a pre-approved query by name.

        Args:
            query_name: key from QUERIES dict e.g. "blank_memo"

        Returns:
            dict with keys: status, query_name, description,
                            row_count, rows, message
        """
        logger.info(f"[sql_tool] Running query: {query_name}")

        # ── Step 1: Look up query definition ─────────────────
        try:
            query = get_query(query_name)
        except KeyError:
            available = get_all_names()
            msg = f"Unknown query '{query_name}'. Available: {available}"
            logger.warning(f"[sql_tool] {msg}")
            return _error(query_name, msg)

        # ── Step 2: Execute against RDS ───────────────────────
        try:
            with get_db() as db:
                result = db.execute(text(query.sql))

                # mappings() gives us dict-like rows by column name
                raw_rows = result.mappings().fetchall()

            logger.info(f"[sql_tool] '{query_name}' returned {len(raw_rows)} rows")

        except Exception as e:
            logger.error(f"[sql_tool] DB error on '{query_name}': {e}")
            return _error(query_name, str(e))

        # ── Step 3: Serialize rows to clean Python dicts ──────
        rows = [_serialize_row(dict(row)) for row in raw_rows]

        # ── Step 4: Return structured response ────────────────
        if not rows:
            return _empty(query)

        return _success(query, rows)

    def run_raw(self, sql: str, description: str = "Dynamic query") -> dict:
        """
        Execute a raw SQL string generated by the LLM.
        Only called from dynamic_sql_node — never directly by the user.
        The caller is responsible for safety validation before calling this.

        Args:
            sql:         validated SELECT statement
            description: human-readable description (the original question)

        Returns:
            Same dict shape as run() for compatibility with validate/respond nodes.
        """
        logger.info(f"[sql_tool] run_raw: {sql[:100]}")

        # Layer 2 safety check — independent of nodes.py guardrail
        try:
            _assert_read_only(sql)
        except ValueError as safety_err:
            logger.error(f"[sql_tool] run_raw BLOCKED by safety guardrail: {safety_err}")
            return {
                "status":      "error",
                "query_name":  "dynamic",
                "description": description,
                "row_count":   0,
                "rows":        [],
                "message":     str(safety_err),
            }

        try:
            with get_db() as db:
                result  = db.execute(text(sql))
                raw_rows = result.mappings().fetchall()

            logger.info(f"[sql_tool] run_raw returned {len(raw_rows)} rows")

        except Exception as e:
            logger.error(f"[sql_tool] run_raw DB error: {e}")
            return {
                "status":      "error",
                "query_name":  "dynamic",
                "description": description,
                "row_count":   0,
                "rows":        [],
                "message":     f"Query failed: {e}",
            }

        rows = [_serialize_row(dict(row)) for row in raw_rows]
        count = len(rows)

        return {
            "status":      "success" if rows else "empty",
            "query_name":  "dynamic",
            "description": description,
            "row_count":   count,
            "rows":        rows,
            "message":     f"Found {count} record{'s' if count != 1 else ''}",
        }

    def run_all(self) -> dict[str, dict]:
        """
        Run all queries and return results keyed by query name.
        Used for full validation reports.
        """
        return {name: self.run(name) for name in get_all_names()}

    def summary(self, query_name: str) -> str:
        """
        Run a query and return a one-line plain English summary.
        Used by the agent when it just needs a quick answer.

        Examples:
            "Found 23 entries with blank memo"
            "Total records: 119"
            "No records found matching this query"
        """
        result = self.run(query_name)

        if result["status"] == "error":
            return f"Error running {query_name}: {result['message']}"

        if query_name == "total_count" and result["rows"]:
            count = result["rows"][0].get("total_records", 0)
            return f"Total time entries: {count}"

        return result["message"]
