"""
app/sql/queries.py
──────────────────
Master SQL library for Agentic Time Entry Validation.

All queries run against:
    Table  : public.fusion_time_entries
    DB     : agentdb
    Schema : public

Columns in fusion_time_entries:
    id             - row identifier
    employee       - employee name / ID
    date           - timesheet date
    hours          - hours worked
    memo           - timesheet memo / description
    project_number - project number (e.g. P-1001), nullable
    project_name   - project name (e.g. Oracle Fusion ERP), nullable

Rules:
  - ONLY pre-approved queries in this file may run against the DB
  - No dynamic SQL construction outside this file
  - Every query has: name, description, sql, sample_questions
  - The agent picks a query by matching sample_questions to user intent

Usage:
    from app.sql.queries import QUERIES, get_query

    q = get_query("blank_memo")
    print(q.sql)
"""

from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────────────────────
# Query definition
# ─────────────────────────────────────────────────────────────

@dataclass
class QueryDef:
    """
    A single pre-approved SQL query definition.

    name             : unique key used to look up this query
    description      : human-readable description (shown in API docs)
    sql              : the actual SQL — no f-strings, no format() here
    sample_questions : phrases the agent uses to match intent → query
    returns_columns  : columns in the result (for response formatting)
    """
    name:             str
    description:      str
    sql:              str
    sample_questions: list[str]
    returns_columns:  list[str]
    requires_params:  bool = False      # True if query needs :param substitution
    params:           dict = field(default_factory=dict)   # default param values


# ─────────────────────────────────────────────────────────────
# The 5 pre-approved queries — your exact SQL
# ─────────────────────────────────────────────────────────────

QUERIES: dict[str, QueryDef] = {

    # ── Query 1: All Rows ─────────────────────────────────────
    "all_entries": QueryDef(
        name="all_entries",
        description="Retrieve all time entry records ordered by ID",
        sql="""
            SELECT *
            FROM public.fusion_time_entries
            ORDER BY id ASC
        """,
        sample_questions=[
            "show me all time entries",
            "get all records",
            "list everything in the timesheet",
            "select all rows",
            "show all data",
            "give me all timesheet entries",
            "display all records",
        ],
        returns_columns=["id", "employee", "date", "hours", "memo", "project_number", "project_name"],
    ),

    # ── Query 2: Total Count ──────────────────────────────────
    "total_count": QueryDef(
        name="total_count",
        description="Return the total number of time entry records",
        sql="""
            SELECT COUNT(*) AS total_records
            FROM public.fusion_time_entries
        """,
        sample_questions=[
            "how many time entries are there",
            "total count of records",
            "how many rows",
            "count all entries",
            "what is the total number of timesheets",
            "how many records in the table",
            "total number of entries",
        ],
        returns_columns=["total_records"],
    ),

    # ── Query 3: Blank Memo ───────────────────────────────────
    "blank_memo": QueryDef(
        name="blank_memo",
        description=(
            "Find all time entries where the memo field is NULL — "
            "these entries are missing a description and need validation"
        ),
        sql="""
            SELECT
                id,
                employee,
                date,
                hours,
                memo,
                project_number,
                project_name
            FROM public.fusion_time_entries
            WHERE memo IS NULL
            ORDER BY date DESC
        """,
        sample_questions=[
            "which entries have blank memos",
            "show me entries with no memo",
            "time entries missing memo",
            "find records with null memo",
            "who has empty memo fields",
            "entries without description",
            "missing memo entries",
            "which employees have no memo",
            "validate missing memos",
        ],
        returns_columns=["id", "employee", "date", "hours", "memo", "project_number", "project_name"],
    ),

    # ── Query 4: Last 7 Days ──────────────────────────────────
    "last_7_days": QueryDef(
        name="last_7_days",
        description="Retrieve all time entries submitted in the last 7 days",
        sql="""
            SELECT
                id,
                employee,
                date,
                hours,
                memo,
                project_number,
                project_name
            FROM public.fusion_time_entries
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY date DESC
        """,
        sample_questions=[
            "show me entries from the last 7 days",
            "recent time entries",
            "entries this week",
            "last week timesheets",
            "what was submitted recently",
            "entries from the past week",
            "show last 7 days of data",
            "recent submissions",
        ],
        returns_columns=["id", "employee", "date", "hours", "memo", "project_number", "project_name"],
    ),

    # ── Query 5: Memo Not Like ERP% ───────────────────────────
    "non_erp_memo": QueryDef(
        name="non_erp_memo",
        description=(
            "Find time entries where memo does NOT start with 'ERP' — "
            "used to identify entries that may not follow the standard "
            "ERP project code naming convention"
        ),
        sql="""
            SELECT
                id,
                employee,
                date,
                hours,
                memo,
                project_number,
                project_name
            FROM public.fusion_time_entries
            WHERE memo NOT LIKE 'ERP%'
            ORDER BY date DESC
        """,
        sample_questions=[
            "entries where memo does not start with ERP",
            "non ERP entries",
            "memos not following ERP format",
            "entries without ERP prefix",
            "which entries don't have ERP in memo",
            "find non standard memos",
            "memo not like ERP",
            "entries not tagged with ERP",
            "invalid memo format",
            "entries not following naming convention",
        ],
        returns_columns=["id", "employee", "date", "hours", "memo", "project_number", "project_name"],
    ),

}


# ─────────────────────────────────────────────────────────────
# Lookup helpers
# ─────────────────────────────────────────────────────────────

def get_query(name: str) -> QueryDef:
    """
    Look up a query by name.
    Raises KeyError with helpful message if not found.

    Usage:
        q = get_query("blank_memo")
        print(q.sql)
    """
    if name not in QUERIES:
        available = list(QUERIES.keys())
        raise KeyError(
            f"Query '{name}' not found. "
            f"Available queries: {available}"
        )
    return QUERIES[name]


def get_all_names() -> list[str]:
    """Return list of all query names."""
    return list(QUERIES.keys())


def get_query_catalog() -> list[dict]:
    """
    Return a summary of all queries — used by the agent to
    pick the right query from a natural language question.

    Returns:
        [
          {
            "name": "blank_memo",
            "description": "Find all time entries...",
            "sample_questions": [...]
          },
          ...
        ]
    """
    return [
        {
            "name":             q.name,
            "description":      q.description,
            "sample_questions": q.sample_questions,
            "returns_columns":  q.returns_columns,
        }
        for q in QUERIES.values()
    ]


def find_query_by_keyword(keyword: str) -> Optional[QueryDef]:
    """
    Simple keyword match against sample_questions.
    Used as a fallback before sending to the LLM for intent detection.

    Returns the first matching query or None.
    """
    keyword_lower = keyword.lower()
    for query in QUERIES.values():
        for question in query.sample_questions:
            if keyword_lower in question.lower():
                return query
    return None
