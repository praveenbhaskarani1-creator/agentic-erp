"""
scripts/ts_agent.py
--------------------
Natural language -> SQL agent for timesheet validation data.

Uses intent detection + pre-approved SQL queries against ts_validation_results.
Optionally uses Groq free-tier (Llama 3.3 70B) for complex questions.
Falls back to keyword matching if no API key is configured — zero cost.

Tables available:
  ts_validation_results  - one row per timecard entry per run
  ts_validation_runs     - one row per validation run (metadata)
"""

import os
import re
from dataclasses import dataclass
from typing import Optional

# ── Schema context for LLM ────────────────────────────────────────────────────
SCHEMA_CONTEXT = """
Oracle Database tables for timesheet validation:

ts_validation_results columns:
  run_id           NUMBER          - links to ts_validation_runs.id
  row_num          NUMBER          - row number in source file
  employee_name    VARCHAR2(200)   - e.g. 'Smith, John'
  employee_number  VARCHAR2(50)    - e.g. '504655'
  project_number   VARCHAR2(100)   - e.g. 'YAKRM-123'
  project_name     VARCHAR2(500)   - full project name
  task_name        VARCHAR2(500)
  entry_date       DATE
  total_hours      NUMBER(6,2)
  memo             VARCHAR2(2000)  - timecard memo text
  has_error        CHAR(1)         - '1' = has error, '0' = clean
  correction_note  VARCHAR2(500)   - e.g. 'No memo', 'Need ticket #', 'Ticket is for Mount Nittany Medical Center'
  error_detail     VARCHAR2(500)   - technical detail
  extracted_ticket VARCHAR2(50)    - ticket found in memo e.g. 'YAKRM-219'
  suggested_ticket VARCHAR2(50)    - fuzzy match suggestion
  jira_oracle_project VARCHAR2(500)
  project_match    VARCHAR2(10)    - 'GOOD', 'BAD', 'SHARED', 'N/A'
  issue_type       VARCHAR2(100)
  jira_labels      VARCHAR2(500)
  timecard_period  VARCHAR2(50)

ts_validation_runs columns:
  id               NUMBER          - primary key
  run_at           TIMESTAMP       - when the run happened
  fusion_file      VARCHAR2(500)   - uploaded fusion filename
  total_errors     NUMBER
  total_clean      NUMBER
  fusion_rows_in   NUMBER

Common correction_note values:
  'No memo'
  'Need ticket #'
  'Remove spaces in ticket #'
  'Edit long dash to a short dash'
  'Add dash after ticket #'
  'Use only one ticket per entry'
  'Check ticket # - not found in Jira'
  'Ticket is for [client name]'  (project mismatch)

Rules:
- Use Oracle SQL syntax (FETCH FIRST N ROWS ONLY, not LIMIT N)
- Always filter by run_id when provided
- Return at most 200 rows
- Use UPPER() for case-insensitive string comparisons
"""

SQL_SYSTEM_PROMPT = f"""You are a SQL generator for an Oracle Database timesheet validation system.
Generate ONLY a single valid Oracle SQL SELECT statement. No explanation, no markdown, no semicolons.

{SCHEMA_CONTEXT}

Rules:
- Return ONLY the SQL query
- Always include ORDER BY
- Use FETCH FIRST 200 ROWS ONLY
- Filter by run_id = {{run_id}} when a run_id is provided
- Use single quotes for strings
- Never use LIMIT (Oracle uses FETCH FIRST)
"""


# ── Pre-approved keyword queries ──────────────────────────────────────────────
@dataclass
class QueryDef:
    name: str
    description: str
    sql_template: str
    keywords: list[str]


QUERIES = [
    QueryDef(
        name="no_memo",
        description="Entries with no memo (blank)",
        sql_template="""SELECT employee_name, project_number, project_name, entry_date, total_hours
FROM ts_validation_results
WHERE run_id = {run_id} AND correction_note = 'No memo'
ORDER BY employee_name, entry_date
FETCH FIRST 200 ROWS ONLY""",
        keywords=["no memo", "blank memo", "missing memo", "empty memo"],
    ),
    QueryDef(
        name="need_ticket",
        description="Entries missing a Jira ticket number",
        sql_template="""SELECT employee_name, project_number, entry_date, total_hours, memo
FROM ts_validation_results
WHERE run_id = {run_id} AND correction_note = 'Need ticket #'
ORDER BY employee_name, entry_date
FETCH FIRST 200 ROWS ONLY""",
        keywords=["need ticket", "missing ticket", "no ticket", "without ticket"],
    ),
    QueryDef(
        name="not_in_jira",
        description="Entries where ticket was not found in Jira",
        sql_template="""SELECT employee_name, project_number, entry_date, memo, extracted_ticket, suggested_ticket
FROM ts_validation_results
WHERE run_id = {run_id} AND correction_note LIKE 'Check ticket%'
ORDER BY extracted_ticket, employee_name
FETCH FIRST 200 ROWS ONLY""",
        keywords=["not found", "not in jira", "invalid ticket", "check ticket"],
    ),
    QueryDef(
        name="wrong_project",
        description="Entries where ticket belongs to a different client project",
        sql_template="""SELECT employee_name, project_number, entry_date, memo, extracted_ticket, correction_note
FROM ts_validation_results
WHERE run_id = {run_id} AND correction_note LIKE 'Ticket is for%'
ORDER BY correction_note, employee_name
FETCH FIRST 200 ROWS ONLY""",
        keywords=["wrong project", "wrong client", "ticket is for", "project mismatch", "bad project"],
    ),
    QueryDef(
        name="format_issues",
        description="Entries with memo format issues (spaces, dashes, multiple tickets)",
        sql_template="""SELECT employee_name, project_number, entry_date, memo, extracted_ticket, correction_note
FROM ts_validation_results
WHERE run_id = {run_id}
  AND correction_note IN ('Remove spaces in ticket #','Edit long dash to a short dash',
      'Add dash after ticket #','Use only one ticket per entry')
ORDER BY correction_note, employee_name
FETCH FIRST 200 ROWS ONLY""",
        keywords=["format", "spaces", "dash", "long dash", "multiple ticket", "format issue"],
    ),
    QueryDef(
        name="top_employees",
        description="Employees with the most errors",
        sql_template="""SELECT employee_name, COUNT(*) AS error_count
FROM ts_validation_results
WHERE run_id = {run_id} AND has_error = '1'
GROUP BY employee_name
ORDER BY error_count DESC
FETCH FIRST 20 ROWS ONLY""",
        keywords=["most errors", "top employee", "who has the most", "worst", "highest error"],
    ),
    QueryDef(
        name="summary_by_type",
        description="Error count grouped by correction type",
        sql_template="""SELECT correction_note, COUNT(*) AS cnt
FROM ts_validation_results
WHERE run_id = {run_id} AND has_error = '1'
GROUP BY correction_note
ORDER BY cnt DESC""",
        keywords=["summary", "breakdown", "by type", "error type", "categories", "group by"],
    ),
    QueryDef(
        name="clean_entries",
        description="Entries with no errors",
        sql_template="""SELECT employee_name, project_number, entry_date, total_hours, memo
FROM ts_validation_results
WHERE run_id = {run_id} AND has_error = '0'
ORDER BY employee_name, entry_date
FETCH FIRST 200 ROWS ONLY""",
        keywords=["clean", "no error", "correct", "valid", "good entries"],
    ),
    QueryDef(
        name="run_history",
        description="Previous validation runs",
        sql_template="""SELECT id, run_at, fusion_file, fusion_rows_in, total_errors, total_clean
FROM ts_validation_runs
ORDER BY run_at DESC
FETCH FIRST 10 ROWS ONLY""",
        keywords=["history", "previous run", "last run", "run history", "past"],
    ),
    QueryDef(
        name="all_errors",
        description="All rows with errors in this run",
        sql_template="""SELECT employee_name, project_number, entry_date, correction_note, memo, extracted_ticket
FROM ts_validation_results
WHERE run_id = {run_id} AND has_error = '1'
ORDER BY correction_note, employee_name
FETCH FIRST 200 ROWS ONLY""",
        keywords=["all errors", "all issues", "everything wrong", "show errors", "list errors"],
    ),
]


def keyword_match(question: str) -> Optional[QueryDef]:
    """Find best matching pre-approved query by keyword."""
    q_lower = question.lower()
    for qdef in QUERIES:
        if any(kw in q_lower for kw in qdef.keywords):
            return qdef
    return None


def extract_employee(question: str) -> Optional[str]:
    """Extract employee name from question like 'show errors for Smith'."""
    patterns = [
        r'for\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
        r'employee\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
        r'([A-Z][a-z]+(?:,\s*[A-Z][a-z]+)?)\s+errors',
    ]
    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def answer_with_keyword(question: str, run_id: int) -> tuple[str, str]:
    """
    Returns (sql, description) using keyword matching.
    Falls back to all_errors if nothing matches.
    """
    emp = extract_employee(question)
    qdef = keyword_match(question)

    if emp and qdef:
        sql = qdef.sql_template.format(run_id=run_id)
        emp_escaped = emp.replace("'", "''")
        if "WHERE" in sql:
            sql = sql.replace(
                "WHERE", f"WHERE UPPER(employee_name) LIKE UPPER('%{emp_escaped}%') AND"
            )
        return sql, f"{qdef.description} — filtered to employee containing '{emp}'"

    if emp:
        sql = f"""SELECT employee_name, project_number, entry_date, correction_note, memo, extracted_ticket
FROM ts_validation_results
WHERE run_id = {run_id}
  AND UPPER(employee_name) LIKE UPPER('%{emp.replace("'","''")}%')
  AND has_error = '1'
ORDER BY entry_date
FETCH FIRST 200 ROWS ONLY"""
        return sql, f"Errors for employee matching '{emp}'"

    if qdef:
        return qdef.sql_template.format(run_id=run_id), qdef.description

    fallback = QUERIES[-1]  # all_errors
    return fallback.sql_template.format(run_id=run_id), fallback.description


def answer_with_groq(question: str, run_id: int, api_key: str) -> tuple[str, str]:
    """Generate SQL using Groq free tier (Llama 3.3 70B)."""
    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        system = SQL_SYSTEM_PROMPT.replace("{run_id}", str(run_id))
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=512,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": question},
            ],
        )
        sql = resp.choices[0].message.content.strip()
        sql = re.sub(r"```sql\s*", "", sql)
        sql = re.sub(r"```\s*", "", sql)
        sql = sql.rstrip(";").strip()
        return sql, "Generated by Groq (Llama 3.3 70B)"
    except Exception:
        return answer_with_keyword(question, run_id)


def get_answer(question: str, run_id: int, groq_api_key: Optional[str] = None) -> tuple[str, str]:
    """
    Main entry point. Returns (sql, method_description).
    Uses Groq Llama 3.3 70B (free) if groq_api_key provided, else keyword matching.
    """
    if groq_api_key:
        return answer_with_groq(question, run_id, groq_api_key)
    return answer_with_keyword(question, run_id)
