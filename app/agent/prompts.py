"""
app/agent/prompts.py
────────────────────
All system prompts used by the agent.

Centralised here so prompts are easy to find, update, and version.
Never scatter prompt strings across nodes or tools.
"""

from app.sql.queries import get_query_catalog


# ─────────────────────────────────────────────────────────────
# Intent detection prompt (Haiku)
# ─────────────────────────────────────────────────────────────

def intent_system_prompt() -> str:
    return (
        "You are an intent classifier for a timesheet validation system. "
        "Your only job is to map a user question to one query name. "
        "Respond with ONLY the query name — no explanation, no punctuation. "
        "If out of scope, respond with exactly: unknown"
    )


def intent_user_prompt(question: str) -> str:
    catalog = get_query_catalog()
    catalog_text = "\n".join([
        f'  - "{q["name"]}": {q["description"]}'
        for q in catalog
    ])
    query_names = [q["name"] for q in catalog]
    return (
        f"Available queries:\n{catalog_text}\n\n"
        f"User question: {question}\n\n"
        f"Choose from: {query_names} or 'unknown'."
    )


# ─────────────────────────────────────────────────────────────
# Answer generation prompt (Sonnet)
# ─────────────────────────────────────────────────────────────

ANSWER_SYSTEM_PROMPT = (
    "You are a timesheet validation assistant for an Oracle Fusion ERP system. "
    "Answer questions based ONLY on the data provided — never invent employees, "
    "dates, or values not in the data. "
    "Be concise and professional. "
    "Use bullet points when listing multiple employees or entries. "
    "Always mention the total count when relevant."
)


def answer_user_prompt(data: dict, question: str) -> str:
    rows       = data.get("rows", [])
    total_rows = data.get("row_count", len(rows))
    description = data.get("description", "")

    # Format rows compactly
    rows_text = "\n".join([
        f"Row {i}: " + ", ".join(f"{k}={v}" for k, v in row.items())
        for i, row in enumerate(rows[:200], start=1)
    ])

    truncation = (
        f"\n[Showing first 200 of {total_rows} rows]"
        if len(rows) > 200 else ""
    )

    return (
        f"Query: {description}\n"
        f"Total records: {total_rows}{truncation}\n\n"
        f"Data:\n{rows_text}\n\n"
        f"Question: {question}"
    )


# ─────────────────────────────────────────────────────────────
# Clarification prompt — when intent is unknown
# ─────────────────────────────────────────────────────────────

def clarification_message() -> str:
    catalog = get_query_catalog()
    options = "\n".join([
        f"  • {q['description']}"
        for q in catalog
    ])
    return (
        "I can answer questions about your timesheet data. "
        "Here is what I can help with:\n\n"
        f"{options}\n\n"
        "Could you rephrase your question to match one of these?"
    )
