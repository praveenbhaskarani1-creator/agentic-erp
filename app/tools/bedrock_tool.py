"""
app/tools/bedrock_tool.py
─────────────────────────
Calls Claude on AWS Bedrock.

Auto-routing:
  Haiku  → intent detection (fast, cheap, simple classification)
  Sonnet → answer generation (quality, handles complex data)

No streaming — full response returned at once.

Usage:
    from app.tools.bedrock_tool import BedrockTool

    tool = BedrockTool()

    # Detect which query to run from a natural language question
    intent = tool.detect_intent("show me entries with blank memos")
    # → "blank_memo"

    # Answer a question using query results
    answer = tool.ask(
        data     = sql_tool.run("blank_memo"),
        question = "which employees have blank memos?"
    )
    # → "23 employees have missing memo entries: John Smith, Jane Doe..."
"""

import json
import logging
import os
from typing import Any

import boto3

from app.sql.queries import get_query_catalog

logger = logging.getLogger(__name__)

# ── Model IDs ─────────────────────────────────────────────────
MODEL_HAIKU  = os.getenv(
    "BEDROCK_MODEL_HAIKU",
    "anthropic.claude-3-5-haiku-20241022-v1:0"
)
MODEL_SONNET = os.getenv(
    "BEDROCK_MODEL_SONNET",
    "anthropic.claude-3-5-sonnet-20241022-v2:0"
)

# ── Safety limits ─────────────────────────────────────────────
MAX_ROWS_TO_SEND  = 200    # never send more than 200 rows to Claude
MAX_TOKENS_HAIKU  = 256    # intent detection needs very few tokens
MAX_TOKENS_SONNET = 1024   # answer generation needs more


class BedrockTool:
    """
    Wraps AWS Bedrock Claude calls for the LangGraph agent.

    Two methods:
      detect_intent() → Haiku  → which query to run
      ask()           → Sonnet → plain English answer from data
    """

    def __init__(self):
        self.client = boto3.client(
            "bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

    # ─────────────────────────────────────────────────────────
    # Public methods
    # ─────────────────────────────────────────────────────────

    def detect_intent(self, question: str) -> str:
        """
        Classify a natural language question into one of the
        5 pre-approved query names.

        Uses Haiku — fast and cheap for simple classification.

        Args:
            question: user's natural language question

        Returns:
            query name string e.g. "blank_memo"
            or "unknown" if question is out of scope
        """
        logger.info(f"[bedrock] detect_intent: {question[:80]}")

        catalog   = get_query_catalog()
        query_names = [q["name"] for q in catalog]

        # Build catalog description for the prompt
        catalog_text = "\n".join([
            f'  - "{q["name"]}": {q["description"]}'
            for q in catalog
        ])

        system_prompt = (
            "You are an intent classifier for a timesheet validation system. "
            "Your only job is to map a user question to one of the available query names. "
            "Respond with ONLY the query name — no explanation, no punctuation, nothing else. "
            f"If the question is out of scope, respond with exactly: unknown"
        )

        user_message = (
            f"Available queries:\n{catalog_text}\n\n"
            f"User question: {question}\n\n"
            f"Which query name best matches? "
            f"Choose from: {query_names} or 'unknown'."
        )

        response = self._call_claude(
            system_prompt = system_prompt,
            user_message  = user_message,
            model         = MODEL_HAIKU,
            max_tokens    = MAX_TOKENS_HAIKU,
        )

        if response["status"] == "error":
            logger.warning(f"[bedrock] detect_intent failed: {response['message']}")
            return "unknown"

        # Clean and validate the response
        intent = response["text"].strip().lower().strip('"').strip("'")

        if intent not in query_names and intent != "unknown":
            logger.warning(f"[bedrock] Unexpected intent: '{intent}' — defaulting to unknown")
            return "unknown"

        logger.info(f"[bedrock] Intent detected: {intent}")
        return intent

    def ask(self, data: dict, question: str) -> str:
        """
        Generate a plain English answer from query results.

        Uses Sonnet — better quality for answer generation.

        Args:
            data:     result dict from sql_tool.run()
            question: user's original question

        Returns:
            Plain English answer string.
            Never returns empty string — always has a message.
        """
        logger.info(
            f"[bedrock] ask: '{question[:60]}' "
            f"| query={data.get('query_name')} "
            f"| rows={data.get('row_count', 0)}"
        )

        # ── Handle empty / error results ──────────────────────
        if data.get("status") == "error":
            return f"I was unable to retrieve data: {data.get('message', 'unknown error')}"

        if data.get("status") == "empty" or data.get("row_count", 0) == 0:
            return (
                f"No records were found for your query. "
                f"Query: {data.get('description', data.get('query_name', 'unknown'))}"
            )

        # ── Format rows for Claude ────────────────────────────
        rows       = data.get("rows", [])
        total_rows = data.get("row_count", len(rows))
        truncated  = len(rows) > MAX_ROWS_TO_SEND
        rows_to_send = rows[:MAX_ROWS_TO_SEND]

        rows_text = self._format_rows(rows_to_send)

        truncation_note = (
            f"\n[Note: Showing first {MAX_ROWS_TO_SEND} of {total_rows} total rows]"
            if truncated else ""
        )

        system_prompt = (
            "You are a timesheet validation assistant for an Oracle Fusion ERP system. "
            "Answer questions based ONLY on the data provided — never invent employees, "
            "dates, or values not present in the data. "
            "Be concise, professional, and specific. "
            "When listing employees or entries, use bullet points for clarity. "
            "Always state the total count when relevant."
        )

        user_message = (
            f"Query: {data.get('description', '')}\n"
            f"Total records: {total_rows}{truncation_note}\n\n"
            f"Data:\n{rows_text}\n\n"
            f"Question: {question}"
        )

        response = self._call_claude(
            system_prompt = system_prompt,
            user_message  = user_message,
            model         = MODEL_SONNET,
            max_tokens    = MAX_TOKENS_SONNET,
        )

        if response["status"] == "error":
            logger.warning(f"[bedrock] ask failed: {response['message']}")
            return f"I encountered an error generating the answer: {response['message']}"

        return response["text"].strip()

    def generate_sql(self, question: str) -> dict:
        """
        Ask Claude Sonnet to write a safe SELECT query for the given question.

        Schema provided:
            Table : public.fusion_time_entries
            Cols  : id (integer), employee (text), date (date),
                    hours (numeric), memo (text nullable)

        Returns:
            {"status": "success", "sql": "SELECT ..."}
            {"status": "error",   "message": "..."}
        """
        logger.info(f"[bedrock] generate_sql: {question[:80]}")

        system_prompt = (
            "You are a read-only PostgreSQL query assistant. "
            "Your ONLY job is to write safe SELECT queries. "
            "You are strictly forbidden from modifying data in any way.\n\n"
            "Table: public.fusion_time_entries\n"
            "Columns:\n"
            "  id             INTEGER  — unique row identifier\n"
            "  employee       TEXT     — employee full name\n"
            "  date           DATE     — timesheet date (YYYY-MM-DD)\n"
            "  hours          NUMERIC  — hours worked per entry (0–24)\n"
            "  memo           TEXT     — timesheet memo, can be NULL\n"
            "  project_number TEXT     — project number (e.g. PROJ-1001), can be NULL\n"
            "  project_name   TEXT     — project name (e.g. Oracle Fusion ERP), can be NULL\n\n"
            "STRICT RULES — violation will cause the query to be rejected:\n"
            "  1. ONLY SELECT statements are allowed. Never write INSERT, UPDATE, DELETE, "
            "DROP, ALTER, TRUNCATE, CREATE, REPLACE, MERGE, GRANT, REVOKE, EXEC, or CALL.\n"
            "  2. Never use semicolons in the query.\n"
            "  3. Never include SQL comments (-- or /* */).\n"
            "  4. Always use public.fusion_time_entries as the table name.\n"
            "  5. Always include LIMIT 200 to prevent large result sets.\n"
            "  6. Return ONLY the raw SQL — no markdown, no explanation, no backticks.\n"
            "  7. If the question cannot be answered from this table, respond with exactly: CANNOT_ANSWER\n"
            "  8. If the question asks you to insert, update, delete or modify data, "
            "respond with exactly: CANNOT_ANSWER"
        )

        user_message = f"Question: {question}"

        response = self._call_claude(
            system_prompt = system_prompt,
            user_message  = user_message,
            model         = MODEL_SONNET,
            max_tokens    = 512,
        )

        if response["status"] == "error":
            return {"status": "error", "message": response["message"]}

        sql = response["text"].strip()

        if sql == "CANNOT_ANSWER":
            return {"status": "error", "message": "Question cannot be answered from available data"}

        # Strip accidental markdown code fences
        if sql.startswith("```"):
            sql = "\n".join(
                line for line in sql.splitlines()
                if not line.strip().startswith("```")
            ).strip()

        return {"status": "success", "sql": sql}

    def health_check(self) -> dict:
        """
        Quick Bedrock connectivity test.
        Calls Haiku with a minimal prompt.
        """
        response = self._call_claude(
            system_prompt = "Reply with exactly: ok",
            user_message  = "ping",
            model         = MODEL_HAIKU,
            max_tokens    = 10,
        )
        if response["status"] == "success":
            return {"status": "ok", "bedrock": "connected", "model": MODEL_HAIKU}
        return {"status": "error", "bedrock": response["message"]}

    # ─────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────

    def _call_claude(
        self,
        system_prompt: str,
        user_message:  str,
        model:         str,
        max_tokens:    int,
    ) -> dict:
        """
        Single Bedrock API call — handles errors, returns structured dict.

        Returns:
            {"status": "success", "text": "...", "model": "..."}
            {"status": "error",   "message": "...", "model": "..."}
        """
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens":        max_tokens,
            "temperature":       0.0,        # deterministic
            "system":            system_prompt,
            "messages": [
                {"role": "user", "content": user_message}
            ],
        }

        try:
            response = self.client.invoke_model(
                modelId     = model,
                body        = json.dumps(body),
                contentType = "application/json",
                accept      = "application/json",
            )

            result = json.loads(response["body"].read())
            text   = result["content"][0]["text"]

            logger.debug(f"[bedrock] {model} responded ({len(text)} chars)")
            return {"status": "success", "text": text, "model": model}

        except Exception as e:
            # Catches both botocore.exceptions.ClientError and any other error
            error_msg = str(e)
            # Extract clean message for ClientError format
            if hasattr(e, "response"):
                error_code = e.response["Error"]["Code"]
                error_msg  = f"{error_code}: {e.response['Error']['Message']}"
            logger.error(f"[bedrock] Error calling {model}: {error_msg}")
            return {
                "status":  "error",
                "message": error_msg,
                "model":   model,
            }

    def _format_rows(self, rows: list[dict]) -> str:
        """
        Format rows as readable text for Claude.
        Keeps it compact — one row per line.

        Example output:
            Row 1: id=5, employee=John Smith, date=2026-01-15, hours=8.0, memo=None
            Row 2: id=12, employee=Jane Doe, date=2026-01-16, hours=7.5, memo=None
        """
        if not rows:
            return "No rows."

        lines = []
        for i, row in enumerate(rows, start=1):
            parts = ", ".join(f"{k}={v}" for k, v in row.items())
            lines.append(f"Row {i}: {parts}")

        return "\n".join(lines)
