"""
app/main.py
───────────
FastAPI application — the backend API for Agentic Time Entry Validation.

Endpoints:
    POST /ask          → run the agent, get a plain English answer
    GET  /export/excel → download all query results as Excel
    GET  /health       → system health check (DB + Bedrock)
    GET  /queries      → list all available queries
    GET  /             → welcome message

Run locally (with SSH tunnel open):
    uvicorn app.main:app --reload --port 8000

Then test:
    curl -X POST http://localhost:8000/ask \
         -H "Content-Type: application/json" \
         -d '{"question": "show me entries with blank memos"}'
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv

from app.models.request  import AskRequest
from app.models.response import (
    AskResponse, HealthResponse, QueryListResponse, QueryInfo,
    PresignedUrlResponse, UploadStatusResponse,
)
from app.db.connection   import DatabaseManager
from app.agent.graph     import run_agent
from app.tools.bedrock_tool import BedrockTool
from app.tools.export_tool  import ExportTool
from app.tools.s3_tool      import S3Tool
from app.sql.queries     import get_query_catalog

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ─────────────────────────────────────────────────────────────
# App lifecycle — init DB pool on startup, close on shutdown
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Runs on startup and shutdown."""

    # ── Startup ───────────────────────────────────────────────
    logger.info("[main] Starting Agentic Time Entry Validation API")

    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER', 'pgadmin')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5433')}/"
        f"{os.getenv('DB_NAME', 'agentdb')}"
    )
    DatabaseManager.init(db_url=db_url, pool_size=5, echo=False)
    logger.info("[main] DB pool ready ✅")

    yield  # app runs here

    # ── Shutdown ──────────────────────────────────────────────
    DatabaseManager.close()
    logger.info("[main] DB pool closed")


# ─────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Agentic Time Entry Validation",
    description = "AI-powered Oracle Fusion timesheet validation using LangGraph + Bedrock",
    version     = "1.0.0",
    lifespan    = lifespan,
)

# Allow Streamlit frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ─────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/", tags=["General"])
def root():
    """Welcome message."""
    return {
        "name":    "Agentic Time Entry Validation",
        "version": "1.0.0",
        "status":  "running",
        "docs":    "/docs",
    }


@app.post("/ask", response_model=AskResponse, tags=["Agent"])
def ask(request: AskRequest):
    """
    Ask a natural language question about timesheet data.

    The agent will:
    1. Detect which query to run from your question
    2. Execute the query against Oracle Fusion data in RDS
    3. Return a plain English answer

    Example questions:
    - "show me entries with blank memos"
    - "how many time entries are there?"
    - "show entries from the last 7 days"
    - "find entries not following ERP naming convention"
    """
    logger.info(f"[/ask] question='{request.question}' user={request.user_id}")

    try:
        result = run_agent(
            question = request.question,
            user_id  = request.user_id or "anonymous",
        )

        return AskResponse(
            answer            = result.get("final_answer", "No answer generated"),
            intent_detected   = result.get("intent_detected"),
            intent_source     = result.get("intent_source"),
            row_count         = result.get("sql_result", {}).get("row_count") if result.get("sql_result") else None,
            query_name        = result.get("intent_detected"),
            validation_passed = result.get("validation_passed"),
            data              = result.get("sql_result", {}).get("rows") if result.get("sql_result") else None,
            error             = result.get("error_message"),
        )

    except Exception as e:
        logger.error(f"[/ask] Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/export/excel", tags=["Export"])
def export_excel():
    """
    Export all query results to Excel.

    Returns a .xlsx file with 6 sheets:
    - Summary (row counts for all queries)
    - All Entries
    - Blank Memo
    - Last 7 Days
    - Non ERP Memo
    - Total Count
    """
    logger.info("[/export/excel] Generating Excel report")

    try:
        export_tool         = ExportTool()
        content, filename   = export_tool.export_to_bytes()

        return StreamingResponse(
            iter([content]),
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers    = {
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length":      str(len(content)),
            },
        )

    except Exception as e:
        logger.error(f"[/export/excel] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", response_model=HealthResponse, tags=["General"])
def health():
    """
    Check health of all system components.

    Returns status of:
    - API itself
    - RDS PostgreSQL connection
    - AWS Bedrock connection
    """
    logger.info("[/health] Health check")

    # Check DB
    db_health      = DatabaseManager.health_check()
    db_status      = db_health.get("db", "unknown")

    # Check Bedrock
    bedrock_tool   = BedrockTool()
    bedrock_health = bedrock_tool.health_check()
    bedrock_status = bedrock_health.get("bedrock", "unknown")

    overall = "ok" if db_status == "connected" and bedrock_status == "connected" else "degraded"

    return HealthResponse(
        status  = overall,
        api     = "ok",
        db      = db_status,
        bedrock = bedrock_status,
    )


@app.post("/upload/presigned-url", response_model=PresignedUrlResponse, tags=["Upload"])
def get_presigned_url(filename: str, content_type: str = "text/csv"):
    """
    Generate a presigned S3 PUT URL for direct browser → S3 upload.

    Steps:
      1. Call this endpoint to get upload_url + s3_key + result_key
      2. PUT your CSV/Excel file directly to upload_url (no auth header needed)
      3. Poll GET /upload/status?result_key=... until ready=true

    Supported content types:
      - text/csv
      - application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
    """
    logger.info(f"[/upload/presigned-url] filename={filename} content_type={content_type}")
    try:
        s3_tool = S3Tool()
        result  = s3_tool.generate_presigned_upload(filename, content_type)
        return PresignedUrlResponse(**result)
    except Exception as e:
        logger.error(f"[/upload/presigned-url] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/upload/status", response_model=UploadStatusResponse, tags=["Upload"])
def get_upload_status(result_key: str):
    """
    Poll for the result of an upload processed by Lambda.

    Returns ready=false while Lambda is still processing.
    Returns ready=true with rows_inserted / errors once complete.
    """
    logger.info(f"[/upload/status] result_key={result_key}")
    try:
        s3_tool = S3Tool()
        result  = s3_tool.get_upload_result(result_key)
        if result is None:
            return UploadStatusResponse(ready=False)
        return UploadStatusResponse(ready=True, **result)
    except Exception as e:
        logger.error(f"[/upload/status] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/queries", response_model=QueryListResponse, tags=["Agent"])
def list_queries():
    """
    List all available queries the agent can run.

    Use this to understand what questions the agent can answer.
    """
    catalog = get_query_catalog()
    queries = [
        QueryInfo(
            name             = q["name"],
            description      = q["description"],
            sample_questions = q["sample_questions"],
            returns_columns  = q["returns_columns"],
        )
        for q in catalog
    ]
    return QueryListResponse(queries=queries, total=len(queries))
