"""
app/models/response.py
──────────────────────
Pydantic models for API responses.
Ensures consistent shape for every endpoint.
"""

from pydantic import BaseModel
from typing import Optional, Any


class AskResponse(BaseModel):
    """Response for POST /ask"""
    answer:           str
    intent_detected:  Optional[str]   = None
    intent_source:    Optional[str]   = None
    row_count:        Optional[int]   = None
    query_name:       Optional[str]   = None
    validation_passed: Optional[bool] = None
    data:              Optional[list[dict]] = None
    error:            Optional[str]   = None


class HealthResponse(BaseModel):
    """Response for GET /health"""
    status:  str
    api:     str
    db:      str
    bedrock: str


class QueryInfo(BaseModel):
    """Single query definition for GET /queries"""
    name:             str
    description:      str
    sample_questions: list[str]
    returns_columns:  list[str]


class QueryListResponse(BaseModel):
    """Response for GET /queries"""
    queries: list[QueryInfo]
    total:   int


class PresignedUrlResponse(BaseModel):
    """Response for POST /upload/presigned-url"""
    upload_url:  str
    s3_key:      str
    result_key:  str
    expires_in:  int
    bucket:      str


class UploadStatusResponse(BaseModel):
    """Response for GET /upload/status"""
    ready:          bool
    status:         Optional[str]  = None
    rows_inserted:  Optional[int]  = None
    rows_skipped:   Optional[int]  = None
    errors:         Optional[list] = None
    processed_at:   Optional[str]  = None
    source_key:     Optional[str]  = None
