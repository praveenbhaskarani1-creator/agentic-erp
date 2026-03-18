"""
app/models/request.py
─────────────────────
Pydantic models for incoming API requests.
FastAPI validates these automatically before the route runs.
"""

from pydantic import BaseModel, Field
from typing import Optional


class AskRequest(BaseModel):
    """Body for POST /ask"""
    question: str = Field(
        ...,
        min_length=3,
        max_length=500,
        description="Natural language question about timesheet data",
        examples=["show me entries with blank memos"],
    )
    user_id: Optional[str] = Field(
        default="anonymous",
        description="User identifier — used for RBAC in future phases",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "question": "show me entries with blank memos",
                    "user_id":  "praveen.b",
                }
            ]
        }
    }
