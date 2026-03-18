"""
app/tools/s3_tool.py
────────────────────
S3 operations for the file upload pipeline.

Responsibilities:
  - Generate presigned PUT URLs for direct browser → S3 uploads
  - Read processing results written by Lambda
  - List pending / processed uploads

Buckets used:
  uploads/pending/{uuid}_{filename}   ← team member uploads here
  uploads/results/{key}.json          ← Lambda writes result here
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

BUCKET          = os.getenv("S3_ARTIFACTS_BUCKET", "agentic-erp-artifacts-241030170015")
PENDING_PREFIX  = "uploads/pending/"
RESULTS_PREFIX  = "uploads/results/"
URL_EXPIRES_SEC = 900   # 15 minutes


class S3Tool:
    """Handles presigned URL generation and result retrieval."""

    def __init__(self):
        self.s3     = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
        self.bucket = BUCKET

    # ─────────────────────────────────────────────────────────
    # Presigned URL
    # ─────────────────────────────────────────────────────────

    def generate_presigned_upload(self, filename: str, content_type: str) -> dict:
        """
        Generate a presigned PUT URL for direct browser → S3 upload.

        Args:
            filename     : original filename, e.g. "timesheets_march.csv"
            content_type : MIME type, e.g. "text/csv" or
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        Returns:
            {
              "upload_url" : "https://s3.amazonaws.com/...",
              "s3_key"     : "uploads/pending/abc123_timesheets_march.csv",
              "result_key" : "uploads/results/abc123_timesheets_march.csv.json",
              "expires_in" : 900
            }
        """
        # Sanitise filename — keep only safe chars
        safe_name = "".join(c for c in filename if c.isalnum() or c in "._-")
        uid       = uuid.uuid4().hex[:8]
        s3_key    = f"{PENDING_PREFIX}{uid}_{safe_name}"
        result_key = f"{RESULTS_PREFIX}{uid}_{safe_name}.json"

        logger.info(f"[s3_tool] Generating presigned URL for key={s3_key}")

        try:
            upload_url = self.s3.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket":      self.bucket,
                    "Key":         s3_key,
                    "ContentType": content_type,
                },
                ExpiresIn=URL_EXPIRES_SEC,
            )
            return {
                "upload_url" : upload_url,
                "s3_key"     : s3_key,
                "result_key" : result_key,
                "expires_in" : URL_EXPIRES_SEC,
                "bucket"     : self.bucket,
            }
        except ClientError as e:
            logger.error(f"[s3_tool] Failed to generate presigned URL: {e}")
            raise

    # ─────────────────────────────────────────────────────────
    # Result polling
    # ─────────────────────────────────────────────────────────

    def get_upload_result(self, result_key: str) -> Optional[dict]:
        """
        Read the processing result JSON written by Lambda.

        Returns None if Lambda hasn't finished yet (key doesn't exist).

        Result JSON shape (written by Lambda):
        {
          "status"         : "success" | "error",
          "rows_inserted"  : 42,
          "rows_skipped"   : 3,
          "errors"         : ["row 5: hours > 24", ...],
          "processed_at"   : "2026-03-18T10:30:00Z",
          "source_key"     : "uploads/pending/abc123_file.csv"
        }
        """
        try:
            obj  = self.s3.get_object(Bucket=self.bucket, Key=result_key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            logger.info(f"[s3_tool] Result found: key={result_key} status={data.get('status')}")
            return data
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                logger.info(f"[s3_tool] Result not yet available: {result_key}")
                return None
            logger.error(f"[s3_tool] Error reading result: {e}")
            raise
