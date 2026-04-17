"""
scripts/oci_db.py
-----------------
Thin ORDS REST client for Oracle Autonomous Database.
Uses HTTPS port 443 — no wallet, no port 1522, no Oracle Client needed.

Usage:
    from scripts.oci_db import OrdsDB

    db = OrdsDB()
    rows = db.query("SELECT table_name FROM user_tables")
    db.execute("INSERT INTO upload_log (table_name) VALUES ('test')")
"""

import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

ORDS_BASE = "https://g1422d2c72c1fe6-agenticerp.adb.us-chicago-1.oraclecloudapps.com/ords"


class OrdsDB:
    """
    Executes SQL against OCI Autonomous Database via ORDS REST API.
    No wallet or Oracle Client required — plain HTTPS.
    """

    def __init__(
        self,
        schema: str = None,
        user: str = None,
        password: str = None,
        base_url: str = ORDS_BASE,
        timeout: int = 30,
    ):
        self.schema   = schema or os.getenv("OCI_DB_USER", "ADMIN").lower()
        self.user     = user   or os.getenv("OCI_DB_USER", "ADMIN")
        self.password = password or os.getenv("OCI_DB_PASSWORD")
        self.base_url = base_url
        self.timeout  = timeout
        self._auth    = HTTPBasicAuth(self.user, self.password)
        self._sql_url = f"{self.base_url}/{self.schema}/_/sql"

    def _post(self, sql: str) -> dict:
        resp = requests.post(
            self._sql_url,
            auth=self._auth,
            headers={"Content-Type": "application/sql"},
            data=sql.encode("utf-8"),
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def query(self, sql: str) -> list[dict]:
        """Execute a SELECT and return list of row dicts."""
        result = self._post(sql)
        rows = []
        for item in result.get("items", []):
            rs = item.get("resultSet", {})
            rows.extend(rs.get("items", []))
        return rows

    def execute(self, sql: str) -> int:
        """Execute a DML statement. Returns rows affected."""
        result = self._post(sql)
        affected = 0
        for item in result.get("items", []):
            affected += item.get("result", 0)
        return affected

    def execute_many(self, statements: list[str]) -> list[int]:
        """Execute multiple SQL statements in one request."""
        combined = ";\n".join(statements)
        result = self._post(combined)
        return [item.get("result", 0) for item in result.get("items", [])]

    def table_exists(self, table_name: str) -> bool:
        rows = self.query(
            f"SELECT COUNT(*) AS cnt FROM user_tables "
            f"WHERE table_name = '{table_name.upper()}'"
        )
        return rows[0]["cnt"] > 0 if rows else False

    def row_count(self, table_name: str) -> int:
        rows = self.query(f"SELECT COUNT(*) AS cnt FROM {table_name.upper()}")
        return rows[0]["cnt"] if rows else 0

    def health_check(self) -> dict:
        try:
            rows = self.query("SELECT 1 AS ok FROM DUAL")
            return {"status": "ok", "db": "connected", "result": rows[0]["ok"]}
        except Exception as e:
            return {"status": "error", "db": str(e)}
