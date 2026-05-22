"""
scripts/rds_db.py
----------------
PostgreSQL database client for AWS RDS.
Replaces OrdsDB for timesheet validation result storage.

Usage:
    from scripts.rds_db import RdsDB

    db = RdsDB()  # Reads from app.config.settings
    rows = db.query("SELECT * FROM fusion_time_entries LIMIT 10")
    db.execute("INSERT INTO ts_validation_results (...) VALUES (...)")
"""

import os
import logging
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class RdsDB:
    """
    PostgreSQL database client for AWS RDS.
    Provides query() and execute() methods compatible with OrdsDB interface.
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        timeout: int = 30,
    ):
        self.host = host or os.getenv("DB_HOST", "agentic-erp-db.cwb0sigyk8na.us-east-1.rds.amazonaws.com")
        self.port = port or int(os.getenv("DB_PORT", "5432"))
        self.database = database or os.getenv("DB_NAME", "timecard_validation")
        self.user = user or os.getenv("DB_USER", "postgres")
        self.password = password or os.getenv("DB_PASSWORD", "")
        self.timeout = timeout
        self._conn = None

    def _get_connection(self):
        """Get or create database connection."""
        if self._conn is None or self._conn.closed:
            try:
                self._conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    connect_timeout=self.timeout,
                )
                logger.debug(f"[rds_db] Connected to {self.host}:{self.port}/{self.database}")
            except psycopg2.Error as e:
                logger.error(f"[rds_db] Connection failed: {e}")
                raise
        return self._conn

    def query(self, sql: str) -> list[dict]:
        """Execute a SELECT and return list of row dicts."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            return [dict(row) for row in rows]
        except psycopg2.Error as e:
            logger.error(f"[rds_db] Query failed: {e}\nSQL: {sql}")
            raise

    def execute(self, sql: str) -> int:
        """Execute a DML statement. Returns rows affected."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            affected = cursor.rowcount
            conn.commit()
            cursor.close()
            return affected
        except psycopg2.Error as e:
            logger.error(f"[rds_db] Execute failed: {e}\nSQL: {sql}")
            if self._conn:
                self._conn.rollback()
            raise

    def execute_many(self, statements: list[str]) -> list[int]:
        """Execute multiple SQL statements. Returns list of rows affected."""
        results = []
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            for sql in statements:
                cursor.execute(sql)
                results.append(cursor.rowcount)
            conn.commit()
            cursor.close()
            return results
        except psycopg2.Error as e:
            logger.error(f"[rds_db] Execute many failed: {e}")
            if self._conn:
                self._conn.rollback()
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in current database."""
        try:
            sql = f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                )
            """
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (table_name,))
            exists = cursor.fetchone()[0]
            cursor.close()
            return exists
        except psycopg2.Error as e:
            logger.error(f"[rds_db] Table exists check failed: {e}")
            return False

    def row_count(self, table_name: str) -> int:
        """Get row count from table."""
        try:
            rows = self.query(f"SELECT COUNT(*) as cnt FROM {table_name}")
            return rows[0]["cnt"] if rows else 0
        except psycopg2.Error as e:
            logger.error(f"[rds_db] Row count failed: {e}")
            return 0

    def health_check(self) -> dict:
        """Quick connectivity test."""
        try:
            rows = self.query("SELECT 1 as ok")
            return {"status": "ok", "db": "connected", "result": rows[0]["ok"] if rows else 0}
        except Exception as e:
            return {"status": "error", "db": str(e)}

    def close(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.debug("[rds_db] Connection closed")

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
