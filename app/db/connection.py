"""
app/db/connection.py
────────────────────
SQLAlchemy connection pool for RDS PostgreSQL.

- Local dev  : connects via SSH tunnel → localhost:5433
- Production : connects directly to RDS private endpoint

Usage:
    from app.db.connection import get_db, engine

    # In a function:
    with get_db() as conn:
        rows = conn.execute(text("SELECT * FROM fusion_time_entries")).fetchall()
"""

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, SQLAlchemyError

logger = logging.getLogger(__name__)


def _build_engine(
    db_url: str,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
):
    """
    Create a SQLAlchemy engine with connection pooling.

    pool_size    = number of persistent connections kept open
    max_overflow = extra connections allowed under heavy load
    pool_timeout = seconds to wait for a connection before error
    pool_recycle = recycle connections after 30 min (avoids RDS idle timeout)
    """
    engine = create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=30,
        pool_recycle=1800,          # recycle every 30 minutes
        pool_pre_ping=True,         # test connection before using it
        echo=echo,                  # set True to log all SQL (dev only)
    )

    # Log every new connection opened (useful for debugging tunnel issues)
    @event.listens_for(engine, "connect")
    def on_connect(dbapi_conn, connection_record):
        logger.debug("[db] New connection opened to RDS")

    @event.listens_for(engine, "checkout")
    def on_checkout(dbapi_conn, connection_record, connection_proxy):
        logger.debug("[db] Connection checked out from pool")

    return engine


class DatabaseManager:
    """
    Singleton database manager.
    Holds the engine + session factory.
    Call init() once at app startup (in main.py lifespan).
    """

    _engine = None
    _session_factory = None

    @classmethod
    def init(cls, db_url: str, pool_size: int = 5, max_overflow: int = 10, echo: bool = False):
        """
        Initialise the connection pool.
        Call once at startup — idempotent.
        """
        if cls._engine is not None:
            logger.info("[db] Engine already initialised — skipping")
            return

        logger.info(f"[db] Initialising connection pool → {db_url.split('@')[-1]}")
        cls._engine = _build_engine(db_url, pool_size, max_overflow, echo)
        cls._session_factory = sessionmaker(bind=cls._engine, autocommit=False, autoflush=False)
        logger.info("[db] Connection pool ready ✅")

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            raise RuntimeError("[db] DatabaseManager not initialised. Call init() first.")
        return cls._engine

    @classmethod
    def get_session_factory(cls):
        if cls._session_factory is None:
            raise RuntimeError("[db] DatabaseManager not initialised. Call init() first.")
        return cls._session_factory

    @classmethod
    def health_check(cls) -> dict:
        """
        Quick connectivity test — used by /health endpoint.
        Returns {"status": "ok", "db": "connected"} or raises.
        """
        try:
            with cls.get_engine().connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                assert result == 1
            return {"status": "ok", "db": "connected"}
        except Exception as e:
            logger.error(f"[db] Health check failed: {e}")
            return {"status": "error", "db": str(e)}

    @classmethod
    def close(cls):
        """Dispose pool — call on app shutdown."""
        if cls._engine:
            cls._engine.dispose()
            cls._engine = None
            cls._session_factory = None
            logger.info("[db] Connection pool closed")


# ─────────────────────────────────────────────────────────────
# Context manager — use this in tools and agents
# ─────────────────────────────────────────────────────────────

@contextmanager
def get_db() -> Generator[Session, None, None]:
    """
    Yields a SQLAlchemy Session.
    Commits on success, rolls back on error, always closes.

    Usage:
        with get_db() as db:
            result = db.execute(text("SELECT 1")).fetchone()
    """
    session: Session = DatabaseManager.get_session_factory()()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"[db] Query error — rolled back: {e}")
        raise
    finally:
        session.close()


# ─────────────────────────────────────────────────────────────
# FastAPI dependency — use with Depends()
# ─────────────────────────────────────────────────────────────

def get_db_session() -> Generator[Session, None, None]:
    """
    FastAPI dependency for injecting DB session into routes.

    Usage in a route:
        @router.get("/query")
        def query(db: Session = Depends(get_db_session)):
            ...
    """
    session: Session = DatabaseManager.get_session_factory()()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        raise
    finally:
        session.close()
