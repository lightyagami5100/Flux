"""Async SQLAlchemy engine/session infrastructure (supports PostgreSQL & SQLite auto-fallback)."""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import get_settings

logger = logging.getLogger("db")
settings = get_settings()

if "sqlite" in settings.database_url:
    engine = create_async_engine(
        settings.database_url,
        echo=False,
    )
else:
    engine = create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
    )

SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a scoped session."""
    async with SessionLocal() as session:
        yield session
