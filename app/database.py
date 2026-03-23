"""
CMS — Database
===============
Async SQLAlchemy engine + session factory wired to settings from config.py.
All ORM models are declared here so Base.metadata.create_all() picks them up.

Usage
-----
  # In a FastAPI route:
  from database import get_db
  async def my_route(db: AsyncSession = Depends(get_db)): ...

  # Anywhere else:
  from database import AsyncSessionFactory
  async with AsyncSessionFactory() as session: ...
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .config import settings


# ---------------------------------------------------------------------------
# Engine — all tunables come from Settings / .env
# ---------------------------------------------------------------------------

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,            # logs SQL in development
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_pre_ping=True,             # drop stale connections automatically
)

AsyncSessionFactory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# ---------------------------------------------------------------------------
# Declarative base — import Base in every model file
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Core analytics models
# (Surveillance models live in models/surveillance.py and import Base here)
# ---------------------------------------------------------------------------

class SearchEvent(Base):
    """
    Recorded every time a user submits a search query.
    """
    __tablename__ = "search_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    entity_type: Mapped[str | None] = mapped_column(
        String(50), nullable=True, index=True,
        comment="facility | medicine | disease | all"
    )
    results_returned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )


class SearchClick(Base):
    """
    Recorded when a user selects a specific result from the dropdown.
    """
    __tablename__ = "search_clicks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    search_event_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True,
    )
    query: Mapped[str] = mapped_column(String(500), nullable=False)
    result_label: Mapped[str] = mapped_column(String(500), nullable=False)
    result_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    result_subtype: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )


# ---------------------------------------------------------------------------
# Table creation helper — called once in main.py lifespan
# ---------------------------------------------------------------------------

async def create_tables() -> None:
    """Creates all tables that don't yet exist. Safe to call on every startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

async def get_db() -> AsyncSession:
    async with AsyncSessionFactory() as session:
        yield session