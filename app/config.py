"""
CMS — Configuration
====================
All environment variables are declared here and loaded from a .env file.
Access settings anywhere via: from config import settings
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── PostgreSQL ────────────────────────────────────────────────────────
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "cms"

    # Optionally set the full DSN directly — overrides the individual fields above
    DATABASE_URL: str | None = None

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def assemble_db_url(cls, v: str | None, info) -> str:
        if v:
            # Normalise to asyncpg driver regardless of what was provided
            return (
                v.replace("postgres://", "postgresql+asyncpg://", 1)
                 .replace("postgresql://", "postgresql+asyncpg://", 1)
            )
        data = info.data
        return (
            f"postgresql+asyncpg://{data['POSTGRES_USER']}:{data['POSTGRES_PASSWORD']}"
            f"@{data['POSTGRES_HOST']}:{data['POSTGRES_PORT']}/{data['POSTGRES_DB']}"
        )

    # ── App ───────────────────────────────────────────────────────────────
    APP_ENV: str = "development"       # development | staging | production
    DEBUG: bool = True

    # ── Connection pool ───────────────────────────────────────────────────
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT: int = 30          # seconds before giving up on a connection
    DB_POOL_RECYCLE: int = 1800        # recycle connections every 30 minutes

    # ── CORS ──────────────────────────────────────────────────────────────
    CORS_ORIGINS: list[str] = ["*"]    # tighten for staging/production


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()