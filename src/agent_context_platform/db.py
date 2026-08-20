from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from agent_context_platform.settings import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    """Build the lazy PostgreSQL engine from explicitly supplied settings."""
    secret_dsn = settings.postgresql.dsn
    if secret_dsn is None:
        raise ValueError("postgresql.dsn is required to create the database engine")

    return create_async_engine(
        secret_dsn.get_secret_value().unicode_string(),
        pool_pre_ping=True,
    )


def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build an async session factory bound to the caller-owned engine."""
    return async_sessionmaker(engine, expire_on_commit=False)
