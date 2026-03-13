import os
import contextvars
import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

_raw_url = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://adscore:adscore_local@localhost:5432/adscore",
)
# Railway gives postgresql:// but asyncpg needs postgresql+asyncpg://
if _raw_url.startswith("postgresql://"):
    DATABASE_URL = _raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
elif _raw_url.startswith("postgres://"):
    DATABASE_URL = _raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
else:
    DATABASE_URL = _raw_url

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=10, max_overflow=20)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ── Tenant isolation helpers ──────────────────────────────────────

tenant_context: contextvars.ContextVar = contextvars.ContextVar(
    "tenant_id", default=None
)


def tenant_query(model, tenant_id=None):
    """Build a SELECT query scoped to the given tenant (or context tenant).

    Usage:
        q = tenant_query(ScoringSession)  # uses tenant from context
        q = tenant_query(ScoringSession, some_uuid)  # explicit tenant
    """
    tid = tenant_id or tenant_context.get()
    if tid is None:
        raise ValueError("No tenant_id in context. Set it via tenant_context.set()")
    return select(model).where(model.tenant_id == tid)
