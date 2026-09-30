from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from config import DATABASE_URL

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=10, max_overflow=0)
Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass

async def get_session():
    """FastAPI dependency: one session per request."""
    async with Session() as session:
        yield session


async def init_db():
    # ponytail: create_all is fine while the schema churns every phase.
    # Switch to Alembic the first time you need to keep existing rows.
    import models  # noqa: F401  -- registers every model on Base.metadata

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
