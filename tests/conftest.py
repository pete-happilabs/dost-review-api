import pytest
import pytest_asyncio
import asyncpg

from app.database import run_migrations, create_pool, close_pool


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pool():
    p = await create_pool("postgresql://dost:dost@localhost:5433/dost_reviews")
    await run_migrations(p)
    yield p
    await close_pool()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def cleanup(pool):
    yield
    await pool.execute("DELETE FROM review")
    await pool.execute("DELETE FROM profile_reputation")
    await pool.execute("DELETE FROM batch_run")
