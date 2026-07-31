import asyncio

import pytest
import pytest_asyncio
import asyncpg

from app.database import run_migrations, create_pool, get_pool, close_pool


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def pool():
    p = await create_pool("postgresql://dost:dost@localhost:5433/dost_reviews")
    await run_migrations(p)
    yield p
    await close_pool()


@pytest.fixture(autouse=True)
async def cleanup(pool):
    yield
    await pool.execute("DELETE FROM review")
    await pool.execute("DELETE FROM profile_reputation")
    await pool.execute("DELETE FROM batch_run")
