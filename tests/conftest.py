import pytest
import pytest_asyncio

from app.database import (
    close_onboard_pool,
    close_pool,
    create_onboard_pool,
    create_pool,
    run_migrations,
)

DSN = "postgresql://dost:dost@localhost:5433/dost_reviews"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pool():
    p = await create_pool(DSN)
    await run_migrations(p)
    yield p
    await close_pool()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def cleanup(pool):
    yield
    await pool.execute("DELETE FROM review")
    await pool.execute("DELETE FROM profile_reputation")
    await pool.execute("DELETE FROM batch_run")


@pytest_asyncio.fixture(loop_scope="session")
async def onboard(pool):
    """Opt-in: stands up a stand-in for Onboard's `profile` table and points the
    onboard pool at it, enabling profile validation for the tests that request it.

    Deliberately NOT autouse — the rest of the suite must keep running with
    _onboard_pool as None, which is the validation-disabled production path.

    The DDL is copied from Onboard's real migration
    (onboard/prisma/migrations/20260709020000_add_profile_service/migration.sql):
    `id` and `status` are TEXT, because Prisma maps `String @id` to TEXT. Using
    UUID here would make these tests pass against a query that cannot run against
    the actual Onboard database — which is exactly how the uuid[] cast shipped.
    """
    await pool.execute(
        'CREATE TABLE IF NOT EXISTS profile ('
        '"id" TEXT NOT NULL PRIMARY KEY, '
        '"status" TEXT NOT NULL DEFAULT \'ACTIVE\')'
    )
    await pool.execute("DELETE FROM profile")
    p = await create_onboard_pool(DSN)
    yield p
    await close_onboard_pool()
    await pool.execute("DROP TABLE IF EXISTS profile")


@pytest.fixture
def seed_profile(pool):
    """Insert a profile row the way Onboard stores it: id as canonical text."""

    async def _seed(profile_id, status="ACTIVE"):
        await pool.execute(
            "INSERT INTO profile (id, status) VALUES ($1, $2) "
            "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status",
            str(profile_id), status,
        )

    return _seed
