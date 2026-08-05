import asyncpg
from pathlib import Path

_pool: asyncpg.Pool | None = None
_onboard_pool: asyncpg.Pool | None = None


async def create_pool(dsn: str) -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    return _pool


async def create_onboard_pool(dsn: str, command_timeout: float | None = None) -> asyncpg.Pool:
    """Read-only pool for Onboard's DB (profile validation).

    default_transaction_read_only makes "read-only" a property of the connection
    rather than a convention: this pool points at another service's production
    database, so a stray write (e.g. run_migrations() handed the wrong pool) must
    be impossible, not merely unintended.
    """
    global _onboard_pool
    _onboard_pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=3,
        command_timeout=command_timeout,
        server_settings={"default_transaction_read_only": "on"},
    )
    return _onboard_pool


async def close_onboard_pool() -> None:
    """Close only the Onboard pool, leaving the primary pool untouched."""
    global _onboard_pool
    if _onboard_pool:
        await _onboard_pool.close()
        _onboard_pool = None


async def close_pool() -> None:
    global _pool, _onboard_pool
    if _pool:
        await _pool.close()
        _pool = None
    if _onboard_pool:
        await _onboard_pool.close()
        _onboard_pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialized")
    return _pool


def get_onboard_pool() -> asyncpg.Pool | None:
    return _onboard_pool


async def run_migrations(pool: asyncpg.Pool) -> None:
    migrations_dir = Path(__file__).parent.parent / "migrations"
    for sql_file in sorted(migrations_dir.glob("*.sql")):
        sql = sql_file.read_text()
        await pool.execute(sql)
