"""Migrations run on EVERY startup (run_migrations re-executes all files),
so every file must be idempotent. The bare ADD CONSTRAINT statements in 002
crashed the second boot with DuplicateObjectError before being wrapped in
guarded DO blocks."""
import pytest

from app.database import run_migrations


@pytest.mark.asyncio
async def test_migrations_are_idempotent(pool):
    # The session fixture already ran migrations once; running them again
    # simulates an app restart. Twice more for good measure.
    await run_migrations(pool)
    await run_migrations(pool)
