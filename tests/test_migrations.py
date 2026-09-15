"""Migrations run on EVERY startup (run_migrations re-executes all files),
so every file must be idempotent. The bare ADD CONSTRAINT statements in 002
crashed the second boot with DuplicateObjectError before being wrapped in
guarded DO blocks."""
import asyncpg
import pytest

from app.database import run_migrations


@pytest.mark.asyncio
async def test_migrations_are_idempotent(pool):
    # The session fixture already ran migrations once; running them again
    # simulates an app restart. Twice more for good measure.
    await run_migrations(pool)
    await run_migrations(pool)


RISK_TABLES = {"signal_event", "conversation_risk", "profile_risk", "outcome_event", "review_queue"}


@pytest.mark.asyncio
async def test_004_creates_risk_tables(pool):
    rows = await pool.fetch(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = ANY($1::text[])",
        list(RISK_TABLES),
    )
    assert {r["table_name"] for r in rows} == RISK_TABLES


@pytest.mark.asyncio
async def test_signal_event_dedupes_on_session_and_event(pool):
    # Ingest is at-least-once (dost-talk retries); a replayed record must not
    # accumulate twice, so the (session_id, event_id) pair is UNIQUE.
    insert = (
        "INSERT INTO signal_event (session_id, event_id, sender_profile_id, "
        "model_version, verdict, record) VALUES ('s1', 'e1', 'hum.a', 'v1', 'suspicious', '{}')"
    )
    try:
        await pool.execute(insert)
        with pytest.raises(asyncpg.UniqueViolationError):
            await pool.execute(insert)
    finally:
        await pool.execute("DELETE FROM signal_event WHERE session_id = 's1'")
