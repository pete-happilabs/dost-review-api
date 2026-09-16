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


@pytest.mark.asyncio
async def test_005_allows_only_one_open_review_per_profile(pool):
    # service._fold enqueues on every ingest above QUEUE_AT, so the queue is the one place
    # a high-risk account piles up. The partial unique index is the durable guard;
    # store.enqueue_review's ON CONFLICT DO NOTHING is what keeps it from raising.
    insert = (
        "INSERT INTO review_queue (profile_id, session_id, reason, risk_score, status) "
        "VALUES ('hum.mig.1', $1, 'score', 72.0, $2)"
    )
    try:
        await pool.execute(insert, "S1", "OPEN")
        with pytest.raises(asyncpg.UniqueViolationError):
            await pool.execute(insert, "S2", "OPEN")
        # The index is partial: a decided row does not block a fresh OPEN one, and any
        # number of non-OPEN rows may coexist.
        await pool.execute(insert, "S3", "EXPIRED")
        await pool.execute(insert, "S4", "EXPIRED")
        assert await pool.fetchval(
            "SELECT count(*) FROM review_queue WHERE profile_id='hum.mig.1'") == 3
    finally:
        await pool.execute("DELETE FROM review_queue WHERE profile_id = 'hum.mig.1'")
