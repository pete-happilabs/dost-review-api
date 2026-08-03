"""M2: Single batch execution path used by both routes and scheduler.

C2: Checks for existing RUNNING batch before starting a new one.
H5: Failure handling wraps the UPDATE in its own try/except.
H3: Sweeps stale RUNNING batches (and requeues their orphaned PROCESSING
    reviews) on startup and before each new batch starts.
"""
import json
import logging
from uuid import UUID, uuid4

import asyncpg

from app.batch_engine import run_batch
from app.config import settings
from app.database import get_pool

logger = logging.getLogger(__name__)


async def start_batch() -> tuple[UUID, bool]:
    """Create a batch_run if none is RUNNING. Returns (batch_id, started).

    started=False means an existing RUNNING batch's id is returned instead.
    Sweeps stale batches first so a crashed run can't block triggering forever.
    """
    pool = get_pool()
    await sweep_stale_batches(pool)

    async with pool.acquire() as conn:
        async with conn.transaction():
            # C2: check-then-insert must be atomic — a manual trigger racing the
            # cron job could otherwise both see "no RUNNING batch" and insert
            # two RUNNING rows. The xact lock serializes starters; it releases
            # on commit so it never outlives this transaction.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('batch_run:start', 0))")

            running = await conn.fetchval(
                "SELECT id FROM batch_run WHERE status = 'RUNNING' LIMIT 1"
            )
            if running:
                logger.info("Batch already running: %s", running)
                return running, False

            batch_id = uuid4()
            await conn.execute(
                "INSERT INTO batch_run (id, status, started_at) VALUES ($1, 'RUNNING', NOW())",
                batch_id,
            )
    return batch_id, True


async def run_batch_to_completion(batch_id: UUID) -> str:
    """Run a started batch and record its final status. Returns the status."""
    pool = get_pool()
    try:
        stats = await run_batch(pool, batch_id)
        await pool.execute(
            "UPDATE batch_run SET status = 'COMPLETED', stats = $1::jsonb, completed_at = NOW() "
            "WHERE id = $2",
            json.dumps(stats), batch_id,
        )
        return "COMPLETED"
    except Exception as e:
        logger.exception("Batch %s failed", batch_id)
        # H5: Wrap the failure UPDATE in its own try/except
        try:
            await pool.execute(
                "UPDATE batch_run SET status = 'FAILED', stats = $1::jsonb, completed_at = NOW() "
                "WHERE id = $2",
                json.dumps({"error": str(e)}), batch_id,
            )
        except Exception:
            logger.exception("Failed to update batch_run status for %s", batch_id)
        # An exception escaping run_batch's per-profile handler leaves the
        # not-yet-handled reviews stuck in PROCESSING. The stale sweep only
        # rescues RUNNING batches — this one is now FAILED — so without this
        # requeue those reviews would never be picked up again.
        try:
            requeued = await pool.execute(
                "UPDATE review SET status = 'GATED' "
                "WHERE status = 'PROCESSING' AND batch_id = $1",
                batch_id,
            )
            logger.warning("Requeued orphaned reviews for failed batch %s: %s", batch_id, requeued)
        except Exception:
            logger.exception("Failed to requeue PROCESSING reviews for batch %s", batch_id)
        return "FAILED"


async def execute_batch() -> tuple[UUID, str]:
    """Start a batch and run it to completion (used by the scheduler).

    Returns (batch_id, status): 'COMPLETED', 'FAILED', or 'ALREADY_RUNNING'.
    """
    batch_id, started = await start_batch()
    if not started:
        return batch_id, "ALREADY_RUNNING"
    status = await run_batch_to_completion(batch_id)
    return batch_id, status


async def sweep_stale_batches(pool: asyncpg.Pool) -> None:
    """H3: Mark stale RUNNING batches as FAILED and requeue their reviews.

    A process that dies mid-batch leaves the batch_run RUNNING *and* its claimed
    reviews stuck in PROCESSING — without the requeue those reviews would never
    be picked up again (pickup only selects GATED), i.e. silent data loss.

    The threshold guards against sweeping a genuinely long-running batch
    (relevant if more than one instance ever runs); tune via
    STALE_BATCH_AFTER_MINUTES if batches legitimately run longer.
    """
    stale = await pool.fetch(
        "UPDATE batch_run SET status = 'FAILED', "
        "stats = '{\"error\": \"stale — process died mid-batch\"}'::jsonb, "
        "completed_at = NOW() "
        "WHERE status = 'RUNNING' AND started_at < NOW() - make_interval(mins => $1) "
        "RETURNING id",
        settings.stale_batch_after_minutes,
    )
    if stale:
        stale_ids = [r["id"] for r in stale]
        requeued = await pool.execute(
            "UPDATE review SET status = 'GATED' "
            "WHERE status = 'PROCESSING' AND batch_id = ANY($1::uuid[])",
            stale_ids,
        )
        logger.warning(
            "Swept %d stale batch run(s) %s; requeued orphaned reviews: %s",
            len(stale_ids), stale_ids, requeued,
        )
