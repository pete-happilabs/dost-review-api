"""M2: Single batch execution function used by both routes and scheduler.

C2: Checks for existing RUNNING batch before starting a new one.
H5: Failure handling wraps the UPDATE in its own try/except.
H3: Sweeps stale RUNNING batches on startup.
"""
import json
import logging
from uuid import UUID, uuid4

import asyncpg

from app.batch_engine import run_batch
from app.database import get_pool

logger = logging.getLogger(__name__)


async def execute_batch() -> tuple[UUID, str]:
    """Execute a batch run with concurrency guard.

    C2: Returns 409-equivalent if a batch is already RUNNING.
    Returns (batch_id, status) where status is 'RUNNING' or 'ALREADY_RUNNING'.
    """
    pool = get_pool()

    # C2: Check for existing RUNNING batch — prevent concurrent runs
    running = await pool.fetchval(
        "SELECT id FROM batch_run WHERE status = 'RUNNING' LIMIT 1"
    )
    if running:
        logger.info("Batch already running: %s", running)
        return running, "ALREADY_RUNNING"

    batch_id = uuid4()
    await pool.execute(
        "INSERT INTO batch_run (id, status, started_at) VALUES ($1, 'RUNNING', NOW())",
        batch_id,
    )

    try:
        stats = await run_batch(pool, batch_id)
        await pool.execute(
            "UPDATE batch_run SET status = 'COMPLETED', stats = $1::jsonb, completed_at = NOW() "
            "WHERE id = $2",
            json.dumps(stats), batch_id,
        )
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

    return batch_id, "RUNNING"


async def sweep_stale_batches(pool: asyncpg.Pool) -> None:
    """H3: Mark stale RUNNING batches (>1 hour old) as FAILED on startup."""
    updated = await pool.execute(
        "UPDATE batch_run SET status = 'FAILED', "
        "stats = '{\"error\": \"stale — process died mid-batch\"}'::jsonb, "
        "completed_at = NOW() "
        "WHERE status = 'RUNNING' AND started_at < NOW() - INTERVAL '1 hour'"
    )
    if updated and updated != "UPDATE 0":
        logger.warning("Swept stale batch runs: %s", updated)
