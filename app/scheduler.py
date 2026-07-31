import json
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.batch_engine import run_batch
from app.config import settings
from app.database import get_pool

scheduler = AsyncIOScheduler()


async def _scheduled_batch() -> None:
    pool = get_pool()
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
        await pool.execute(
            "UPDATE batch_run SET status = 'FAILED', stats = $1::jsonb, completed_at = NOW() "
            "WHERE id = $2",
            json.dumps({"error": str(e)}), batch_id,
        )


def start_scheduler() -> None:
    scheduler.add_job(
        _scheduled_batch,
        CronTrigger.from_crontab(settings.batch_cadence_cron),
        id="daily_batch",
        replace_existing=True,
    )
    scheduler.start()


def stop_scheduler() -> None:
    scheduler.shutdown(wait=False)
