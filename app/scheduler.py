"""M2: Uses shared execute_batch instead of duplicating batch logic.
M8: Timezone explicitly set to UTC.
"""
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.batch_runner import execute_batch
from app.config import settings

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone=settings.batch_timezone)


async def _scheduled_batch() -> None:
    logger.info("Scheduled batch starting")
    batch_id, status = await execute_batch()
    if status == "ALREADY_RUNNING":
        logger.info("Scheduled batch skipped — batch %s already running", batch_id)
    else:
        # Log the real outcome — this previously said "completed" even on FAILED
        logger.info("Scheduled batch %s finished with status %s", batch_id, status)


def start_scheduler() -> None:
    scheduler.add_job(
        _scheduled_batch,
        CronTrigger.from_crontab(settings.batch_cadence_cron),
        id="daily_batch",
        replace_existing=True,
        misfire_grace_time=300,  # M8: 5 min grace for missed fires after deploy
        coalesce=True,  # M8: Coalesce missed fires into one run
    )
    scheduler.start()


def stop_scheduler() -> None:
    scheduler.shutdown(wait=False)
