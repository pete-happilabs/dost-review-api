import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.auth import verify_guard_key
from app.batch_runner import sweep_stale_batches
from app.config import settings
from app.database import close_pool, create_pool, run_migrations
from app.routes import batch, health, reputation, reviews
from app.scheduler import start_scheduler, stop_scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # M4: Set API key env var for engine — document this contract
    os.environ.setdefault("ANTHROPIC_API_KEY", settings.anthropic_api_key)

    # M4: Warn loudly if API key is empty
    if not settings.anthropic_api_key:
        logger.warning("ANTHROPIC_API_KEY is not set — batch processing will fail")

    # C1: Auth fails OPEN when no key is configured (dev convenience) — make
    # that state impossible to miss in logs so it never reaches production silently.
    if not settings.guard_access_key:
        logger.warning(
            "GUARD_ACCESS_KEY is not set — API authentication is DISABLED; "
            "all endpoints are open"
        )

    pool = await create_pool(settings.database_url)
    await run_migrations(pool)

    # H3: Sweep stale RUNNING batches from previous process crashes
    await sweep_stale_batches(pool)

    start_scheduler()
    logger.info("Review API started — batch cron: %s", settings.batch_cadence_cron)
    yield
    stop_scheduler()
    await close_pool()


app = FastAPI(title="DOST Review API", version="1.0.0", lifespan=lifespan)


# C1: Auth middleware — verify Guard access key on all non-public endpoints
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    try:
        await verify_guard_key(request)
    except Exception as exc:
        if hasattr(exc, "status_code"):
            return JSONResponse(
                {"detail": exc.detail}, status_code=exc.status_code
            )
        raise
    return await call_next(request)


app.include_router(health.router)
app.include_router(reviews.router)
app.include_router(reputation.router)
app.include_router(batch.router)
