import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.database import close_pool, create_pool, run_migrations
from app.routes import batch, health, reputation, reviews
from app.scheduler import start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.environ.setdefault("ANTHROPIC_API_KEY", settings.anthropic_api_key)
    pool = await create_pool(settings.database_url)
    await run_migrations(pool)
    start_scheduler()
    yield
    stop_scheduler()
    await close_pool()


app = FastAPI(title="DOST Review API", version="1.0.0", lifespan=lifespan)
app.include_router(health.router)
app.include_router(reviews.router)
app.include_router(reputation.router)
app.include_router(batch.router)
