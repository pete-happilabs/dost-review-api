# Code Review: DOST Review API & Batch Engine

You are reviewing a FastAPI microservice that accepts reviews via dostEvent envelopes, stores them in PostgreSQL, and processes them through a 4-step batch pipeline by calling an external Reputation Engine.

## Architecture

```
                                       ┌─────────────────────┐
dostEvent ──→  Review API  ──→  DB  ──→│  Batch Engine       │
(reviews)      (gate,store)   (review  │  1. Gate            │
                               table)  │  2. Prepare ────────┼──→ profile_reputation (read state)
                                       │  3. Process ────────┼──→ Reputation Engine
               GET /reputation         │                     │    (process_reputation)
                  ←──────────── DB  ←──│  4. Commit ←────────┼────── output + state
                          (profile     │                     │
                        reputation)    └─────────────────────┘
```

**Two services:**
1. **Reputation Engine** (`HappiDost/reputation-engine`) — Existing, NOT modified. Stateless library with one function: `process_reputation(data) → (output, metrics)`. Takes profile + new reviews + stored state, returns reputation (tier, score, tags, summary) + updated state. Uses Claude Haiku for AI tag extraction and summary generation.
2. **Review API & Batch Engine** (this code) — NEW microservice. Owns the DB. Accepts reviews, gates them, and runs a 4-step batch pipeline that calls the engine.

**Stack:** Python 3.12, FastAPI, asyncpg, PostgreSQL 16, APScheduler, Pydantic v2

## What the Engine Expects and Returns

```python
# Input:
{"profileId": "uuid", "about": "", "reviews": [{"description": "text", "createdAt": "iso"}], "state": null_or_stored_state}

# Success output:
{"profileId": "...", "reputation": "very good", "reputationScore": 3.7, "totalReviews": 42,
 "topTags": [{"tag": "on-time-delivery", "weight": 28, "score": 4.1}, ...],  # top 5 with score
 "allTags": {"on-time-delivery": 28, "product-quality": 24, ...},            # all, count only
 "summary": "...", "state": {opaque_blob}, "createdAt": "iso"}

# Failure output (has "error" key):
{"profileId": "...", "error": {"message": "tag extraction failed", "willRetry": true}}
```

## DB Schema (mirrors engine I/O)

**review** — What the engine needs as input:
- id (UUID PK = reviewId), target_profile_id, rater_profile_id, rater_weight (DECIMAL 0-2, default 1), review_text, multimedia_json (JSONB), status (GATED/GATE_REJECTED/COMMITTED), batch_id, event_id, created_at, processed_at

**profile_reputation** — What the engine returns as output:
- profile_id (UUID PK), reputation (tier string), reputation_score (0-5), total_reviews, all_tags (JSONB — merged from engine's topTags+allTags into `{tag: {weight, score}}`), summary, state (JSONB opaque), created_at, updated_at

**batch_run** — Operational tracking:
- id (UUID PK), status (RUNNING/COMPLETED/FAILED), stats (JSONB), started_at, completed_at

## 4-Step Pipeline
1. **Gate** (sync, at ingestion) — Self-review check, dedup by reviewId, rate limit (5/rater/target/24h)
2. **Prepare** (batch) — Pull GATED reviews, group by target, load stored state from profile_reputation
3. **Process** (batch) — Call `process_reputation()` per profile. On error → reviews stay GATED.
4. **Commit** (batch) — Upsert profile_reputation with engine output, mark reviews COMMITTED. Transactional per profile.

## allTags Merge Logic
Engine returns `topTags` (array with weight+score, top 5 only) and `allTags` (dict with counts for all tags) separately. We merge into one JSONB column:
```python
merged = {}
for tag, count in engine_output["allTags"].items():
    merged[tag] = {"weight": count}
for entry in engine_output["topTags"]:
    merged[entry["tag"]] = {"weight": entry["weight"], "score": entry["score"]}
```
Top tags are derived at read time by sorting allTags by weight descending.

## DES Compliance
POST /reviews accepts both:
- Raw `dostReview` JSON (internal callers)
- Full `dostEventEnvelope` with `eventType: "MSG_START"` and base64-encoded inner event containing the review in `message.text`

Detection: if `"eventType"` in request body → envelope mode → parse via `event_parser.py`

## Known Gap
`rater_weight` is stored in the review table but NOT passed to the engine — the engine's input format (`{description, createdAt}`) doesn't accept it yet. Documented in `batch_engine.py` with a ready-to-uncomment line for when the engine adds support.

## API Endpoints
| Method | Path | Status Codes |
|--------|------|-------------|
| POST | /api/v1/reviews | 201 (GATED), 400 (self-review/invalid), 409 (duplicate), 429 (rate limited) |
| GET | /api/v1/reputation/{profileId} | 200, 404 |
| POST | /api/v1/batch/trigger | 202 |
| GET | /api/v1/batch/status/{batchId} | 200, 404 |
| GET | /health | 200 |
| GET | /ready | 200, 503 |

## Test Results
13/13 tests passing. Smoke-tested with real Anthropic API — submitted 3 reviews, triggered batch, got computed reputation with AI-generated summary and extracted tags.

## Your Review Task

Please review the complete codebase below for:
1. **Correctness** — Does the implementation match the spec? Any logical bugs?
2. **Security** — SQL injection, input validation, auth gaps?
3. **Error handling** — Silent failures, unhandled edge cases, data loss risks?
4. **Production readiness** — Would you deploy this? What's missing?
5. **Code quality** — Clean, maintainable, follows Python/FastAPI conventions?
6. **Test coverage** — Are the tests meaningful? What's missing?

Be thorough and critical. Flag everything — we want this production-grade.

---

## Complete Source Code

### pyproject.toml
```toml
[project]
name = "dost-review-api"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi[standard]>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "asyncpg>=0.30.0",
    "pydantic-settings>=2.6.0",
    "apscheduler>=3.10.0",
    "anthropic>=0.40.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3.0",
    "pytest-asyncio>=0.24.0",
    "httpx>=0.28.0",
    "ruff>=0.8.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
testpaths = ["tests"]

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.setuptools.packages.find]
include = ["app*"]
```

### app/config.py
```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://dost:dost@localhost:5433/dost_reviews"
    anthropic_api_key: str = ""
    batch_cadence_cron: str = "0 6 * * *"
    max_reviews_per_batch: int = 10_000
    rate_limit_per_rater_per_target: int = 5
    engine_path: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
```

### app/database.py
```python
import asyncpg
from pathlib import Path

_pool: asyncpg.Pool | None = None


async def create_pool(dsn: str) -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialized")
    return _pool


async def run_migrations(pool: asyncpg.Pool) -> None:
    migrations_dir = Path(__file__).parent.parent / "migrations"
    for sql_file in sorted(migrations_dir.glob("*.sql")):
        sql = sql_file.read_text()
        await pool.execute(sql)
```

### app/models.py
```python
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class DostReview(BaseModel):
    reviewId: UUID
    targetProfileId: UUID
    raterProfileId: UUID
    raterWeight: float = Field(default=1.0, ge=0.0, le=2.0)
    reviewText: str = Field(min_length=1, max_length=5000)
    multimedia: list[dict[str, Any]] | None = None
    createdAt: datetime

    @field_validator("reviewText")
    @classmethod
    def text_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("reviewText must not be blank")
        return v


class ReviewResponse(BaseModel):
    reviewId: UUID
    status: str
    message: str


class TagInfo(BaseModel):
    weight: float
    score: float | None = None


class DostReputation(BaseModel):
    profileId: str
    reputation: str
    reputationScore: float
    totalReviews: int
    allTags: dict[str, TagInfo]
    summary: str
    createdAt: datetime


class BatchStatus(BaseModel):
    batchId: UUID
    status: str
    stats: dict[str, Any] | None = None
    startedAt: datetime
    completedAt: datetime | None = None
```

### app/gate.py
```python
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg

from app.config import settings
from app.models import DostReview


@dataclass
class GateResult:
    passed: bool
    error_code: str = ""
    message: str = ""


async def gate_review(review: DostReview, pool: asyncpg.Pool) -> GateResult:
    # 1. Self-review check
    if review.raterProfileId == review.targetProfileId:
        return GateResult(False, "SELF_REVIEW", "Cannot review yourself")

    # 2. Duplicate check
    exists = await pool.fetchval("SELECT 1 FROM review WHERE id = $1", review.reviewId)
    if exists:
        return GateResult(False, "DUPLICATE", "Review already submitted")

    # 3. Rate limit check (max N reviews per rater per target per 24h)
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    count = await pool.fetchval(
        "SELECT COUNT(*) FROM review "
        "WHERE rater_profile_id = $1 AND target_profile_id = $2 AND created_at >= $3",
        review.raterProfileId, review.targetProfileId, since,
    )
    if count >= settings.rate_limit_per_rater_per_target:
        return GateResult(False, "RATE_LIMITED", "Too many reviews for this target today")

    return GateResult(True)
```

### app/event_parser.py
```python
import base64
import json
from uuid import UUID

from app.models import DostReview


class EnvelopeError(Exception):
    def __init__(self, message: str):
        self.message = message


def parse_event_envelope(body: dict) -> tuple[DostReview, UUID | None]:
    event_type = body.get("eventType")
    if event_type != "MSG_START":
        raise EnvelopeError(f"Expected eventType MSG_START, got {event_type}")

    encrypted = body.get("encryptedEvent")
    if not encrypted:
        raise EnvelopeError("Missing encryptedEvent field")

    event_id_str = body.get("eventId")
    event_id = UUID(event_id_str) if event_id_str else None

    try:
        decoded = base64.b64decode(encrypted)
        inner = json.loads(decoded)
    except Exception as e:
        raise EnvelopeError(f"Failed to decode encryptedEvent: {e}")

    message = inner.get("message", {})
    review_json_str = message.get("text")
    if not review_json_str:
        raise EnvelopeError("Inner dostEvent has no message.text field")

    try:
        review_data = json.loads(review_json_str)
        review = DostReview(**review_data)
    except Exception as e:
        raise EnvelopeError(f"Failed to parse DostReview from message.text: {e}")

    return review, event_id
```

### app/main.py
```python
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
```

### app/routes/reviews.py
```python
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.database import get_pool
from app.event_parser import EnvelopeError, parse_event_envelope
from app.gate import gate_review
from app.models import DostReview, ReviewResponse

router = APIRouter(prefix="/api/v1")


@router.post("/reviews", status_code=201, response_model=ReviewResponse)
async def submit_review(request: Request):
    body: dict[str, Any] = await request.json()

    event_id = None
    if "eventType" in body:
        try:
            review, event_id = parse_event_envelope(body)
        except EnvelopeError as e:
            raise HTTPException(status_code=400, detail=e.message)
    else:
        try:
            review = DostReview(**body)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    pool = get_pool()
    result = await gate_review(review, pool)

    if not result.passed:
        status_code = {"SELF_REVIEW": 400, "DUPLICATE": 409, "RATE_LIMITED": 429}.get(
            result.error_code, 400
        )
        raise HTTPException(status_code=status_code, detail=result.message)

    await pool.execute(
        "INSERT INTO review "
        "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
        "multimedia_json, status, event_id, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, 'GATED', $7, $8)",
        review.reviewId, review.targetProfileId, review.raterProfileId,
        review.raterWeight, review.reviewText,
        json.dumps(review.multimedia) if review.multimedia else None,
        event_id, review.createdAt,
    )

    return ReviewResponse(
        reviewId=review.reviewId, status="GATED",
        message="Review accepted. Will be processed in the next batch run.",
    )
```

### app/routes/reputation.py
```python
import json
from uuid import UUID

from fastapi import APIRouter, HTTPException

from app.database import get_pool
from app.models import DostReputation, TagInfo

router = APIRouter(prefix="/api/v1")


@router.get("/reputation/{profile_id}", response_model=DostReputation)
async def get_reputation(profile_id: UUID):
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM profile_reputation WHERE profile_id = $1", profile_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="No reputation found for this profile")

    raw_tags = row["all_tags"]
    if isinstance(raw_tags, str):
        raw_tags = json.loads(raw_tags)

    all_tags = {}
    for tag, info in raw_tags.items():
        if isinstance(info, dict):
            all_tags[tag] = TagInfo(**info)
        else:
            all_tags[tag] = TagInfo(weight=float(info))

    return DostReputation(
        profileId=str(row["profile_id"]), reputation=row["reputation"],
        reputationScore=float(row["reputation_score"]), totalReviews=row["total_reviews"],
        allTags=all_tags, summary=row["summary"], createdAt=row["updated_at"],
    )
```

### app/routes/batch.py
```python
import json
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.batch_engine import run_batch
from app.database import get_pool
from app.models import BatchStatus

router = APIRouter(prefix="/api/v1")


async def _run_batch_task(batch_id: UUID) -> None:
    pool = get_pool()
    try:
        stats = await run_batch(pool, batch_id)
        await pool.execute(
            "UPDATE batch_run SET status = 'COMPLETED', stats = $1::jsonb, completed_at = NOW() "
            "WHERE id = $2", json.dumps(stats), batch_id,
        )
    except Exception as e:
        await pool.execute(
            "UPDATE batch_run SET status = 'FAILED', stats = $1::jsonb, completed_at = NOW() "
            "WHERE id = $2", json.dumps({"error": str(e)}), batch_id,
        )


@router.post("/batch/trigger", status_code=202)
async def trigger_batch(background_tasks: BackgroundTasks):
    pool = get_pool()
    batch_id = uuid4()
    await pool.execute(
        "INSERT INTO batch_run (id, status, started_at) VALUES ($1, 'RUNNING', NOW())", batch_id,
    )
    background_tasks.add_task(_run_batch_task, batch_id)
    return {"batchId": str(batch_id), "status": "RUNNING", "message": "Batch processing started."}


@router.get("/batch/status/{batch_id}", response_model=BatchStatus)
async def batch_status(batch_id: UUID):
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM batch_run WHERE id = $1", batch_id)
    if not row:
        raise HTTPException(status_code=404, detail="Batch not found")
    raw_stats = row["stats"]
    stats = json.loads(raw_stats) if isinstance(raw_stats, str) else raw_stats
    return BatchStatus(
        batchId=row["id"], status=row["status"], stats=stats,
        startedAt=row["started_at"], completedAt=row["completed_at"],
    )
```

### app/batch_engine.py
```python
import importlib
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)


def _load_engine():
    """Import process_reputation from the reputation engine."""
    engine_path = settings.engine_path
    if not engine_path:
        vendor = Path(__file__).parent.parent / "vendor"
        if (vendor / "engine.py").exists():
            engine_path = str(vendor)
    if engine_path and engine_path not in sys.path:
        sys.path.insert(0, engine_path)
    spec = importlib.util.spec_from_file_location(
        "reputation_engine", Path(engine_path or ".") / "engine.py",
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot find engine.py at {engine_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.process_reputation


process_reputation = _load_engine()


def _merge_tags(engine_output: dict[str, Any]) -> dict[str, Any]:
    """Merge engine's topTags (weight+score) and allTags (counts) into single dict."""
    merged: dict[str, dict[str, Any]] = {}
    for tag, count in engine_output.get("allTags", {}).items():
        merged[tag] = {"weight": count}
    for entry in engine_output.get("topTags", []):
        merged[entry["tag"]] = {"weight": entry["weight"], "score": entry["score"]}
    return merged


async def run_batch(pool: asyncpg.Pool, batch_id: UUID) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "totalReviews": 0, "committed": 0, "failed": 0, "profilesUpdated": 0,
    }

    rows = await pool.fetch(
        "SELECT * FROM review WHERE status = 'GATED' ORDER BY created_at LIMIT $1",
        settings.max_reviews_per_batch,
    )
    stats["totalReviews"] = len(rows)
    if not rows:
        return stats

    profiles: dict[UUID, list] = defaultdict(list)
    for row in rows:
        profiles[row["target_profile_id"]].append(row)

    for profile_id, review_rows in profiles.items():
        try:
            rep_row = await pool.fetchrow(
                "SELECT state FROM profile_reputation WHERE profile_id = $1", profile_id
            )
            stored_state = None
            if rep_row and rep_row["state"]:
                raw = rep_row["state"]
                stored_state = json.loads(raw) if isinstance(raw, str) else raw

            # NOTE: rater_weight is stored but engine doesn't accept it yet.
            # When engine adds support, add: "raterWeight": float(r["rater_weight"])
            engine_input: dict[str, Any] = {
                "profileId": str(profile_id), "about": "",
                "reviews": [
                    {"description": r["review_text"], "createdAt": r["created_at"].isoformat()}
                    for r in review_rows
                ],
            }
            if stored_state:
                engine_input["state"] = stored_state

            output, _metrics = process_reputation(engine_input)

            if "error" in output:
                logger.warning("Engine error for profile %s: %s",
                    profile_id, output["error"].get("message", "unknown"))
                stats["failed"] += len(review_rows)
                continue

            merged_tags = _merge_tags(output)

            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO profile_reputation "
                        "(profile_id, reputation, reputation_score, total_reviews, "
                        "all_tags, summary, state, created_at, updated_at) "
                        "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, NOW(), NOW()) "
                        "ON CONFLICT (profile_id) DO UPDATE SET "
                        "reputation=$2, reputation_score=$3, total_reviews=$4, "
                        "all_tags=$5::jsonb, summary=$6, state=$7::jsonb, updated_at=NOW()",
                        profile_id, output["reputation"], output["reputationScore"],
                        output["totalReviews"], json.dumps(merged_tags),
                        output.get("summary", ""), json.dumps(output["state"]),
                    )
                    review_ids = [r["id"] for r in review_rows]
                    await conn.execute(
                        "UPDATE review SET status='COMMITTED', processed_at=NOW(), "
                        "batch_id=$1 WHERE id = ANY($2::uuid[])",
                        batch_id, review_ids,
                    )

            stats["committed"] += len(review_rows)
            stats["profilesUpdated"] += 1

        except Exception:
            logger.exception("Batch failed for profile %s", profile_id)
            stats["failed"] += len(review_rows)

    return stats
```

### app/scheduler.py
```python
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
        "INSERT INTO batch_run (id, status, started_at) VALUES ($1, 'RUNNING', NOW())", batch_id,
    )
    try:
        stats = await run_batch(pool, batch_id)
        await pool.execute(
            "UPDATE batch_run SET status='COMPLETED', stats=$1::jsonb, completed_at=NOW() "
            "WHERE id=$2", json.dumps(stats), batch_id,
        )
    except Exception as e:
        await pool.execute(
            "UPDATE batch_run SET status='FAILED', stats=$1::jsonb, completed_at=NOW() "
            "WHERE id=$2", json.dumps({"error": str(e)}), batch_id,
        )


def start_scheduler() -> None:
    scheduler.add_job(
        _scheduled_batch, CronTrigger.from_crontab(settings.batch_cadence_cron),
        id="daily_batch", replace_existing=True,
    )
    scheduler.start()


def stop_scheduler() -> None:
    scheduler.shutdown(wait=False)
```

### app/routes/health.py
```python
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.database import get_pool

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/ready")
async def ready():
    try:
        pool = get_pool()
        await pool.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception:
        return JSONResponse({"status": "unavailable"}, status_code=503)
```

### migrations/001_initial.sql
```sql
CREATE TABLE IF NOT EXISTS review (
    id                UUID PRIMARY KEY,
    target_profile_id UUID NOT NULL,
    rater_profile_id  UUID NOT NULL,
    rater_weight      DECIMAL(3,2) NOT NULL DEFAULT 1.00,
    review_text       TEXT NOT NULL,
    multimedia_json   JSONB,
    status            VARCHAR(20) NOT NULL DEFAULT 'GATED',
    batch_id          UUID,
    event_id          UUID,
    created_at        TIMESTAMPTZ NOT NULL,
    processed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_review_target_status ON review(target_profile_id, status);
CREATE INDEX IF NOT EXISTS idx_review_rate_limit ON review(rater_profile_id, target_profile_id, created_at);
CREATE INDEX IF NOT EXISTS idx_review_batch_pickup ON review(status, created_at);

CREATE TABLE IF NOT EXISTS profile_reputation (
    profile_id       UUID PRIMARY KEY,
    reputation       VARCHAR(20) NOT NULL DEFAULT 'average',
    reputation_score DECIMAL(3,1) NOT NULL DEFAULT 3.0,
    total_reviews    INTEGER NOT NULL DEFAULT 0,
    all_tags         JSONB NOT NULL DEFAULT '{}',
    summary          TEXT NOT NULL DEFAULT '',
    state            JSONB NOT NULL DEFAULT '{}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS batch_run (
    id           UUID PRIMARY KEY,
    status       VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    stats        JSONB,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
```

### Dockerfile
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY vendor/engine.py /engine/engine.py
ENV ENGINE_PATH=/engine
COPY app/ app/
COPY migrations/ migrations/
EXPOSE 8013
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8013"]
```

### docker-compose.yml
```yaml
services:
  review-api:
    build: .
    ports:
      - "8013:8013"
    env_file: .env
    depends_on:
      postgres:
        condition: service_healthy

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: dost_reviews
      POSTGRES_USER: dost
      POSTGRES_PASSWORD: dost
    ports:
      - "5433:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "dost"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  pgdata:
```

### Tests (13/13 passing)

**tests/conftest.py** — Session-scoped asyncpg pool, auto-cleanup between tests.

**tests/test_gate.py** (4 tests) — Valid review passes, self-review rejected, duplicate rejected, rate limit (5/day) enforced.

**tests/test_reviews_api.py** (4 tests) — Valid submit (201 GATED), self-review (400), duplicate (409), dostEvent envelope parsing (201).

**tests/test_reputation_api.py** (2 tests) — Not found (404), found with allTags weight+score verified (200).

**tests/test_batch_engine.py** (3 tests) — Processes GATED→COMMITTED with mock engine, engine failure keeps GATED, empty batch no-ops.
