# DOST Review API — Round 3 Review + Test Verification

You are reviewing a FastAPI microservice that ingests reviews and processes them through a reputation engine. The code has been through 2 prior review rounds. Your job is to **understand the codebase, run all tests, and review for production readiness**.

## Repo

GitHub: `HappiDost/dost-review-api` (private)
Local path: `D:\Projects\dost-review-api\`

## What This Service Does

A review ingestion + batch processing pipeline:
1. Accepts reviews via `POST /api/v1/reviews` (raw JSON or DES-compliant `dostEvent` envelope)
2. Gates them (self-review block, dedup, rate limiting)
3. Stores in PostgreSQL as `GATED`
4. Batch pipeline (daily cron + manual trigger) calls the Reputation Engine
5. Engine does AI work (tag extraction via Claude Haiku, Bayesian scoring, summary generation)
6. Commits output to `profile_reputation` table
7. Serves reputation via `GET /api/v1/reputation/{profileId}`

## Architecture

```
dostEvent ──→ Review API ──→ PostgreSQL ──→ Batch Engine
(reviews)     (gate+store)   (review tbl)   1. Claim (FOR UPDATE SKIP LOCKED)
                                             2. Prepare (load state)
                                             3. Process (call engine)
              GET /reputation ←── DB ←──     4. Commit (upsert reputation)
                            (profile_reputation)
```

## Stack

- Python 3.12+, FastAPI, asyncpg, Pydantic Settings, APScheduler
- PostgreSQL 16 (Docker), pytest + pytest-asyncio + httpx
- Auth: Guard access key via `X-Access-Key` header
- Engine: vendored `vendor/engine.py` (READ-ONLY, do not modify)

## Running Tests

```bash
# 1. Start PostgreSQL
docker compose up -d postgres

# 2. Wait for healthy
docker compose ps  # should show "healthy"

# 3. Install dev deps (if not already)
pip install -e ".[dev]"

# 4. Run all tests
pytest tests/ -v

# Expected: 41 tests passing
```

Test database: `postgresql://dost:dost@localhost:5433/dost_reviews`
Tests auto-run migrations and clean up after each test (DELETE FROM review/profile_reputation/batch_run).

## Project Structure

```
app/
  config.py          Settings from .env (Pydantic Settings)
  database.py        asyncpg pool + migration runner
  models.py          Pydantic models (DostReview, ReviewResponse, DostReputation, BatchStatus)
  auth.py            Guard access key middleware
  gate.py            Gate logic (self-review, dedup, rate limit) + persist rejected
  event_parser.py    DES dostEvent envelope → DostReview
  batch_engine.py    4-step batch pipeline (claim → prepare → process → commit)
  batch_runner.py    Batch orchestration (execute_batch, sweep_stale)
  scheduler.py       APScheduler cron job
  main.py            FastAPI app with lifespan
  routes/
    health.py        /health, /ready
    reviews.py       POST /api/v1/reviews
    reputation.py    GET /api/v1/reputation/{profileId}
    batch.py         POST /api/v1/batch/trigger, GET /api/v1/batch/status/{batchId}

tests/
  conftest.py        Session-scoped pool fixture + cleanup
  test_auth.py       Access key auth tests
  test_gate.py       Gate logic unit tests
  test_reviews_api.py  Review submission API tests
  test_batch_engine.py Batch pipeline + merge_tags tests
  test_batch_api.py  Batch trigger/status API tests
  test_migrations.py Migration idempotency tests
  test_reputation_api.py Reputation lookup tests

migrations/
  001_initial.sql    Tables: review, profile_reputation, batch_run + indexes
  002_review_fixes.sql  received_at, retry_count, CHECK constraints, batch_id index
  003_hardening.sql  Additional hardening constraints

vendor/
  engine.py          Vendored reputation engine (READ-ONLY)
```

## Engine Contract (DO NOT MODIFY vendor/engine.py)

```python
# Input:
{"profileId": "uuid", "about": "", "reviews": [{"description": "text", "createdAt": "iso"}], "state": null_or_dict}

# Success output:
{"profileId": "...", "reputation": "very good", "reputationScore": 3.7, "totalReviews": 42,
 "topTags": [{"tag": "on-time-delivery", "weight": 28, "score": 4.1}],
 "allTags": {"on-time-delivery": 28}, "summary": "...", "state": {opaque}, "createdAt": "iso"}

# Error output:
{"profileId": "...", "error": {"message": "...", "willRetry": true/false}}
```

## Key Design Decisions (Already Made — Don't Redesign)

- **Auth:** Guard access key middleware. Public paths skip auth.
- **raterWeight:** Server-assigned 1.0. Client cannot set it. Future: internal trust service.
- **Concurrency:** `FOR UPDATE SKIP LOCKED` for atomic row claiming. 409 on concurrent batch.
- **Rate limit:** Server-side `received_at`, not client `createdAt`. 5 reviews/rater/target/24h.
- **TOCTOU:** Gate + insert in single transaction. UniqueViolationError → 409.
- **Engine loading:** Lazy load via importlib. Runs in `asyncio.to_thread`.
- **Retry:** `retry_count` + `FAILED` after max retries. Honors engine's `willRetry` flag.
- **Rejected reviews:** Persisted with `GATE_REJECTED` status for audit trail.
- **Stale batches:** Swept on startup (RUNNING > 1hr → FAILED).

## Known Gaps (Accepted — Don't Fix)

- No migration tooling (alembic). Manual SQL files for v1.
- Envelope "encryption" is base64 within Guard trust boundary. Full crypto is socket.io phase.
- `rater_weight` stored but not sent to engine (engine doesn't accept it yet).

## Review History

**Round 1** found 3 critical, 5 high, 8 medium issues — all fixed.
**Round 2** found and fixed: gate_reason tracking, orphan review requeue, constant-time auth comparison, structured validation errors, naive datetime fix, multimedia size cap per item.

## Your Task

1. **Read every source file** — understand the full codebase
2. **Run `pytest tests/ -v`** — confirm all 41 tests pass (Docker postgres must be up)
3. **Review for:** correctness, security, error handling, production readiness, test coverage gaps
4. **If you find issues:** fix them directly, then re-run tests
5. **Report:** what you found, what you changed, final test results

Rules:
- Do NOT modify `vendor/engine.py`
- Do NOT redesign the 4-step pipeline architecture
- Do NOT refactor for style preference — only fix real bugs or clear improvements
- Every change must have a stated reason
- Run tests after every change
