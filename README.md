# DOST Review API & Batch Engine

Accepts reviews as `dostEvent` envelopes, stores them in PostgreSQL, and processes them through a 4-step batch pipeline using the [Reputation Engine](https://github.com/HappiDost/reputation-engine).

## Quick Start

```bash
cp .env.example .env    # Edit with your ANTHROPIC_API_KEY
docker compose up -d
```

API available at `http://localhost:8013`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/reviews` | Submit a review (raw or dostEvent envelope) |
| GET | `/api/v1/reputation/{profileId}` | Get computed reputation |
| POST | `/api/v1/batch/trigger` | Manually trigger batch processing |
| GET | `/api/v1/batch/status/{batchId}` | Check batch run status |
| GET | `/health` | Health check |
| GET | `/ready` | Readiness probe (DB) |

## Pipeline

1. **Gate** — Validate at ingestion (self-review, rate limit, dedup)
2. **Prepare** — Pull GATED reviews, load stored state, build engine input
3. **Process** — Call `process_reputation()` from the Reputation Engine
4. **Commit** — Upsert profile_reputation, mark reviews COMMITTED

## Development

```bash
pip install -e ".[dev]"
docker compose up -d postgres
pytest tests/ -v
```
