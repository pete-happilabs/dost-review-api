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

1. **Gate** — Validate at ingestion (self-review, profile validation, dedup, rate limit)
2. **Prepare** — Pull GATED reviews, load stored state, build engine input
3. **Process** — Call `process_reputation()` from the Reputation Engine
4. **Commit** — Upsert profile_reputation, mark reviews COMMITTED

## Development

```bash
pip install -e ".[dev]"
docker compose up -d postgres
pytest tests/ -v    # 69 tests
```

## Profile validation (optional)

If `ONBOARD_DATABASE_URL` is set, the gate checks that both `targetProfileId` and
`raterProfileId` exist and are `ACTIVE` in Onboard's `profile` table, rejecting
otherwise with 422. Leave it empty to skip validation entirely.

Notes for deployers:

- Point it at a role with `SELECT` on `profile` only — **not** Onboard's application
  role, which owns tables holding plaintext PII. The pool also sets
  `default_transaction_read_only`, so writes are refused by the connection itself.
- Onboard's `profile.id` is `TEXT` (Prisma maps `String @id` to TEXT), not `uuid`.
  Ids are compared as text; a `uuid[]` comparison cannot even be planned.
- The dependency is optional in both directions: an unreachable Onboard DB at
  startup or at query time logs and disables validation rather than blocking
  reviews or the service. Lookups are bounded by `ONBOARD_QUERY_TIMEOUT` (2s).

## Auth

All endpoints except `/health`, `/ready`, `/docs`, `/redoc`, and `/openapi.json` require an `X-Access-Key` header. Set `GUARD_ACCESS_KEY` in `.env` to enable (leave empty to disable).

## Status

- 69 tests passing (4 rounds of code review complete)
- CI via GitHub Actions (Postgres service + pytest on push/PR)
