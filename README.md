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
pytest tests/ -v    # 110 tests
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

## Risk service

The same app also ingests the intent gate's signal records, folds them through the
Reputation Engine's fraud family (`process_conversation`, `process_signals`) and serves a
per-profile risk tier. Tables come from `migrations/004_risk.sql` (plus `005_*` for the
one-OPEN-review-per-profile index); code lives in `app/risk/`
(`ids.py`, `mapping.py`, `store.py`, `service.py`) and `app/routes/{signals,risk,outcomes}.py`.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/signals` | Ingest one gate record. 202 with the sender's tier; 200 `{"duplicate": true}` on replay of a `(sessionId, eventId)`; 422 on a malformed record or an engine rejection |
| GET | `/api/v1/risk/profile/{profileId}` | Current `riskTier` / `riskScore` / `repeatedAcross`; an unknown profile answers `riskTier: "none"` |
| POST | `/api/v1/outcomes` | Report an outcome (`paid_not_delivered`, `confirmed_fraud`, ...). 202 after the fold; the profile is queued for human review at score >= 60 |

Behaviour that goes beyond the plan text (Plan 3, Task 6) and is kept on purpose:

- **One transaction per ingest, serialized per key.** dost-talk forwards records
  fire-and-forget, so two records for one session (or one sender) are routinely in flight
  together, and each ingest is a read-modify-write on `conversation_risk` and `profile_risk`.
  `RiskStore.transaction(lock_key)` runs the whole fold in one Postgres transaction under
  `pg_advisory_xact_lock(hashtext(key))` — `session:<id>` around the conversation state,
  `profile:<id>` around the account state — so the second writer never discards the first
  one's fold. Every store call inside the block rides the same connection (a ContextVar);
  the base-class version is a no-op so test fakes inherit the interface.
- **Nothing is on file unless it was folded.** An engine input error, or any failure after
  the `signal_event` insert, rolls the insert back too. A retry therefore re-processes the
  record instead of being answered `duplicate: true` for an event that was never folded.
  The route contract is unchanged: error -> 422, duplicate -> 200, otherwise 202.
- **Typed signal payload.** `signals[]` is validated as
  `{name, confidence: float, tier, detail, entities}` — exactly the shape the gate emits — so
  a missing, null or non-numeric `confidence` is a 422 before any row is written. Nothing is
  stripped from the record stored in `signal_event.record`: both models set
  `extra="allow"`, so a field a DES bump adds — at the top level or inside a signal — is
  stored verbatim even though this service does not read it yet, while the declared fields
  keep their types. `confidence` has no default on
  purpose: a default would compose with the mapper's confidence floor into a silent no-op
  (202, no tag, a fabricated `0.0` on the stored record), so a DES bump that renamed or
  dropped the field would turn the fraud path off with a green health check. The names the
  floor does drop are logged (`app.risk.mapping`, INFO, with the `eventId`).

### Which engine runs

`app/batch_engine.load_engine_module()` loads `$ENGINE_PATH/engine.py` when `ENGINE_PATH` is
set, else `vendor/engine.py`, and logs the resolved file at first use
(`Reputation engine loaded from ...`). The Docker image copies `vendor/engine.py` to `/engine`
and pins `ENGINE_PATH=/engine`, and CI runs without a `.env`, so the vendored copy is what runs
in a container and in CI. Set `ENGINE_PATH` only to develop against a checkout of
`dost-reputation`; leave it empty wherever the vendored, reviewed copy is meant to be
authoritative. Refresh it with `cp ../dost-reputation/engine.py vendor/engine.py` — byte for
byte, never edited here.

## Auth

All endpoints except `/health`, `/ready`, `/docs`, `/redoc`, and `/openapi.json` — the risk
routes above included — require an `X-Access-Key` header matching `GUARD_ACCESS_KEY`.

**The check fails open.** With `GUARD_ACCESS_KEY` empty or unset the middleware skips auth
entirely and the service logs `GUARD_ACCESS_KEY is not set — API authentication is DISABLED`
at startup. That is a local-development convenience only: set it in every environment this
service is deployed to. `docker compose` reads it from `.env`, and `.env.example` ships it
empty, so a copied example file runs unauthenticated until you fill it in.

## Status

- 110 tests passing (4 rounds of code review complete; risk service reviewed once more)
- CI via GitHub Actions (Postgres service + pytest on push/PR)
