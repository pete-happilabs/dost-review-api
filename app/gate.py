import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg

from app.config import settings
from app.database import get_onboard_pool
from app.models import DostReview

logger = logging.getLogger(__name__)


@dataclass
class GateResult:
    passed: bool
    error_code: str = ""
    message: str = ""


async def gate_and_store(
    review: DostReview, pool: asyncpg.Pool, event_id=None
) -> GateResult:
    """Gate check + atomic insert. Returns GateResult.

    H1: Gate + insert in one transaction to prevent TOCTOU races.
    M1: Rejected reviews are persisted with GATE_REJECTED + gate_reason for audit.
    C3: Rate limiting uses server-side received_at, not client createdAt.

    A previously GATE_REJECTED reviewId may be resubmitted: the rejected row is
    an audit record, not a permanent claim on the id — otherwise a client that
    was rate-limited once could never retry the same review.
    """
    # 1. Self-review check (no DB needed). Kept ahead of profile validation: it is
    # free, and rater==target is immutable for a given reviewId, so it can never
    # become wrong later — no point spending a cross-service round trip to relabel
    # an already-doomed request.
    if review.raterProfileId == review.targetProfileId:
        await _persist_rejected(pool, review, event_id, "SELF_REVIEW")
        return GateResult(False, "SELF_REVIEW", "Cannot review yourself")

    # 2. Profile existence validation (Onboard DB). The lookup happens HERE, outside
    # the transaction, because a cross-service round trip must never run while the
    # advisory lock and a main-pool connection are held — a wedged Onboard would
    # otherwise drain the main pool and take reputation reads and /ready down with it.
    # The verdict is applied inside the transaction, after the duplicate check.
    profile_error = await _validate_profiles(review.targetProfileId, review.raterProfileId)

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Serialize concurrent submissions for the same (rater, target) pair:
            # under READ COMMITTED the count-then-insert below is racy — N
            # parallel requests could each see count < limit and all insert,
            # bypassing the rate limit entirely. The xact lock releases on
            # commit/rollback; other pairs are unaffected (modulo hash collisions,
            # which only cost a moment of serialization).
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2, 0))",
                str(review.raterProfileId), str(review.targetProfileId),
            )

            # 3. Duplicate check — GATE_REJECTED rows don't block resubmission
            existing_status = await conn.fetchval(
                "SELECT status FROM review WHERE id = $1", review.reviewId
            )
            if existing_status is not None and existing_status != "GATE_REJECTED":
                return GateResult(False, "DUPLICATE", "Review already submitted")

            # 4. Apply the profile verdict from step 2 — deliberately after the
            # duplicate check. Profile status is mutable in Onboard, so the verdict
            # for a fixed reviewId can change between submissions. If this review was
            # already accepted, its row stays GATED and the batch will still commit
            # it; answering "rejected" would tell the caller the opposite of what the
            # system does. An already-accepted reviewId gets DUPLICATE either way.
            if profile_error:
                await _persist_rejected(conn, review, event_id, profile_error.error_code)
                return profile_error

            # 5. Rate limit — C3: use received_at (server time), not client createdAt.
            # Excludes GATE_REJECTED rows: only *accepted* reviews consume quota,
            # otherwise rejected attempts would extend the lockout indefinitely.
            since = datetime.now(timezone.utc) - timedelta(hours=24)
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM review "
                "WHERE rater_profile_id = $1 AND target_profile_id = $2 "
                "AND received_at >= $3 AND status != 'GATE_REJECTED'",
                review.raterProfileId, review.targetProfileId, since,
            )
            if count >= settings.rate_limit_per_rater_per_target:
                await _persist_rejected(conn, review, event_id, "RATE_LIMITED")
                return GateResult(False, "RATE_LIMITED", "Too many reviews for this target today")

            # 6. Insert as GATED — H1: inside the same transaction.
            # ON CONFLICT upgrades a prior GATE_REJECTED row in place; the WHERE
            # clause means a concurrent duplicate (row already GATED/COMMITTED)
            # updates 0 rows instead of raising, which we detect via the tag.
            result = await conn.execute(
                "INSERT INTO review "
                "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
                "multimedia_json, status, event_id, created_at, received_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, 'GATED', $7, $8, NOW()) "
                "ON CONFLICT (id) DO UPDATE SET "
                "target_profile_id = EXCLUDED.target_profile_id, "
                "rater_profile_id = EXCLUDED.rater_profile_id, "
                "review_text = EXCLUDED.review_text, "
                "multimedia_json = EXCLUDED.multimedia_json, "
                "status = 'GATED', gate_reason = NULL, "
                "event_id = EXCLUDED.event_id, "
                "created_at = EXCLUDED.created_at, received_at = NOW() "
                "WHERE review.status = 'GATE_REJECTED'",
                review.reviewId,
                review.targetProfileId,
                review.raterProfileId,
                1.00,  # C1: rater_weight is server-assigned, default 1.0
                review.reviewText,
                json.dumps(review.multimedia) if review.multimedia else None,
                event_id,
                review.createdAt,
            )
            # H1: 0 rows affected = lost a race to a concurrent non-rejected insert
            if int(result.split()[-1]) == 0:
                return GateResult(False, "DUPLICATE", "Review already submitted")

    return GateResult(True)


async def _persist_rejected(executor, review, event_id, reason):
    """M1: Persist rejected reviews (with the rejection reason) for audit trail.

    `executor` is either a pool (own connection) or a connection already inside
    the gate transaction — asyncpg exposes the same .execute() on both.

    A repeat rejection refreshes the reason instead of freezing the first one:
    profile status is mutable in Onboard, so one reviewId can legitimately be
    rejected for TARGET_NOT_FOUND then RATER_INACTIVE, and an audit row stuck on
    the first reason sends whoever debugs it after the wrong profile. The WHERE
    guard mirrors the accept path — a GATED/PROCESSING/COMMITTED row is never
    touched, it just updates 0 rows.
    """
    try:
        await executor.execute(
            "INSERT INTO review "
            "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
            "status, gate_reason, event_id, created_at, received_at) "
            "VALUES ($1, $2, $3, $4, $5, 'GATE_REJECTED', $6, $7, $8, NOW()) "
            "ON CONFLICT (id) DO UPDATE SET "
            "gate_reason = EXCLUDED.gate_reason, "
            "event_id = EXCLUDED.event_id, "
            "received_at = NOW() "
            "WHERE review.status = 'GATE_REJECTED'",
            review.reviewId, review.targetProfileId, review.raterProfileId,
            1.00, review.reviewText, reason, event_id, review.createdAt,
        )
    except Exception:
        logger.warning("Failed to persist GATE_REJECTED review %s", review.reviewId)


async def _validate_profiles(target_id: UUID, rater_id: UUID) -> GateResult | None:
    """Check both profile IDs exist and are ACTIVE in Onboard's database.
    Returns None if valid (or validation is disabled), GateResult if invalid."""
    onboard = get_onboard_pool()
    if onboard is None:
        return None  # Validation disabled — no Onboard DB configured

    # Onboard's profile.id is TEXT, not UUID: Prisma maps `String @id` to TEXT
    # unless told @db.Uuid, and it is not (onboard/prisma/schema.prisma). Postgres
    # has no implicit text<->uuid cast, so comparing against a uuid[] fails to plan
    # at all ("operator does not exist: text = uuid") on every request. Compare as
    # text, which also keeps the profile_pkey index usable — casting the *column*
    # (id::uuid) would both force a scan and blow up on any non-UUID row.
    target_key, rater_key = str(target_id), str(rater_id)

    try:
        # Pool.fetch(timeout=) bounds only query execution, not the wait for a free
        # connection — with max_size=3 that wait is the likelier place to hang, so
        # bound the whole operation. TimeoutError is an Exception, so it fails open
        # below like any other Onboard failure.
        rows = await asyncio.wait_for(
            onboard.fetch(
                "SELECT id, status FROM profile WHERE id = ANY($1::text[])",
                [target_key, rater_key],
            ),
            timeout=settings.onboard_query_timeout,
        )
    except Exception:
        # exc_info is load-bearing: the fail-open path is otherwise indistinguishable
        # between "Onboard is down" and "our query is permanently broken", and the
        # latter looks exactly like a healthy service that validates nothing.
        logger.warning(
            "Onboard DB query failed — skipping profile validation", exc_info=True
        )
        return None  # Fail open on DB errors to avoid blocking reviews

    # Keys are str here (TEXT column): probing this dict with UUID objects would
    # never match, rejecting every review as TARGET_NOT_FOUND.
    found = {row["id"]: row["status"] for row in rows}

    if target_key not in found:
        return GateResult(False, "TARGET_NOT_FOUND", "Target profile does not exist")
    if found[target_key] != "ACTIVE":
        return GateResult(False, "TARGET_INACTIVE", "Target profile is not active")
    if rater_key not in found:
        return GateResult(False, "RATER_NOT_FOUND", "Rater profile does not exist")
    if found[rater_key] != "ACTIVE":
        return GateResult(False, "RATER_INACTIVE", "Rater profile is not active")

    return None
