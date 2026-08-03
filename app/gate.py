import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg

from app.config import settings
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
    # 1. Self-review check (no DB needed)
    if review.raterProfileId == review.targetProfileId:
        await _persist_rejected(pool, review, event_id, "SELF_REVIEW")
        return GateResult(False, "SELF_REVIEW", "Cannot review yourself")

    async with pool.acquire() as conn:
        async with conn.transaction():
            # 2. Duplicate check — GATE_REJECTED rows don't block resubmission
            existing_status = await conn.fetchval(
                "SELECT status FROM review WHERE id = $1", review.reviewId
            )
            if existing_status is not None and existing_status != "GATE_REJECTED":
                return GateResult(False, "DUPLICATE", "Review already submitted")

            # 3. Rate limit — C3: use received_at (server time), not client createdAt.
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

            # 4. Insert as GATED — H1: inside the same transaction.
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
    """
    try:
        await executor.execute(
            "INSERT INTO review "
            "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
            "status, gate_reason, event_id, created_at, received_at) "
            "VALUES ($1, $2, $3, $4, $5, 'GATE_REJECTED', $6, $7, $8, NOW()) "
            "ON CONFLICT (id) DO NOTHING",
            review.reviewId, review.targetProfileId, review.raterProfileId,
            1.00, review.reviewText, reason, event_id, review.createdAt,
        )
    except Exception:
        logger.warning("Failed to persist GATE_REJECTED review %s", review.reviewId)
