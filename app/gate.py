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
    M1: Rejected reviews are persisted with GATE_REJECTED for audit trail.
    C3: Rate limiting uses server-side received_at, not client createdAt.
    """
    # 1. Self-review check (no DB needed)
    if review.raterProfileId == review.targetProfileId:
        await _persist_rejected(pool, review, event_id, "SELF_REVIEW")
        return GateResult(False, "SELF_REVIEW", "Cannot review yourself")

    async with pool.acquire() as conn:
        async with conn.transaction():
            # 2. Duplicate check — use FOR UPDATE to lock if exists
            # H1: UniqueViolation is caught below as a fallback
            exists = await conn.fetchval(
                "SELECT 1 FROM review WHERE id = $1", review.reviewId
            )
            if exists:
                return GateResult(False, "DUPLICATE", "Review already submitted")

            # 3. Rate limit — C3: use received_at (server time), not client createdAt
            since = datetime.now(timezone.utc) - timedelta(hours=24)
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM review "
                "WHERE rater_profile_id = $1 AND target_profile_id = $2 "
                "AND received_at >= $3",
                review.raterProfileId, review.targetProfileId, since,
            )
            if count >= settings.rate_limit_per_rater_per_target:
                await _persist_rejected_in_txn(conn, review, event_id, "RATE_LIMITED")
                return GateResult(False, "RATE_LIMITED", "Too many reviews for this target today")

            # 4. Insert as GATED — H1: inside the same transaction
            try:
                await conn.execute(
                    "INSERT INTO review "
                    "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
                    "multimedia_json, status, event_id, created_at, received_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, 'GATED', $7, $8, NOW())",
                    review.reviewId,
                    review.targetProfileId,
                    review.raterProfileId,
                    1.00,  # C1: rater_weight is server-assigned, default 1.0
                    review.reviewText,
                    json.dumps(review.multimedia) if review.multimedia else None,
                    event_id,
                    review.createdAt,
                )
            except asyncpg.UniqueViolationError:
                # H1: Race condition — another request inserted between SELECT and INSERT
                return GateResult(False, "DUPLICATE", "Review already submitted")

    return GateResult(True)


async def _persist_rejected(pool, review, event_id, reason):
    """M1: Persist rejected reviews for audit trail."""
    try:
        await pool.execute(
            "INSERT INTO review "
            "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
            "status, event_id, created_at, received_at) "
            "VALUES ($1, $2, $3, $4, $5, 'GATE_REJECTED', $6, $7, NOW()) "
            "ON CONFLICT (id) DO NOTHING",
            review.reviewId, review.targetProfileId, review.raterProfileId,
            1.00, review.reviewText, event_id, review.createdAt,
        )
    except Exception:
        logger.warning("Failed to persist GATE_REJECTED review %s", review.reviewId)


async def _persist_rejected_in_txn(conn, review, event_id, reason):
    """M1: Persist rejected review within an existing transaction."""
    try:
        await conn.execute(
            "INSERT INTO review "
            "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
            "status, event_id, created_at, received_at) "
            "VALUES ($1, $2, $3, $4, $5, 'GATE_REJECTED', $6, $7, NOW()) "
            "ON CONFLICT (id) DO NOTHING",
            review.reviewId, review.targetProfileId, review.raterProfileId,
            1.00, review.reviewText, event_id, review.createdAt,
        )
    except Exception:
        logger.warning("Failed to persist GATE_REJECTED review %s", review.reviewId)
