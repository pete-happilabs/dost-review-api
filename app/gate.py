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
