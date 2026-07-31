import pytest
from datetime import datetime, timezone
from uuid import uuid4

from app.gate import gate_review
from app.models import DostReview


def _review(**overrides) -> DostReview:
    defaults = {
        "reviewId": uuid4(),
        "targetProfileId": uuid4(),
        "raterProfileId": uuid4(),
        "reviewText": "Great service, very friendly",
        "createdAt": datetime.now(timezone.utc),
    }
    defaults.update(overrides)
    return DostReview(**defaults)


@pytest.mark.asyncio
async def test_gate_passes_valid_review(pool):
    review = _review()
    result = await gate_review(review, pool)
    assert result.passed is True


@pytest.mark.asyncio
async def test_gate_rejects_self_review(pool):
    pid = uuid4()
    review = _review(targetProfileId=pid, raterProfileId=pid)
    result = await gate_review(review, pool)
    assert result.passed is False
    assert result.error_code == "SELF_REVIEW"


@pytest.mark.asyncio
async def test_gate_rejects_duplicate(pool):
    review = _review()
    await pool.execute(
        "INSERT INTO review (id, target_profile_id, rater_profile_id, review_text, status, created_at) "
        "VALUES ($1, $2, $3, $4, 'GATED', $5)",
        review.reviewId, review.targetProfileId, review.raterProfileId,
        review.reviewText, review.createdAt,
    )
    result = await gate_review(review, pool)
    assert result.passed is False
    assert result.error_code == "DUPLICATE"


@pytest.mark.asyncio
async def test_gate_rejects_rate_limited(pool):
    target = uuid4()
    rater = uuid4()
    for _ in range(5):
        await pool.execute(
            "INSERT INTO review (id, target_profile_id, rater_profile_id, review_text, status, created_at) "
            "VALUES ($1, $2, $3, $4, 'GATED', $5)",
            uuid4(), target, rater, "good", datetime.now(timezone.utc),
        )
    review = _review(targetProfileId=target, raterProfileId=rater)
    result = await gate_review(review, pool)
    assert result.passed is False
    assert result.error_code == "RATE_LIMITED"
