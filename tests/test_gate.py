import pytest
from datetime import datetime, timezone
from uuid import uuid4

from app.gate import gate_and_store
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
    result = await gate_and_store(review, pool)
    assert result.passed is True
    # Verify review was stored as GATED
    row = await pool.fetchrow("SELECT status FROM review WHERE id = $1", review.reviewId)
    assert row["status"] == "GATED"


@pytest.mark.asyncio
async def test_gate_rejects_self_review(pool):
    pid = uuid4()
    review = _review(targetProfileId=pid, raterProfileId=pid)
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "SELF_REVIEW"
    # M1: Verify rejected review is persisted for audit
    row = await pool.fetchrow("SELECT status FROM review WHERE id = $1", review.reviewId)
    assert row is not None
    assert row["status"] == "GATE_REJECTED"


@pytest.mark.asyncio
async def test_gate_rejects_duplicate(pool):
    review = _review()
    # First submission succeeds
    result1 = await gate_and_store(review, pool)
    assert result1.passed is True
    # Second submission is a duplicate
    result2 = await gate_and_store(review, pool)
    assert result2.passed is False
    assert result2.error_code == "DUPLICATE"


@pytest.mark.asyncio
async def test_gate_rejects_rate_limited(pool):
    target = uuid4()
    rater = uuid4()
    # Submit 5 reviews from same rater to same target
    for _ in range(5):
        r = _review(targetProfileId=target, raterProfileId=rater)
        await gate_and_store(r, pool)
    # 6th should be rate limited
    review = _review(targetProfileId=target, raterProfileId=rater)
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "RATE_LIMITED"
