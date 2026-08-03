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


@pytest.mark.asyncio
async def test_gate_rejection_reason_persisted(pool):
    """M1 completion: the audit row must record WHY it was rejected —
    the reason argument was previously dropped on the floor."""
    pid = uuid4()
    review = _review(targetProfileId=pid, raterProfileId=pid)
    await gate_and_store(review, pool)
    row = await pool.fetchrow(
        "SELECT status, gate_reason FROM review WHERE id = $1", review.reviewId
    )
    assert row["status"] == "GATE_REJECTED"
    assert row["gate_reason"] == "SELF_REVIEW"


@pytest.mark.asyncio
async def test_rejected_rows_do_not_consume_rate_limit(pool):
    """M1 regression: persisting rejections must not change rate-limit
    semantics — only accepted reviews consume quota. Otherwise rejected
    attempts would extend the lockout indefinitely."""
    target = uuid4()
    rater = uuid4()
    for _ in range(5):
        await pool.execute(
            "INSERT INTO review (id, target_profile_id, rater_profile_id, rater_weight, "
            "review_text, status, gate_reason, created_at, received_at) "
            "VALUES ($1, $2, $3, 1.00, 'rejected earlier', 'GATE_REJECTED', "
            "'RATE_LIMITED', NOW(), NOW())",
            uuid4(), target, rater,
        )
    review = _review(targetProfileId=target, raterProfileId=rater)
    result = await gate_and_store(review, pool)
    assert result.passed is True


@pytest.mark.asyncio
async def test_resubmit_after_rejection_allowed(pool):
    """A GATE_REJECTED row is an audit record, not a permanent claim on the
    reviewId — resubmitting the same id after the block clears must succeed
    (previously it 409'd as DUPLICATE forever)."""
    review = _review()
    await pool.execute(
        "INSERT INTO review (id, target_profile_id, rater_profile_id, rater_weight, "
        "review_text, status, gate_reason, created_at, received_at) "
        "VALUES ($1, $2, $3, 1.00, $4, 'GATE_REJECTED', 'RATE_LIMITED', NOW(), NOW())",
        review.reviewId, review.targetProfileId, review.raterProfileId, review.reviewText,
    )
    result = await gate_and_store(review, pool)
    assert result.passed is True
    row = await pool.fetchrow(
        "SELECT status, gate_reason FROM review WHERE id = $1", review.reviewId
    )
    assert row["status"] == "GATED"
    assert row["gate_reason"] is None


@pytest.mark.asyncio
async def test_duplicate_of_committed_review_still_rejected(pool):
    """Resubmission is only allowed over GATE_REJECTED — a COMMITTED review
    must never be overwritten."""
    review = _review()
    await pool.execute(
        "INSERT INTO review (id, target_profile_id, rater_profile_id, rater_weight, "
        "review_text, status, created_at, received_at) "
        "VALUES ($1, $2, $3, 1.00, $4, 'COMMITTED', NOW(), NOW())",
        review.reviewId, review.targetProfileId, review.raterProfileId, review.reviewText,
    )
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "DUPLICATE"
