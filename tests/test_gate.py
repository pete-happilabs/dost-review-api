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


# ---------------------------------------------------------------------------
# Profile validation against Onboard (opt-in `onboard` fixture — the rest of the
# suite runs with _onboard_pool None, i.e. the validation-disabled path).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validation_disabled_accepts_unknown_profiles(pool):
    """No ONBOARD_DATABASE_URL => no onboard pool => validation skipped entirely.
    Pins the documented 'fail open when unconfigured' behaviour."""
    result = await gate_and_store(_review(), pool)
    assert result.passed is True


@pytest.mark.asyncio
async def test_both_profiles_active_passes(pool, onboard, seed_profile):
    """Regression: asyncpg returns a TEXT id as `str`, so keying `found` by str and
    probing it with UUID objects would reject every valid review."""
    review = _review()
    await seed_profile(review.targetProfileId)
    await seed_profile(review.raterProfileId)
    result = await gate_and_store(review, pool)
    assert result.passed is True
    row = await pool.fetchrow("SELECT status FROM review WHERE id = $1", review.reviewId)
    assert row["status"] == "GATED"


@pytest.mark.asyncio
async def test_unknown_target_rejected(pool, onboard, seed_profile):
    """Regression: `id = ANY($1::uuid[])` cannot run against Onboard's TEXT id
    column, and the fail-open handler turned that into a silent accept — so this
    assertion is what actually proves validation runs."""
    review = _review()
    await seed_profile(review.raterProfileId)
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "TARGET_NOT_FOUND"
    row = await pool.fetchrow(
        "SELECT status, gate_reason FROM review WHERE id = $1", review.reviewId
    )
    assert row["status"] == "GATE_REJECTED"
    assert row["gate_reason"] == "TARGET_NOT_FOUND"


@pytest.mark.asyncio
async def test_unknown_rater_rejected(pool, onboard, seed_profile):
    review = _review()
    await seed_profile(review.targetProfileId)
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "RATER_NOT_FOUND"


@pytest.mark.asyncio
async def test_inactive_target_rejected(pool, onboard, seed_profile):
    review = _review()
    await seed_profile(review.targetProfileId, status="RETIRED")
    await seed_profile(review.raterProfileId)
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "TARGET_INACTIVE"


@pytest.mark.asyncio
async def test_inactive_rater_rejected(pool, onboard, seed_profile):
    """Onboard writes only ACTIVE and RETIRED to profile.status today, but this
    service does not own that column — anything that is not ACTIVE must be
    treated as inactive rather than assumed to be a known value."""
    review = _review()
    await seed_profile(review.targetProfileId)
    await seed_profile(review.raterProfileId, status="SUSPENDED")
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "RATER_INACTIVE"


@pytest.mark.asyncio
async def test_target_is_checked_before_rater(pool, onboard, seed_profile):
    """Pins the documented precedence: with neither profile present the caller is
    told about the target, not the rater."""
    result = await gate_and_store(_review(), pool)
    assert result.error_code == "TARGET_NOT_FOUND"


@pytest.mark.asyncio
async def test_null_status_treated_as_inactive(pool, onboard, seed_profile):
    """Onboard declares status NOT NULL, but this service does not own that schema —
    a NULL must not read as ACTIVE."""
    review = _review()
    await pool.execute("ALTER TABLE profile ALTER COLUMN status DROP NOT NULL")
    await pool.execute(
        "INSERT INTO profile (id, status) VALUES ($1, NULL)", str(review.targetProfileId)
    )
    await seed_profile(review.raterProfileId)
    result = await gate_and_store(review, pool)
    assert result.passed is False
    assert result.error_code == "TARGET_INACTIVE"


@pytest.mark.asyncio
async def test_validation_fails_open_on_onboard_error(pool, onboard):
    """Onboard being broken must not block reviews — the stated design decision."""
    await pool.execute("DROP TABLE profile")
    result = await gate_and_store(_review(), pool)
    assert result.passed is True


@pytest.mark.asyncio
async def test_self_review_beats_profile_validation(pool, onboard):
    """rater == target is immutable for a reviewId, so it is answered without
    spending an Onboard round trip — and keeps its own audit reason."""
    pid = uuid4()
    review = _review(targetProfileId=pid, raterProfileId=pid)
    result = await gate_and_store(review, pool)
    assert result.error_code == "SELF_REVIEW"
    row = await pool.fetchrow("SELECT gate_reason FROM review WHERE id = $1", review.reviewId)
    assert row["gate_reason"] == "SELF_REVIEW"


@pytest.mark.asyncio
async def test_accepted_review_stays_duplicate_after_target_retires(
    pool, onboard, seed_profile
):
    """Idempotency regression: profile status is mutable, so a retry of an
    already-accepted reviewId must still answer DUPLICATE. Answering
    TARGET_INACTIVE would contradict reality — ON CONFLICT DO NOTHING leaves the
    row GATED and the batch commits it regardless."""
    review = _review()
    await seed_profile(review.targetProfileId)
    await seed_profile(review.raterProfileId)
    assert (await gate_and_store(review, pool)).passed is True

    await seed_profile(review.targetProfileId, status="RETIRED")
    result = await gate_and_store(review, pool)
    assert result.error_code == "DUPLICATE"
    row = await pool.fetchrow("SELECT status FROM review WHERE id = $1", review.reviewId)
    assert row["status"] == "GATED"


@pytest.mark.asyncio
async def test_repeat_rejection_refreshes_gate_reason(pool, onboard, seed_profile):
    """The audit row must report why the review is being rejected NOW, not why it
    was rejected the first time — profile state moves independently of us."""
    review = _review()
    await seed_profile(review.raterProfileId)
    assert (await gate_and_store(review, pool)).error_code == "TARGET_NOT_FOUND"

    await seed_profile(review.targetProfileId)
    await seed_profile(review.raterProfileId, status="RETIRED")
    assert (await gate_and_store(review, pool)).error_code == "RATER_INACTIVE"

    row = await pool.fetchrow("SELECT gate_reason FROM review WHERE id = $1", review.reviewId)
    assert row["gate_reason"] == "RATER_INACTIVE"


@pytest.mark.asyncio
async def test_validate_profiles_handles_target_equal_rater(onboard, seed_profile):
    """A two-element array with identical ids returns exactly ONE row; both checks
    must still resolve against it. Unreachable via gate_and_store (self-review is
    answered first), so it is pinned directly."""
    from app.gate import _validate_profiles

    pid = uuid4()
    await seed_profile(pid, status="RETIRED")
    result = await _validate_profiles(pid, pid)
    assert result is not None
    assert result.error_code == "TARGET_INACTIVE"

    await seed_profile(pid)
    assert await _validate_profiles(pid, pid) is None
