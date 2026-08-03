import json

import pytest
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import uuid4

from app.batch_engine import run_batch, _merge_tags
from app.batch_runner import sweep_stale_batches
from app.database import get_pool


def _mock_engine_success(data):
    pid = data["profileId"] if isinstance(data, dict) else data[0]["profileId"]
    n = len(data["reviews"]) if isinstance(data, dict) else 1
    output = {
        "profileId": pid,
        "reputation": "very good",
        "reputationScore": 3.7,
        "totalReviews": n,
        "topTags": [{"tag": "on-time-delivery", "weight": 5, "score": 4.1}],
        "allTags": {"on-time-delivery": 5, "tasty": 3},
        "summary": "Good food place.",
        "state": {"lastUpdated": datetime.now(timezone.utc).isoformat(), "totalReviews": n, "tagStates": {}},
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    return output, {"models": []}


def _mock_engine_failure(data):
    pid = data["profileId"] if isinstance(data, dict) else data[0]["profileId"]
    return {"profileId": pid, "error": {"message": "AI failed", "willRetry": True}}, {"models": []}


def _mock_engine_no_retry(data):
    pid = data["profileId"] if isinstance(data, dict) else data[0]["profileId"]
    return {"profileId": pid, "error": {"message": "permanent failure", "willRetry": False}}, {"models": []}


async def _insert_gated(pool, target, count=1):
    """Helper to insert GATED reviews."""
    for _ in range(count):
        await pool.execute(
            "INSERT INTO review (id, target_profile_id, rater_profile_id, rater_weight, "
            "review_text, status, created_at, received_at, retry_count) "
            "VALUES ($1, $2, $3, $4, $5, 'GATED', $6, NOW(), 0)",
            uuid4(), target, uuid4(), 1.00, "Great food", datetime.now(timezone.utc),
        )


@pytest.mark.asyncio
async def test_batch_processes_gated_reviews():
    pool = get_pool()
    target = uuid4()
    await _insert_gated(pool, target, 2)

    with patch("app.batch_engine._engine_fn", _mock_engine_success):
        stats = await run_batch(pool, uuid4())

    assert stats["committed"] == 2
    assert stats["profilesUpdated"] == 1

    # C2: Verify reviews are COMMITTED (not double-processed)
    count = await pool.fetchval(
        "SELECT COUNT(*) FROM review WHERE target_profile_id = $1 AND status = 'COMMITTED'", target
    )
    assert count == 2

    row = await pool.fetchrow("SELECT * FROM profile_reputation WHERE profile_id = $1", target)
    assert row is not None
    assert row["reputation"] == "very good"
    assert float(row["reputation_score"]) == 3.7


@pytest.mark.asyncio
async def test_batch_engine_failure_keeps_reviews_gated():
    pool = get_pool()
    target = uuid4()
    await _insert_gated(pool, target, 1)

    with patch("app.batch_engine._engine_fn", _mock_engine_failure):
        stats = await run_batch(pool, uuid4())

    assert stats["failed"] == 1
    # H3: Review should be back to GATED with retry_count incremented
    row = await pool.fetchrow(
        "SELECT status, retry_count FROM review WHERE target_profile_id = $1", target
    )
    assert row["status"] == "GATED"
    assert row["retry_count"] == 1


@pytest.mark.asyncio
async def test_batch_engine_no_retry_dead_letters():
    """H3: willRetry=false should immediately dead-letter."""
    pool = get_pool()
    target = uuid4()
    await _insert_gated(pool, target, 1)

    with patch("app.batch_engine._engine_fn", _mock_engine_no_retry):
        stats = await run_batch(pool, uuid4())

    assert stats["deadLettered"] == 1
    row = await pool.fetchrow("SELECT status FROM review WHERE target_profile_id = $1", target)
    assert row["status"] == "FAILED"


@pytest.mark.asyncio
async def test_batch_empty_no_error():
    pool = get_pool()
    stats = await run_batch(pool, uuid4())
    assert stats["committed"] == 0
    assert stats["profilesUpdated"] == 0


# Unit test for _merge_tags
def test_merge_tags_combines_top_and_all():
    engine_output = {
        "topTags": [
            {"tag": "on-time-delivery", "weight": 28, "score": 4.1},
            {"tag": "product-quality", "weight": 24, "score": 3.8},
        ],
        "allTags": {
            "on-time-delivery": 28,
            "product-quality": 24,
            "tasty": 8,
            "late": 3,
        },
    }
    merged = _merge_tags(engine_output)
    # topTags entries get weight + score
    assert merged["on-time-delivery"] == {"weight": 28, "score": 4.1}
    assert merged["product-quality"] == {"weight": 24, "score": 3.8}
    # allTags-only entries get weight only (no score)
    assert merged["tasty"] == {"weight": 8}
    assert merged["late"] == {"weight": 3}


def test_merge_tags_empty_inputs():
    assert _merge_tags({}) == {}
    assert _merge_tags({"topTags": [], "allTags": {}}) == {}


def test_merge_tags_top_overrides_all():
    """topTags score should override allTags count-only entry."""
    engine_output = {
        "topTags": [{"tag": "tasty", "weight": 10, "score": 4.5}],
        "allTags": {"tasty": 8},  # count differs from topTags weight
    }
    merged = _merge_tags(engine_output)
    # topTags entry wins
    assert merged["tasty"] == {"weight": 10, "score": 4.5}


# Engine input shape test
@pytest.mark.asyncio
async def test_batch_engine_input_shape():
    """Verify the engine receives correctly shaped input."""
    pool = get_pool()
    target = uuid4()
    await _insert_gated(pool, target, 1)

    captured_input = {}

    def capture_engine(data):
        captured_input.update(data)
        return _mock_engine_success(data)

    with patch("app.batch_engine._engine_fn", capture_engine):
        await run_batch(pool, uuid4())

    # Verify engine contract
    assert "profileId" in captured_input
    assert "reviews" in captured_input
    assert len(captured_input["reviews"]) == 1
    review = captured_input["reviews"][0]
    assert "description" in review  # NOT "reviewText"
    assert "createdAt" in review
    assert "about" in captured_input


@pytest.mark.asyncio
async def test_sweep_requeues_orphaned_processing_reviews():
    """H3 regression: a crash mid-batch left claimed reviews stuck in
    PROCESSING forever — pickup only selects GATED, so they were silently
    lost. The sweep must both fail the stale batch AND requeue its reviews."""
    pool = get_pool()
    stale_batch = uuid4()
    await pool.execute(
        "INSERT INTO batch_run (id, status, started_at) "
        "VALUES ($1, 'RUNNING', NOW() - INTERVAL '2 hours')",
        stale_batch,
    )
    review_id = uuid4()
    await pool.execute(
        "INSERT INTO review (id, target_profile_id, rater_profile_id, rater_weight, "
        "review_text, status, batch_id, created_at, received_at) "
        "VALUES ($1, $2, $3, 1.00, 'orphaned', 'PROCESSING', $4, NOW(), NOW())",
        review_id, uuid4(), uuid4(), stale_batch,
    )

    await sweep_stale_batches(pool)

    assert await pool.fetchval(
        "SELECT status FROM batch_run WHERE id = $1", stale_batch
    ) == "FAILED"
    assert await pool.fetchval(
        "SELECT status FROM review WHERE id = $1", review_id
    ) == "GATED"


@pytest.mark.asyncio
async def test_sweep_leaves_fresh_running_batch_alone():
    pool = get_pool()
    fresh_batch = uuid4()
    await pool.execute(
        "INSERT INTO batch_run (id, status, started_at) VALUES ($1, 'RUNNING', NOW())",
        fresh_batch,
    )
    await sweep_stale_batches(pool)
    assert await pool.fetchval(
        "SELECT status FROM batch_run WHERE id = $1", fresh_batch
    ) == "RUNNING"
