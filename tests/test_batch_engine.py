import pytest
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import uuid4

from app.batch_engine import run_batch
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


@pytest.mark.asyncio
async def test_batch_processes_gated_reviews():
    pool = get_pool()
    target = uuid4()
    for _ in range(2):
        await pool.execute(
            "INSERT INTO review (id, target_profile_id, rater_profile_id, review_text, status, created_at) "
            "VALUES ($1, $2, $3, $4, 'GATED', $5)",
            uuid4(), target, uuid4(), "Great food", datetime.now(timezone.utc),
        )

    with patch("app.batch_engine.process_reputation", side_effect=_mock_engine_success):
        stats = await run_batch(pool, uuid4())

    assert stats["committed"] == 2
    assert stats["profilesUpdated"] == 1

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
    await pool.execute(
        "INSERT INTO review (id, target_profile_id, rater_profile_id, review_text, status, created_at) "
        "VALUES ($1, $2, $3, $4, 'GATED', $5)",
        uuid4(), target, uuid4(), "Bad experience", datetime.now(timezone.utc),
    )

    with patch("app.batch_engine.process_reputation", side_effect=_mock_engine_failure):
        stats = await run_batch(pool, uuid4())

    assert stats["failed"] == 1
    status = await pool.fetchval("SELECT status FROM review WHERE target_profile_id = $1", target)
    assert status == "GATED"


@pytest.mark.asyncio
async def test_batch_empty_no_error():
    pool = get_pool()
    stats = await run_batch(pool, uuid4())
    assert stats["committed"] == 0
    assert stats["profilesUpdated"] == 0
