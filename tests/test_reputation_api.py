import json

import pytest
from uuid import uuid4

from httpx import ASGITransport, AsyncClient

from app.database import get_pool
from app.main import app


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_get_reputation_not_found(client):
    resp = await client.get(f"/api/v1/reputation/{uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_reputation_found(client):
    pool = get_pool()
    pid = uuid4()
    all_tags = {"on-time-delivery": {"weight": 28, "score": 4.1}, "late": {"weight": 3}}
    await pool.execute(
        "INSERT INTO profile_reputation "
        "(profile_id, reputation, reputation_score, total_reviews, all_tags, summary, state) "
        "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb)",
        pid, "very good", 3.7, 42, json.dumps(all_tags), "Great food place", json.dumps({}),
    )
    resp = await client.get(f"/api/v1/reputation/{pid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["reputation"] == "very good"
    assert data["reputationScore"] == 3.7
    assert "on-time-delivery" in data["allTags"]
    assert data["allTags"]["on-time-delivery"]["weight"] == 28
    assert data["allTags"]["on-time-delivery"]["score"] == 4.1
    assert data["allTags"]["late"]["score"] is None  # no score for non-top tags
