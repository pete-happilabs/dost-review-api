"""Batch trigger/status endpoint tests — these routes previously had no
coverage, and the trigger regressed to running the whole batch inline."""
import pytest
from unittest.mock import patch
from uuid import uuid4

from httpx import ASGITransport, AsyncClient

from app.database import get_pool
from app.main import app
from tests.test_batch_engine import _insert_gated, _mock_engine_success


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_trigger_returns_202_and_batch_completes(client):
    pool = get_pool()
    await _insert_gated(pool, uuid4(), 1)

    with patch("app.batch_engine._engine_fn", _mock_engine_success):
        resp = await client.post("/api/v1/batch/trigger")
    assert resp.status_code == 202
    batch_id = resp.json()["batchId"]

    # Background task has run by the time the ASGI call returns
    status_resp = await client.get(f"/api/v1/batch/status/{batch_id}")
    assert status_resp.status_code == 200
    data = status_resp.json()
    assert data["status"] == "COMPLETED"
    assert data["stats"]["committed"] == 1


@pytest.mark.asyncio
async def test_trigger_conflicts_while_batch_running(client):
    pool = get_pool()
    await pool.execute(
        "INSERT INTO batch_run (id, status, started_at) VALUES ($1, 'RUNNING', NOW())",
        uuid4(),
    )
    resp = await client.post("/api/v1/batch/trigger")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_batch_status_not_found(client):
    resp = await client.get(f"/api/v1/batch/status/{uuid4()}")
    assert resp.status_code == 404
