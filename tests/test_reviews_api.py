import base64
import json

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_submit_valid_review(client):
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Great biryani, on time delivery",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "GATED"


@pytest.mark.asyncio
async def test_submit_self_review_rejected(client):
    pid = str(uuid4())
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": pid,
        "raterProfileId": pid,
        "reviewText": "I am great",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_submit_duplicate_review(client):
    rid = str(uuid4())
    body = {
        "reviewId": rid,
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Tasty food",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp1 = await client.post("/api/v1/reviews", json=body)
    assert resp1.status_code == 201
    resp2 = await client.post("/api/v1/reviews", json=body)
    assert resp2.status_code == 409


@pytest.mark.asyncio
async def test_submit_dost_event_envelope(client):
    review_data = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Amazing service",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    inner_event = {"message": {"text": json.dumps(review_data)}}
    encrypted = base64.b64encode(json.dumps(inner_event).encode()).decode()

    envelope = {
        "version": "01.00.00",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "eventId": str(uuid4()),
        "sessionId": str(uuid4()),
        "eventType": "MSG_START",
        "isDostListening": False,
        "security": {"header": "", "nonce": "", "tag": ""},
        "encryptedEvent": encrypted,
    }
    resp = await client.post("/api/v1/reviews", json=envelope)
    assert resp.status_code == 201
    assert resp.json()["status"] == "GATED"
