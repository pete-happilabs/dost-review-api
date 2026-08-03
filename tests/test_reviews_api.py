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


# H4: Malformed body tests
@pytest.mark.asyncio
async def test_submit_non_json_body(client):
    resp = await client.post(
        "/api/v1/reviews",
        content=b"not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_submit_json_array_body(client):
    resp = await client.post("/api/v1/reviews", json=[1, 2, 3])
    assert resp.status_code == 400


# Envelope failure tests
@pytest.mark.asyncio
async def test_submit_envelope_bad_base64(client):
    envelope = {
        "eventType": "MSG_START",
        "eventId": str(uuid4()),
        "encryptedEvent": "not-valid-base64!!!",
    }
    resp = await client.post("/api/v1/reviews", json=envelope)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_submit_envelope_missing_text(client):
    inner = {"message": {}}
    encrypted = base64.b64encode(json.dumps(inner).encode()).decode()
    envelope = {
        "eventType": "MSG_START",
        "eventId": str(uuid4()),
        "encryptedEvent": encrypted,
    }
    resp = await client.post("/api/v1/reviews", json=envelope)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_submit_envelope_invalid_event_id(client):
    review_data = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Good",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    inner = {"message": {"text": json.dumps(review_data)}}
    encrypted = base64.b64encode(json.dumps(inner).encode()).decode()
    envelope = {
        "eventType": "MSG_START",
        "eventId": "not-a-uuid",
        "encryptedEvent": encrypted,
    }
    resp = await client.post("/api/v1/reviews", json=envelope)
    assert resp.status_code == 400


# Rate limit test at API level
@pytest.mark.asyncio
async def test_submit_rate_limited(client):
    target = str(uuid4())
    rater = str(uuid4())
    for i in range(5):
        body = {
            "reviewId": str(uuid4()),
            "targetProfileId": target,
            "raterProfileId": rater,
            "reviewText": f"Review number {i+1}",
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        resp = await client.post("/api/v1/reviews", json=body)
        assert resp.status_code == 201
    # 6th should be 429
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": target,
        "raterProfileId": rater,
        "reviewText": "One too many",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 429


@pytest.mark.asyncio
async def test_submit_naive_datetime_accepted(client):
    """A createdAt without timezone info previously raised an aware-vs-naive
    TypeError inside the validator and surfaced as an opaque 400 — it is now
    interpreted as UTC."""
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "No timezone on my clock",
        "createdAt": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_validation_error_names_the_field(client):
    """Q2 follow-up: rejections should say which field failed, not just
    'Invalid review payload'."""
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "",  # invalid: min_length=1
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert any("reviewText" in d["field"] for d in detail)
