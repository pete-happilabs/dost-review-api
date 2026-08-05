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


@pytest.mark.asyncio
async def test_submit_future_date_rejected(client):
    """createdAt more than 1 hour in the future should be rejected."""
    from datetime import timedelta

    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Time traveler review",
        "createdAt": (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert any("future" in str(d).lower() for d in detail)


@pytest.mark.asyncio
async def test_submit_ancient_date_rejected(client):
    """createdAt more than 1 year in the past should be rejected."""
    from datetime import timedelta

    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Ancient review",
        "createdAt": (datetime.now(timezone.utc) - timedelta(days=400)).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert any("past" in str(d).lower() or "year" in str(d).lower() for d in detail)


@pytest.mark.asyncio
async def test_submit_multimedia_over_10_items(client):
    """Multimedia list capped at 10 items."""
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Too many photos",
        "multimedia": [{"type": "image", "url": f"https://example.com/{i}.jpg"} for i in range(11)],
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_submit_multimedia_over_100kb(client):
    """Multimedia payload over 100KB serialized should be rejected."""
    big_item = {"type": "image", "data": "x" * 120_000}
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Huge multimedia",
        "multimedia": [big_item],
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_submit_valid_multimedia_accepted(client):
    """A valid multimedia list under limits should be accepted."""
    body = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Photos attached",
        "multimedia": [
            {"type": "image", "url": "https://example.com/photo1.jpg"},
            {"type": "image", "url": "https://example.com/photo2.jpg"},
        ],
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 201


# ---------------------------------------------------------------------------
# Profile validation — HTTP contract
# ---------------------------------------------------------------------------


def _body(**overrides):
    b = {
        "reviewId": str(uuid4()),
        "targetProfileId": str(uuid4()),
        "raterProfileId": str(uuid4()),
        "reviewText": "Prompt and polite",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    b.update(overrides)
    return b


@pytest.mark.asyncio
async def test_api_unknown_target_returns_422(client, pool, onboard, seed_profile):
    body = _body()
    await seed_profile(body["raterProfileId"])
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 422
    # detail is a plain string here, unlike the list-of-fields a payload
    # validation error returns — pinned so callers can rely on the shape.
    assert resp.json()["detail"] == "Target profile does not exist"


@pytest.mark.asyncio
async def test_api_inactive_target_returns_422(client, pool, onboard, seed_profile):
    body = _body()
    await seed_profile(body["targetProfileId"], status="RETIRED")
    await seed_profile(body["raterProfileId"])
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 422
    assert resp.json()["detail"] == "Target profile is not active"


@pytest.mark.asyncio
async def test_api_unknown_rater_returns_422(client, pool, onboard, seed_profile):
    body = _body()
    await seed_profile(body["targetProfileId"])
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 422
    assert resp.json()["detail"] == "Rater profile does not exist"


@pytest.mark.asyncio
async def test_api_valid_profiles_still_accepted(client, pool, onboard, seed_profile):
    body = _body()
    await seed_profile(body["targetProfileId"])
    await seed_profile(body["raterProfileId"])
    resp = await client.post("/api/v1/reviews", json=body)
    assert resp.status_code == 201
    assert resp.json()["status"] == "GATED"
