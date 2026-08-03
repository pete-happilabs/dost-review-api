"""C1: Guard access key middleware tests — previously the auth layer had
zero test coverage."""
import pytest
from uuid import uuid4

from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.main import app


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def guard_key(monkeypatch):
    monkeypatch.setattr(settings, "guard_access_key", "test-secret-key")


@pytest.mark.asyncio
async def test_missing_key_rejected(client, guard_key):
    resp = await client.get(f"/api/v1/reputation/{uuid4()}")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_wrong_key_rejected(client, guard_key):
    resp = await client.get(
        f"/api/v1/reputation/{uuid4()}", headers={"X-Access-Key": "wrong-key"}
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_correct_key_accepted(client, guard_key):
    resp = await client.get(
        f"/api/v1/reputation/{uuid4()}", headers={"X-Access-Key": "test-secret-key"}
    )
    # 404 = auth passed, profile simply doesn't exist
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_non_ascii_key_rejected_cleanly(client, guard_key):
    """compare_digest on str requires ASCII — a header byte >= 0x80 used to
    raise TypeError and surface as a 500 instead of a 401."""
    resp = await client.get(
        f"/api/v1/reputation/{uuid4()}", headers={b"X-Access-Key": b"caf\xe9-key"}
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_health_is_public(client, guard_key):
    resp = await client.get("/health")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_no_key_configured_fails_open(client):
    # settings.guard_access_key defaults to "" in tests — auth is disabled
    resp = await client.get(f"/api/v1/reputation/{uuid4()}")
    assert resp.status_code == 404
