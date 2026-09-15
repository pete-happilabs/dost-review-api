"""Risk service: id bridge, record mapping, and ingest -> engine -> tier."""
import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.main import app
from app.risk import ids, mapping, service, store

# ── ids + mapping (pure functions) ───────────────────────────────────────────


def test_is_chat_wire_id():
    assert ids.is_wire_id("hum.rahul.sharma.0001")
    assert ids.is_wire_id("agent.dpa.dost")
    assert not ids.is_wire_id("3f9c2b8e-6a41-4d2e-9c77-1b0a5e3d8f42")


def _record(**over):
    r = {
        "modelVersion": "gate-2026-09-02", "ts": "2026-09-01T10:00:00.000Z",
        "eventId": "E1", "sessionId": "S1",
        "srcProfileId": "hum.a.b.1", "dstProfileId": "hum.c.d.2",
        "verdict": "accept", "category": "safe", "tier": None,
        "context": {"turnIndex": 3, "dealStage": "none", "senderTier": "none",
                    "counterpartyTenureDays": None, "isFirstContact": False},
        "textHash": "abc", "textLength": 20,
        "signals": [{"name": "money_ask", "confidence": 0.8, "tier": "text_model",
                     "detail": "token", "entities": []}],
    }
    r.update(over); return r


def test_record_to_conversation_event():
    ev = mapping.to_conversation_event(_record())
    assert ev == {"ts": "2026-09-01T10:00:00.000Z", "senderId": "hum.a.b.1",
                  "signals": [{"name": "money_ask", "confidence": 0.8}],
                  "milestones": [], "textHash": "abc", "turnIndex": 3}


def test_categorical_reject_becomes_a_milestone_free_tag():
    ev = mapping.to_conversation_event(_record(verdict="reject", category="otp_request"))
    assert ev["signals"][0]["name"] == "categorical_block" or \
        any(s["name"] == "categorical_block" for s in ev["signals"])


def test_derived_tags_to_signals_attribute_counterparty():
    derived = [{"tag": "money_ask_no_milestone", "profileId": "hum.a.b.1", "ts": "2026-09-01T10:00:00Z"}]
    sigs = mapping.derived_to_signals(derived, session_id="S1",
                                      participants={"hum.a.b.1", "hum.c.d.2"})
    assert sigs == [{"tag": "money_ask_no_milestone", "conversationId": "S1",
                     "counterpartyId": "hum.c.d.2", "createdAt": "2026-09-01T10:00:00Z"}]


# ── ingest -> engine -> tier, against a fake store ───────────────────────────


class FakeStore(store.RiskStore):
    def __init__(self):
        self.conv, self.prof, self.events, self.outcomes, self.queue = {}, {}, [], [], []
    async def get_conversation_state(self, sid): return self.conv.get(sid)
    async def put_conversation_state(self, sid, state): self.conv[sid] = state
    async def get_profile(self, pid): return self.prof.get(pid)
    async def put_profile(self, pid, **row): self.prof[pid] = row
    async def insert_signal_event(self, rec): self.events.append(rec); return True
    async def insert_outcome(self, **row): self.outcomes.append(row)
    async def enqueue_review(self, **row): self.queue.append(row)
    async def participants_of(self, sid, fallback): return fallback


@pytest.fixture
def fake_store(monkeypatch):
    fs = FakeStore()
    monkeypatch.setattr(service, "get_store", lambda: fs)
    return fs


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t",
                           headers={"x-access-key": "test"}) as c:
        yield c


def _rec(i, sender="hum.a.b.1", receiver="hum.c.d.2", signals=(), verdict="accept", category="safe"):
    return {"modelVersion": "g", "ts": f"2026-09-01T10:{i:02d}:00.000Z", "eventId": f"E{i}",
            "sessionId": "S1", "srcProfileId": sender, "dstProfileId": receiver,
            "verdict": verdict, "category": category, "tier": None,
            "context": {"turnIndex": i, "dealStage": "none", "senderTier": "none",
                        "counterpartyTenureDays": None, "isFirstContact": i == 1},
            "textHash": f"h{i}", "textLength": 10,
            "signals": [{"name": s, "confidence": 0.9, "tier": "text_model", "detail": "", "entities": []}
                        for s in signals]}


async def test_money_ask_before_milestone_raises_sender_tier(client, fake_store):
    for i, sigs in enumerate([("identity_claim",), (), ("money_ask",)], start=1):
        r = await client.post("/api/v1/signals", json=_rec(i, signals=sigs))
        assert r.status_code == 202, r.text
    prof = fake_store.prof["hum.a.b.1"]
    assert "money_ask_no_milestone" in prof["state"]["fraudTagStates"]
    assert "identity_then_money" in prof["state"]["fraudTagStates"]
    assert prof["risk_tier"] in ("none", "elevated")          # one conversation: not yet high


async def test_duplicate_event_is_idempotent(client, fake_store):
    rec = _rec(1, signals=("money_ask",))
    await client.post("/api/v1/signals", json=rec)
    fake_store.insert_signal_event = lambda r: _false()          # second insert reports duplicate
    r = await client.post("/api/v1/signals", json=rec)
    assert r.status_code == 200 and r.json()["duplicate"] is True


async def _false(): return False


async def test_tier_read(client, fake_store):
    fake_store.prof["hum.x.y.9"] = {"state": {}, "risk_score": 71.0, "risk_tier": "high",
                                    "repeated_across": [], "account_id": None}
    r = await client.get("/api/v1/risk/profile/hum.x.y.9")
    assert r.status_code == 200 and r.json() == {"profileId": "hum.x.y.9", "riskTier": "high",
                                                 "riskScore": 71.0, "repeatedAcross": []}
    r = await client.get("/api/v1/risk/profile/hum.unknown")
    assert r.status_code == 200 and r.json()["riskTier"] == "none"


async def test_outcome_report_folds_and_queues_above_threshold(client, fake_store):
    r = await client.post("/api/v1/outcomes", json={
        "profileId": "hum.a.b.1", "reporterId": "hum.c.d.2", "sessionId": "S1",
        "tag": "paid_not_delivered", "evidence": {"amount": 5000}})
    assert r.status_code == 202
    assert fake_store.prof["hum.a.b.1"]["risk_tier"] in ("high", "critical")
    assert fake_store.queue and fake_store.queue[0]["reason"] == "outcome:paid_not_delivered"


async def test_unknown_outcome_tag_is_rejected(client, fake_store):
    r = await client.post("/api/v1/outcomes", json={"profileId": "hum.a.b.1", "tag": "made_up"})
    assert r.status_code == 422
    assert not fake_store.outcomes and "hum.a.b.1" not in fake_store.prof


async def test_risk_routes_inherit_the_access_key_check(client, fake_store, monkeypatch):
    monkeypatch.setattr(settings, "guard_access_key", "test-secret-key")
    r = await client.get("/api/v1/risk/profile/hum.x.y.9", headers={"x-access-key": "wrong"})
    assert r.status_code == 401


# ── PostgresRiskStore against the real 004_risk schema ───────────────────────
# asyncpg hands JSONB back as str in this service (see routes/reputation.py), so
# the store must decode before the engine sees the state blob.


async def test_pg_store_round_trips_json_columns(pool):
    pg = store.PostgresRiskStore()
    assert await pg.get_conversation_state("S-pg") is None
    await pg.put_conversation_state("S-pg", {"turns": 2, "fired": ["a|b"]})
    assert await pg.get_conversation_state("S-pg") == {"turns": 2, "fired": ["a|b"]}

    assert await pg.get_profile("hum.pg.1") is None
    await pg.put_profile("hum.pg.1", account_id=None, state={"fraudTagStates": {}},
                         risk_score=63.2, risk_tier="high", repeated_across=["template_reuse"])
    row = await pg.get_profile("hum.pg.1")
    assert row["state"] == {"fraudTagStates": {}}
    assert row["repeated_across"] == ["template_reuse"]
    assert float(row["risk_score"]) == 63.2 and row["risk_tier"] == "high"
    # A later write that does not know the account id must not erase it.
    await pg.put_profile("hum.pg.1", account_id="acct-1", state={}, risk_score=0.0, risk_tier="none")
    await pg.put_profile("hum.pg.1", account_id=None, state={}, risk_score=0.0, risk_tier="none")
    assert (await pg.get_profile("hum.pg.1"))["account_id"] == "acct-1"


async def test_pg_store_signal_event_reports_duplicate(pool):
    pg = store.PostgresRiskStore()
    rec = _rec(1, signals=("money_ask",))
    assert await pg.insert_signal_event(rec) is True
    assert await pg.insert_signal_event(rec) is False


async def test_pg_store_outcome_and_queue_rows(pool):
    pg = store.PostgresRiskStore()
    await pg.insert_outcome(profile_id="hum.pg.2", reporter_id="hum.pg.3", session_id="S1",
                            tag="paid_not_delivered", evidence={"amount": 5000})
    await pg.enqueue_review(profile_id="hum.pg.2", session_id="S1",
                            reason="outcome:paid_not_delivered", risk_score=63.2,
                            evidence={"riskTags": {"paid_not_delivered": 1.0}})
    assert await pool.fetchval("SELECT count(*) FROM outcome_event WHERE profile_id='hum.pg.2'") == 1
    q = await pool.fetchrow("SELECT status, reason FROM review_queue WHERE profile_id='hum.pg.2'")
    assert q["status"] == "OPEN" and q["reason"] == "outcome:paid_not_delivered"


async def test_ingest_end_to_end_through_postgres(client, pool):
    for i, sigs in enumerate([("identity_claim",), ("money_ask",)], start=1):
        r = await client.post("/api/v1/signals", json=_rec(i, signals=sigs))
        assert r.status_code == 202, r.text
    r = await client.post("/api/v1/signals", json=_rec(2, signals=("money_ask",)))
    assert r.status_code == 200 and r.json() == {"duplicate": True}
    r = await client.get("/api/v1/risk/profile/hum.a.b.1")
    assert r.status_code == 200 and r.json()["riskTier"] in ("none", "elevated")
    assert await pool.fetchval("SELECT count(*) FROM signal_event WHERE session_id='S1'") == 2
    state = await store.PostgresRiskStore().get_conversation_state("S1")
    assert "money_ask_no_milestone|hum.a.b.1" in state["fired"]
