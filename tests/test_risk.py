"""Risk service: id bridge, record mapping, and ingest -> engine -> tier."""
import asyncio
import logging
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app import batch_engine
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


def test_low_confidence_signals_are_dropped_before_the_engine():
    # The engine reads only signal NAMES (vendor/engine.py: names = [s.get("name") ...]),
    # so a 0.05-confidence money_ask would otherwise weigh exactly as much as a 0.95 one.
    # But a name the engine keeps STATE for only has to clear the noise floor — dropping it
    # also drops the seed a much later, high-confidence tag reads (see the chain test below).
    rec = _record()
    rec["signals"] = [{"name": "urgency", "confidence": 0.2},         # not a seed, < 0.5
                      {"name": "investment_pitch", "confidence": 0.5},
                      {"name": "money_ask", "confidence": 0.4},       # seed, >= noise floor
                      {"name": "identity_claim", "confidence": 0.05}]  # seed, but noise
    ev = mapping.to_conversation_event(rec)
    assert [s["name"] for s in ev["signals"]] == ["investment_pitch", "money_ask"]


def test_state_seeding_signals_survive_the_confidence_floor():
    # engine._CONV_SIGNAL_STAGES: every one of these seeds conversation state that a later
    # message reads. A soft opening ask is exactly what a model scores low.
    rec = _record()
    rec["signals"] = [{"name": n, "confidence": 0.3} for n in
                      ("channel_shift_request", "identity_claim", "money_ask",
                       "payee_asked_to_act", "payment_claim", "contact_share",
                       "refusal_to_meet")]
    ev = mapping.to_conversation_event(rec)
    assert len(ev["signals"]) == 7


def test_mapping_constants_track_the_vendored_engine():
    # _STATE_SEEDING and _MILESTONE_STAGES are hand-copies of engine._CONV_SIGNAL_STAGES /
    # engine._CONV_MILESTONES, kept local on purpose (importing the engine into a pure
    # mapper would couple them). This test is the seam: a vendor/engine.py refresh — Task 6
    # copies it byte for byte from an engine still under development — that adds a stage
    # name must be mirrored here, or that signal is silently held to the 0.5 floor and the
    # state it seeds is lost, and a new milestone name is silently ignored so
    # contact_share_pre_milestone / money_ask_no_milestone fire on legitimate deals.
    eng = service.load_engine_module()
    assert mapping._STATE_SEEDING == set(eng._CONV_SIGNAL_STAGES)
    assert mapping._MILESTONE_STAGES == set(eng._CONV_MILESTONES)
    assert set(mapping._MILESTONE_LADDER) <= mapping._MILESTONE_STAGES


def test_dropped_low_confidence_signals_are_logged(caplog):
    # A floor that silently drops names turns a gate-side contract break (a renamed or
    # dropped field in a DES bump) into a fraud path that stops working with a green health
    # check. The names it drops have to be answerable from production logs.
    rec = _record()
    rec["signals"] = [{"name": "urgency", "confidence": 0.2},          # not a seed, < 0.5
                      {"name": "identity_claim", "confidence": 0.05},  # seed, below noise
                      {"name": "investment_pitch", "confidence": 0.9}]  # kept
    with caplog.at_level(logging.INFO, logger="app.risk.mapping"):
        ev = mapping.to_conversation_event(rec)
    assert [s["name"] for s in ev["signals"]] == ["investment_pitch"]
    assert "urgency" in caplog.text and "identity_claim" in caplog.text
    assert "investment_pitch" not in caplog.text
    assert rec["eventId"] in caplog.text
    # Nothing dropped -> nothing logged.
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="app.risk.mapping"):
        mapping.to_conversation_event(_record())
    assert caplog.text == ""


def test_low_confidence_is_dropped_but_a_categorical_reject_still_fires():
    rec = _record(verdict="reject", category="otp_request")
    rec["signals"] = [{"name": "money_ask", "confidence": 0.1}]
    ev = mapping.to_conversation_event(rec)
    assert [s["name"] for s in ev["signals"]] == ["categorical_block"]


def test_deal_stage_becomes_an_engine_milestone():
    assert mapping.to_conversation_event(_record())["milestones"] == []   # dealStage "none"
    # The ladder is ordinal (plan line 106): any rung past price_agreed means the price WAS
    # agreed, so the implied rung is emitted too — otherwise engine.stage_ts("price_agreed")
    # stays None and contact_share_pre_milestone fires on a normal post-price number swap.
    expected = {
        "price_agreed": ["price_agreed"],
        "meet_scheduled": ["price_agreed", "meet_scheduled"],
        "ledger_credit": ["price_agreed", "ledger_credit"],
        "handover_confirmed": ["price_agreed", "handover_confirmed"],
        # never synthesised from a later rung: fee_escalation / jumped_deposit read the
        # RECORDED ts of these two, so inventing one would invent a credit time.
        "ledger_credit_small": ["ledger_credit_small"],
    }
    for stage, milestones in expected.items():
        rec = _record()
        rec["context"]["dealStage"] = stage
        got = mapping.to_conversation_event(rec)["milestones"]
        assert got == milestones, stage
        # price_agreed is the ONLY rung ever synthesised.
        assert set(got) - {stage} <= {"price_agreed"}, stage
    rec = _record()
    rec["context"]["dealStage"] = "not_a_stage"
    assert mapping.to_conversation_event(rec)["milestones"] == []
    assert mapping.to_conversation_event(_record(context={}))["milestones"] == []


def test_derived_tags_to_signals_attribute_counterparty():
    derived = [{"tag": "money_ask_no_milestone", "profileId": "hum.a.b.1", "ts": "2026-09-01T10:00:00Z"}]
    sigs = mapping.derived_to_signals(derived, session_id="S1",
                                      participants={"hum.a.b.1", "hum.c.d.2"})
    assert sigs == [{"tag": "money_ask_no_milestone", "conversationId": "S1",
                     "counterpartyId": "hum.c.d.2", "createdAt": "2026-09-01T10:00:00Z"}]


def test_derived_tags_can_be_filtered_to_one_profile():
    # The caller folds the returned list into ONE profile. That is safe only while every
    # tag belongs to that profile, which is true today because one record is mapped per
    # call — profile_id makes it enforced instead of implied.
    derived = [{"tag": "money_ask_no_milestone", "profileId": "hum.a.b.1", "ts": "2026-09-01T10:00:00Z"},
               {"tag": "contact_share_pre_milestone", "profileId": "hum.c.d.2", "ts": "2026-09-01T10:01:00Z"}]
    parts = {"hum.a.b.1", "hum.c.d.2"}
    mine = mapping.derived_to_signals(derived, session_id="S1", participants=parts,
                                      profile_id="hum.a.b.1")
    assert mine == [{"tag": "money_ask_no_milestone", "conversationId": "S1",
                     "counterpartyId": "hum.c.d.2", "createdAt": "2026-09-01T10:00:00Z"}]
    theirs = mapping.derived_to_signals(derived, session_id="S1", participants=parts,
                                        profile_id="hum.c.d.2")
    assert [s["tag"] for s in theirs] == ["contact_share_pre_milestone"]
    assert theirs[0]["counterpartyId"] == "hum.a.b.1"
    # profile_id omitted keeps the old, unfiltered behaviour.
    assert len(mapping.derived_to_signals(derived, session_id="S1", participants=parts)) == 2


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


def _tags(fake_store, profile="hum.a.b.1"):
    return set(fake_store.prof.get(profile, {}).get("state", {}).get("fraudTagStates", {}))


async def test_money_ask_before_milestone_raises_sender_tier(client, fake_store):
    for i, sigs in enumerate([("identity_claim",), (), ("money_ask",)], start=1):
        r = await client.post("/api/v1/signals", json=_rec(i, signals=sigs))
        assert r.status_code == 202, r.text
    prof = fake_store.prof["hum.a.b.1"]
    assert "money_ask_no_milestone" in prof["state"]["fraudTagStates"]
    assert "identity_then_money" in prof["state"]["fraudTagStates"]
    assert prof["risk_tier"] in ("none", "elevated")          # one conversation: not yet high


async def test_money_ask_after_a_milestone_fires_nothing(client, fake_store):
    # dost-talk hard-codes dealStage "none" today; the moment a milestone source lands this
    # is the path that stops money_ask_no_milestone firing on every legitimate seller.
    rec = _rec(1, signals=("money_ask",))
    rec["context"]["dealStage"] = "meet_scheduled"
    r = await client.post("/api/v1/signals", json=rec)
    assert r.status_code == 202, r.text
    assert fake_store.conv["S1"]["stages"].get("meet_scheduled")
    assert "hum.a.b.1" not in fake_store.prof          # no tag fired, so no profile fold


async def test_contact_share_after_a_milestone_fires_nothing(client, fake_store):
    # Decision D4 (plan line 49) and the gate's own prompt: sharing a number once the price
    # is agreed is normal in India. dealStage is an ordinal ladder, so "meet_scheduled"
    # already implies price_agreed — contact_share_pre_milestone must not fire.
    rec = _rec(1, signals=("contact_share",))
    rec["context"]["dealStage"] = "meet_scheduled"
    r = await client.post("/api/v1/signals", json=rec)
    assert r.status_code == 202, r.text
    assert fake_store.conv["S1"]["stages"].get("price_agreed")
    assert "hum.a.b.1" not in fake_store.prof


async def test_noise_confidence_money_ask_fires_nothing(client, fake_store):
    rec = _rec(1, signals=("money_ask",))
    rec["signals"][0]["confidence"] = 0.1
    r = await client.post("/api/v1/signals", json=rec)
    assert r.status_code == 202, r.text
    assert "hum.a.b.1" not in fake_store.prof


async def test_a_soft_opening_ask_still_seeds_fee_escalation(client, fake_store):
    # The advance-fee sequence fee_escalation (weight 6.0) exists to catch: soft opening
    # ask -> deposit -> bigger ask. The opening ask is the one a model scores low, and the
    # engine seeds askers[sender] from it — so dropping it disables the later detection.
    soft = _rec(1, signals=("money_ask",))
    soft["signals"][0]["confidence"] = 0.4
    credited = _rec(2)
    credited["context"]["dealStage"] = "ledger_credit"
    hard = _rec(3, signals=("money_ask",))
    hard["signals"][0]["confidence"] = 0.95
    for rec in (soft, credited, hard):
        r = await client.post("/api/v1/signals", json=rec)
        assert r.status_code == 202, r.text
    assert "fee_escalation" in _tags(fake_store)


async def test_a_low_confidence_identity_claim_still_seeds_identity_then_money(client, fake_store):
    claim = _rec(1, signals=("identity_claim",))
    claim["signals"][0]["confidence"] = 0.4
    for rec in (claim, _rec(2, signals=("money_ask",))):
        r = await client.post("/api/v1/signals", json=rec)
        assert r.status_code == 202, r.text
    assert "identity_then_money" in _tags(fake_store)


async def test_two_senders_in_one_session_do_not_cross_attribute(client, fake_store):
    for i, sender, receiver in ((1, "hum.a.b.1", "hum.c.d.2"), (2, "hum.c.d.2", "hum.a.b.1")):
        r = await client.post("/api/v1/signals",
                              json=_rec(i, sender=sender, receiver=receiver, signals=("money_ask",)))
        assert r.status_code == 202, r.text
    for pid in ("hum.a.b.1", "hum.c.d.2"):
        st = fake_store.prof[pid]["state"]["fraudTagStates"]["money_ask_no_milestone"]
        assert st["count"] == 1, pid


async def test_ingest_folds_only_the_senders_own_tags(client, fake_store, monkeypatch):
    """A caller that ever maps more than one event per call (a replay, the Task 10 queue
    path) must not fold the counterparty's fraud tags into the sender's profile."""
    real = service.load_engine_module()

    class Stub:
        process_signals = staticmethod(real.process_signals)

        @staticmethod
        def process_conversation(data):
            out, metrics = real.process_conversation(data)
            out["derivedTags"].append({"tag": "contact_share_pre_milestone",
                                       "profileId": "hum.c.d.2",
                                       "ts": data["events"][0]["ts"]})
            return out, metrics

    monkeypatch.setattr(service, "load_engine_module", lambda: Stub)
    r = await client.post("/api/v1/signals", json=_rec(1, signals=("money_ask",)))
    assert r.status_code == 202, r.text
    assert _tags(fake_store) == {"money_ask_no_milestone"}
    assert "hum.c.d.2" not in fake_store.prof


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


# ── concurrency and atomicity against the real store ─────────────────────────
# dost-talk forwards records fire-and-forget, so two records for one session
# (or one sender) are routinely in flight together. Each ingest is a
# read-modify-write on conversation_risk and profile_risk: without a per-key
# lock the second writer silently discards the first one's fold.


async def test_concurrent_records_for_one_session_do_not_lose_updates(pool):
    # Record 1 carries the claim and the ask together so identity_then_money fires on it
    # whichever record wins the lock: the assertions cannot depend on lock order.
    a = _rec(1, signals=("identity_claim", "money_ask"))
    b = _rec(2, signals=("money_ask",))
    outs = await asyncio.gather(service.ingest_record(a), service.ingest_record(b))
    assert all(o.get("duplicate") is False for o in outs), outs
    state = await store.PostgresRiskStore().get_conversation_state("S1")
    assert state["turns"] == 2
    assert {"money_ask_no_milestone|hum.a.b.1", "identity_then_money|hum.a.b.1"} <= set(state["fired"])
    assert await pool.fetchval("SELECT count(*) FROM signal_event WHERE session_id='S1'") == 2


async def test_concurrent_records_for_one_sender_across_sessions_count_both(pool):
    a = _rec(1, signals=("money_ask",))
    b = _rec(2, signals=("money_ask",))
    b["sessionId"] = "S2"
    await asyncio.gather(service.ingest_record(a), service.ingest_record(b))
    prof = await store.PostgresRiskStore().get_profile("hum.a.b.1")
    assert prof["state"]["fraudTagStates"]["money_ask_no_milestone"]["count"] == 2


async def test_null_confidence_is_rejected_before_any_write(client, fake_store):
    # A missing confidence is the same contract break as a null one — and the more likely
    # one, since a DES bump that renames or drops the field produces exactly this shape.
    # With a default it used to compose with the mapper's floor into a silent no-op: 202,
    # no tag, nothing logged, and a pydantic-fabricated 0.0 on the stored record so even a
    # forensic reader could not tell the field was absent.
    for bad in ({"name": "money_ask", "confidence": None},
                {"name": "money_ask"},
                {"name": "money_ask", "confidence": "high"}):
        rec = _rec(1)
        rec["signals"] = [bad]
        r = await client.post("/api/v1/signals", json=rec)
        assert r.status_code == 422, (bad, r.text)
        assert fake_store.events == [] and fake_store.conv == {} and fake_store.prof == {}


async def test_failure_after_insert_rolls_the_signal_event_back(pool, monkeypatch):
    real = store.PostgresRiskStore.put_conversation_state
    calls = []

    async def flaky(self, sid, state):
        calls.append(sid)
        if len(calls) == 1:
            raise RuntimeError("db went away")
        await real(self, sid, state)

    monkeypatch.setattr(store.PostgresRiskStore, "put_conversation_state", flaky)
    rec = _rec(1, signals=("money_ask",))
    with pytest.raises(RuntimeError):
        await service.ingest_record(rec)
    # The half-processed record must not be on file, or the retry answers "duplicate"
    # and the fold never happens.
    assert await pool.fetchval("SELECT count(*) FROM signal_event WHERE session_id='S1'") == 0
    out = await service.ingest_record(rec)
    assert out["duplicate"] is False and out["riskTier"] in ("none", "elevated")
    assert await pool.fetchval("SELECT count(*) FROM signal_event WHERE session_id='S1'") == 1
    assert "money_ask_no_milestone|hum.a.b.1" in \
        (await store.PostgresRiskStore().get_conversation_state("S1"))["fired"]


async def test_engine_rejection_leaves_no_signal_event_row(pool, monkeypatch):
    class Stub:
        @staticmethod
        def process_conversation(data):
            return {"conversationId": data["conversationId"],
                    "error": {"message": "events must be a list"}}, {"models": []}

    monkeypatch.setattr(service, "load_engine_module", lambda: Stub)
    out = await service.ingest_record(_rec(1, signals=("money_ask",)))
    assert out == {"error": {"message": "events must be a list"}}
    assert await pool.fetchval("SELECT count(*) FROM signal_event WHERE session_id='S1'") == 0


# ── which engine runs ────────────────────────────────────────────────────────
# ENGINE_PATH wins when set (local engine development); otherwise the vendored,
# reviewed copy — the one the Docker image pins — runs. The resolved file is
# logged so a deployment can see which copy it is on.


def test_vendored_engine_is_the_default_and_carries_the_fraud_family(monkeypatch, caplog):
    monkeypatch.setattr(batch_engine, "_engine_mod", None)
    monkeypatch.setattr(batch_engine, "_engine_fn", None)
    monkeypatch.setattr(settings, "engine_path", "")
    vendored = Path(batch_engine.__file__).resolve().parent.parent / "vendor" / "engine.py"
    with caplog.at_level(logging.INFO, logger="app.batch_engine"):
        mod = batch_engine.load_engine_module()
    assert Path(mod.__file__).resolve() == vendored
    assert callable(mod.process_conversation) and callable(mod.process_signals)
    assert str(vendored) in caplog.text and "vendor" in caplog.text
