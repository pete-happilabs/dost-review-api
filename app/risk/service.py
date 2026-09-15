"""One gate record in -> conversation stage machine -> sender's account state -> tier."""
from __future__ import annotations

from typing import Any

from app.batch_engine import load_engine_module  # already loads vendor/engine.py
from app.risk import mapping
from app.risk.store import PostgresRiskStore, RiskStore

_store: RiskStore | None = None
QUEUE_AT = 60.0   # risk score at which a human looks (research §4 starting bands)


def get_store() -> RiskStore:
    global _store
    if _store is None:
        _store = PostgresRiskStore()
    return _store


async def ingest_record(record: dict[str, Any]) -> dict[str, Any]:
    """One gate record → conversation stage machine → sender's account state → tier."""
    engine = load_engine_module()
    store = get_store()

    inserted = await store.insert_signal_event(record)
    if not inserted:
        return {"duplicate": True}

    session_id = record["sessionId"]
    sender = record["srcProfileId"]
    receiver = record.get("dstProfileId")

    conv_state = await store.get_conversation_state(session_id)
    conv_out, _ = engine.process_conversation({
        "conversationId": session_id, "state": conv_state,
        "events": [mapping.to_conversation_event(record)],
    })
    if "error" in conv_out:
        return {"error": conv_out["error"]}
    await store.put_conversation_state(session_id, conv_out["state"])

    if not conv_out["derivedTags"]:
        return {"duplicate": False, "derived": [], "riskTier": await _tier_of(sender)}

    participants = await store.participants_of(session_id, {p for p in (sender, receiver) if p})
    signals = mapping.derived_to_signals(conv_out["derivedTags"], session_id=session_id,
                                         participants=participants)
    return await _fold(sender, signals=signals, outcomes=[], session_id=session_id)


async def ingest_outcome(*, profile_id: str, tag: str, reporter_id: str | None,
                         session_id: str | None, evidence: dict) -> dict[str, Any]:
    store = get_store()
    await store.insert_outcome(profile_id=profile_id, reporter_id=reporter_id,
                               session_id=session_id, tag=tag, evidence=evidence)
    return await _fold(profile_id, signals=[],
                       outcomes=[{"tag": tag, "conversationId": session_id or f"outcome:{tag}",
                                  "counterpartyId": reporter_id}],
                       session_id=session_id, reason=f"outcome:{tag}", evidence=evidence)


async def _fold(profile_id: str, *, signals, outcomes, session_id, reason=None, evidence=None):
    engine = load_engine_module()
    store = get_store()
    prev = await store.get_profile(profile_id) or {}
    out, _ = engine.process_signals({
        "profileId": profile_id, "state": prev.get("state"),
        # 0 until Onboard's tenure read exists: the engine then discounts velocity
        # evidence for young accounts, which is the conservative default.
        "tenureDays": prev.get("tenure_days", 0),
        "signals": signals, "outcomes": outcomes,
    })
    if "error" in out:
        return {"error": out["error"]}
    await store.put_profile(profile_id, account_id=prev.get("account_id"), state=out["state"],
                            risk_score=out["riskScore"], risk_tier=out["riskTier"],
                            repeated_across=out["repeatedAcross"])
    if out["riskScore"] >= QUEUE_AT:
        await store.enqueue_review(profile_id=profile_id, session_id=session_id,
                                   reason=reason or "score", risk_score=out["riskScore"],
                                   evidence={"riskTags": out["riskTags"],
                                             "repeatedAcross": out["repeatedAcross"],
                                             **(evidence or {})})
    return {"duplicate": False, "riskTier": out["riskTier"], "riskScore": out["riskScore"],
            "repeatedAcross": out["repeatedAcross"]}


async def _tier_of(profile_id: str) -> str:
    row = await get_store().get_profile(profile_id)
    return row["risk_tier"] if row else "none"


async def read_tier(profile_id: str) -> dict[str, Any]:
    row = await get_store().get_profile(profile_id)
    if not row:
        return {"profileId": profile_id, "riskTier": "none", "riskScore": 0.0, "repeatedAcross": []}
    return {"profileId": profile_id, "riskTier": row["risk_tier"],
            "riskScore": float(row["risk_score"]), "repeatedAcross": row.get("repeated_across") or []}
