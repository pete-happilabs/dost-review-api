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


class _EngineRejected(Exception):
    """Raised inside a store transaction so an engine input error rolls the whole ingest
    back: the event must not stay on file as "seen" when it was never folded.

    Deliberately beyond the plan text, which returned {"error": ...} after the insert had
    committed — a retry then answered duplicate:true and the record was never folded. The
    route contract is unchanged: error -> 422, duplicate -> 200, else 202.
    """

    def __init__(self, error: dict[str, Any]):
        super().__init__(error.get("message"))
        self.error = error


async def ingest_record(record: dict[str, Any]) -> dict[str, Any]:
    """One gate record → conversation stage machine → sender's account state → tier.

    Records for one session arrive fire-and-forget and overlap; the whole fold runs in one
    transaction under a per-session lock (and a per-profile lock inside _fold), so two
    records never read the same state and each overwrite the other's turn, and a failure
    after the insert leaves no row behind — the retry re-processes instead of answering
    "duplicate".
    """
    engine = load_engine_module()
    store = get_store()

    session_id = record["sessionId"]
    sender = record["srcProfileId"]
    receiver = record.get("dstProfileId")
    # Shape the engine event before touching the store: a record the mapper cannot read
    # must fail before any row exists for it.
    event = mapping.to_conversation_event(record)

    try:
        async with store.transaction(f"session:{session_id}"):
            inserted = await store.insert_signal_event(record)
            if not inserted:
                return {"duplicate": True}

            conv_state = await store.get_conversation_state(session_id)
            conv_out, _ = engine.process_conversation({
                "conversationId": session_id, "state": conv_state, "events": [event],
            })
            if "error" in conv_out:
                raise _EngineRejected(conv_out["error"])
            await store.put_conversation_state(session_id, conv_out["state"])

            if not conv_out["derivedTags"]:
                return {"duplicate": False, "derived": [], "riskTier": await _tier_of(sender)}

            participants = await store.participants_of(
                session_id, {p for p in (sender, receiver) if p})
            # profile_id=sender: _fold writes into the SENDER's account state, so only the
            # sender's tags may go into it. One record is mapped per call today, but the
            # filter is what makes that a contract rather than a coincidence.
            signals = mapping.derived_to_signals(conv_out["derivedTags"], session_id=session_id,
                                                 participants=participants, profile_id=sender)
            if not signals:
                # Every derived tag belonged to someone else: nothing to fold here, and an
                # empty fold would still write an all-zero profile row for the sender.
                return {"duplicate": False, "derived": [], "riskTier": await _tier_of(sender)}
            return await _fold(sender, signals=signals, outcomes=[], session_id=session_id)
    except _EngineRejected as rejected:
        return {"error": rejected.error}


async def ingest_outcome(*, profile_id: str, tag: str, reporter_id: str | None,
                         session_id: str | None, evidence: dict) -> dict[str, Any]:
    store = get_store()
    try:
        async with store.transaction(f"profile:{profile_id}"):
            await store.insert_outcome(profile_id=profile_id, reporter_id=reporter_id,
                                       session_id=session_id, tag=tag, evidence=evidence)
            return await _fold(profile_id, signals=[],
                               outcomes=[{"tag": tag,
                                          "conversationId": session_id or f"outcome:{tag}",
                                          "counterpartyId": reporter_id}],
                               session_id=session_id, reason=f"outcome:{tag}", evidence=evidence)
    except _EngineRejected as rejected:
        return {"error": rejected.error}


async def _fold(profile_id: str, *, signals, outcomes, session_id, reason=None, evidence=None):
    """Fold into the profile under its lock. Nested inside a session transaction when called
    from ingest_record (same connection, one more advisory lock); standalone otherwise."""
    engine = load_engine_module()
    store = get_store()
    async with store.transaction(f"profile:{profile_id}"):
        prev = await store.get_profile(profile_id) or {}
        payload = {
            "profileId": profile_id, "state": prev.get("state"),
            "signals": signals, "outcomes": outcomes,
        }
        # Tenure is unknown until Onboard's read exists, and unknown is NOT zero. The engine
        # reads an absent/None tenureDays as "unknown" and applies NO young-account discount
        # (it falls back to RISK_TENURE_PRIOR_DAYS); an explicit 0 means a 0-day-old account
        # and DOES discount, roughly halving every velocity-derived score and so doubling the
        # QUEUE_AT threshold in practice. Pass the key only when a real value exists.
        tenure_days = prev.get("tenure_days")
        if tenure_days is not None:
            payload["tenureDays"] = tenure_days
        out, _ = engine.process_signals(payload)
        if "error" in out:
            raise _EngineRejected(out["error"])
        await store.put_profile(profile_id, account_id=prev.get("account_id"),
                                state=out["state"], risk_score=out["riskScore"],
                                risk_tier=out["riskTier"], repeated_across=out["repeatedAcross"])
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
