"""Shape adapters between the gate's signal record and the engine's inputs."""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Gate categories that are blocked on one message and therefore count as an
# account-level velocity tag on their own (engine.FRAUD_VELOCITY_TAGS).
_CATEGORICAL = {"payment_manipulation", "otp_request", "phishing"}

# The engine reads signal NAMES only — vendor/engine.py builds
# `names = [s.get("name") for s in raw_sigs ...]` and never looks at confidence — so a
# 0.05-confidence money_ask would fire money_ask_no_milestone at exactly the weight of a
# 0.95 one. The gate does not filter either (it only clamps to [0,1]). This mapper is the
# only place on the risk path that still sees confidence, so the floor has to live here.
# 0.5 matches the gate's own "low confidence" band; dost-talk's soft warning path uses a
# stricter 0.8.
_MIN_CONFIDENCE = 0.5

# ...but a floor that DROPS a name also drops the conversation STATE the engine seeds from
# it, and that disables detections on much later, high-confidence messages:
# money_ask seeds moneyAskers (read by fee_escalation, weight 6.0) and identity_claim seeds
# identityClaimants (read by identity_then_money, 3.0). The advance-fee sequence
# fee_escalation exists to catch — soft opening ask, deposit, bigger ask — opens with
# exactly the ask a model scores low. So the names the engine keeps state for clear a noise
# floor only: 0.2, the band the gate itself uses for its contextual downgrades
# (dost-classifier/app/classifier.py worked examples). The deal-stage awareness the gate
# encodes as low confidence is handled properly by the milestone ladder below instead.
_NOISE_FLOOR = 0.2

# engine._CONV_SIGNAL_STAGES: the signal names process_conversation records in `stages`
# (plus the identityClaimants / moneyAskers maps).
_STATE_SEEDING = {"channel_shift_request", "identity_claim", "money_ask",
                  "payee_asked_to_act", "payment_claim", "contact_share", "refusal_to_meet"}

# engine._CONV_MILESTONES: the stage names process_conversation records as milestones.
# The record's context.dealStage vocabulary is this set minus ledger_credit_small.
_MILESTONE_STAGES = {"price_agreed", "meet_scheduled", "ledger_credit",
                     "ledger_credit_small", "handover_confirmed"}

# context.dealStage is an ORDINAL ladder (plan line 106: none | price_agreed |
# meet_scheduled | ledger_credit | handover_confirmed). ledger_credit_small is not on the
# wire and is deliberately not on this tuple.
_MILESTONE_LADDER = ("price_agreed", "meet_scheduled", "ledger_credit", "handover_confirmed")


def _keeps(name: str, confidence: float) -> bool:
    return confidence >= (_NOISE_FLOOR if name in _STATE_SEEDING else _MIN_CONFIDENCE)


def to_conversation_event(record: dict[str, Any]) -> dict[str, Any]:
    named = [s for s in record.get("signals", []) if isinstance(s, dict) and "name" in s]
    signals = [{"name": s["name"], "confidence": float(s.get("confidence", 0.0))}
               for s in named if _keeps(s["name"], float(s.get("confidence", 0.0)))]
    # The floor is the only place a named signal disappears from the risk path, so the
    # names it drops have to be recoverable from production logs: without this, "why did
    # this scam not fire?" is unanswerable, and a gate-side contract break (a renamed or
    # dropped confidence field in a DES bump) looks identical to a quiet conversation.
    dropped = [s["name"] for s in named
               if not _keeps(s["name"], float(s.get("confidence", 0.0)))]
    if dropped:
        logger.info("risk mapping %s: dropped low-confidence signals %s",
                    record.get("eventId"), dropped)
    if record.get("verdict") == "reject" and record.get("category") in _CATEGORICAL:
        signals.append({"name": "categorical_block", "confidence": 1.0})
    ctx = record.get("context") or {}
    # dost-talk sends dealStage="none" until a milestone source exists (plan §ruled out);
    # this maps it the moment it does. "none"/unknown still yields [].
    stage = ctx.get("dealStage")
    milestones: list[str] = []
    if stage in _MILESTONE_STAGES:
        # The ladder is ordinal: any rung past price_agreed means the price WAS agreed.
        # Without the implied rung, engine.stage_ts("price_agreed") stays None at every
        # later stage and contact_share_pre_milestone (1.0) fires on a perfectly normal
        # post-price number exchange — the false positive decision D4 rules out.
        # ledger_credit / ledger_credit_small are never synthesised: fee_escalation and
        # jumped_deposit read their RECORDED ts, so inventing one invents a credit time.
        if stage in _MILESTONE_LADDER and stage != "price_agreed":
            milestones.append("price_agreed")
        milestones.append(stage)
    ev: dict[str, Any] = {
        "ts": record["ts"],
        "senderId": record["srcProfileId"],
        "signals": signals,
        "milestones": milestones,
    }
    if record.get("textHash"):
        ev["textHash"] = record["textHash"]
    if isinstance(ctx.get("turnIndex"), int):
        ev["turnIndex"] = ctx["turnIndex"]
    return ev


def derived_to_signals(derived: list[dict[str, Any]], *, session_id: str,
                       participants: set[str], profile_id: str | None = None,
                       ) -> list[dict[str, Any]]:
    """engine.process_conversation output → engine.process_signals input, per sender.

    The caller folds the returned list into ONE profile's account state, so every item in
    it must belong to that profile. That holds today only because one record — hence one
    sender — is mapped per call; `profile_id` makes it enforced instead of implied, so a
    replay or a queue path that ever passes two senders' events cannot silently fold the
    counterparty's fraud tags into the sender's profile. Omitted = unfiltered (the old
    behaviour), for a caller that groups by profileId itself.
    """
    out = []
    for d in derived:
        sender = d["profileId"]
        if profile_id is not None and sender != profile_id:
            continue
        others = sorted(p for p in participants if p != sender)
        out.append({
            "tag": d["tag"],
            "conversationId": session_id,
            "counterpartyId": others[0] if others else None,
            "createdAt": d["ts"],
        })
    return out
