"""Shape adapters between the gate's signal record and the engine's inputs."""
from __future__ import annotations

from typing import Any

# Gate categories that are blocked on one message and therefore count as an
# account-level velocity tag on their own (engine.FRAUD_VELOCITY_TAGS).
_CATEGORICAL = {"payment_manipulation", "otp_request", "phishing"}

# The engine reads signal NAMES only — vendor/engine.py builds
# `names = [s.get("name") for s in raw_sigs ...]` and never looks at confidence — so a
# 0.05-confidence money_ask would fire money_ask_no_milestone at exactly the weight of a
# 0.95 one. The gate does not filter either (it only clamps to [0,1]), and it deliberately
# encodes deal-stage awareness AS low confidence. This mapper is the only place on the
# risk path that still sees confidence, so the floor has to live here. 0.5 matches the
# gate's own "low confidence" band; dost-talk's soft warning path uses a stricter 0.8.
_MIN_CONFIDENCE = 0.5

# engine._CONV_MILESTONES: the stage names process_conversation records as milestones.
# The record's context.dealStage vocabulary is this set minus ledger_credit_small.
_MILESTONE_STAGES = {"price_agreed", "meet_scheduled", "ledger_credit",
                     "ledger_credit_small", "handover_confirmed"}


def to_conversation_event(record: dict[str, Any]) -> dict[str, Any]:
    signals = [{"name": s["name"], "confidence": float(s.get("confidence", 0.0))}
               for s in record.get("signals", [])
               if isinstance(s, dict) and "name" in s
               and float(s.get("confidence", 0.0)) >= _MIN_CONFIDENCE]
    if record.get("verdict") == "reject" and record.get("category") in _CATEGORICAL:
        signals.append({"name": "categorical_block", "confidence": 1.0})
    ctx = record.get("context") or {}
    # dost-talk sends dealStage="none" until a milestone source exists (plan §ruled out);
    # this maps it the moment it does. "none"/unknown still yields [].
    stage = ctx.get("dealStage")
    ev: dict[str, Any] = {
        "ts": record["ts"],
        "senderId": record["srcProfileId"],
        "signals": signals,
        "milestones": [stage] if stage in _MILESTONE_STAGES else [],
    }
    if record.get("textHash"):
        ev["textHash"] = record["textHash"]
    if isinstance(ctx.get("turnIndex"), int):
        ev["turnIndex"] = ctx["turnIndex"]
    return ev


def derived_to_signals(derived: list[dict[str, Any]], *, session_id: str,
                       participants: set[str]) -> list[dict[str, Any]]:
    """engine.process_conversation output → engine.process_signals input, per sender."""
    out = []
    for d in derived:
        sender = d["profileId"]
        others = sorted(p for p in participants if p != sender)
        out.append({
            "tag": d["tag"],
            "conversationId": session_id,
            "counterpartyId": others[0] if others else None,
            "createdAt": d["ts"],
        })
    return out
