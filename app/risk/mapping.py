"""Shape adapters between the gate's signal record and the engine's inputs."""
from __future__ import annotations

from typing import Any

# Gate categories that are blocked on one message and therefore count as an
# account-level velocity tag on their own (engine.FRAUD_VELOCITY_TAGS).
_CATEGORICAL = {"payment_manipulation", "otp_request", "phishing"}


def to_conversation_event(record: dict[str, Any]) -> dict[str, Any]:
    signals = [{"name": s["name"], "confidence": float(s.get("confidence", 0.0))}
               for s in record.get("signals", []) if isinstance(s, dict) and "name" in s]
    if record.get("verdict") == "reject" and record.get("category") in _CATEGORICAL:
        signals.append({"name": "categorical_block", "confidence": 1.0})
    ctx = record.get("context") or {}
    ev: dict[str, Any] = {
        "ts": record["ts"],
        "senderId": record["srcProfileId"],
        "signals": signals,
        "milestones": [],            # no milestone source exists today — see plan header
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
