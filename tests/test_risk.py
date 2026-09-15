from app.risk import ids, mapping


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
