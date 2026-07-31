import base64
import json
from uuid import UUID

from app.models import DostReview


class EnvelopeError(Exception):
    def __init__(self, message: str):
        self.message = message


def parse_event_envelope(body: dict) -> tuple[DostReview, UUID | None]:
    """Parse a dostEventEnvelope and extract the DostReview payload.

    Within the Guard trust boundary, encryptedEvent is plaintext JSON in base64
    (not actually encrypted). Full Double Ratchet decryption is deferred to
    socket.io integration.

    Returns (DostReview, event_id).
    """
    event_type = body.get("eventType")
    if event_type != "MSG_START":
        raise EnvelopeError(f"Expected eventType MSG_START, got {event_type}")

    encrypted = body.get("encryptedEvent")
    if not encrypted:
        raise EnvelopeError("Missing encryptedEvent field")

    event_id_str = body.get("eventId")
    event_id = UUID(event_id_str) if event_id_str else None

    try:
        decoded = base64.b64decode(encrypted)
        inner = json.loads(decoded)
    except Exception as e:
        raise EnvelopeError(f"Failed to decode encryptedEvent: {e}")

    message = inner.get("message", {})
    review_json_str = message.get("text")
    if not review_json_str:
        raise EnvelopeError("Inner dostEvent has no message.text field")

    try:
        review_data = json.loads(review_json_str)
        review = DostReview(**review_data)
    except Exception as e:
        raise EnvelopeError(f"Failed to parse DostReview from message.text: {e}")

    return review, event_id
