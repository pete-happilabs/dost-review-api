import base64
import json
from uuid import UUID

from app.models import DostReview


class EnvelopeError(Exception):
    """Q6: Properly call super so it stringifies in logs."""
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


def parse_event_envelope(body: dict) -> tuple[DostReview, UUID | None]:
    """Parse a dostEventEnvelope and extract the DostReview payload.

    Within the Guard trust boundary, encryptedEvent is plaintext JSON in base64.
    Full Double Ratchet decryption is deferred to socket.io integration.
    """
    event_type = body.get("eventType")
    if event_type != "MSG_START":
        raise EnvelopeError(f"Expected eventType MSG_START, got {event_type}")

    encrypted = body.get("encryptedEvent")
    if not encrypted:
        raise EnvelopeError("Missing encryptedEvent field")

    # H4: Wrap eventId parsing — invalid UUID should be 400, not 500
    event_id = None
    event_id_str = body.get("eventId")
    if event_id_str:
        try:
            event_id = UUID(event_id_str)
        except (ValueError, AttributeError):
            raise EnvelopeError(f"Invalid eventId: {event_id_str}")

    try:
        decoded = base64.b64decode(encrypted)
        inner = json.loads(decoded)
    except Exception as e:
        raise EnvelopeError(f"Failed to decode encryptedEvent: {e}")

    if not isinstance(inner, dict):
        raise EnvelopeError("Inner event must be a JSON object")

    message = inner.get("message", {})
    if not isinstance(message, dict):
        raise EnvelopeError("Inner event message must be a JSON object")

    review_json_str = message.get("text")
    if not review_json_str:
        raise EnvelopeError("Inner dostEvent has no message.text field")

    try:
        review_data = json.loads(review_json_str)
        review = DostReview(**review_data)
    except json.JSONDecodeError as e:
        raise EnvelopeError(f"message.text is not valid JSON: {e}")
    except Exception as e:
        raise EnvelopeError(f"Failed to parse DostReview from message.text: {e}")

    return review, event_id
