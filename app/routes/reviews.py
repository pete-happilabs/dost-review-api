import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.database import get_pool
from app.event_parser import EnvelopeError, parse_event_envelope
from app.gate import gate_and_store
from app.models import DostReview, ReviewResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")


@router.post("/reviews", status_code=201, response_model=ReviewResponse)
async def submit_review(request: Request):
    # H4: Handle non-JSON and malformed bodies cleanly
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")

    # Detect format: if "eventType" present, it's a dostEvent envelope
    # Q6: A raw DostReview with an extra "eventType" key would be misrouted —
    # check for "encryptedEvent" too which is envelope-specific
    event_id = None
    if "eventType" in body and "encryptedEvent" in body:
        try:
            review, event_id = parse_event_envelope(body)
        except EnvelopeError as e:
            raise HTTPException(status_code=400, detail=e.message)
    else:
        try:
            review = DostReview(**body)
        except Exception as e:
            # Q2: Don't leak full Pydantic error internals
            raise HTTPException(status_code=400, detail="Invalid review payload")

    pool = get_pool()
    # H1: gate_and_store is transactional — gate + insert are atomic
    result = await gate_and_store(review, pool, event_id)

    if not result.passed:
        status_code = {"SELF_REVIEW": 400, "DUPLICATE": 409, "RATE_LIMITED": 429}.get(
            result.error_code, 400
        )
        raise HTTPException(status_code=status_code, detail=result.message)

    return ReviewResponse(
        reviewId=review.reviewId,
        status="GATED",
        message="Review accepted. Will be processed in the next batch run.",
    )
