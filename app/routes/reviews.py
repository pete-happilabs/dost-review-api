import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.database import get_pool
from app.event_parser import EnvelopeError, parse_event_envelope
from app.gate import gate_review
from app.models import DostReview, ReviewResponse

router = APIRouter(prefix="/api/v1")


@router.post("/reviews", status_code=201, response_model=ReviewResponse)
async def submit_review(request: Request):
    body: dict[str, Any] = await request.json()

    # Detect format: if "eventType" present, it's a dostEvent envelope
    event_id = None
    if "eventType" in body:
        try:
            review, event_id = parse_event_envelope(body)
        except EnvelopeError as e:
            raise HTTPException(status_code=400, detail=e.message)
    else:
        try:
            review = DostReview(**body)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    pool = get_pool()
    result = await gate_review(review, pool)

    if not result.passed:
        status_code = {"SELF_REVIEW": 400, "DUPLICATE": 409, "RATE_LIMITED": 429}.get(
            result.error_code, 400
        )
        raise HTTPException(status_code=status_code, detail=result.message)

    await pool.execute(
        "INSERT INTO review "
        "(id, target_profile_id, rater_profile_id, rater_weight, review_text, "
        "multimedia_json, status, event_id, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, 'GATED', $7, $8)",
        review.reviewId,
        review.targetProfileId,
        review.raterProfileId,
        review.raterWeight,
        review.reviewText,
        json.dumps(review.multimedia) if review.multimedia else None,
        event_id,
        review.createdAt,
    )

    return ReviewResponse(
        reviewId=review.reviewId,
        status="GATED",
        message="Review accepted. Will be processed in the next batch run.",
    )
