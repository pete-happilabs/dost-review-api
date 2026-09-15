from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.risk import service

router = APIRouter(prefix="/api/v1", tags=["risk"])

OUTCOME_TAGS = {"counterparty_reported", "paid_not_delivered", "not_received", "dispute_lost",
                "counterparty_silent_after_money_ask", "confirmed_fraud",
                "shared_instrument_with_confirmed"}


class Outcome(BaseModel):
    profileId: str
    tag: str
    reporterId: str | None = None
    sessionId: str | None = None
    evidence: dict = Field(default_factory=dict)


@router.post("/outcomes", status_code=202)
async def post_outcome(o: Outcome):
    if o.tag not in OUTCOME_TAGS:
        raise HTTPException(422, f"unknown outcome tag {o.tag}")
    return await service.ingest_outcome(profile_id=o.profileId, tag=o.tag, reporter_id=o.reporterId,
                                        session_id=o.sessionId, evidence=o.evidence)
