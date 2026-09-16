from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.risk import service

router = APIRouter(prefix="/api/v1", tags=["risk"])


class Signal(BaseModel):
    """One gate signal as the classifier emits it. Typed so a malformed shape (a null or
    non-numeric confidence) is a 422 before any row is written, not a 500 after one is.

    Stricter than the plan's `list[dict]` on purpose. The fields are exactly what
    dost-classifier emits ({name, confidence, tier, detail, entities:[{kind, hash}]}), so
    nothing is stripped from the record stored in signal_event.record."""
    name: str
    # No default on purpose. A default composes with the mapper's confidence floor into a
    # silent no-op: an absent field becomes 0.0, every signal is dropped below the floor,
    # and the request still answers 202 with a pydantic-fabricated 0.0 on the stored
    # record — so a DES bump that renames or drops this field would turn the fraud path
    # off with a green health check. Required, so it is a 422 before any row is written.
    confidence: float
    tier: str | None = None
    detail: str = ""
    entities: list[dict] = Field(default_factory=list)


class SignalRecord(BaseModel):
    modelVersion: str
    ts: str
    eventId: str
    sessionId: str
    srcProfileId: str
    dstProfileId: str | None = None
    verdict: str
    category: str
    tier: str | None = None
    context: dict = Field(default_factory=dict)
    textHash: str | None = None
    textLength: int = 0
    signals: list[Signal] = Field(default_factory=list)


@router.post("/signals", status_code=202)
async def post_signal(record: SignalRecord):
    out = await service.ingest_record(record.model_dump())
    if "error" in out:
        raise HTTPException(422, out["error"]["message"])
    if out.get("duplicate"):
        return JSONResponse(status_code=200, content={"duplicate": True})
    return out
