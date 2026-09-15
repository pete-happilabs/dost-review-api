from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.risk import service

router = APIRouter(prefix="/api/v1", tags=["risk"])


class Signal(BaseModel):
    """One gate signal as the classifier emits it. Typed so a malformed shape (a null or
    non-numeric confidence) is a 422 before any row is written, not a 500 after one is."""
    name: str
    confidence: float = 0.0
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
