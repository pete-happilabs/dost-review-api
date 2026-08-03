import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class DostReview(BaseModel):
    reviewId: UUID
    targetProfileId: UUID
    raterProfileId: UUID
    # C1: raterWeight removed — must come from internal trust service, not client
    reviewText: str = Field(min_length=1, max_length=5000)
    # M7: Cap multimedia list to 10 items
    multimedia: list[dict[str, Any]] | None = Field(default=None, max_length=10)
    createdAt: datetime

    @field_validator("reviewText")
    @classmethod
    def text_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("reviewText must not be blank")
        return v

    # M7: max_length caps the item count but not the payload — a single item
    # could still be megabytes of JSON stored verbatim into JSONB.
    @field_validator("multimedia")
    @classmethod
    def multimedia_not_huge(cls, v: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
        if v is not None and len(json.dumps(v)) > 100_000:
            raise ValueError("multimedia payload too large (max 100KB serialized)")
        return v

    # C3: Reject far-future or ancient dates
    @field_validator("createdAt")
    @classmethod
    def reasonable_date(cls, v: datetime) -> datetime:
        # A naive timestamp used to hit an aware-vs-naive TypeError below and
        # surface as an opaque 400 — treat naive as UTC instead.
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if v > now + timedelta(hours=1):
            raise ValueError("createdAt cannot be in the future")
        if v < now - timedelta(days=365):
            raise ValueError("createdAt cannot be more than 1 year in the past")
        return v


class ReviewResponse(BaseModel):
    reviewId: UUID
    status: str
    message: str


class TagInfo(BaseModel):
    weight: float
    score: float | None = None


class DostReputation(BaseModel):
    profileId: str
    reputation: str
    reputationScore: float
    totalReviews: int
    allTags: dict[str, TagInfo]
    summary: str
    updatedAt: datetime  # M6: renamed — this is last computation time, not creation


class BatchStatus(BaseModel):
    batchId: UUID
    status: str
    stats: dict[str, Any] | None = None
    startedAt: datetime
    completedAt: datetime | None = None
