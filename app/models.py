from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class DostReview(BaseModel):
    reviewId: UUID
    targetProfileId: UUID
    raterProfileId: UUID
    raterWeight: float = Field(default=1.0, ge=0.0, le=2.0)
    reviewText: str = Field(min_length=1, max_length=5000)
    multimedia: list[dict[str, Any]] | None = None
    createdAt: datetime

    @field_validator("reviewText")
    @classmethod
    def text_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("reviewText must not be blank")
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
    createdAt: datetime


class BatchStatus(BaseModel):
    batchId: UUID
    status: str
    stats: dict[str, Any] | None = None
    startedAt: datetime
    completedAt: datetime | None = None
