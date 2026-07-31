import json
from uuid import UUID

from fastapi import APIRouter, HTTPException

from app.database import get_pool
from app.models import DostReputation, TagInfo

router = APIRouter(prefix="/api/v1")


@router.get("/reputation/{profile_id}", response_model=DostReputation)
async def get_reputation(profile_id: UUID):
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM profile_reputation WHERE profile_id = $1", profile_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="No reputation found for this profile")

    raw_tags = row["all_tags"]
    if isinstance(raw_tags, str):
        raw_tags = json.loads(raw_tags)

    all_tags = {}
    for tag, info in raw_tags.items():
        if isinstance(info, dict):
            all_tags[tag] = TagInfo(**info)
        else:
            all_tags[tag] = TagInfo(weight=float(info))

    return DostReputation(
        profileId=str(row["profile_id"]),
        reputation=row["reputation"],
        reputationScore=float(row["reputation_score"]),
        totalReviews=row["total_reviews"],
        allTags=all_tags,
        summary=row["summary"],
        createdAt=row["updated_at"],
    )
