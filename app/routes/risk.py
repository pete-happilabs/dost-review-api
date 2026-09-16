from fastapi import APIRouter

from app.risk import service

router = APIRouter(prefix="/api/v1/risk", tags=["risk"])


@router.get("/profile/{profile_id}")
async def get_profile_risk(profile_id: str):
    return await service.read_tier(profile_id)
