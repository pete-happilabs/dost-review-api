from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.database import get_pool

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/ready")
async def ready():
    try:
        pool = get_pool()
        await pool.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception:
        return JSONResponse({"status": "unavailable"}, status_code=503)
