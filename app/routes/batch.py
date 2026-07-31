import json
from uuid import UUID

from fastapi import APIRouter, HTTPException

from app.batch_runner import execute_batch
from app.database import get_pool
from app.models import BatchStatus

router = APIRouter(prefix="/api/v1")


@router.post("/batch/trigger", status_code=202)
async def trigger_batch():
    # C2: execute_batch checks for concurrent RUNNING batch
    batch_id, status = await execute_batch()

    if status == "ALREADY_RUNNING":
        raise HTTPException(
            status_code=409,
            detail=f"Batch {batch_id} is already running",
        )

    return {
        "batchId": str(batch_id),
        "status": "RUNNING",
        "message": "Batch processing started.",
    }


@router.get("/batch/status/{batch_id}", response_model=BatchStatus)
async def batch_status(batch_id: UUID):
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM batch_run WHERE id = $1", batch_id)
    if not row:
        raise HTTPException(status_code=404, detail="Batch not found")
    raw_stats = row["stats"]
    stats = json.loads(raw_stats) if isinstance(raw_stats, str) else raw_stats
    return BatchStatus(
        batchId=row["id"],
        status=row["status"],
        stats=stats,
        startedAt=row["started_at"],
        completedAt=row["completed_at"],
    )
