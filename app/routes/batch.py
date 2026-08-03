import json
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.batch_runner import run_batch_to_completion, start_batch
from app.database import get_pool
from app.models import BatchStatus

router = APIRouter(prefix="/api/v1")


@router.post("/batch/trigger", status_code=202)
async def trigger_batch(background_tasks: BackgroundTasks):
    # C2: start_batch refuses to start while another batch is RUNNING.
    # The batch itself runs as a background task — awaiting it inline would
    # block this request for the whole run (minutes+) while claiming "202
    # Accepted / started", and would tie batch completion to the HTTP client's
    # connection lifetime.
    batch_id, started = await start_batch()

    if not started:
        raise HTTPException(
            status_code=409,
            detail=f"Batch {batch_id} is already running",
        )

    background_tasks.add_task(run_batch_to_completion, batch_id)
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
