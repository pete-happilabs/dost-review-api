import json
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.batch_engine import run_batch
from app.database import get_pool
from app.models import BatchStatus

router = APIRouter(prefix="/api/v1")


async def _run_batch_task(batch_id: UUID) -> None:
    pool = get_pool()
    try:
        stats = await run_batch(pool, batch_id)
        await pool.execute(
            "UPDATE batch_run SET status = 'COMPLETED', stats = $1::jsonb, completed_at = NOW() "
            "WHERE id = $2",
            json.dumps(stats), batch_id,
        )
    except Exception as e:
        await pool.execute(
            "UPDATE batch_run SET status = 'FAILED', stats = $1::jsonb, completed_at = NOW() "
            "WHERE id = $2",
            json.dumps({"error": str(e)}), batch_id,
        )


@router.post("/batch/trigger", status_code=202)
async def trigger_batch(background_tasks: BackgroundTasks):
    pool = get_pool()
    batch_id = uuid4()
    await pool.execute(
        "INSERT INTO batch_run (id, status, started_at) VALUES ($1, 'RUNNING', NOW())",
        batch_id,
    )
    background_tasks.add_task(_run_batch_task, batch_id)
    return {"batchId": str(batch_id), "status": "RUNNING", "message": "Batch processing started."}


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
