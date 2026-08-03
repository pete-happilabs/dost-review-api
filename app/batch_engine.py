"""Batch engine — 4-step pipeline: Gate → Prepare → Process → Commit.

C2: Atomic row claiming with FOR UPDATE SKIP LOCKED prevents double-processing.
H2: Engine loaded lazily, called via asyncio.to_thread to avoid blocking event loop.
H3: Retry counter with dead-letter after max_review_retries. Honors willRetry flag.
"""
import asyncio
import importlib
import importlib.util  # H2: explicit import — don't rely on transitive
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)

# H2: Lazy engine loading — don't break startup if engine is missing
_engine_fn = None


def _load_engine():
    """Import process_reputation from the reputation engine."""
    global _engine_fn
    if _engine_fn is not None:
        return _engine_fn

    engine_path = settings.engine_path
    if not engine_path:
        vendor = Path(__file__).parent.parent / "vendor"
        if (vendor / "engine.py").exists():
            engine_path = str(vendor)

    if not engine_path:
        raise ImportError("ENGINE_PATH not set and vendor/engine.py not found")

    engine_file = Path(engine_path) / "engine.py"
    if not engine_file.exists():
        raise ImportError(f"engine.py not found at {engine_file}")

    spec = importlib.util.spec_from_file_location("reputation_engine", str(engine_file))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load engine.py from {engine_file}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _engine_fn = mod.process_reputation
    return _engine_fn


def _merge_tags(engine_output: dict[str, Any]) -> dict[str, Any]:
    """Merge engine's topTags (weight+score) and allTags (counts) into single dict."""
    merged: dict[str, dict[str, Any]] = {}
    for tag, count in engine_output.get("allTags", {}).items():
        merged[tag] = {"weight": count}
    for entry in engine_output.get("topTags", []):
        merged[entry["tag"]] = {"weight": entry["weight"], "score": entry["score"]}
    return merged


async def run_batch(pool: asyncpg.Pool, batch_id: UUID) -> dict[str, Any]:
    """Run the full 4-step batch pipeline.

    C2: Uses FOR UPDATE SKIP LOCKED to atomically claim reviews,
    preventing concurrent batch runs from double-processing.
    """
    process_reputation = _load_engine()

    stats: dict[str, Any] = {
        "totalReviews": 0,
        "committed": 0,
        "failed": 0,
        "deadLettered": 0,
        "profilesUpdated": 0,
    }

    # --- Step 2: PREPARE ---
    # C2: Atomically claim GATED reviews by setting status to PROCESSING
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "UPDATE review SET status = 'PROCESSING', batch_id = $1 "
                "WHERE id IN ("
                "  SELECT id FROM review "
                "  WHERE status = 'GATED' "
                "  ORDER BY received_at "
                "  LIMIT $2 "
                "  FOR UPDATE SKIP LOCKED"
                ") RETURNING *",
                batch_id, settings.max_reviews_per_batch,
            )

    stats["totalReviews"] = len(rows)
    if not rows:
        return stats

    # Group by target profile
    profiles: dict[UUID, list] = defaultdict(list)
    for row in rows:
        profiles[row["target_profile_id"]].append(row)

    for profile_id, review_rows in profiles.items():
        try:
            # Load existing state
            rep_row = await pool.fetchrow(
                "SELECT state FROM profile_reputation WHERE profile_id = $1", profile_id
            )
            stored_state = None
            if rep_row and rep_row["state"]:
                raw = rep_row["state"]
                stored_state = json.loads(raw) if isinstance(raw, str) else raw

            # Build engine input
            engine_input: dict[str, Any] = {
                "profileId": str(profile_id),
                "about": "",
                "reviews": [
                    {
                        "description": r["review_text"],
                        "createdAt": r["created_at"].isoformat(),
                    }
                    for r in review_rows
                ],
            }
            if stored_state:
                engine_input["state"] = stored_state

            # --- Step 3: PROCESS ---
            # H2: Run synchronous engine call in a thread to avoid blocking event loop
            output, _metrics = await asyncio.to_thread(process_reputation, engine_input)

            # Check for engine error
            if "error" in output:
                will_retry = output["error"].get("willRetry", True)
                logger.warning(
                    "Engine error for profile %s: %s (willRetry=%s)",
                    profile_id, output["error"].get("message", "unknown"), will_retry,
                )
                # H3: Honor willRetry flag and track retry count
                await _handle_failed_reviews(pool, review_rows, will_retry, stats)
                continue

            # --- Step 4: COMMIT ---
            merged_tags = _merge_tags(output)

            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO profile_reputation "
                        "(profile_id, reputation, reputation_score, total_reviews, "
                        "all_tags, summary, state, created_at, updated_at) "
                        "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, NOW(), NOW()) "
                        "ON CONFLICT (profile_id) DO UPDATE SET "
                        "reputation = $2, reputation_score = $3, total_reviews = $4, "
                        "all_tags = $5::jsonb, summary = $6, state = $7::jsonb, updated_at = NOW()",
                        profile_id,
                        output["reputation"],
                        output["reputationScore"],
                        output["totalReviews"],
                        json.dumps(merged_tags),
                        output.get("summary", ""),
                        json.dumps(output["state"]),
                    )

                    review_ids = [r["id"] for r in review_rows]
                    await conn.execute(
                        "UPDATE review SET status = 'COMMITTED', processed_at = NOW() "
                        "WHERE id = ANY($1::uuid[])",
                        review_ids,
                    )

            stats["committed"] += len(review_rows)
            stats["profilesUpdated"] += 1

        except Exception:
            logger.exception("Batch failed for profile %s", profile_id)
            # Return reviews to GATED so they're retried, with incremented retry_count
            await _handle_failed_reviews(pool, review_rows, True, stats)

    return stats


async def _handle_failed_reviews(
    pool: asyncpg.Pool, review_rows: list, will_retry: bool, stats: dict
) -> None:
    """H3: Handle failed reviews — retry or dead-letter based on retry count and willRetry."""
    for row in review_rows:
        new_retry = row["retry_count"] + 1
        if not will_retry or new_retry >= settings.max_review_retries:
            # Dead-letter: permanently mark as FAILED
            await pool.execute(
                "UPDATE review SET status = 'FAILED', retry_count = $1, processed_at = NOW() "
                "WHERE id = $2",
                new_retry, row["id"],
            )
            stats["deadLettered"] += 1
        else:
            # Return to GATED for retry on next batch
            await pool.execute(
                "UPDATE review SET status = 'GATED', retry_count = $1 WHERE id = $2",
                new_retry, row["id"],
            )
            stats["failed"] += 1
