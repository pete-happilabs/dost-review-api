import importlib
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)


def _load_engine():
    """Import process_reputation from the reputation engine."""
    engine_path = settings.engine_path
    if not engine_path:
        # Try vendor/ fallback
        vendor = Path(__file__).parent.parent / "vendor"
        if (vendor / "engine.py").exists():
            engine_path = str(vendor)
    if engine_path and engine_path not in sys.path:
        sys.path.insert(0, engine_path)
    # Use importlib to avoid name collision with other 'engine' packages
    spec = importlib.util.spec_from_file_location(
        "reputation_engine",
        Path(engine_path or ".") / "engine.py",
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot find engine.py at {engine_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.process_reputation


process_reputation = _load_engine()


def _merge_tags(engine_output: dict[str, Any]) -> dict[str, Any]:
    """Merge engine's topTags (weight+score) and allTags (counts) into single dict."""
    merged: dict[str, dict[str, Any]] = {}
    for tag, count in engine_output.get("allTags", {}).items():
        merged[tag] = {"weight": count}
    for entry in engine_output.get("topTags", []):
        merged[entry["tag"]] = {"weight": entry["weight"], "score": entry["score"]}
    return merged


async def run_batch(pool: asyncpg.Pool, batch_id: UUID) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "totalReviews": 0,
        "committed": 0,
        "failed": 0,
        "profilesUpdated": 0,
    }

    # --- Step 2: PREPARE ---
    rows = await pool.fetch(
        "SELECT * FROM review WHERE status = 'GATED' ORDER BY created_at LIMIT $1",
        settings.max_reviews_per_batch,
    )
    stats["totalReviews"] = len(rows)

    if not rows:
        return stats

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
            # NOTE: rater_weight is stored in the review table but the engine's
            # current input format ({"description", "createdAt"}) does not accept it.
            # When the engine adds raterWeight support, add it here:
            #   "raterWeight": float(r["rater_weight"])
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
            output, _metrics = process_reputation(engine_input)

            if "error" in output:
                logger.warning(
                    "Engine error for profile %s: %s",
                    profile_id, output["error"].get("message", "unknown"),
                )
                stats["failed"] += len(review_rows)
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
                        "UPDATE review SET status = 'COMMITTED', processed_at = NOW(), "
                        "batch_id = $1 WHERE id = ANY($2::uuid[])",
                        batch_id, review_ids,
                    )

            stats["committed"] += len(review_rows)
            stats["profilesUpdated"] += 1

        except Exception:
            logger.exception("Batch failed for profile %s", profile_id)
            stats["failed"] += len(review_rows)

    return stats
