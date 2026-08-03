"""
Full E2E test: submit reviews -> batch process -> verify reputations with real AI tags.

Requires:
  - Review API running on localhost:8013
  - PostgreSQL on localhost:5433
  - ANTHROPIC_API_KEY set in .env (engine calls Claude Haiku)

Run directly:  python tests/test_e2e_full_flow.py
Run via pytest: pytest tests/test_e2e_full_flow.py -v -s  (with server running)
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

import httpx

BASE = "http://localhost:8013"

# --- Test profiles ---
PETE = str(uuid4())
RATERS = [str(uuid4()) for _ in range(5)]
TARGET_SHOP = str(uuid4())
TARGET_DRIVER = str(uuid4())

# --- Review content ---
REVIEWS_ABOUT_PETE = [
    {
        "rater": RATERS[0],
        "text": (
            "Excellent service from start to finish. Always on time, very professional, "
            "and the product quality is consistently top-notch. Packaging is always clean "
            "and hygienic. Would highly recommend to anyone."
        ),
    },
    {
        "rater": RATERS[1],
        "text": (
            "Very friendly and respectful person. Fair pricing on everything, never tries "
            "to overcharge. Communication is clear and honest. A pleasure to deal with."
        ),
    },
    {
        "rater": RATERS[2],
        "text": (
            "Reliable delivery service, always punctual. The packaging is hygienic and "
            "well-sealed. Products arrive fresh every single time. Trustworthy."
        ),
    },
    {
        "rater": RATERS[3],
        "text": (
            "Great communication throughout the entire process. Honest and transparent "
            "about delays when they happen. Keeps promises. Very dependable partner."
        ),
    },
    {
        "rater": RATERS[4],
        "text": (
            "Was rude to me once when I asked about a delivery delay, seemed dismissive. "
            "But the food quality itself is good and portions are generous. Mixed feelings."
        ),
    },
]

PETE_REVIEWS_OTHERS = [
    {
        "target": TARGET_SHOP,
        "text": (
            "Amazing food quality, incredibly fresh ingredients every time I order. "
            "The taste is authentic and portions are generous. A bit overpriced for the "
            "area, but the quality justifies it. Clean kitchen and great presentation."
        ),
    },
    {
        "target": TARGET_DRIVER,
        "text": (
            "Very fast delivery most of the time, usually arrives within 20 minutes. "
            "Was late once by almost an hour without any update. Overall reliable though, "
            "and always polite when dropping off. Handles packages carefully."
        ),
    },
]

SECOND_BATCH_REVIEWS = [
    {
        "rater": str(uuid4()),
        "text": (
            "Absolutely fantastic experience. Quick response time, safe packaging, "
            "and the products were exactly as described. Five stars without hesitation."
        ),
    },
    {
        "rater": str(uuid4()),
        "text": (
            "Good overall but the customer support could be better. Had trouble reaching "
            "them about a missing item. Product quality was fine though."
        ),
    },
]


def ts():
    return datetime.now(timezone.utc).isoformat()


async def submit_review(client, target, rater, text, expect=201):
    review_id = str(uuid4())
    payload = {
        "reviewId": review_id,
        "targetProfileId": target,
        "raterProfileId": rater,
        "reviewText": text,
        "createdAt": ts(),
    }
    resp = await client.post("/api/v1/reviews", json=payload)
    assert resp.status_code == expect, f"Expected {expect}, got {resp.status_code}: {resp.text}"
    return resp.json(), review_id


async def trigger_and_wait(client, timeout=120):
    resp = await client.post("/api/v1/batch/trigger")
    assert resp.status_code == 202, f"Batch trigger failed: {resp.text}"
    batch_id = resp.json()["batchId"]
    print(f"\n  Batch {batch_id} triggered, waiting...")

    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = await client.get(f"/api/v1/batch/status/{batch_id}")
        data = resp.json()
        status = data["status"]
        if status in ("COMPLETED", "FAILED"):
            return data
        await asyncio.sleep(2)

    raise TimeoutError(f"Batch {batch_id} did not complete in {timeout}s")


async def get_reputation(client, profile_id):
    resp = await client.get(f"/api/v1/reputation/{profile_id}")
    if resp.status_code == 404:
        return None
    assert resp.status_code == 200, f"Reputation query failed: {resp.text}"
    return resp.json()


def print_reputation(label, rep):
    if not rep:
        print(f"\n  {label}: NO REPUTATION (404)")
        return
    print(f"\n  {label}:")
    print(f"    Tier: {rep['reputation']} ({rep['reputationScore']}/5.0)")
    print(f"    Total reviews: {rep['totalReviews']}")
    print(f"    Summary: {rep['summary']}")
    tags = rep.get("allTags", {})
    if tags:
        print(f"    Tags ({len(tags)}):")
        for tag, info in sorted(tags.items(), key=lambda x: -x[1].get("weight", 0)):
            score_str = f", score={info['score']:.1f}" if info.get("score") is not None else ""
            print(f"      #{tag} (weight={info['weight']}{score_str})")


async def clean_db():
    """Wipe test data before running."""
    import asyncpg
    pool = await asyncpg.create_pool("postgresql://dost:dost@localhost:5433/dost_reviews")
    await pool.execute("DELETE FROM review")
    await pool.execute("DELETE FROM profile_reputation")
    await pool.execute("DELETE FROM batch_run")
    await pool.close()


async def run_e2e():
    passed = 0
    failed = 0

    await clean_db()
    print("=== DB cleaned ===")

    async with httpx.AsyncClient(base_url=BASE, timeout=30) as client:
        # --- Health check ---
        resp = await client.get("/health")
        assert resp.status_code == 200
        print("=== Server healthy ===")

        # ============================================================
        # PHASE 1: Submit reviews
        # ============================================================
        print("\n=== PHASE 1: Submitting reviews ===")

        # A. Reviews about Pete (5 raters -> Pete)
        for i, r in enumerate(REVIEWS_ABOUT_PETE):
            data, rid = await submit_review(client, PETE, r["rater"], r["text"])
            assert data["status"] == "GATED"
            print(f"  [OK] Rater {i+1} -> Pete: GATED")
            passed += 1

        # B. Pete's reviews about others (Pete -> 2 targets)
        for r in PETE_REVIEWS_OTHERS:
            data, rid = await submit_review(client, r["target"], PETE, r["text"])
            assert data["status"] == "GATED"
            print(f"  [OK] Pete -> {r['target'][:8]}...: GATED")
            passed += 1

        # ============================================================
        # PHASE 2: Edge cases
        # ============================================================
        print("\n=== PHASE 2: Edge cases ===")

        # C. Self-review (Pete -> Pete)
        resp = await client.post("/api/v1/reviews", json={
            "reviewId": str(uuid4()),
            "targetProfileId": PETE,
            "raterProfileId": PETE,
            "reviewText": "Reviewing myself",
            "createdAt": ts(),
        })
        assert resp.status_code == 400, f"Self-review should be 400, got {resp.status_code}"
        print("  [OK] Self-review rejected (400)")
        passed += 1

        # D. Duplicate review
        dup_id = str(uuid4())
        resp1 = await client.post("/api/v1/reviews", json={
            "reviewId": dup_id,
            "targetProfileId": PETE,
            "raterProfileId": str(uuid4()),
            "reviewText": "First submission",
            "createdAt": ts(),
        })
        assert resp1.status_code == 201
        resp2 = await client.post("/api/v1/reviews", json={
            "reviewId": dup_id,
            "targetProfileId": PETE,
            "raterProfileId": str(uuid4()),
            "reviewText": "Duplicate submission",
            "createdAt": ts(),
        })
        assert resp2.status_code == 409, f"Duplicate should be 409, got {resp2.status_code}"
        print("  [OK] Duplicate rejected (409)")
        passed += 1

        # E. Blank reviewText
        resp = await client.post("/api/v1/reviews", json={
            "reviewId": str(uuid4()),
            "targetProfileId": PETE,
            "raterProfileId": str(uuid4()),
            "reviewText": "   ",
            "createdAt": ts(),
        })
        assert resp.status_code == 400
        detail = resp.json().get("detail", [])
        assert any("reviewText" in str(d) for d in detail), f"Expected field-level error: {detail}"
        print("  [OK] Blank text rejected with field-level error (400)")
        passed += 1

        # F. Reputation before batch (should be 404)
        rep = await get_reputation(client, PETE)
        assert rep is None
        print("  [OK] No reputation before batch (404)")
        passed += 1

        # ============================================================
        # PHASE 3: Batch processing
        # ============================================================
        print("\n=== PHASE 3: Batch processing (calling Claude Haiku for tags) ===")

        batch_result = await trigger_and_wait(client)
        stats = batch_result.get("stats", {})
        print(f"  Batch status: {batch_result['status']}")
        print(f"  Stats: {json.dumps(stats, indent=4)}")

        assert batch_result["status"] == "COMPLETED", f"Batch failed: {batch_result}"
        # 5 about Pete + 2 from Pete + 1 duplicate's first submit = 8 reviews
        assert stats["committed"] == 8, f"Expected 8 committed, got {stats['committed']}"
        assert stats["profilesUpdated"] == 3, f"Expected 3 profiles, got {stats['profilesUpdated']}"
        assert stats["failed"] == 0
        assert stats["deadLettered"] == 0
        print(f"  [OK] Batch completed: {stats['committed']} committed, {stats['profilesUpdated']} profiles")
        passed += 1

        # ============================================================
        # PHASE 4: Verify reputations
        # ============================================================
        print("\n=== PHASE 4: Reputation verification ===")

        # Pete's reputation (5+1 reviews about him: 5 from raters + 1 dup first submit)
        pete_rep = await get_reputation(client, PETE)
        assert pete_rep is not None, "Pete should have reputation"
        assert pete_rep["totalReviews"] >= 5
        assert 0 <= pete_rep["reputationScore"] <= 5.0
        assert pete_rep["reputation"] in ("very bad", "bad", "average", "very good", "excellent")
        assert len(pete_rep["allTags"]) > 0, "Should have AI-extracted tags"
        assert len(pete_rep["summary"]) > 0, "Should have AI summary"
        print_reputation("PETE", pete_rep)
        passed += 1

        # Check Pete's expected tags (mostly positive reviews)
        pete_tags = set(pete_rep["allTags"].keys())
        expected_positive = {"on-time-delivery", "product-quality", "professional", "friendly",
                             "respectful", "fair-pricing", "reliable", "honest", "hygienic",
                             "punctual", "trustworthy", "good-communication"}
        expected_negative = {"rude"}
        found_positive = pete_tags & expected_positive
        found_negative = pete_tags & expected_negative
        print(f"\n  Tag accuracy check (Pete):")
        print(f"    Expected positive tags found: {found_positive} ({len(found_positive)}/{len(expected_positive)})")
        print(f"    Expected negative tags found: {found_negative}")
        print(f"    All extracted tags: {pete_tags}")
        # At least 3 expected positive tags should appear
        assert len(found_positive) >= 3, f"Expected >= 3 positive tags, got {len(found_positive)}: {found_positive}"
        print(f"  [OK] Pete has {len(found_positive)} expected positive tags")
        passed += 1

        # Shop reputation
        shop_rep = await get_reputation(client, TARGET_SHOP)
        assert shop_rep is not None
        print_reputation("TARGET_SHOP", shop_rep)
        shop_tags = set(shop_rep["allTags"].keys())
        print(f"    Extracted tags: {shop_tags}")
        passed += 1

        # Driver reputation
        driver_rep = await get_reputation(client, TARGET_DRIVER)
        assert driver_rep is not None
        print_reputation("TARGET_DRIVER", driver_rep)
        driver_tags = set(driver_rep["allTags"].keys())
        print(f"    Extracted tags: {driver_tags}")
        passed += 1

        # ============================================================
        # PHASE 5: Incremental batch (state preservation)
        # ============================================================
        print("\n=== PHASE 5: Incremental batch (2 more reviews for Pete) ===")

        for r in SECOND_BATCH_REVIEWS:
            data, _ = await submit_review(client, PETE, r["rater"], r["text"])
            assert data["status"] == "GATED"
        print("  2 additional reviews submitted")

        batch2 = await trigger_and_wait(client)
        stats2 = batch2.get("stats", {})
        print(f"  Batch 2 stats: {json.dumps(stats2, indent=4)}")
        assert batch2["status"] == "COMPLETED"
        assert stats2["committed"] == 2
        assert stats2["profilesUpdated"] == 1
        print("  [OK] Incremental batch completed")
        passed += 1

        # Verify Pete's reputation updated
        pete_rep2 = await get_reputation(client, PETE)
        assert pete_rep2["totalReviews"] > pete_rep["totalReviews"], \
            f"totalReviews should increase: {pete_rep['totalReviews']} -> {pete_rep2['totalReviews']}"
        print_reputation("PETE (after batch 2)", pete_rep2)
        print(f"  [OK] totalReviews: {pete_rep['totalReviews']} -> {pete_rep2['totalReviews']}")
        passed += 1

        # ============================================================
        # PHASE 6: Concurrent batch rejection
        # ============================================================
        print("\n=== PHASE 6: Concurrent batch test ===")

        # Submit a review so there's something to process
        await submit_review(client, PETE, str(uuid4()),
                            "Quick test review for concurrent batch check")

        # Trigger twice rapidly
        resp1 = await client.post("/api/v1/batch/trigger")
        resp2 = await client.post("/api/v1/batch/trigger")
        statuses = {resp1.status_code, resp2.status_code}
        # One should be 202, other should be 409 (or both 202 if first completed instantly)
        assert 202 in statuses, f"At least one trigger should succeed: {statuses}"
        print(f"  Trigger 1: {resp1.status_code}, Trigger 2: {resp2.status_code}")
        if 409 in statuses:
            print("  [OK] Concurrent batch rejected (409)")
            passed += 1
        else:
            print("  [INFO] Both returned 202 (first batch completed before second trigger)")
            passed += 1

        # Wait for any running batch
        await asyncio.sleep(5)

    # ============================================================
    # SUMMARY
    # ============================================================
    print(f"\n{'='*60}")
    print(f"  E2E RESULTS: {passed} passed, {failed} failed")
    print(f"{'='*60}")
    return failed == 0


if __name__ == "__main__":
    ok = asyncio.run(run_e2e())
    sys.exit(0 if ok else 1)
