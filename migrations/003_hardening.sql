-- Follow-up hardening after the C2/C3 fixes.

-- The C3 fix switched the rate-limit query to received_at and the C2 fix switched
-- batch pickup ordering to received_at, but the indexes from 001 still covered
-- created_at — both hot queries would seq-scan at scale. Replace them.
CREATE INDEX IF NOT EXISTS idx_review_batch_pickup_received ON review(status, received_at);
CREATE INDEX IF NOT EXISTS idx_review_rate_limit_received
    ON review(rater_profile_id, target_profile_id, received_at);
DROP INDEX IF EXISTS idx_review_batch_pickup;
DROP INDEX IF EXISTS idx_review_rate_limit;

-- M1 completion: the audit trail stored THAT a review was rejected but not WHY —
-- the reason argument to _persist_rejected() was silently dropped.
ALTER TABLE review ADD COLUMN IF NOT EXISTS gate_reason VARCHAR(30);
