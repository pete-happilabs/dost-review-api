-- C3: Server-side received_at for rate limiting (untrusted created_at kept for engine)
ALTER TABLE review ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- H3: Retry counter for poison-pill handling
ALTER TABLE review ADD COLUMN IF NOT EXISTS retry_count SMALLINT NOT NULL DEFAULT 0;

-- Q4: CHECK constraints
ALTER TABLE review ADD CONSTRAINT chk_review_status
    CHECK (status IN ('GATED', 'GATE_REJECTED', 'PROCESSING', 'COMMITTED', 'FAILED'));
ALTER TABLE review ADD CONSTRAINT chk_rater_weight
    CHECK (rater_weight BETWEEN 0.00 AND 2.00);
ALTER TABLE profile_reputation ADD CONSTRAINT chk_reputation_score
    CHECK (reputation_score BETWEEN 0.0 AND 5.0);
ALTER TABLE batch_run ADD CONSTRAINT chk_batch_status
    CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'));

-- Q4: Index on batch_id for debugging
CREATE INDEX IF NOT EXISTS idx_review_batch_id ON review(batch_id);

-- Backfill received_at for existing rows
UPDATE review SET received_at = created_at WHERE received_at IS NULL;
