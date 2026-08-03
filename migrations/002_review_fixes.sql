-- C3: Server-side received_at for rate limiting (untrusted created_at kept for engine)
ALTER TABLE review ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- H3: Retry counter for poison-pill handling
ALTER TABLE review ADD COLUMN IF NOT EXISTS retry_count SMALLINT NOT NULL DEFAULT 0;

-- Q4: CHECK constraints.
-- Wrapped in guarded DO blocks: run_migrations() re-executes every file on every
-- startup, and ADD CONSTRAINT has no IF NOT EXISTS — a bare ADD CONSTRAINT made
-- the second boot (and every later one) crash with DuplicateObjectError.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_review_status') THEN
        ALTER TABLE review ADD CONSTRAINT chk_review_status
            CHECK (status IN ('GATED', 'GATE_REJECTED', 'PROCESSING', 'COMMITTED', 'FAILED'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_rater_weight') THEN
        ALTER TABLE review ADD CONSTRAINT chk_rater_weight
            CHECK (rater_weight BETWEEN 0.00 AND 2.00);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_reputation_score') THEN
        ALTER TABLE profile_reputation ADD CONSTRAINT chk_reputation_score
            CHECK (reputation_score BETWEEN 0.0 AND 5.0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_batch_status') THEN
        ALTER TABLE batch_run ADD CONSTRAINT chk_batch_status
            CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'));
    END IF;
END $$;

-- Q4: Index on batch_id for debugging
CREATE INDEX IF NOT EXISTS idx_review_batch_id ON review(batch_id);

-- NOTE: the old "UPDATE review SET received_at = created_at WHERE received_at IS NULL"
-- backfill was dead code — the column is added NOT NULL DEFAULT NOW(), so it is never
-- NULL. Pre-existing rows (if any) received the migration-time NOW(); acceptable for v1.
