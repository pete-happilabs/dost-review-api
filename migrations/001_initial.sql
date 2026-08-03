CREATE TABLE IF NOT EXISTS review (
    id                UUID PRIMARY KEY,
    target_profile_id UUID NOT NULL,
    rater_profile_id  UUID NOT NULL,
    rater_weight      DECIMAL(3,2) NOT NULL DEFAULT 1.00,
    review_text       TEXT NOT NULL,
    multimedia_json   JSONB,
    status            VARCHAR(20) NOT NULL DEFAULT 'GATED',
    batch_id          UUID,
    event_id          UUID,
    created_at        TIMESTAMPTZ NOT NULL,
    processed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_review_target_status
    ON review(target_profile_id, status);
-- idx_review_rate_limit and idx_review_batch_pickup (on created_at) were
-- superseded by the received_at indexes in 003. Since every file re-runs on
-- every startup, creating them here meant a full index build on `review` at
-- each boot only for 003 to drop them again — so they are gone from 001;
-- 003 keeps the DROPs for databases that already have them.

CREATE TABLE IF NOT EXISTS profile_reputation (
    profile_id       UUID PRIMARY KEY,
    reputation       VARCHAR(20) NOT NULL DEFAULT 'average',
    reputation_score DECIMAL(3,1) NOT NULL DEFAULT 3.0,
    total_reviews    INTEGER NOT NULL DEFAULT 0,
    all_tags         JSONB NOT NULL DEFAULT '{}',
    summary          TEXT NOT NULL DEFAULT '',
    state            JSONB NOT NULL DEFAULT '{}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS batch_run (
    id           UUID PRIMARY KEY,
    status       VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    stats        JSONB,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
