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
CREATE INDEX IF NOT EXISTS idx_review_rate_limit
    ON review(rater_profile_id, target_profile_id, created_at);
CREATE INDEX IF NOT EXISTS idx_review_batch_pickup
    ON review(status, created_at);

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
