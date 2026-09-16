-- 004_risk.sql — fraud-signal accumulation. Separate from profile_reputation.state on
-- purpose: the review engine rebuilds that blob and would erase a sibling family.

CREATE TABLE IF NOT EXISTS signal_event (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id          TEXT        NOT NULL,
    event_id            TEXT        NOT NULL,
    sender_profile_id   TEXT        NOT NULL,   -- hum.* as on the chat wire
    receiver_profile_id TEXT,
    model_version       TEXT        NOT NULL,
    verdict             TEXT        NOT NULL,
    record              JSONB       NOT NULL,   -- the Plan 1 record, verbatim (no raw text, no raw PII)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_signal_event_sender ON signal_event (sender_profile_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_event_session ON signal_event (session_id, created_at);

CREATE TABLE IF NOT EXISTS conversation_risk (
    session_id   TEXT PRIMARY KEY,
    state        JSONB       NOT NULL DEFAULT '{}'::jsonb,   -- process_conversation state blob
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS profile_risk (
    profile_id    TEXT PRIMARY KEY,                       -- hum.* (chat-wire id); see ids.py
    account_id    TEXT,                                   -- Onboard account uuid when resolvable
    state         JSONB       NOT NULL DEFAULT '{}'::jsonb,   -- process_signals state blob
    risk_score    NUMERIC(5,1) NOT NULL DEFAULT 0,
    risk_tier     TEXT        NOT NULL DEFAULT 'none'
                  CHECK (risk_tier IN ('none','elevated','high','critical')),
    repeated_across JSONB     NOT NULL DEFAULT '[]'::jsonb,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_profile_risk_tier ON profile_risk (risk_tier) WHERE risk_tier <> 'none';

CREATE TABLE IF NOT EXISTS outcome_event (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    profile_id   TEXT NOT NULL,          -- the account the outcome is ABOUT
    reporter_id  TEXT,                   -- who reported, if a report
    session_id   TEXT,
    tag          TEXT NOT NULL,          -- one of engine.FRAUD_OUTCOME_TAGS
    evidence     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outcome_profile ON outcome_event (profile_id, created_at DESC);

CREATE TABLE IF NOT EXISTS review_queue (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    profile_id   TEXT NOT NULL,
    session_id   TEXT,
    reason       TEXT NOT NULL,
    risk_score   NUMERIC(5,1) NOT NULL,
    evidence     JSONB NOT NULL DEFAULT '{}'::jsonb,
    status       TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','DECIDED','EXPIRED')),
    decision     TEXT CHECK (decision IN ('CLEAR','RESTRICT','SUSPEND')),
    decided_by   TEXT,
    decided_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL DEFAULT now() + interval '7 days'
);
CREATE INDEX IF NOT EXISTS idx_review_queue_open ON review_queue (status, risk_score DESC) WHERE status = 'OPEN';
