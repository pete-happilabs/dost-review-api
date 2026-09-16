-- 005_review_queue_single_open.sql — one OPEN review per profile.
--
-- service._fold enqueues on every ingest once riskScore >= QUEUE_AT, so a persistently
-- high-risk account produced one OPEN review_queue row per message and a human reviewer
-- saw the same account over and over. 004 is left alone (it may already be applied).
--
-- Ships together with `ON CONFLICT DO NOTHING` in store.enqueue_review: the index alone
-- would turn the existing unconditional INSERT into a UniqueViolation on the second
-- message, which would roll the whole ingest back.

-- Collapse duplicates that accumulated before the index existed, or CREATE UNIQUE INDEX
-- cannot be created at all on a database that has been running the old code. The newest
-- OPEN row per profile is kept; older ones are EXPIRED rather than deleted, because the
-- queue is evidence a human reads. Naturally idempotent: a second run finds no duplicates.
UPDATE review_queue q
   SET status = 'EXPIRED'
 WHERE q.status = 'OPEN'
   AND EXISTS (
       SELECT 1 FROM review_queue newer
        WHERE newer.profile_id = q.profile_id
          AND newer.status = 'OPEN'
          AND (newer.created_at, newer.id) > (q.created_at, q.id)
   );

CREATE UNIQUE INDEX IF NOT EXISTS uq_review_queue_open
    ON review_queue (profile_id) WHERE status = 'OPEN';
