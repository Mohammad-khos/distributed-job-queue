DROP INDEX IF EXISTS idx_jobs_dequeue;

ALTER TABLE jobs
    ADD COLUMN payload BYTEA NOT NULL DEFAULT ''::bytea;

ALTER TABLE jobs
    DROP COLUMN scheduled_at,
    DROP COLUMN timeout_seconds,
    DROP COLUMN max_retries,
    DROP COLUMN retry_count,
    DROP COLUMN priority;
