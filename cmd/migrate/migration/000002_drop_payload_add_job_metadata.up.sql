ALTER TABLE jobs
    ADD COLUMN priority INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 5,
    ADD COLUMN timeout_seconds INTEGER NOT NULL DEFAULT 60,
    ADD COLUMN scheduled_at TIMESTAMPTZ;

ALTER TABLE jobs
    DROP COLUMN payload;

CREATE INDEX idx_jobs_dequeue ON jobs (status, priority DESC, scheduled_at, created_at);
