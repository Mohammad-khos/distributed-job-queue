CREATE EXTENSION IF NOT EXISTS pgcrypto; 

CREATE TYPE job_status AS ENUM ('pending', 'reserved', 'processing', 'done', 'failed');

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            VARCHAR(100) NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    status          job_status NOT NULL DEFAULT 'pending',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    timeout_seconds INTEGER NOT NULL DEFAULT 60,

    last_error      TEXT,
    locked_at       TIMESTAMPTZ,
    scheduled_at    TIMESTAMPTZ,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at         TIMESTAMPTZ
);

CREATE INDEX idx_jobs_status ON jobs (status, created_at);
CREATE INDEX idx_jobs_dequeue ON jobs (status, priority DESC, scheduled_at, created_at);
