CREATE EXTENSION IF NOT EXISTS pgcrypto; 

CREATE TYPE job_status AS ENUM ('pending', 'reserved', 'processing', 'done', 'failed');

CREATE TABLE jobs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type        VARCHAR(100) NOT NULL,
    payload     BYTEA NOT NULL,
    status      job_status NOT NULL DEFAULT 'pending',

    last_error  TEXT,
    locked_at   TIMESTAMPTZ,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at     TIMESTAMPTZ
);

CREATE INDEX idx_jobs_status ON jobs (status, created_at);