-- Job records: the authoritative, queryable state of every job.
-- Kafka moves work; this table answers "what is the status of job X?".
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT        PRIMARY KEY,
    type        TEXT        NOT NULL,
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status      TEXT        NOT NULL,
    attempts    INT         NOT NULL DEFAULT 0,
    max_retries INT         NOT NULL DEFAULT 5,
    last_error  TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Status is the hot filter for dashboards and the /stats endpoint.
CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_type       ON jobs (type);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);
