-- Strict enum representing every valid lifecycle state of a background job
CREATE TYPE job_status AS ENUM (
    'pending',      -- Enqueued and waiting to be picked up (or waiting for scheduled_at)
    'processing',   -- Currently leased and executing on a worker
    'completed',    -- Finished successfully
    'retrying',     -- Failed temporarily; waiting for exponential backoff before retry
    'dlq'           -- Exceeded max_retries; moved to Dead-Letter Queue for inspection
);

-- Durable audit table storing full job metadata, payload, and failure history
CREATE TABLE jobs (
    id UUID PRIMARY KEY,                                -- Unique job identifier (UUIDv4)
    queue_name VARCHAR(100) NOT NULL,                   -- Logical queue (e.g., 'emails', 'default')
    job_type VARCHAR(100) NOT NULL,                     -- Handler name to execute (e.g., 'send_email')
    idempotency_key VARCHAR(255) UNIQUE,                -- Prevents duplicate job creation on retried requests
    payload JSONB NOT NULL,                             -- Arbitrary JSON arguments passed to the worker
    status job_status NOT NULL DEFAULT 'pending',       -- Current lifecycle state
    attempts INT NOT NULL DEFAULT 0,                    -- Number of times workers have tried this job
    max_retries INT NOT NULL DEFAULT 3,                 -- Maximum allowed attempts before moving to DLQ
    last_error TEXT,                                    -- Stack trace / message from the most recent failure
    worker_id VARCHAR(100),                             -- Identifier of the worker currently/last running it
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),    -- Earliest timestamp the job is eligible to run
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),      -- When the job was first enqueued
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()       -- Last state transition timestamp
);

-- Composite index to speed up dashboard queries like "show all DLQ jobs in queue X"
CREATE INDEX idx_jobs_status_queue ON jobs (queue_name, status);