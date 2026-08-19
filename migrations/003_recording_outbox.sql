CREATE TABLE IF NOT EXISTS recording_outbox (
    id             BIGSERIAL PRIMARY KEY,
    event_id       TEXT NOT NULL UNIQUE,
    call_id        TEXT NOT NULL,
    account_id     TEXT NOT NULL,
    recording_url  TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recording_outbox_created_at
ON recording_outbox (created_at);