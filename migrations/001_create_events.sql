CREATE TABLE IF NOT EXISTS events (
    event_id    UUID        PRIMARY KEY,
    type        TEXT        NOT NULL,
    session_id  UUID        NOT NULL,
    page        TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    properties  JSONB       NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_session_id ON events (session_id);
CREATE INDEX IF NOT EXISTS idx_events_page       ON events (page);
CREATE INDEX IF NOT EXISTS idx_events_timestamp  ON events (timestamp);
