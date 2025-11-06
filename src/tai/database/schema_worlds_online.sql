CREATE TABLE IF NOT EXISTS worlds_online (
    name TEXT,
    players INTEGER,
    static BOOLEAN,
    ssmp BOOLEAN,
    saved_at TIMESTAMP_S
);

CREATE INDEX IF NOT EXISTS idx_worlds_online_name ON worlds_online (name);