CREATE TABLE IF NOT EXISTS worlds_online (
    name TEXT PRIMARY KEY,
    players INTEGER,
    static BOOLEAN,
    ssmp BOOLEAN,
    saved_at TIMESTAMP_S
);
