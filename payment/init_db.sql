
CREATE TABLE IF NOT EXISTS user_snapshots (
    user_id TEXT    PRIMARY KEY,
    credit  INTEGER NOT NULL,
    version INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS received_events (
    event_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    status TEXT DEFAULT 'RECEIVED',
    result JSONB DEFAULT '{}'
);


-- -- 3. Create the log table
-- CREATE TABLE IF NOT EXISTS log (
--     id         TEXT        PRIMARY KEY,
--     order_id   TEXT        NOT NULL,
--     event_type TEXT        NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT now(),
--     payload    JSONB       NOT NULL,
--     version   INTEGER     NOT NULL DEFAULT 0
-- );

-- 4. Create the outbox table
CREATE TABLE IF NOT EXISTS outbox (
    id         TEXT        PRIMARY KEY,
    topic      TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    sent       BOOLEAN     DEFAULT FALSE
);