CREATE SEQUENCE IF NOT EXISTS item_snapshots_item_id_seq
START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS item_snapshots (
    item_id    TEXT    PRIMARY KEY
                        DEFAULT nextval('item_snapshots_item_id_seq')::text,
    stock      INTEGER NOT NULL,
    price      INTEGER NOT NULL,
    version    INTEGER NOT NULL
);

-- CREATE TABLE IF NOT EXISTS log (
--     id         TEXT        PRIMARY KEY,
--     item_id    TEXT        NOT NULL,
--     event_type TEXT        NOT NULL,
--     payload    JSONB       NOT NULL,
--     version    INTEGER     NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT now()
-- );

CREATE TABLE IF NOT EXISTS received_events (
    event_id   TEXT        PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    status     TEXT        DEFAULT 'RECEIVED',
    result     JSONB       DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS outbox (
    id         TEXT        PRIMARY KEY,
    topic      TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    sent       BOOLEAN     DEFAULT FALSE
);
