
-- 1. Create the orders table
CREATE TABLE IF NOT EXISTS sagas (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    payment TEXT NOT NULL DEFAULT 'PENDING',
    stock TEXT NOT NULL DEFAULT 'PENDING',
    status TEXT NOT NULL DEFAULT 'CREATED',
    results JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    version  INTEGER NOT NULL DEFAULT 1
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
--     data       TEXT        NOT NULL
-- );

-- 4. Create the outbox table
CREATE TABLE IF NOT EXISTS outbox (
    id         TEXT        PRIMARY KEY,
    topic      TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    sent       BOOLEAN     DEFAULT FALSE
);

-- CREATE OR REPLACE FUNCTION notify_new_insert() RETURNS trigger AS $$
-- BEGIN
--   PERFORM pg_notify('outbox_insert', row_to_json(NEW)::text);
--   RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER new_outbox_trigger
-- AFTER INSERT ON outbox
-- FOR EACH ROW EXECUTE FUNCTION notify_new_insert();

-- Indexes for query optimization
CREATE INDEX IF NOT EXISTS idx_sagas_id ON sagas(id);
CREATE INDEX IF NOT EXISTS idx_sagas_order_id ON sagas(order_id);
CREATE INDEX IF NOT EXISTS idx_received_events_event_id ON received_events(event_id);
CREATE INDEX IF NOT EXISTS idx_received_events_status ON received_events(status);
CREATE INDEX IF NOT EXISTS idx_outbox_sent ON outbox(sent) WHERE sent = FALSE;

