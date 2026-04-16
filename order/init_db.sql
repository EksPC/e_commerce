
-- 1. Create the orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id   TEXT    PRIMARY KEY,
    paid       BOOLEAN NOT NULL,
    items      JSONB   NOT NULL,
    user_id    TEXT    NOT NULL,
    total_cost INTEGER NOT NULL
);

-- 2. Setup the sequence for order IDs
CREATE SEQUENCE IF NOT EXISTS orders_order_id_seq
START WITH 1 INCREMENT BY 1;

ALTER TABLE orders
ALTER COLUMN order_id
SET DEFAULT nextval('orders_order_id_seq')::text;

-- 3. Create the log table
CREATE TABLE IF NOT EXISTS log (
    id         TEXT        PRIMARY KEY,
    order_id   TEXT        NOT NULL,
    event_type TEXT        NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    data       TEXT        NOT NULL
);

-- 4. Create the outbox table
CREATE TABLE IF NOT EXISTS outbox (
    id         TEXT        PRIMARY KEY,
    topic      TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    sent       BOOLEAN     DEFAULT FALSE
);