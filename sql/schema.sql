CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    partition_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    sequence_number BIGINT NOT NULL CHECK (sequence_number > 0),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (partition_key, sequence_number)
);

CREATE TABLE outbox (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES orders(order_id),
    partition_key TEXT NOT NULL,
    sequence_number BIGINT NOT NULL CHECK (sequence_number > 0),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    UNIQUE (partition_key, sequence_number)
);

CREATE INDEX outbox_unpublished_order
    ON outbox (created_at, partition_key, sequence_number)
    WHERE published_at IS NULL;

CREATE TABLE producer_partition_cursors (
    partition_key TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0)
);

CREATE TABLE processed_events (
    consumer_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id),
    UNIQUE (consumer_name, partition_key, sequence_number)
);

CREATE TABLE order_effects (
    effect_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (consumer_name, event_id),
    UNIQUE (consumer_name, partition_key, sequence_number)
);

CREATE TABLE consumer_partition_cursors (
    consumer_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    PRIMARY KEY (consumer_name, partition_key)
);

CREATE TABLE dead_letters (
    event_id TEXT NOT NULL,
    consumer_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    payload JSONB NOT NULL,
    failure_reason TEXT NOT NULL,
    attempts INTEGER NOT NULL CHECK (attempts > 0),
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, event_id)
);

-- Producer transaction: lock producer_partition_cursors for the key, increment
-- last_sequence, and insert the order plus outbox event in the same transaction.
-- The ordered outbox relay may publish repeatedly; it never owns correctness.
--
-- Consumer transaction:
--   1. INSERT processed_events ... ON CONFLICT (consumer_name, event_id)
--      DO NOTHING RETURNING event_id. Other uniqueness failures are not
--      duplicates: they indicate conflicting key/sequence data and must fail.
--   2. If no row was returned, acknowledge the duplicate without reapplying.
--   3. Lock consumer_partition_cursors and assert sequence = last_sequence + 1.
--   4. Apply the business effect, advance the cursor, and commit with the
--      processed_events row.
-- A crash before commit rolls both writes back. A crash after commit but before
-- broker acknowledgement causes a redelivery rejected by the primary key.
