CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    total NUMERIC(19, 2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregatetype VARCHAR(255) NOT NULL,
    aggregateid VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    payload TEXT NOT NULL,
    tracingspancontext VARCHAR(256)
);

CREATE INDEX idx_outbox_event_aggregate_id ON outbox_event (aggregateid);
CREATE INDEX idx_outbox_event_timestamp ON outbox_event (timestamp);
