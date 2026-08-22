CREATE TABLE received_order_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    order_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    total NUMERIC(19, 2) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);
