package dev.pedro.outbox.order.event;

import io.debezium.outbox.quarkus.ExportedEvent;

import java.time.Instant;

public class OrderCreatedOutboxEvent implements ExportedEvent<String, String> {

    private static final String AGGREGATE_TYPE = "orders";
    private static final String EVENT_TYPE = "OrderCreated";

    private final String orderId;
    private final String payload;
    private final Instant occurredAt;

    public OrderCreatedOutboxEvent(String orderId, String payload, Instant occurredAt) {
        this.orderId = orderId;
        this.payload = payload;
        this.occurredAt = occurredAt;
    }

    @Override
    public String getAggregateId() {
        return orderId;
    }

    @Override
    public String getAggregateType() {
        return AGGREGATE_TYPE;
    }

    @Override
    public String getType() {
        return EVENT_TYPE;
    }

    @Override
    public Instant getTimestamp() {
        return occurredAt;
    }

    @Override
    public String getPayload() {
        return payload;
    }
}
