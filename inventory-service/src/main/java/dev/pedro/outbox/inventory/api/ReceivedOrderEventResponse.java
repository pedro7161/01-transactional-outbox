package dev.pedro.outbox.inventory.api;

import dev.pedro.outbox.inventory.domain.ReceivedOrderEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ReceivedOrderEventResponse(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID orderId,
        UUID customerId,
        BigDecimal total,
        Instant receivedAt
) {
    public static ReceivedOrderEventResponse from(ReceivedOrderEvent event) {
        return new ReceivedOrderEventResponse(
                event.eventId,
                event.eventType,
                event.occurredAt,
                event.orderId,
                event.customerId,
                event.total,
                event.receivedAt
        );
    }
}
