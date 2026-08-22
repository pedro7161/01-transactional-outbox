package dev.pedro.outbox.order.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedPayload(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID orderId,
        UUID customerId,
        BigDecimal total
) {
}
