package dev.pedro.outbox.inventory.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedMessage(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID orderId,
        UUID customerId,
        BigDecimal total
) {
}
