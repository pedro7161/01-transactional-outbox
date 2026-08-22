package dev.pedro.outbox.order.api;

import dev.pedro.outbox.order.domain.OrderEntity;
import dev.pedro.outbox.order.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        UUID customerId,
        BigDecimal total,
        OrderStatus status,
        Instant createdAt
) {
    public static OrderResponse from(OrderEntity order) {
        return new OrderResponse(order.id, order.customerId, order.total, order.status, order.createdAt);
    }
}
