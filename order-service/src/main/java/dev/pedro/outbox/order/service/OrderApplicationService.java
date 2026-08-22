package dev.pedro.outbox.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.pedro.outbox.order.api.CreateOrderRequest;
import dev.pedro.outbox.order.domain.OrderEntity;
import dev.pedro.outbox.order.domain.OrderRepository;
import dev.pedro.outbox.order.domain.OrderStatus;
import dev.pedro.outbox.order.event.OrderCreatedOutboxEvent;
import dev.pedro.outbox.order.event.OrderCreatedPayload;
import io.debezium.outbox.quarkus.ExportedEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class OrderApplicationService {

    private static final Logger LOG = Logger.getLogger(OrderApplicationService.class);

    @Inject
    OrderRepository orderRepository;

    @Inject
    Event<ExportedEvent<?, ?>> outboxEvents;

    @Inject
    ObjectMapper objectMapper;

    @Transactional
    public OrderEntity create(CreateOrderRequest request) {
        Instant now = Instant.now();

        OrderEntity order = new OrderEntity();
        order.id = UUID.randomUUID();
        order.customerId = request.customerId();
        order.total = request.total();
        order.status = OrderStatus.CREATED;
        order.createdAt = now;
        orderRepository.persistAndFlush(order);

        LOG.infof("Order persisted orderId=%s", order.id);

        UUID eventId = UUID.randomUUID();
        OrderCreatedPayload payload = new OrderCreatedPayload(
                eventId,
                "OrderCreated",
                now,
                order.id,
                order.customerId,
                order.total
        );

        outboxEvents.fire(new OrderCreatedOutboxEvent(order.id.toString(), toJson(payload), now));
        LOG.infof("Outbox event persisted eventId=%s orderId=%s", eventId, order.id);

        return order;
    }

    private String toJson(OrderCreatedPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize OrderCreated payload", exception);
        }
    }
}
