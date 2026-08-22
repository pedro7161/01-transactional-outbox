package dev.pedro.outbox.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.pedro.outbox.inventory.domain.ReceivedOrderEvent;
import dev.pedro.outbox.inventory.domain.ReceivedOrderEventRepository;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.time.Instant;

@ApplicationScoped
public class OrderCreatedConsumer {

    private static final Logger LOG = Logger.getLogger(OrderCreatedConsumer.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    ReceivedOrderEventRepository repository;

    @Incoming("order-created")
    @Blocking
    @Transactional
    public void consume(String json) {
        LOG.infof("Kafka message received payload=%s", json);

        OrderCreatedMessage message = deserialize(json);
        if (repository.findById(message.eventId()) != null) {
            LOG.infof("Inventory already processed OrderCreated eventId=%s", message.eventId());
            return;
        }

        ReceivedOrderEvent received = new ReceivedOrderEvent();
        received.eventId = message.eventId();
        received.eventType = message.eventType();
        received.occurredAt = message.occurredAt();
        received.orderId = message.orderId();
        received.customerId = message.customerId();
        received.total = message.total();
        received.receivedAt = Instant.now();
        repository.persist(received);

        LOG.infof("Inventory processed OrderCreated eventId=%s orderId=%s", message.eventId(), message.orderId());
    }

    private OrderCreatedMessage deserialize(String json) {
        try {
            return objectMapper.readValue(json, OrderCreatedMessage.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Invalid OrderCreated Kafka message", exception);
        }
    }
}
