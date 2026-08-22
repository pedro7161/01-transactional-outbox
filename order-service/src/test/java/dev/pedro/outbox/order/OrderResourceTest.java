package dev.pedro.outbox.order;

import dev.pedro.outbox.order.api.CreateOrderRequest;
import dev.pedro.outbox.order.api.OrderResponse;
import dev.pedro.outbox.order.domain.OrderRepository;
import dev.pedro.outbox.order.service.OrderApplicationService;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class OrderResourceTest {

    @Inject
    OrderRepository orderRepository;

    @Inject
    OrderApplicationService orderApplicationService;

    @Inject
    EntityManager entityManager;

    @BeforeEach
    void cleanDatabase() {
        QuarkusTransaction.requiringNew().run(() -> {
            entityManager.createNativeQuery("DELETE FROM outbox_event").executeUpdate();
            entityManager.createNativeQuery("DELETE FROM orders").executeUpdate();
        });
    }

    @Test
    void creatingOrderPersistsOrderAndOutboxEvent() {
        UUID customerId = UUID.randomUUID();

        OrderResponse response = given()
                .contentType(ContentType.JSON)
                .body(new CreateOrderRequest(customerId, new BigDecimal("49.99")))
                .when()
                .post("/orders")
                .then()
                .statusCode(201)
                .extract()
                .as(OrderResponse.class);

        assertEquals(1, orderRepository.count());
        assertEquals(1L, outboxCount(response.id()));

        String payload = outboxPayload(response.id());
        assertTrue(payload.contains("\"eventType\":\"OrderCreated\""));
        assertTrue(payload.contains(response.id().toString()));
        assertTrue(payload.contains(customerId.toString()));
    }

    @Test
    void invalidRequestReturnsBadRequest() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"total\":0}")
                .when()
                .post("/orders")
                .then()
                .statusCode(400);
    }

    @Test
    void rollbackLeavesNeitherOrderNorOutboxEvent() {
        UUID customerId = UUID.randomUUID();
        UUID[] orderId = new UUID[1];

        assertThrows(RuntimeException.class, () ->
                QuarkusTransaction.requiringNew().run(() -> {
                    orderId[0] = orderApplicationService.create(
                            new CreateOrderRequest(customerId, new BigDecimal("10.00"))
                    ).id;
                    throw new IllegalStateException("Force rollback after order and outbox writes");
                })
        );

        assertEquals(0, orderRepository.count());
        assertEquals(0L, totalOutboxCount());
    }

    private long outboxCount(UUID orderId) {
        Number count = (Number) entityManager.createNativeQuery(
                        "SELECT COUNT(*) FROM outbox_event WHERE aggregateid = :aggregateId")
                .setParameter("aggregateId", orderId.toString())
                .getSingleResult();
        return count.longValue();
    }

    private long totalOutboxCount() {
        Number count = (Number) entityManager.createNativeQuery("SELECT COUNT(*) FROM outbox_event")
                .getSingleResult();
        return count.longValue();
    }

    private String outboxPayload(UUID orderId) {
        return (String) entityManager.createNativeQuery(
                        "SELECT payload FROM outbox_event WHERE aggregateid = :aggregateId")
                .setParameter("aggregateId", orderId.toString())
                .getSingleResult();
    }
}
