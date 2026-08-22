package dev.pedro.outbox.inventory.domain;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "received_order_events")
public class ReceivedOrderEvent extends PanacheEntityBase {

    @Id
    @Column(name = "event_id")
    public UUID eventId;

    @Column(name = "event_type", nullable = false, length = 64)
    public String eventType;

    @Column(name = "occurred_at", nullable = false)
    public Instant occurredAt;

    @Column(name = "order_id", nullable = false)
    public UUID orderId;

    @Column(name = "customer_id", nullable = false)
    public UUID customerId;

    @Column(nullable = false, precision = 19, scale = 2)
    public BigDecimal total;

    @Column(name = "received_at", nullable = false)
    public Instant receivedAt;
}
