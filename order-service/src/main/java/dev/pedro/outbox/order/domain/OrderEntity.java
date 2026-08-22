package dev.pedro.outbox.order.domain;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity extends PanacheEntityBase {

    @Id
    public UUID id;

    @Column(name = "customer_id", nullable = false)
    public UUID customerId;

    @Column(nullable = false, precision = 19, scale = 2)
    public BigDecimal total;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    public OrderStatus status;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;
}
