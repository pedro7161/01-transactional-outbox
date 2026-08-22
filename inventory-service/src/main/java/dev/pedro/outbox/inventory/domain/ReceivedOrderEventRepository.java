package dev.pedro.outbox.inventory.domain;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;

@ApplicationScoped
public class ReceivedOrderEventRepository implements PanacheRepositoryBase<ReceivedOrderEvent, UUID> {
}
