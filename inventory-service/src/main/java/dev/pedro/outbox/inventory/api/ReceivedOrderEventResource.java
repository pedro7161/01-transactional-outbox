package dev.pedro.outbox.inventory.api;

import dev.pedro.outbox.inventory.domain.ReceivedOrderEventRepository;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

@Path("/received-events")
@Produces(MediaType.APPLICATION_JSON)
public class ReceivedOrderEventResource {

    @Inject
    ReceivedOrderEventRepository repository;

    @GET
    public List<ReceivedOrderEventResponse> list() {
        return repository.listAll().stream()
                .map(ReceivedOrderEventResponse::from)
                .toList();
    }
}
