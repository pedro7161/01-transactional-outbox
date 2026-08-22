package dev.pedro.outbox.order.api;

import dev.pedro.outbox.order.domain.OrderRepository;
import dev.pedro.outbox.order.service.OrderApplicationService;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@Path("/orders")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class OrderResource {

    @Inject
    OrderApplicationService orderApplicationService;

    @Inject
    OrderRepository orderRepository;

    @POST
    public Response create(@Valid CreateOrderRequest request) {
        OrderResponse response = OrderResponse.from(orderApplicationService.create(request));
        return Response.created(URI.create("/orders/" + response.id())).entity(response).build();
    }

    @GET
    public List<OrderResponse> list() {
        return orderRepository.listAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @GET
    @Path("/{id}")
    public OrderResponse get(@PathParam("id") UUID id) {
        return orderRepository.findByIdOptional(id)
                .map(OrderResponse::from)
                .orElseThrow(NotFoundException::new);
    }
}
