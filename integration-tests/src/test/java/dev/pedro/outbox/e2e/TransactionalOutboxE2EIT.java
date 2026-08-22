package dev.pedro.outbox.e2e;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionalOutboxE2EIT {

    private static final String POSTGRES_SERVICE = "postgres-1";
    private static final String CONNECT_SERVICE = "connect-1";
    private static final String ORDER_SERVICE = "order-service-1";
    private static final String INVENTORY_SERVICE = "inventory-service-1";
    private static final Pattern ORDER_ID_PATTERN = Pattern.compile("\\\"id\\\"\\s*:\\s*\\\"([0-9a-fA-F-]{36})\\\"");

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private static ComposeContainer environment;
    private static Path projectRoot;

    @BeforeAll
    static void startEnvironment() throws Exception {
        projectRoot = Path.of(System.getProperty("project.root", ".."))
                .toAbsolutePath()
                .normalize();

        environment = new ComposeContainer(projectRoot.resolve("integration-tests/docker-compose.e2e.yml").toFile())
                .withBuild(true)
                .withRemoveVolumes(true)
                .withStartupTimeout(Duration.ofMinutes(4))
                .withExposedService(
                        POSTGRES_SERVICE,
                        5432,
                        Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)))
                .withExposedService(
                        CONNECT_SERVICE,
                        8083,
                        Wait.forHttp("/connectors").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
                .withExposedService(
                        ORDER_SERVICE,
                        8080,
                        Wait.forHttp("/q/health/ready").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
                .withExposedService(
                        INVENTORY_SERVICE,
                        8081,
                        Wait.forHttp("/q/health/ready").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

        environment.start();
        resetDatabases();
        registerDebeziumConnector();
    }

    @AfterAll
    static void stopEnvironment() {
        if (environment != null) {
            environment.stop();
        }
    }

    @Test
    void orderCreatedFlowsThroughOutboxDebeziumKafkaToInventory() throws Exception {
        UUID customerId = UUID.randomUUID();

        HttpResponse<String> createResponse = postOrder(customerId, "49.99");

        assertEquals(201, createResponse.statusCode(), createResponse.body());
        UUID orderId = extractOrderId(createResponse.body());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertEquals(1L, countOrderRows(orderId)));

        AtomicReference<UUID> eventId = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertEquals(1L, countOutboxRows(orderId));
            UUID foundEventId = findOutboxEventId(orderId);
            assertNotNull(foundEventId);
            eventId.set(foundEventId);
        });

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            String kafkaMessage = readFirstKafkaMessage();
            assertTrue(kafkaMessage.contains(orderId.toString()), kafkaMessage);
            assertTrue(kafkaMessage.contains(eventId.get().toString()), kafkaMessage);
            assertTrue(kafkaMessage.contains("OrderCreated"), kafkaMessage);
        });

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertEquals(1L, countInventoryRows(orderId, eventId.get())));
    }

    private static HttpResponse<String> postOrder(UUID customerId, String total)
            throws IOException, InterruptedException {
        String body = """
                {
                  "customerId": "%s",
                  "total": %s
                }
                """.formatted(customerId, total);

        HttpRequest request = HttpRequest.newBuilder(orderServiceUri("/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static UUID extractOrderId(String responseBody) {
        Matcher matcher = ORDER_ID_PATTERN.matcher(responseBody);
        assertTrue(matcher.find(), "Order response did not contain an id: " + responseBody);
        return UUID.fromString(matcher.group(1));
    }

    private static void resetDatabases() throws SQLException {
        try (Connection connection = orderDatabaseConnection()) {
            connection.createStatement().executeUpdate("DELETE FROM outbox_event");
            connection.createStatement().executeUpdate("DELETE FROM orders");
        }

        try (Connection connection = inventoryDatabaseConnection()) {
            connection.createStatement().executeUpdate("DELETE FROM received_order_events");
        }
    }

    private static void registerDebeziumConnector() throws Exception {
        String connectorJson = Files.readString(
                projectRoot.resolve("infrastructure/debezium/register-order-outbox.json"));

        HttpRequest request = HttpRequest.newBuilder(connectUri("/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorJson))
                .build();

        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode(), response.body());

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            HttpRequest statusRequest = HttpRequest.newBuilder(
                            connectUri("/connectors/order-outbox-connector/status"))
                    .GET()
                    .build();
            HttpResponse<String> status = HTTP.send(statusRequest, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, status.statusCode(), status.body());
            assertTrue(status.body().contains("\"state\":\"RUNNING\""), status.body());
        });
    }

    private static long countOrderRows(UUID orderId) throws SQLException {
        return count(orderDatabaseConnection(),
                "SELECT COUNT(*) FROM orders WHERE id = ?", orderId);
    }

    private static long countOutboxRows(UUID orderId) throws SQLException {
        return count(orderDatabaseConnection(),
                "SELECT COUNT(*) FROM outbox_event WHERE aggregateid = ?", orderId.toString());
    }

    private static UUID findOutboxEventId(UUID orderId) throws SQLException {
        try (Connection connection = orderDatabaseConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT payload::jsonb ->> 'eventId' FROM outbox_event WHERE aggregateid = ?")) {
            statement.setString(1, orderId.toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                String value = resultSet.getString(1);
                return value == null ? null : UUID.fromString(value);
            }
        }
    }

    private static long countInventoryRows(UUID orderId, UUID eventId) throws SQLException {
        try (Connection connection = inventoryDatabaseConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM received_order_events WHERE order_id = ? AND event_id = ?")) {
            statement.setObject(1, orderId);
            statement.setObject(2, eventId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    private static long count(Connection connection, String sql, Object value) throws SQLException {
        try (connection; PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, value);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    private static String readFirstKafkaMessage() throws Exception {
        ContainerState kafka = kafkaContainer();
        Container.ExecResult result = kafka.execInContainer(
                "bash",
                "-lc",
                "/kafka/bin/kafka-console-consumer.sh " +
                        "--bootstrap-server kafka:9092 " +
                        "--topic order-events " +
                        "--from-beginning " +
                        "--max-messages 1 " +
                        "--timeout-ms 3000");
        return result.getStdout() + result.getStderr();
    }

    private static ContainerState kafkaContainer() {
        Optional<ContainerState> kafka = environment.getContainerByServiceName("kafka-1");
        if (kafka.isEmpty()) {
            kafka = environment.getContainerByServiceName("kafka");
        }
        return kafka.orElseThrow(() -> new IllegalStateException("Kafka container not found"));
    }

    private static Connection orderDatabaseConnection() throws SQLException {
        return DriverManager.getConnection(postgresJdbcUrl("orderdb"), "postgres", "postgres");
    }

    private static Connection inventoryDatabaseConnection() throws SQLException {
        return DriverManager.getConnection(postgresJdbcUrl("inventorydb"), "postgres", "postgres");
    }

    private static String postgresJdbcUrl(String database) {
        return "jdbc:postgresql://%s:%d/%s".formatted(
                environment.getServiceHost(POSTGRES_SERVICE, 5432),
                environment.getServicePort(POSTGRES_SERVICE, 5432),
                database);
    }

    private static URI orderServiceUri(String path) {
        return URI.create("http://%s:%d%s".formatted(
                environment.getServiceHost(ORDER_SERVICE, 8080),
                environment.getServicePort(ORDER_SERVICE, 8080),
                path));
    }

    private static URI connectUri(String path) {
        return URI.create("http://%s:%d%s".formatted(
                environment.getServiceHost(CONNECT_SERVICE, 8083),
                environment.getServicePort(CONNECT_SERVICE, 8083),
                path));
    }
}
