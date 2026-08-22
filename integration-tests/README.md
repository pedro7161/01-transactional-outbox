# End-to-end integration test

Run the real Transactional Outbox infrastructure test from the repository root:

```bash
mvn verify -Pintegration
```

The test starts PostgreSQL, Kafka, Kafka Connect/Debezium, `order-service`, and `inventory-service` with Testcontainers and Docker Compose. It registers the existing Debezium connector automatically, calls `POST /orders`, verifies the order and outbox rows, checks the real Kafka topic, and waits until exactly one matching event is stored by `inventory-service`.

Docker and Docker Compose V2 are required. No manually running Compose stack is needed before the test.
