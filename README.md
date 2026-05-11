# Delivery Count Example (RabbitMQ 4.2 vs 4.3)

This project demonstrates a behavior change in RabbitMQ quorum queues that affects retry and dead-letter expectations when upgrading from RabbitMQ 4.2 to 4.3.

Issue: https://github.com/spring-projects/spring-amqp/issues/3435

## What this repository shows

- A minimal Spring AMQP setup with a quorum queue and a configured `delivery-limit`.
- Integration tests that run against two RabbitMQ versions:
  - `rabbitmq:4.2.5-management`
  - `rabbitmq:4.3.0-management`
- The difference between message counters in RabbitMQ 4.3:
  - `x-acquired-count` increments on every requeue/return.
  - `x-delivery-count` increments only when a delivery attempt is considered failed.

Because poison handling is tied to delivery attempts, this can change when dead-lettering is triggered after an upgrade.

## Relevant test

- `src/test/java/org/synyx/deliverycountexample/RabbitRetryIT.java`

## Run the relevant tests from CLI

```bash
./mvnw -Dtest=RabbitRetryIT test
```

This command runs the parameterized integration test that starts RabbitMQ via Testcontainers and compares behavior across 4.2.5 and 4.3.0.

