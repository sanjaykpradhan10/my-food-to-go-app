# ftgo-consumer-service

**Port:** 8081
**Bounded context:** Consumer management

## Role

Owns the `Consumer` aggregate and answers one question for the rest of the system: is this consumer allowed to place an order? It's a separate bounded context from Order/Ticket/Delivery per the Ch.2 decomposition — a consumer's identity and standing (active/blocked) are meaningful independently of any specific order, and no other service needs to know how that decision is made, only the outcome.

In the Create Order saga (Ch.4), consumer-service is the first participant consulted — it reacts to a new order and reports back whether the consumer is verified, without touching the order or any other aggregate itself.

## API

None. This service has no REST controllers — it's purely event/message-driven, participating only via Kafka.

## Events

### Publishes

| Topic | Event type | When | Key fields |
|---|---|---|---|
| `consumer.events` (choreography) | `ConsumerVerified` | Consumer found and active | `orderId`, `consumerId` |
| `consumer.events` (choreography) | `ConsumerVerificationFailed` | Consumer not found, or found but inactive | `orderId`, `consumerId`, `reason` |
| `saga.replies` (orchestration) | `ConsumerVerified` / `ConsumerVerificationFailed` | Same decision, reported to the orchestrator instead of broadcast | `participant="consumer"`, `orderId`, `reason` |

### Consumes

| Topic | Event type | Mode | Handler |
|---|---|---|---|
| `order.events` | `OrderCreated` | choreography (`saga.mode=choreography`, default) | `ConsumerVerificationService.handleOrderCreated` |
| `consumer.commands` | `VerifyConsumerCommand` | orchestration (`saga.mode=orchestration`) | `ConsumerVerificationService.handleVerifyConsumerCommand` |

Both handlers share the same verification decision (see below) — only the outbound topic/event shape differs.

## Domain model

`Consumer(id, name, active)` — a minimal aggregate; `active` is the entire verification rule surface.

Verification decision (`ConsumerVerificationService.verify`):
1. Consumer not found by id → fails, reason `"consumer not found"`.
2. Consumer found but `active=false` → fails, reason `"consumer is not active"`.
3. Otherwise → verified.

Seed data (`DataSeeder`, runs once on an empty table): consumer id **1**, "Sanjay", `active=true`; consumer id **2**, "Blocked Consumer", `active=false`. These specific ids are load-bearing — the project's manual end-to-end verification scripts (see `docs/session-2026-07-17*.md`) place orders as consumer 1 to exercise the happy path and consumer 2 to exercise the "consumer verification fails" compensation case.

## Idempotency & reliability

Every inbound message is deduped via a `processed_events` ledger (insert-then-act in one local transaction) before any business logic runs, so Kafka's at-least-once delivery can't double-verify a consumer. Outbound events use the transactional outbox pattern: `ConsumerVerificationService` writes an `OutboxEvent` row in the same transaction as its business decision, and a separate `@Scheduled` `OutboxPublisher` polls unsent rows and publishes them to Kafka. Since this service now needs to publish to two different topics (`consumer.events` for choreography, `saga.replies` for orchestration) from the same outbox table, `OutboxEvent` carries a `topic` column set per row rather than the publisher hardcoding one topic — the Ch.4 orchestration pass generalized this across all four saga-participating services. `OutboxEvent`/`OutboxPublisher`/`KafkaProducerConfig` themselves now live in the shared `ftgo-common` module (see the root `docs/ARCHITECTURE.md`), not this service's own source tree.

## Running standalone

```bash
./gradlew :ftgo-consumer-service:test
```

Runs fully offline against H2 — no Docker required. To run the service live, it needs the shared MySQL/Kafka infrastructure; see the root [`README.md`](../README.md) for `docker compose up -d`.
