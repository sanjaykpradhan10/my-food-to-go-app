# ftgo-consumer-service

**Port:** 8081
**Bounded context:** Consumer management

## Role

Owns the `Consumer` aggregate and answers one question for the rest of the system: is this consumer allowed to place an order? It's a separate bounded context from Order/Ticket/Delivery per the Ch.2 decomposition — a consumer's identity and standing (active/blocked) are meaningful independently of any specific order, and no other service needs to know how that decision is made, only the outcome.

In the Create Order saga (Ch.4), consumer-service is the first participant consulted — it reacts to a new order and reports back whether the consumer is verified, without touching the order or any other aggregate itself.

## API

`POST /consumers` — **Auth:** `ADMIN` (bearer JWT issued by `ftgo-authorization-server`, Ch.11 §11.1; validated by this service as an OAuth2 resource server).

Request:
```json
{"name": "E2E Consumer", "active": true}
```

Response (`201`):
```json
{"id": 7, "name": "E2E Consumer", "active": true}
```

Added in Ch.10 sub-project 3 (end-to-end tests, §10.3) so the end-to-end test can create its own consumer rather than depend on `DataSeeder`'s fixed seed ids. This is this service's only REST controller — everything else below is still purely event/message-driven via Kafka. Not exposed through either gateway. `DataSeeder` is untouched and still separately seeds "Sanjay" (active) / "Blocked Consumer" (inactive) on every startup against an empty table.

## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only (no custom
`HealthIndicator` code). Reports:
- `db` — MySQL reachability via the service's `DataSource`.

`ftgo-consumer-service` has no `eureka-client` dependency (pre-existing, unrelated to
Ch.11), so it never registers with Eureka and has no `discoveryComposite` component — only `db`.

There is no `kafka` component: Spring Boot's actuator-autoconfigure no longer ships a Kafka
health contributor as of this project's Spring Boot version (3.5.16) — verified directly against
the built jars (`spring-boot-actuator-autoconfigure` retains only `KafkaMetricsAutoConfiguration`
under `actuate.autoconfigure.kafka`; `spring-kafka` ships no health-indicator class either) — and
adding a custom one is out of scope for this sub-project.

`management.endpoint.health.show-details: always` — safe here since these ports aren't exposed
to untrusted clients in this project; full component detail is the point of exercising this
pattern. Verified against the real, running stack by `ftgo-end-to-end-test`'s
`AllServicesReportHealthy.feature`.

## Metrics (Ch.11, §11.3.4)

`GET /actuator/prometheus` — Micrometer `PrometheusMeterRegistry`, unauthenticated. Custom business
counter:

- `consumers_created` — `ConsumerController`, on consumer creation.

Appears in the exposition output with a `_total` suffix (`consumers_created_total`). Scraped every
5s by the `prometheus` compose service.

## Tracing (Ch.11, §11.3.3)

Traces exported via OTLP/HTTP to Grafana Tempo (`http://tempo:4318/v1/traces`), 100% sampled
(`management.tracing.sampling.probability: 1.0`). HTTP and JDBC spans come free from Spring
Boot's autoconfiguration. This service's `@KafkaListener`s (`VerifyConsumerCommandListener`,
`OrderEventListener`) run on Boot's autoconfigured, property-driven listener container factory —
unlike `ftgo-order-history-service`'s hand-built one — so `spring.kafka.listener.observation-enabled: true`
alone is enough to get their consumer spans; there is no matching
`spring.kafka.template.observation-enabled` property here since this service publishes its own
events via the Ch.3 CDC/outbox pipeline rather than a `KafkaTemplate`. Viewable in Grafana via the
provisioned Tempo datasource, or queried directly against Tempo's search API.

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
