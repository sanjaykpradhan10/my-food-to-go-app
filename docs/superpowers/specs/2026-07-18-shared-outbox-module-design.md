# Design: Extract shared `ftgo-common` module (outbox/dedup infrastructure)

**Date**: 2026-07-18
**Status**: Approved

## Goal

Eliminate the 4x duplication of generic transactional-outbox/idempotent-receiver infrastructure across `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, and `ftgo-accounting-service`. This is a code-quality cleanup, not a functional change — every saga scenario (choreography and orchestration, happy path and all compensation cases) must reach the exact same observable outcomes after the refactor as before it.

This closes the "optional, not blocking" item noted at the end of the Ch. 4 orchestration session (`docs/session-2026-07-17-orchestration.md`).

## Scope decisions

- **Infra only, not wire-format records.** The shared module holds `OutboxEvent`, `OutboxEventRepository`, `ProcessedEvent`, `ProcessedEventRepository`, `OutboxPublisher`, `KafkaProducerConfig` — six files, verified byte-for-byte identical (aside from package declarations and cosmetic bean names) across all four services. The saga wire-format records (`SagaReply`, `OrderCreatedEvent`, `ConsumerVerificationEvent`, `KitchenEvent`, `AccountingEvent`, `VerifyConsumerCommand`, `KitchenCommand`, `AuthorizeCardCommand`) stay per-service. Those carry business meaning specific to who produces/consumes them and were deliberately left out of this pass — extracting them would start turning this into a shared API/schema-versioning concern, which is a different decision with different tradeoffs than "de-duplicate generic plumbing."
- **New Gradle module `ftgo-common`**, added to `settings.gradle`, depended on by the four saga services only (not `ftgo-restaurant-service`, `ftgo-service-registry`, or the still-stub `ftgo-delivery-service` — none of them use the outbox pattern today).
- **Not an executable service.** The root `build.gradle`'s `subprojects {}` block currently applies `org.springframework.boot` + `io.spring.dependency-management` to every module uniformly, which makes each one a runnable boot jar. `ftgo-common` needs to opt out of the boot-jar behavior (`bootJar.enabled = false`, `jar.enabled = true`, or applying `java-library` instead within its own `build.gradle`) so it produces a plain consumable library jar.
- **Package**: `com.sanjay.ftgo.common.outbox`, deliberately outside every service's own base package (`com.sanjay.ftgo.order`, `.kitchen`, `.consumer`, `.accounting`). Spring Boot's component/entity scan only covers the `@SpringBootApplication` class's own package by default, so each of the four services' application class needs explicit `@EntityScan` and `@EnableJpaRepositories` pointing at `com.sanjay.ftgo.common.outbox`, alongside its own package. This was a deliberate choice over restructuring base packages to make scanning implicit — it's the standard, well-understood Spring Boot multi-module pattern and keeps each service's own package structure untouched.
- **`OutboxPublisher` keeps its `@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)` gate** in the shared class, even though today only order-service ever sets `outbox.publish-mode` (for the Ch. 3 CDC/Debezium alternative). `matchIfMissing = true` means the other three services activate it by default exactly as they do today (they never set the property) — behaviorally identical, and it keeps CDC available to any service later without further shared-module changes.
- **`KafkaProducerConfig` bean names become generic** (e.g. `eventProducerFactory`/`eventKafkaTemplate` rather than today's per-service `orderEventKafkaTemplate`/`kitchenEventKafkaTemplate`/etc.). This is safe: each service is a separate Spring application context with exactly one `KafkaTemplate<String, String>` bean, so consumers autowire by type — the per-service name prefix was never load-bearing.
- **Table names unaffected.** `outbox_events`/`processed_events` stay the same names; each service already owns its own MySQL schema (single MySQL instance, one schema per service, per `CONTEXT.md`'s existing architecture decisions), so there's no collision risk in having the same table name defined by a shared entity across four schemas.

## Architecture

```
ftgo-common (new, java-library, not a runnable service)
┌──────────────────────────────────────────────────┐
│ com.sanjay.ftgo.common.outbox                      │
│  ├─ OutboxEvent (@Entity, outbox_events)             │
│  ├─ OutboxEventRepository                              │
│  ├─ ProcessedEvent (@Entity, processed_events)           │
│  ├─ ProcessedEventRepository                                │
│  ├─ OutboxPublisher (@Scheduled poller, polling-mode gated)  │
│  └─ KafkaProducerConfig (generic ProducerFactory/KafkaTemplate)│
└──────────────────────────────────────────────────────────────┘
        ▲ implementation project(':ftgo-common')
        │ (each also adds @EntityScan + @EnableJpaRepositories
        │  on com.sanjay.ftgo.common.outbox)
┌───────┴────────┬────────────────┬─────────────────┬──────────────────┐
│ order-service    │ kitchen-service  │ consumer-service  │ accounting-service │
└──────────────────┴──────────────────┴───────────────────┴────────────────────┘
```

`ftgo-restaurant-service`, `ftgo-service-registry`, `ftgo-delivery-service` are unaffected — they don't depend on `ftgo-common` and aren't touched by this change.

## Migration mechanics (per saga service)

1. Delete the service's local copies of `OutboxEvent`, `OutboxEventRepository`, `ProcessedEvent`, `ProcessedEventRepository`, `OutboxPublisher`, `KafkaProducerConfig`.
2. Update every remaining file that imports those types (domain services, listeners, tests) to import from `com.sanjay.ftgo.common.outbox` instead of the service's own `domain`/`infrastructure` package.
3. Add `implementation project(':ftgo-common')` to the service's `build.gradle`.
4. Add `@EntityScan(basePackages = "com.sanjay.ftgo.common.outbox")` and `@EnableJpaRepositories(basePackages = "com.sanjay.ftgo.common.outbox")` to the service's `@SpringBootApplication` class (alongside implicit scanning of its own package — Boot still needs `@EnableJpaRepositories`/`@EntityScan` to also cover the service's own `domain` package once an explicit `basePackages` is given, since specifying `basePackages` on either annotation turns off the implicit default; this needs to list **both** packages, not just the shared one).
5. Order-service specifically: confirm `outbox.publish-mode` property wiring (env var `OUTBOX_PUBLISH_MODE` in `compose.yml`) is untouched — it's read by the shared `OutboxPublisher` the same way it was read by order-service's own copy.

## Data model

No schema changes. `outbox_events` and `processed_events` table definitions are unchanged — only the Java entity mapping them now lives in one place instead of four.

## Error handling

No behavior change. Idempotent-receiver dedup via `processed_events`, outbox retry-on-next-poll semantics, and the polling/CDC mode switch all carry over unchanged — this pass only moves *where the code lives*, not what it does.

## Testing

- **Move, don't rewrite**: any existing `OutboxPublisher`-focused unit tests move into `ftgo-common`'s own test source set and run against the shared class directly, rather than being duplicated per service.
- **Per-service unit/integration test suites** (all 4 saga services) must pass unchanged after the import/dependency updates — these tests exercise domain logic (`ConsumerVerificationService`, `TicketService`, `SagaJoinService`, `CreateOrderSagaOrchestrator`, etc.) that doesn't itself change, only its outbox/dedup collaborators move.
- **`SchedulingEnabledTest`** (added in the Ch. 4 choreography session as a regression guard after a real `@EnableScheduling` bug) stays in each service — it verifies each service's own application context wiring, which is exactly the kind of thing this refactor could silently break if a service's `@EntityScan`/`@EnableJpaRepositories` update is wrong.
- **Manual e2e verification via Docker**: re-run one full saga scenario per mode to prove the refactor didn't break the real Kafka/MySQL wiring, not just unit tests:
  1. Choreography happy path (`SAGA_MODE=choreography`, default) — `Order.APPROVED`, `Ticket.AWAITING_ACCEPTANCE`, `Authorization.AUTHORIZED`.
  2. Orchestration happy path (`SAGA_MODE=orchestration`) — same end states, via `CreateOrderSagaOrchestrator`.
  3. One compensation case (either mode) — confirms `OutboxPublisher`'s shared instance still correctly publishes compensating commands/events.
  4. Redelivery/idempotency spot-check — confirms `ProcessedEvent` dedup still works from the shared entity/table.

## Deferred (not in this pass)

- Extracting the saga wire-format records (`SagaReply`, `KitchenCommand`, etc.) into a shared "contracts" module — explicitly out of scope per the scope decision above; a different kind of decision (schema/versioning ownership) than this infra-only pass.
- Any change to `ftgo-restaurant-service`, `ftgo-service-registry`, or `ftgo-delivery-service` — none of them use the outbox pattern and none are touched here.
