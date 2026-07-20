# my-food-to-go-app

A hands-on implementation of the FTGO (Food To Go) application from [*Microservices Patterns*](https://microservices.io/book) by Chris Richardson, built chapter by chapter alongside the book.

## What this is

This project follows the book's progression, adding real code at each chapter. It is not a clone of the [reference implementation](https://github.com/microservices-patterns/ftgo-application) — it's a ground-up build used to develop a deep, working understanding of microservices patterns.

**Progress:** Chapters 1–3 complete. Chapter 4 (sagas) implemented — Create Order saga built twice, choreography and orchestration, switchable via `SAGA_MODE`.

## Services

| Service | Port | Domain | Status |
|---------|------|--------|--------|
| ftgo-consumer-service | 8081 | Consumer management | Verifies consumer, publishes `ConsumerVerified`/`Failed` (choreography) or replies to `VerifyConsumerCommand` (orchestration) |
| ftgo-order-service | 8082 | Order lifecycle (saga participant/coordinator) | `POST /orders`; choreography: reacts to 3 event topics; orchestration: `CreateOrderSagaOrchestrator` sends commands and reacts to replies |
| ftgo-kitchen-service | 8083 | Ticket management (separate bounded context from Order) | `Ticket` is a DDD aggregate (Ch.5) with an enforced state machine and class-per-event domain events; creates capacity-gated `Ticket`s, confirms/cancels based on saga outcome (either style); REST API for restaurant staff (`accept`/`preparing`/`ready-for-pickup`/`picked-up`) |
| ftgo-accounting-service | 8084 | Payment authorisation | Authorizes/declines by order quantity threshold; choreography needs a local join, orchestration doesn't (orchestrator already waited for both prerequisites) |
| ftgo-restaurant-service | 8085 | Restaurant/menu management | `GET /restaurants/{id}`, registers with Eureka |
| ftgo-service-registry | 8761 | Eureka service registry | Standalone |
| ftgo-delivery-service | 8086 | Delivery tracking (separate bounded context from Order) | Stub — not yet in scope |

Each service has its own `README.md` with its full API/events/domain model. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the project-level event catalog, the shared outbox pattern, and sequence diagrams for both saga styles.

## Architecture

Each service follows **hexagonal architecture** (ports and adapters):

```
src/main/java/com/sanjay/ftgo/<service>/
├── api/            ← inbound adapters (REST controllers, messaging listeners)
├── domain/         ← aggregates, domain services, ports (interfaces)
└── infrastructure/ ← outbound adapters (JPA repositories, Kafka publishers)
```

Services communicate via messaging (Apache Kafka), introduced in Chapter 3 and extended in Chapter 4 for saga coordination. Each service owns its own MySQL schema — no shared database. Every service that publishes events uses a hand-rolled transactional outbox (not Eventuate Tram) so the mechanics stay visible for learning purposes.

## Tech stack

| Layer | Technology |
|-------|-----------|
| Language | Java 21 |
| Framework | Spring Boot 3.5.16 (final 3.5.x release — line reached EOL 2026-06-30) |
| Build | Gradle 8.14.2 (multi-module) |
| Messaging | Apache Kafka, hand-rolled transactional outbox pattern (Ch. 3), optional CDC via Debezium/Kafka Connect |
| Database | MySQL 8.4 (one schema per service) |
| Infrastructure | Docker Compose (local) |
| Testing | JUnit 5, H2 (in-memory, MySQL mode) |

## Running locally

**Prerequisites:** Docker, Java 21, `./gradlew` (wrapper included)

**Start infrastructure:**
```bash
docker compose up -d
```

This starts MySQL (port 3306), Zookeeper (2181), and Kafka (9092). On first boot, MySQL creates all six service schemas and grants the `ftgo` user access to each.

**Run all tests (no Docker needed — uses H2 in-memory):**
```bash
./gradlew test
```

**Build all services:**
```bash
./gradlew build
```

**Run a single service** (requires `docker compose up -d` first):
```bash
./gradlew :ftgo-order-service:bootRun
```

**Tear down infrastructure (including volume):**
```bash
docker compose down -v
```
> Note: use `-v` when re-initialising from scratch — MySQL's init script only runs on a fresh volume.

## Project structure

```
my-food-to-go-app/
├── build.gradle              ← shared plugin versions and dependencies
├── settings.gradle           ← declares all 8 sub-projects
├── compose.yml               ← local MySQL + Kafka infrastructure
├── infrastructure/
│   └── mysql/
│       └── init.sql          ← creates schemas and grants on first boot
├── ftgo-common/               ← shared library: outbox/dedup infra (OutboxEvent, OutboxPublisher, etc.), not a runnable service
├── ftgo-consumer-service/
├── ftgo-order-service/
├── ftgo-kitchen-service/
├── ftgo-accounting-service/
├── ftgo-restaurant-service/
├── ftgo-delivery-service/
├── ftgo-service-registry/
└── docs/
    ├── ARCHITECTURE.md       ← event catalog, outbox pattern, saga sequence diagrams
    ├── session-*.md          ← per-session summaries
    └── superpowers/
        └── specs/            ← design decisions per chapter
        └── plans/            ← implementation plans per chapter
```

## Book progress

| Ch | Topic | Status |
|----|-------|--------|
| 1 | Escaping monolithic hell | Done |
| 2 | Decomposition strategies | Done |
| 3 | Interprocess communication | Done — RPI + circuit breaker, messaging, transactional outbox, service discovery, transaction log tailing (CDC) |
| 4 | Managing transactions with sagas | Create Order saga implemented both ways (choreography, orchestration) |
| 5 | Designing business logic | `Ticket` (kitchen-service) refactored into a DDD aggregate with enforced state transitions and domain events; `Order` (order-service) not yet done |
| 6–13 | … | Not started |

See [`CONTEXT.md`](CONTEXT.md) for detailed notes and concept understanding per chapter.
