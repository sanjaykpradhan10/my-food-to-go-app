# my-food-to-go-app

A hands-on implementation of the FTGO (Food To Go) application from [*Microservices Patterns*](https://microservices.io/book) by Chris Richardson, built chapter by chapter alongside the book.

## What this is

This project follows the book's progression, adding real code at each chapter. It is not a clone of the [reference implementation](https://github.com/microservices-patterns/ftgo-application) — it's a ground-up build used to develop a deep, working understanding of microservices patterns.

**Progress:** Chapters 1–11 done. Ch.11 (production-ready services) covers §11.1 security (OAuth2/JWT, role- and instance-based access control), §11.2 externalized configuration (Spring Cloud Config Server) and all six §11.3 observability patterns — health checks, application metrics, distributed tracing, log aggregation, exception tracking, and audit logging (the last adding `ftgo-audit-log-service`). Sagas (Ch.4) and DDD aggregates (Ch.5) cover all three `Order` sub-sagas in both styles; event sourcing (Ch.6) adds a hand-rolled, switchable persistence path for the `Order` aggregate; queries (Ch.7) adds two contrasting patterns — API composition (`GET /orders/{id}/view` on order-service) and CQRS (a new standalone `ftgo-order-history-service`); external API patterns (Ch.8) adds two BFF-style gateways — `ftgo-mobile-gateway` and `ftgo-public-gateway` — sharing a `ftgo-gateway-common` edge-function library; testing (Ch.9) audited the existing test suite against the book's unit-testing techniques and tightened saga/event payload assertions plus added a value-object worked example (`OrderLineItemTest`) — no production code changed; testing part 2 (Ch.10) adds three layers on top of that: consumer-driven contract tests (Spring Cloud Contract Verifier), an out-of-process Cucumber component test for order-service's Place Order flow, and a new `ftgo-end-to-end-test` module driving a full-stack Create→Revise→Cancel Order journey through the real, containerized application via the public gateway.

## Services

| Service | Port | Domain | Status |
|---------|------|--------|--------|
| ftgo-consumer-service | 8081 | Consumer management | Verifies consumer, publishes `ConsumerVerified`/`Failed` (choreography) or replies to `VerifyConsumerCommand` (orchestration); `POST /consumers` (Ch.10, this service's first-ever REST controller, for end-to-end test fixture creation) |
| ftgo-order-service | 8082 | Order lifecycle (saga participant/coordinator); `Order` is a DDD aggregate (Ch.5) with the full create/cancel/revise state machine, persisted either via JPA or hand-rolled event sourcing (Ch.6), switchable via `PERSISTENCE_MODE`; Cancel Order and Revise Order saga participant (both modes) | `POST /orders`, `POST /orders/{id}/cancel`, `POST /orders/{id}/revise`, `GET /orders/{id}/view` (API composition, Ch.7); choreography: `OrderSagaService`/`OrderCancelSagaService`/`OrderReviseSagaService` react to event topics; orchestration: `CreateOrderSagaOrchestrator` (create) + stateless `CancelOrderSagaOrchestrator` (cancel) + stateless `ReviseOrderSagaOrchestrator` (revise), routed from one shared `saga.replies` listener via `SagaReply.sagaType()`; both saga styles work identically against either persistence mode via the `OrderTransitions`/`SagaCommandPublisher` facades |
| ftgo-kitchen-service | 8083 | Ticket management (separate bounded context from Order); Cancel Order and Revise Order saga participant (both modes) | `Ticket` is a DDD aggregate (Ch.5) with an enforced state machine, a persisted `totalQuantity`, and class-per-event domain events; creates capacity-gated `Ticket`s, confirms/cancels based on saga outcome (either style); asks-kitchen-first gate for Cancel Order (`handleOrderCancelled`/`handleCancelTicketCommand`) and for Revise Order (`reviseQuantity`/`undoRevision`, provisionally applying a revised quantity before accounting is ever asked, reverting it if accounting later declines); REST API for restaurant staff (`accept`/`preparing`/`ready-for-pickup`/`picked-up`), plus a read-only `GET /tickets/order/{orderId}` (API composition, Ch.7); registers with Eureka |
| ftgo-accounting-service | 8084 | Payment authorisation; `Authorization` is a DDD aggregate (Ch.5) with a persisted `totalQuantity`; Cancel Order and Revise Order saga participant (both modes) | Authorizes/declines by order quantity threshold; choreography needs a local join, orchestration doesn't (orchestrator already waited for both prerequisites); reverses an authorization only after kitchen confirms a ticket cancellable, and re-checks the threshold on a revision (`reviseAuthorization`) without reversing anything if declined — `reverseForChoreography`/`reverseForCommand` and `reviseForChoreography`/`reviseForCommand` each publish to different channels (`accounting.events` vs. a `saga.replies` reply) and are not interchangeable; read-only `GET /authorizations/order/{orderId}` (API composition, Ch.7 — this service's first-ever REST controller); registers with Eureka |
| ftgo-restaurant-service | 8085 | Restaurant/menu management | `GET /restaurants/{id}`, `POST /restaurants` (Ch.10, for end-to-end test fixture creation), registers with Eureka |
| ftgo-delivery-service | 8086 | Delivery tracking (separate bounded context from Order); Create Order saga's 3rd parallel-join leg and Cancel Order saga's delivery-release step (both saga modes) | `Delivery` is a DDD aggregate (`SCHEDULED → PICKED_UP → DELIVERED`, or `CANCELLED` from `SCHEDULED`) with a seeded 3-courier pool; `POST /deliveries/{id}/picked-up`/`delivered`; read-only `GET /deliveries/order/{orderId}` (API composition, Ch.7); registers with Eureka |
| ftgo-order-history-service | 8088 | Order history / order view (CQRS read model, Ch.7 — no bounded context of its own, no aggregate) | Pure Kafka consumer (`order.events`/`kitchen.events`/`accounting.events`/`delivery.events`) maintaining a denormalized `order_views` table via an upsert-on-any-event pattern; `GET /order-views/{orderId}`; no Eureka, no synchronous calls to or from anything |
| ftgo-audit-log-service | 8089 | Audit log / activity ledger (Ch.11 §11.3.6 — no bounded context of its own, no aggregate) | Pure Kafka consumer over the `audit-log` topic, persisting an append-only `audit_log_entries` table; events are produced by `ftgo-common`'s `AuditLoggingAspect`, which intercepts every `@PostMapping`+`@PreAuthorize` controller method in the services that depend on `ftgo-common`; `GET /audit-log` (`ADMIN`-only, filter by `userId`, or `entityType`+`entityId`, or `from`+`to`); publishes nothing, no synchronous calls to or from anything |
| ftgo-service-registry | 8761 | Eureka service registry | Standalone |
| ftgo-authorization-server | 9000 | OAuth2 Authorization Server (Ch.11, §11.1, not a bounded-context service) | Issues JWTs via a custom resource-owner-password grant (end users) and a `client_credentials` grant (`ftgo-order-service`'s service-to-service calls); hardcoded seed users, `/oauth2/jwks` public key endpoint; dev/learning project only — see its own README |
| ftgo-config-server | 8888 | Spring Cloud Config Server (Ch.11, §11.2, not a bounded-context service) | Git-backed by this repository's own `config-repo/`; serves shared defaults (`application.yml`) plus per-service overrides for the 5 outbox services; non-blocking `optional:configserver` import contract, so an unreachable config server never blocks a service's startup |
| ftgo-mobile-gateway | 8090 | Mobile BFF gateway (Ch.8, mobile-team-owned) | 3 declared routes (create/cancel/revise order → order-service) plus one hand-composed `GET /mobile/orders/{orderId}` (a WebFlux `RouterFunction`, not a Gateway route) fanning out via `Mono.zip`/`ReactiveCircuitBreaker` to order/kitchen/accounting/delivery-service; JWT bearer-token auth, 20 req/s per caller |
| ftgo-public-gateway | 8091 | Public/3rd-party API gateway (Ch.8, public-API-team-owned) | Pure Spring Cloud Gateway routing (no composition code) to 6 backends under `/api/v1/...`; JWT bearer-token auth, 5 req/s per caller |
| ftgo-gateway-common | — | Shared WebFlux library (Ch.8, not a runnable service) | `RequestLoggingFilter`/`JwtValidationFilter` (`GlobalFilter`s, Ch.11 §11.1 replaced the original API-key filter with JWT bearer-token auth) and `PerKeyRateLimiterGatewayFilterFactory` (named `PerKeyRateLimiter`, keyed off the validated JWT's `sub` claim), consumed by both gateways above |
| ftgo-end-to-end-test | — | End-to-end test module (Ch.10, not a runnable service) | Cucumber (JUnit Platform engine) suite driving the real, full-stack application (all 8 business services + both gateways, root `compose.yml`, `SAGA_MODE=orchestration`) through one Create→Revise→Cancel Order journey via `ftgo-public-gateway`; kept out of the default `test`/`check` graph, run via `./gradlew :ftgo-end-to-end-test:e2eTest` |

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
| Infrastructure | Docker Compose (local, default/primary path); Kubernetes via `kind` + Helm — alternative local deployment target for the whole stack (Ch.12 §12.4 sub-project B1, `k8s/`) |
| Configuration | Spring Cloud Config Server, git-backed (`config-repo/`), centralized with per-service overrides; non-blocking "optional" startup contract; live refresh of select properties via `POST /actuator/refresh` (Ch.11 §11.2) |
| Testing | JUnit 5, H2 (in-memory, MySQL mode); Spring Cloud Contract Verifier (Ch.10 consumer-driven contract tests); Cucumber (JUnit Platform engine) + `com.avast.gradle.docker-compose` Gradle plugin (Ch.10 component tests and end-to-end tests) |
| Security | Spring Authorization Server (JWT issuance, `ftgo-authorization-server`, Ch.11 §11.1); Spring Security OAuth2 Resource Server (JWT validation at both gateways and all 8 business services); method-level `@PreAuthorize` role checks plus order-service's instance-based ACL for per-consumer order access |
| Observability | Micrometer + Prometheus (`/actuator/prometheus` on all 10 services, custom business counters on 7; Ch.11 §11.3.4); Grafana ("FTGO Overview" dashboard, 8 panels); Micrometer Tracing + OpenTelemetry + Grafana Tempo (distributed tracing, all 10 services, 100% sampled; Ch.11 §11.3.3); ELK stack (Elasticsearch, Logstash, Kibana) + Filebeat (log aggregation — all 10 services log structured JSON via `logstash-logback-encoder` to stdout, Filebeat ships to Logstash, indexed into Elasticsearch as `ftgo-logs-*`, searchable and correlatable by `traceId` in Kibana; Ch.11 §11.3.2); GlitchTip (`sentry-spring-boot-starter-jakarta` on all 10 services, auto-capturing uncaught exceptions only — existing `@ExceptionHandler`s are untouched; NOT correlated with Tempo/Kibana `traceId`s — the Sentry SDK keeps its own independent trace id, no OpenTelemetry integration is wired in; DSN provisioned at compose startup by a one-shot `glitchtip-provisioner` container; Ch.11 §11.3.5); audit logging (`ftgo-common`'s `AuditLoggingAspect` → Kafka `audit-log` topic → `ftgo-audit-log-service`, `ADMIN`-only `GET /audit-log`; Ch.11 §11.3.6) |

## Running locally

**Prerequisites:** Docker, Java 21, `./gradlew` (wrapper included)

**Start infrastructure:**
```bash
docker compose up -d
```

This starts MySQL (port 3306), Zookeeper (2181), Kafka (9092), and the observability stack: Prometheus (9090), Grafana (3000), Tempo (3200/4318), Elasticsearch (9200), Logstash (5044), Kibana (5601), and GlitchTip (8000). On first boot, MySQL creates all service schemas — including `ftgo_audit_log` — and grants the `ftgo` user access to each.

It also starts the application services, each on the port listed in the [Services](#services) table above: consumer 8081, order 8082, kitchen 8083, accounting 8084, restaurant 8085, delivery 8086, order-history 8088, **audit-log 8089**, mobile gateway 8090, public gateway 8091, plus the service registry (8761), config server (8888) and authorization server (9000).

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

### Running on Kubernetes (alternative to compose)

The whole stack can also be deployed to a local `kind` cluster via a Helm chart (Ch.12 §12.4 sub-project B1) — same services, same functional behaviour, verified by the same `ftgo-end-to-end-test` suite against a Kubernetes-profile base URL. Compose remains the default/primary path above; see [`k8s/README.md`](k8s/README.md) for setup and `docs/ARCHITECTURE.md`'s Kubernetes deployment section for the full `compose.yml`-to-Kubernetes resource mapping.

## Project structure

```
my-food-to-go-app/
├── build.gradle              ← shared plugin versions and dependencies
├── settings.gradle           ← declares all 18 sub-projects
├── compose.yml               ← local MySQL + Kafka infrastructure
├── infrastructure/
│   └── mysql/
│       └── init.sql          ← creates schemas and grants on first boot
├── config-repo/              ← git-backed externalized configuration served by ftgo-config-server (Ch.11, §11.2)
├── ftgo-common/               ← shared library: outbox/dedup infra (OutboxEvent, OutboxPublisher, etc.), not a runnable service
├── ftgo-consumer-service/
├── ftgo-order-service/
├── ftgo-kitchen-service/
├── ftgo-accounting-service/
├── ftgo-restaurant-service/
├── ftgo-delivery-service/
├── ftgo-order-history-service/
├── ftgo-audit-log-service/    ← audit log consumer + ADMIN query API (Ch.11, §11.3.6)
├── ftgo-service-registry/
├── ftgo-gateway-common/       ← shared library: gateway edge functions (logging/auth/rate-limit), not a runnable service
├── ftgo-mobile-gateway/
├── ftgo-public-gateway/
├── ftgo-authorization-server/ ← OAuth2 Authorization Server (Ch.11, §11.1, JWT issuance)
├── ftgo-config-server/        ← Spring Cloud Config Server, git-backed by `config-repo/` (Ch.11, §11.2)
├── ftgo-end-to-end-test/     ← end-to-end test module (Ch.10, not a runnable service)
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
| 5 | Designing business logic | `Ticket` (kitchen-service), `Order` (order-service), and `Authorization` (accounting-service) all refactored into DDD aggregates with enforced state transitions and domain events. Cancel Order and Revise Order sagas both implemented (both modes) — all three `Order` sub-projects complete |
| 6 | Event sourcing | Done — `Order` (order-service) gained a hand-rolled event-sourced persistence path (event store, snapshots, dedicated optimistic-lock version table), switchable against JPA via `PERSISTENCE_MODE`; covers all three `Order` sagas (create/cancel/revise) in both styles, including the book's full pseudo-event mechanism (`SagaCommandEvent`-style) for orchestration-mode sagas; publishes via the existing Ch.3 CDC pipeline |
| 7 | Implementing queries | Done — two contrasting query patterns: API composition (`GET /orders/{id}/view` on order-service, parallel virtual-thread fan-out to restaurant/kitchen/accounting/delivery-service, per-service circuit breakers, `SectionResult` graceful degradation) and CQRS (new standalone `ftgo-order-history-service`, a pure Kafka consumer maintaining a denormalized `order_views` read model via an upsert-on-any-event pattern, `GET /order-views/{orderId}`, no Eureka, no synchronous calls) |
| 8 | External API patterns | Done — API gateway + Backends for Frontends: `ftgo-mobile-gateway` (routing + one hand-composed `GET /mobile/orders/{orderId}`) and `ftgo-public-gateway` (pure routing to 6 backends), sharing `ftgo-gateway-common`'s edge functions (request logging, API-key auth, per-key rate limiting); a RouterFunction-vs-Gateway-route filter-isolation finding documented in `docs/ARCHITECTURE.md` |
| 9 | Testing microservices: Part 1 | Done — §9.1 (test pyramid, solitary/sociable) is conceptual, no code. §9.2 unit-testing techniques audited against the existing suite: 4 of 6 already matched independently; tightened saga/event payload assertions in `CreateOrderSagaOrchestratorTest`, `CancelOrderSagaOrchestratorTest`, `DeliveryServiceTest`, and added a standalone `OrderLineItemTest` value-object worked example. No production code changed |
| 10 | Testing microservices: Part 2 | Done — all 3 sub-projects complete. Sub-project 1 (consumer-driven contract tests): REST contract (`ftgo-mobile-gateway`↔`ftgo-order-service`), pub/sub contract (`ftgo-order-service`→`ftgo-order-history-service`), async request/response contract (`ftgo-order-service`↔`ftgo-kitchen-service`), all via Spring Cloud Contract Verifier with a hand-written embedded-Kafka messaging bridge for the two Kafka-based contracts (`ftgo-common`'s `KafkaContractTestSupport`). Sub-project 2 (component tests): an out-of-process Cucumber suite drives the real, containerized order-service through its Place Order flow (orchestration mode, JPA persistence) against a slimmed Docker Compose stack, restaurant-service stubbed via WireMock and the four saga participants stood in for by a single `SagaParticipantStub`. Sub-project 3 (end-to-end tests): a new `ftgo-end-to-end-test` module drives one Cucumber scenario — Create→Revise→Cancel Order — through the real, full-stack application (all 7 business services + both gateways, unmodified root `compose.yml`, `SAGA_MODE=orchestration`) entered via `ftgo-public-gateway`, exercising all three `Order` sagas in one journey. `POST /restaurants`/`POST /consumers` added as prerequisite fixture-creation endpoints for this sub-project |
| 11 | Developing production-ready services | Done — §11.1 (security), §11.2 (externalized configuration) and all six §11.3 observability patterns implemented and live-verified. **§11.1 security**: new `ftgo-authorization-server` issuing JWTs via a custom resource-owner-password grant (end users) and a `client_credentials` grant (order-service's service-to-service calls, `SERVICE` role); both gateways and all 8 business services are OAuth2 resource servers with `@PreAuthorize` role requirements; order-service adds an instance-based ACL (`OrderAccessControl` — a `CONSUMER` sees only their own order) and derives `consumerId` from the JWT, not the request body. **§11.2 externalized configuration**: `ftgo-config-server` (port 8888), git-backed by `config-repo/`, shared defaults plus per-service overrides for the 5 outbox services, non-blocking `fail-fast: false` startup contract, live refresh of `outbox.poll-fixed-delay-ms` via `POST /actuator/refresh`. **§11.3.1 health checks**: `/actuator/health` on all 10 services (auto-configured indicators only) driving compose `healthcheck`/`depends_on: service_healthy` gates. **§11.3.4 application metrics**: Micrometer + Prometheus (`/actuator/prometheus` on all 10 services, custom business counters on 7), `prometheus`/`grafana` compose services, 3 alert rules, an 8-panel dashboard. **§11.3.3 distributed tracing**: Micrometer Tracing + OpenTelemetry (replacing the deprecated Sleuth/Zipkin stack) on all 10 services, OTLP/HTTP to Grafana Tempo at 100% sampling, explicit Kafka observation properties on the Kafka-touching services and explicit Reactor context propagation for both gateways. **§11.3.2 log aggregation**: ELK + Filebeat; all 10 services log structured JSON to stdout with `traceId`/`spanId` from the tracing MDC, indexed as `ftgo-logs-*` and correlatable with Tempo traces in Kibana. **§11.3.5 exception tracking**: GlitchTip (Postgres + Redis-backed) with a one-shot `glitchtip-provisioner` bootstrapping the org/project/DSN into a shared volume; `sentry-spring-boot-starter-jakarta` on all 10 services captures only uncaught exceptions (existing `@ExceptionHandler`s untouched); NOT correlated with Tempo/Kibana `traceId`s. **§11.3.6 audit logging**: `ftgo-common`'s `AuditLoggingAspect` (`@Around`, matching `@PostMapping`+`@PreAuthorize` controller methods, registered by auto-configuration so no service opts in by hand) publishes `AuditLogEntryEvent`s best-effort to the Kafka `audit-log` topic; a new `ftgo-audit-log-service` (port 8089) consumes them into an append-only `audit_log_entries` table behind an `ADMIN`-only `GET /audit-log`; both SUCCESS and FAILURE outcomes are recorded and the audited call's behaviour is completely unchanged. Verified by a Cucumber e2e scenario polling the query API. Two honest gaps documented rather than papered over: `POST /restaurants` is unaudited (restaurant-service is the one business service that doesn't depend on `ftgo-common`), and publishing is best-effort rather than going through the transactional outbox (an audit record can be lost while its business transaction commits). §11.4 (microservice chassis / service mesh) is conceptual reading, not implemented. |
| 12 | Deploying microservices | Done — §12.1–12.3 (deployment patterns, Docker, Docker Compose — sub-project A) done, see [`docs/CH12-DEPLOYMENT.md`](docs/CH12-DEPLOYMENT.md). §12.4 (Kubernetes) sub-project B1 (core deployment) done — the entire `compose.yml` stack redeployed to a local `kind` cluster via a Helm chart at `k8s/ftgo/`, verified by the `ftgo-end-to-end-test` suite against a Kubernetes profile; see `docs/ARCHITECTURE.md`'s Kubernetes deployment section. B2 (zero-downtime rolling deployment, §12.4.4) done — see docs/ARCHITECTURE.md; B3a (Linkerd service mesh, auto-mTLS) done — see docs/ARCHITECTURE.md; B3b (mesh observability — linkerd-viz golden-metrics dashboard) done — see docs/ARCHITECTURE.md; B3c (mesh traffic management, ServiceProfiles) done — see docs/ARCHITECTURE.md. B3c's own fault-injection verification found the ServiceProfiles have no effect on order-service's real traffic, since it resolves its four downstream calls via Eureka rather than Kubernetes Service DNS — a genuine architectural finding about mesh vs. client-side service discovery, not an implementation defect |
| 13 | Refactoring to microservices | Not started |

See [`CONTEXT.md`](CONTEXT.md) for detailed notes and concept understanding per chapter.
