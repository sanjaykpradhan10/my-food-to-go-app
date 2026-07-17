# FTGO Study Context — Microservices Patterns (Chris Richardson)

## About this file
This file is the shared brain between Claude Chat (claude.ai) and Claude Code.
- **In Claude Chat**: paste this file at the start of a session to restore context
- **In Claude Code**: this file lives in the project root — Claude Code reads it automatically

Update this file at the end of every session (either tool can do it).

---

## Learner profile
- **Name**: Sanjay
- **Goal**: Deep, job-ready understanding of microservices patterns — not just interview prep
- **Background**: Senior software engineer, Java/Spring Boot experience
- **Approach**: Read the entire book chapter by chapter, building the FTGO app alongside it
- **Reference implementation**: https://github.com/microservices-patterns/ftgo-application
- **Local project**: ~/Sanjay/Projects/Spring/my-food-to-go-app

---

## Book structure & progress

| Ch | Title | Status | Confidence | Notes |
|----|-------|--------|------------|-------|
| 1  | Escaping monolithic hell | Done | High | Hexagonal arch, scale cube (X/Y/Z), monolithic hell symptoms, pattern language structure |
| 2  | Decomposition strategies | Done | High | 4+1 views, hexagonal arch, system ops, business capability, DDD subdomains, SRP/CCP, god classes, bounded contexts |
| 3  | Interprocess communication in a microservice architecture | Implementing | — | |
| 4  | Managing transactions with sagas | Reading | — | |
| 5  | Designing business logic in a microservice architecture | Not started | — | |
| 6  | Developing business logic with event sourcing | Not started | — | |
| 7  | Implementing queries in a microservice architecture | Not started | — | |
| 8  | External API patterns | Not started | — | |
| 9  | Testing microservices: Part 1 | Not started | — | |
| 10 | Testing microservices: Part 2 | Not started | — | |
| 11 | Developing production-ready services | Not started | — | |
| 12 | Deploying microservices | Not started | — | |
| 13 | Refactoring to microservices | Not started | — | |

**Status options**: Not started → Reading → Implementing → Review → Done
**Confidence**: Low / Medium / High

---

## Current position

- **Chapter**: 4 — Managing transactions with sagas
- **Status**: Implementing (Create Order saga — both choreography and orchestration styles implemented, switchable via SAGA_MODE, and manually verified to reach identical outcomes)
- **Last session**: 2026-07-17
- **Last tool used**: Claude Code

---

## Session log
<!-- Add a one-liner after each session: date · tool · what was covered -->
- 2026-06-18 · Claude Chat · Ch. 1 complete — monolithic hell, scale cube, benefits/drawbacks, pattern language overview
- 2026-06-18 · Claude Chat · Ch. 2 complete — 4+1 view model, hexagonal architecture, system operations, decompose by business capability, decompose by DDD subdomain, SRP/CCP, god classes resolved via bounded contexts
- 2026-06-25 · Claude Code · Scaffolded full multi-module Gradle project (6 Spring Boot 3.5 stubs, compose.yml, hexagonal packages), pushed to GitHub; Ch. 3 structured walkthrough begun
- 2026-07-15 · Claude Code · Implemented and manually verified Ch. 3 synchronous REST + circuit breaker pattern: restaurant-service exposes GET /restaurants/{id} (Restaurant/MenuItem JPA entities, seed data); order-service exposes POST /orders, calling restaurant-service via RestClient wrapped in a Resilience4j circuit breaker (2s connect/read timeout, sliding-window-size 5, failure-rate-threshold 50%, wait-duration-in-open-state 5s); verified happy path (201/APPROVED), circuit opening under sustained failure (first 5 calls ~2s each then fail-fast in ~15-20ms), and recovery to CLOSED after the wait duration; PR merged to main
- 2026-07-15 · Claude Code · Moved on to Ch. 4 (sagas) reading; Ch. 3 left partially implemented (messaging, transactional outbox, transaction log tailing, discovery not yet built) — revisit later if desired
- 2026-07-15 · Claude Code · Implemented and manually verified Ch. 3 async messaging + transactional outbox pattern: order-service now persists Order via JPA and writes OrderCreated to a transactional outbox table in the same DB transaction, with a @Scheduled poller publishing unsent outbox rows to Kafka topic order.events; kitchen-service (previously an empty stub) now consumes that topic and creates Tickets idempotently via a processed_events dedup ledger; docker-compose wires all 6 containers (mysql, zookeeper, kafka, restaurant-service, order-service, kitchen-service) with a Kafka dual internal/external listener setup; verified end-to-end via Docker — order placed, outbox row written then flipped to sent, Ticket created, and forced redelivery (resetting sent_at) confirmed deduped (ticket count and processed_events unchanged); still on feature branch worktree-ch3-messaging-outbox, not yet merged to main
- 2026-07-15 · Claude Code · Implemented and manually verified Ch. 3 client-side service discovery pattern: new ftgo-service-registry module (standalone Eureka server, port 8761); restaurant-service now registers itself on startup; order-service resolves restaurant-service dynamically via a @LoadBalanced RestClient (base URL http://ftgo-restaurant-service, resolved by Spring Cloud LoadBalancer against the registry) instead of a hardcoded URL — RestaurantServiceProxy itself untouched, only how its RestClient bean is built changed; docker-compose wires all 7 containers (mysql, zookeeper, kafka, service-registry, restaurant-service, order-service, kitchen-service) with eureka.instance.prefer-ip-address for correct container networking; verified end-to-end via Docker including instance eviction (registry entry removed on graceful shutdown, order-service's circuit breaker degrading to 503) and dynamic recovery (registry re-populated on restart, order-service resumed 201s) without restarting order-service, proving discovery is dynamic and not resolved once at startup; still on feature branch worktree-ch3-service-discovery, not yet merged to main
- 2026-07-16 · Claude Code · Implemented and manually verified Ch. 3 transaction log tailing (CDC) pattern, the last remaining Ch. 3 IPC pattern: added Debezium + Kafka Connect as a second outbox delivery mechanism, using the Outbox Event Router SMT to publish the existing outbox_events.payload column unchanged to Kafka topic order.events; switchable against the existing polling publisher via one OUTBOX_PUBLISH_MODE env var reconciled on both sides — OutboxPublisher's @ConditionalOnProperty gate and a new idempotent connector-registrar container that registers/deregisters the Debezium connector; OrderService's write side and kitchen-service's consumer are both unchanged; hit and fixed a real MySQL 8.4 compatibility gap along the way (Debezium 2.7.3.Final issues the removed `SHOW MASTER STATUS` syntax — MySQL 8.4 only supports `SHOW BINARY LOG STATUS` — causing the connector to loop on retriable snapshot errors despite reporting RUNNING; fixed by bumping the kafka-connect image to debezium/connect:3.0.0.Final); manually verified polling-mode regression (outbox sent_at populated, Ticket created), CDC-mode delivery (connector RUNNING, outbox sent_at stayed NULL, Ticket still created via Debezium alone), and mode-switch replay safety (ticket count and processed_events count unchanged across a fresh CDC re-registration — confirmed via kafka-connect logs that this was due to snapshot.mode: no_data skipping historical data entirely, not dedup absorbing a replay); still on feature branch worktree-ch3-cdc-transaction-log-tailing, not yet merged to main
- 2026-07-17 · Claude Code · Implemented and manually verified the Ch. 4 Create Order saga, choreography style, via a full brainstorm → spec → plan → subagent-driven-development cycle (9 tasks): consumer-service and accounting-service got their first real code (previously empty stubs); order-service now creates Order in APPROVAL_PENDING (added consumerId, previously absent from the schema) and a new OrderSagaService transitions it to APPROVED/REJECTED based on three Kafka listeners; kitchen-service extended with outbox publishing (its first — it only ever consumed before) and capacity-gated ticket creation; all four services now share the Ch.3 transactional-outbox + processed_events dedup pattern, one Kafka topic per service (order.events/consumer.events/kitchen.events/accounting.events); accounting-service's SagaJoinService is the trickiest piece — a local SagaJoinState table (with @Version optimistic locking) lets it wait for both ConsumerVerified and TicketCreated regardless of arrival order before authorizing a card; full compensation matrix implemented for all three failure points (consumer verification fails, kitchen capacity exceeded, card authorization declined), each with a real compensating transaction (ticket cancellation) rather than just a rejected order; since no pricing data flows through saga events, total line-item quantity substitutes for "order total" as the threshold input (kitchen capacity limit 20, accounting authorization limit 10) — a deliberate, documented simplification. Manual e2e verification via Docker caught a real bug: kitchen/consumer/accounting-service's brand-new OutboxPublisher schedulers never fired because their main application classes were missing @EnableScheduling (only order-service had it, pre-existing from Ch.3) — fixed, and a SchedulingEnabledTest regression guard was added to all four services so this class of bug fails fast in the test suite rather than silently stalling the saga. Verified happy path (Order APPROVED, Ticket AWAITING_ACCEPTANCE, Authorization AUTHORIZED), all three compensation cases, and redelivery/idempotency (forced Kafka redelivery, confirmed no double-processing). PR #6 merged to main.
- 2026-07-17 · Claude Code · Implemented and manually verified the Ch. 4 Create Order saga a second time, orchestration style, switchable against choreography via one SAGA_MODE env var per service (default choreography), via a full brainstorm → spec → plan → subagent-driven-development cycle (7 tasks): a new CreateOrderSagaOrchestrator in order-service (persisted CreateOrderSagaInstance table, @Version optimistic locking — same accepted pattern as choreography's SagaJoinState) sends explicit commands (VerifyConsumerCommand/KitchenCommand/AuthorizeCardCommand) to 3 new dedicated topics and reacts to replies on a shared saga.replies topic, rather than participants reacting to each other's domain events; every existing choreography listener across all 4 services got gated behind @ConditionalOnProperty(saga.mode=choreography); each participant's existing decision logic (consumer verification, ticket capacity check, authorization threshold) was extracted into a small shared method reused by both the choreography and orchestration code paths, so no business rule was duplicated, only the outbound wire format differs; OutboxEvent gained a per-row topic column (generalizing OutboxPublisher, since order-service now fans out to 3 different command topics from one outbox table) — a mechanical, behavior-preserving change applied uniformly across all 4 services. The clearest architectural contrast versus choreography: accounting-service needs zero join logic in orchestration mode (the orchestrator already waited for both prerequisites before ever sending AuthorizeCardCommand), and order-service can approve the order directly on CardAuthorized without waiting for kitchen's downstream confirmation echo, since the orchestrator is already authoritative. Code review caught and fixed one real defect during Task 4 (unguarded null totalQuantity unboxing risking an NPE on a malformed command) — proactively pre-empted in Task 5's dispatch, so no second review round was needed there. Manually verified via Docker with SAGA_MODE=orchestration: happy path, all three compensation cases, and redelivery/idempotency — all reaching the exact same Order/Ticket/Authorization end states as the choreography run, confirming the two styles are genuinely interchangeable at the observable-outcome level despite very different internal mechanisms. Added a project-wide CLAUDE.md codifying two standing rules going forward: documentation updates land in the same change as the code they describe, and code comments should explain *why* not *what*.

---

## Concept understanding

### Understood well
- Ch. 1: Hexagonal architecture, monolithic hell (6 symptoms), scale cube (X/Y/Z axes), microservices as Y-axis decomposition, database-per-service rationale, pattern language structure (forces / resulting context / related patterns)
- Ch. 2: 4+1 view model (logical/implementation/process/deployment/scenarios), hexagonal architecture (inbound/outbound adapters and ports), three-step architecture process (system ops → services → APIs), decompose by business capability, decompose by DDD subdomain, SRP and CCP applied to services, god class problem and resolution via per-service domain models, bounded context = service boundary, Ubiquitous Language per service (Order vs Ticket vs Delivery)

### Needs more depth
- (none yet)

### Open questions
- (none yet)

---

## FTGO app build log

### Project setup
- [ ] Clone reference implementation for reference: `git clone https://github.com/microservices-patterns/ftgo-application`
- [ ] Initialise my-food-to-go-app project structure
- [ ] Set up Docker Compose for local infrastructure (MySQL, Kafka, Zookeeper)
- [ ] Verify local environment runs

### Services to build (one per chapter as relevant)
| Service | Introduced | Status | Notes |
|---------|-----------|--------|-------|
| ftgo-consumer-service | Ch. 1–2, 4 | Verifies consumer, publishes ConsumerVerified/Failed | First real code in Ch.4; seeds consumerId 1 (active) / 2 (inactive) for testing both outcomes; own outbox + processed_events, topic consumer.events |
| ftgo-order-service | Ch. 2–4 | REST call + circuit breaker to restaurant-service (discovered via Eureka); publishes via outbox (polling or CDC); saga participant, both styles switchable via SAGA_MODE (creates in APPROVAL_PENDING, transitions to APPROVED/REJECTED) | Owns Order domain model (now with consumerId); POST /orders calls restaurant-service via a @LoadBalanced RestClient wrapped in a Resilience4j circuit breaker; persists Order + OrderCreated outbox row in one transaction; choreography mode: three Kafka listeners (consumer.events/kitchen.events/accounting.events) drive the saga's terminal transition; orchestration mode: CreateOrderSagaOrchestrator (persisted CreateOrderSagaInstance, @Version) sends commands and reacts to replies on saga.replies |
| ftgo-kitchen-service | Ch. 2, 4, 5 | Consumes order.events, creates Ticket (capacity-gated); own outbox publishes TicketCreated/TicketCreationFailed/TicketConfirmed/TicketCancelled to kitchen.events | Ticket status now CREATE_PENDING → AWAITING_ACCEPTANCE or CANCELLED; reacts to accounting.events and consumer.events for confirmation/compensation; deduped via processed_events ledger |
| ftgo-accounting-service | Ch. 2, 4 | Joins on ConsumerVerified + TicketCreated (either order), authorizes card, publishes CardAuthorized/Failed | First real code in Ch.4; SagaJoinService + SagaJoinState (local join table, @Version optimistic locking) is the trickiest piece of the saga — resolves exactly once regardless of event arrival order; own outbox + processed_events, topic accounting.events |
| ftgo-restaurant-service | Ch. 2 | GET /restaurants/{id} implemented | Restaurant/MenuItem JPA entities + seed data (2 restaurants); now registers with the Eureka service registry on startup (spring.application.name: ftgo-restaurant-service) |
| ftgo-service-registry | Ch. 3 | Eureka server, port 8761 | Standalone registry; restaurant-service registers on startup, order-service discovers it via @LoadBalanced RestClient |
| ftgo-delivery-service | Ch. 2 | Ready to scaffold | Uses Delivery not Order; separate bounded context |
| ftgo-api-gateway | Ch. 8 | Not started | |

### Architecture decisions made
- Hexagonal layers as Java packages (not Gradle sub-projects) — matches reference impl, simpler
- No shared common module yet — Ch.4's saga now duplicates OutboxEvent/ProcessedEvent/OutboxPublisher/KafkaProducerConfig near-verbatim across all four services (order/kitchen/consumer/accounting), plus each saga event record (ConsumerVerificationEvent, KitchenEvent, AccountingEvent) is copy-pasted into every consuming service. This was a deliberate, reviewed choice for the Ch.4 pass — extraction into a shared module is the natural next step if a 5th service joins a saga
- Minimal dependencies per service (web, jpa, mysql, test) — Eventuate Tram added in Ch. 3
- Single MySQL instance, one schema per service — lower local overhead, matches Richardson's Compose setup
- H2 in test scope with MODE=MySQL — allows contextLoads tests without a running DB

### Code patterns implemented
- (none yet)

---

## Patterns reference
<!-- Tick off as each pattern is understood AND implemented -->

### Decomposition
- [x] Decompose by business capability (Ch. 2)
- [x] Decompose by subdomain (Ch. 2)

### Communication
- [x] Remote procedure invocation / REST (Ch. 3)
- [x] Messaging (Ch. 3)
- [x] Circuit breaker (Ch. 3)
- [x] Client-side discovery / Server-side discovery (Ch. 3)
- [x] Transactional outbox (Ch. 3)
- [x] Transaction log tailing (Ch. 3)

### Data consistency
- [x] Saga — choreography (Ch. 4)
- [x] Saga — orchestration (Ch. 4) — same Create Order use case, switchable via SAGA_MODE, verified to reach identical outcomes to choreography

### Business logic
- [ ] Domain model / DDD aggregates (Ch. 5)
- [ ] Domain events (Ch. 5)
- [ ] Transaction script (Ch. 5)
- [ ] Event sourcing (Ch. 6)

### Querying
- [ ] API composition (Ch. 7)
- [ ] CQRS (Ch. 7)

### External API
- [ ] API gateway (Ch. 8)
- [ ] Backends for frontends (Ch. 8)

### Testing
- [ ] Consumer-driven contract test (Ch. 9)
- [ ] Component test (Ch. 10)
- [ ] Service component test (Ch. 10)

### Observability & operations
- [ ] Health check API (Ch. 11)
- [ ] Log aggregation (Ch. 11)
- [ ] Distributed tracing (Ch. 11)
- [ ] Externalized configuration (Ch. 11)

### Deployment
- [ ] Deploy as container (Ch. 12)
- [ ] Service mesh (Ch. 12)
- [ ] Sidecar (Ch. 12)

### Refactoring
- [ ] Strangler application (Ch. 13)
- [ ] Anti-corruption layer (Ch. 13)

---

## How to use this file

### Starting a session in Claude Chat (claude.ai)
Paste the following prompt:
> "I'm working through Microservices Patterns by Chris Richardson, building the FTGO app as I go.
> Here is my current CONTEXT.md: [paste file]. Please resume from where I left off."

### Starting a session in Claude Code
Claude Code reads this file from the project root automatically. You can also say:
> "Read CONTEXT.md and resume my FTGO study session. I want to work on [concept / service / chapter]."

### Ending any session
Ask whichever tool you're in:
> "Update CONTEXT.md to reflect what we covered today."

---

## Tech stack decisions
- **Language**: Java 17+
- **Framework**: Spring Boot 3.x
- **Messaging**: Apache Kafka (via Eventuate Tram)
- **Database**: MySQL per service (each service owns its schema)
- **Infrastructure**: Docker Compose (local), Kubernetes deferred to Ch. 12
- **Build tool**: Gradle (matches reference implementation)
- **Testing**: JUnit 5, Mockito, Spring Boot Test, Pact (contract tests in Ch. 9)

---
*Last updated: 2026-06-04 — initial setup*
