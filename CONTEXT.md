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
| 3  | Interprocess communication in a microservice architecture | Done | High | RPI/REST + circuit breaker, messaging, transactional outbox, service discovery, transaction log tailing (CDC) — all implemented and verified |
| 4  | Managing transactions with sagas | Done | High | Create Order saga implemented twice — choreography and orchestration, switchable via SAGA_MODE, verified to reach identical outcomes, fully documented (docs/ARCHITECTURE.md) |
| 5  | Designing business logic in a microservice architecture | Implementing | High | Read directly from the book PDF (pages 146–182). `Ticket` (kitchen-service) and `Order` (order-service) both refactored into real DDD aggregates — state machines, class-per-event domain events, domain event publishers. `Order`'s Cancel Order saga is now implemented (kitchen-gates-accounting, both saga modes); Revise Order saga remains, a separate future session. |
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

- **Chapter**: 5 — Designing business logic in a microservice architecture — Implementing. `Ticket` (kitchen-service, 2026-07-20) and `Order` (order-service, 2026-07-21) both refactored into real DDD aggregates via full brainstorm → spec → plan → subagent-driven-development cycles. `Order`'s refactor covers the full Fig. 5.14 state machine (create, cancel, revise) — 8 guarded state-changing methods returning `List<OrderDomainEvent>`, a new `OrderDomainEventPublisher` generalizing `order.events`'s wire format, both saga participants (`OrderSagaService`/`CreateOrderSagaOrchestrator`) rewired to use the aggregate, and new `POST /orders/{id}/cancel`/`POST /orders/{id}/revise` REST endpoints. The **Cancel Order saga** (sub-project 2 of 3) is now also implemented — a real cross-service saga (kitchen ticket cancellation gating accounting authorization reversal) in both saga modes, resolving `Order.CANCEL_PENDING`.
- **Status**: `Order` aggregate (sub-project 1) and Cancel Order saga (sub-project 2) both done. One sub-project remains: **Revise Order saga** (sub-project 3) — the book's trickiest saga (quantity-based revision, kitchen capacity re-check, accounting re-authorization delta), a separate future session, needed to resolve `Order.REVISION_PENDING`.
- **Last session**: 2026-07-21
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
- 2026-07-18 · Claude Code · Extracted the 4x-duplicated outbox/dedup infrastructure (OutboxEvent, ProcessedEvent, their repositories, OutboxPublisher, KafkaProducerConfig) into a new ftgo-common Gradle module via a full brainstorm → spec → plan → subagent-driven-development cycle (9 tasks): each of the four saga services (order/kitchen/consumer/accounting) now depends on it via `implementation project(':ftgo-common')`, built as a plain library (bootJar disabled, java-library's `api` configuration exposing JPA/Kafka types transitively) rather than a fifth executable Spring Boot service. Each service gained a small `PersistenceConfig` class rather than annotating the `@SpringBootApplication` class directly with `@EntityScan`/`@EnableJpaRepositories` — doing that broke order-service's `OrderControllerTest` (`@WebMvcTest` slice tests filter out `@Configuration`-discovered beans but not annotations on the primary configuration class itself), a real regression caught during task review and fixed by moving the annotations into the separate config class, then applied consistently to the other three services from the start. Docker e2e verification caught a second, more serious gap the plan itself had missed: `@EntityScan`/`@EnableJpaRepositories` alone don't register `@Component`/`@Configuration` beans, so the shared `OutboxPublisher` and `KafkaProducerConfig` were never picked up by any of the four services — no startup error since nothing else required those beans, but the outbox poller silently never ran and every order stayed stuck in `APPROVAL_PENDING` forever. Fixed by adding `@ComponentScan(basePackages = "com.sanjay.ftgo.common.outbox")` to all four `PersistenceConfig` classes, alongside the existing entity/repository scan annotations. Re-verified via Docker after the fix: choreography happy path (including the two orders that had been stuck before the fix, both recovering to `APPROVED` once the poller started running), orchestration happy path, one compensation case (inactive consumer → `Order.REJECTED`/`Ticket.CANCELLED`), and redelivery/idempotency (forced outbox redelivery, confirmed no duplicate authorization). Saga wire-format records (SagaReply, KitchenCommand, etc.) deliberately stayed per-service, out of scope for this pass. PR #8 merged to main. Independent post-merge code review found the per-service `@ComponentScan` copy-paste itself was a maintainability risk (a future 5th consumer could omit it and reproduce the exact stuck-order bug); fixed by shipping a real Spring Boot auto-configuration from `ftgo-common` (`OutboxAutoConfiguration` via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`) so `OutboxPublisher`/`KafkaProducerConfig` register automatically for any consumer — no annotation to forget. Also cleaned up redundant dependency declarations, excluded `ftgo-common` from inheriting `spring-boot-starter-web` it never uses, and renamed `OutboxEvent.orderId` → `aggregateId` (Java-level only, DB column unchanged) since the entity is now generic/shared.
- 2026-07-18 · Claude Code · Upgraded Spring Boot 3.5.3 → 3.5.16 (the final 3.5.x release — the line reached open-source end-of-life on 2026-06-30) and the Spring Cloud BOM 2025.0.0 → 2025.0.3 (latest patch compatible with Boot 3.5.x; Spring Cloud 2025.1.x requires Boot 4.0) across all three modules that declare it (order-service, restaurant-service, service-registry — the PR's first pass only updated order-service, missing that restaurant-service and service-registry, the actual Eureka client/server pair, also pin the same BOM; caught by independent code review before merge). Checked every documented 3.5.x behavioral change (TaskExecutor bean renaming, heapdump endpoint default, stricter `.enabled` boolean parsing, profile-name restrictions, Redis/Prometheus/Groovy property renames, docker-compose SSL fixes) against this codebase — none apply (no actuator, no `taskExecutor` bean, no `.enabled` properties, no `@Profile` usage, no Spring Boot docker-compose integration), so no application code changes were needed. Verified via full `./gradlew clean build` and two Docker e2e passes (before and after the Spring Cloud BOM fix) reusing the same four scenarios as every other dependency-touching change in this project (choreography happy path, orchestration happy path, one compensation case, redelivery/idempotency) — all passed, including Eureka registration/discovery specifically re-verified after aligning all three modules on the same BOM version.
- 2026-07-18 · Claude Code · Started Ch. 5 (designing business logic) — no book reading yet, just a first-pass conceptual walkthrough of the three named patterns (transaction script, DDD aggregates/domain model, domain events), each explained by mapping it onto this codebase's actual current code rather than abstractly. See "Concept understanding" below for the specifics carried forward. No implementation done. Handed off to continue reading in Claude Chat before returning to build.
- 2026-07-20 · Claude Code · Read Ch. 5 directly from the book PDF (installed poppler to enable PDF page rendering, then read pages 146–182 — business logic organization patterns, DDD aggregate rules 1–3, aggregate granularity, domain events, and both worked examples, `Ticket`/Kitchen Service and `Order`/Order Service — confirming the book's own examples match this project's exact candidates flagged on 2026-07-18). Corrected two inaccuracies from the earlier conceptual walkthrough: the book's actual `Ticket` method is `cancel()` not `markCancelled()`, and rule #2 (one transaction, one aggregate) — the formal justification for why sagas exist at all — had been glossed over. Then refactored `Ticket` (kitchen-service) into a real DDD aggregate via a full brainstorm → spec → plan → inline-execution cycle: a new `TicketState` enum (`CREATE_PENDING → AWAITING_ACCEPTANCE → ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP`, plus `CANCELLED`) replaces the old unguarded `String status`; 8 new class-per-event domain events (`TicketCreatedEvent`, `TicketAcceptedEvent`, etc., behind a sealed `TicketDomainEvent` interface) are returned from `Ticket`'s state-changing methods instead of being hand-built inline in `TicketService`; a new `TicketDomainEventPublisher` translates those typed events back into the existing flat `KitchenEvent` wire record before writing to the outbox — a deliberate wire-format-compatibility constraint, since `order-service`'s existing `KitchenEventListener` deserializes `kitchen.events` with Jackson's default strict unknown-property handling and would break if any new field leaked onto the wire. Also added kitchen-service's first REST controller (`TicketController`: `accept`/`preparing`/`ready-for-pickup`/`picked-up`) for the restaurant-worker lifecycle the book models but this app never had — a deliberate scope decision made during brainstorming, chosen over staying within the 3 pre-existing saga-driven states only. `cancel()` extends the book's own two-tier exception distinction (`TicketCannotBeCancelledException` from `READY_FOR_PICKUP` vs. generic `UnsupportedStateTransitionException` elsewhere) to this app's extra pre-accept state. 35 new/updated unit tests (19 aggregate state-machine tests, 3 publisher tests, 13 service tests, 8 controller tests via `@WebMvcTest`), full kitchen-service build green throughout. Work done in git worktree `worktree-ch5-ticket-aggregate`; Docker e2e verification and merge still pending.
- 2026-07-21 · Claude Code · Implemented and manually verified the `Order` (order-service) DDD aggregate refactor — the book's other Ch.5 worked example — via a full brainstorm → spec → plan → subagent-driven-development cycle (7 tasks), scoped deliberately to the full Fig. 5.14 state machine (create, cancel, revise) as sub-project 1 of 3 (Cancel Order saga and Revise Order saga deferred to future sessions). `OrderStatus` gained 3 states (`CANCEL_PENDING`, `CANCELLED`, `REVISION_PENDING`); `Order` gained 8 guarded methods (`noteApproved`/`noteRejected`/`cancel`/`noteCancelled`/`undoCancel`/`revise`/`confirmRevision`/`rejectRevision`) returning class-per-event `List<OrderDomainEvent>`, replacing the old unguarded `markApproved()`/`markRejected()`; a new `OrderDomainEventPublisher` replaced the single-purpose `OrderCreatedEvent` wire record with a generic `OrderEvent` record (matching `KitchenEvent`'s shape) so `order.events` can carry all 9 event types while keeping `OrderCreated`'s JSON byte-for-byte unchanged; both saga participants (`OrderSagaService` for choreography, `CreateOrderSagaOrchestrator` for orchestration) now call the aggregate instead of duplicating an inline status guard; `OrderController` gained `POST /orders/{id}/cancel` and `POST /orders/{id}/revise`, a known temporary gap since no saga yet resolves the resulting pending states. Caught and fixed a real cross-service bug during the design phase: `ftgo-kitchen-service`'s `OrderEventListener` deserialized every `order.events` message as `OrderCreatedEvent` and called `handleOrderCreated` with no `eventType` check — once `order.events` carries other event types this would have created bogus tickets from null fields; fixed with the same discriminator-first pattern already used by this codebase's other listeners. Subagent-driven-development execution hit two real coordination issues, both resolved by the controller: Task 3 discovered a genuine plan-sequencing gap (deleting the old wire record broke a file scoped to Task 5) resolved by pulling that file's already-written rewrite forward; Task 4's first implementer attempt corrupted the branch history (committed on the wrong git parent, silently reintroducing an already-deleted file) caught during review and repaired by resetting to the correct parent and reapplying only the legitimate changes. All 7 tasks passed task-level review clean after these fixes. Manually verified via Docker: choreography happy path (`Order.APPROVED`), `/cancel` reaching `CANCEL_PENDING` with kitchen-service correctly *not* reacting (ticket count unchanged across 12s of polling, confirming the listener fix), `/revise` reaching `REVISION_PENDING` with line items unchanged until a future confirmation, 404/409 edge cases, one compensation case (inactive consumer → `Order.REJECTED`/`Ticket.CANCELLED`), redelivery/idempotency (`processed_events` and ticket counts unchanged after a kitchen-service restart), and orchestration-mode happy path plus its own compensation case — all reaching the same end states as choreography. PR #12 merged to main.
- 2026-07-21 · Claude Code · Implemented and manually verified the Cancel Order saga (sub-project 2 of 3) — a full brainstorm → spec → plan → subagent-driven-development cycle (7 tasks) resolving `Order.CANCEL_PENDING` via a real cross-service saga, in both saga modes. Sequential design: order-service asks kitchen first (`Ticket.cancel()`), and only if kitchen confirms cancellable does it proceed to reverse the accounting authorization — if kitchen rejects (ticket already `READY_FOR_PICKUP`/`PREPARING`/`PICKED_UP`), `Order.undoCancel()` fires immediately and accounting is never contacted, so no compensating re-authorization step is ever needed. `Authorization` (accounting-service) became a DDD aggregate too (`AUTHORIZED`/`DECLINED`/`REVERSED`), going beyond Ch.5's two book-named worked examples, so "reverse" is a real guarded transition. `SagaReply` and the shared `KitchenCommand`/new `AccountingCommand` (renamed and generalized from `AuthorizeCardCommand`, which had no `commandType` discriminator at all) both gained a `sagaType` field so order-service's one shared `saga.replies` listener can route between `CreateOrderSagaOrchestrator` and a new, deliberately stateless `CancelOrderSagaOrchestrator` (no join needed — Cancel is a strict linear pipeline, unlike Create's parallel join). Fixed a real, previously-flagged gap in kitchen-service: `TicketService.handleCancelTicketCommand` used to silently swallow `Ticket.cancel()`'s outcome (no reply, no exception handling); it now replies `TicketCancelled`/`TicketCancellationRejected`. Two real design/planning gaps surfaced and were corrected before implementation: `KitchenCommand`'s `"CancelTicket"` command turned out to be shared by both sagas (Create's compensation path and Cancel's primary flow), so it needed its own `sagaType` field, not just `SagaReply`; and `accounting.commands` needed the same `KitchenCommand`-style generalization as `AuthorizeCardCommand` had no discriminator at all. A third, more serious bug was caught only during Docker e2e verification (orchestration mode): `AuthorizationCancelService.reverse()` was shared by the choreography listener (kitchen's `TicketCancelled` event) and the orchestration command listener (`ReverseAuthorization`), but always published the reversal as a domain event on `accounting.events` — a channel nothing consumes in orchestration mode — leaving `CancelOrderSagaOrchestrator` waiting forever on a `saga.replies` message that never arrived, sticking every orchestration-mode cancellation in `CANCEL_PENDING`. Fixed post-hoc by splitting into `reverseForChoreography()`/`reverseForCommand()`, mirroring the choreography/command split every other saga participant already used. All 7 tasks passed task-level review clean; the orchestration-mode bug was caught by hands-on Docker testing, not by review (task-level tests mocked the collaborator and never actually round-tripped the message between services). Manually verified via Docker, both saga modes: cancel success (`Order.CANCELLED`/`Ticket.CANCELLED`/`Authorization.REVERSED`) and cancel rejection (ticket driven to `READY_FOR_PICKUP` via the REST worker API, then `Order` back to `APPROVED`, ticket unchanged, `Authorization` confirmed still `AUTHORIZED` — accounting never contacted). PR opened, review pending.

---

## Concept understanding

### Understood well
- Ch. 1: Hexagonal architecture, monolithic hell (6 symptoms), scale cube (X/Y/Z axes), microservices as Y-axis decomposition, database-per-service rationale, pattern language structure (forces / resulting context / related patterns)
- Ch. 2: 4+1 view model (logical/implementation/process/deployment/scenarios), hexagonal architecture (inbound/outbound adapters and ports), three-step architecture process (system ops → services → APIs), decompose by business capability, decompose by DDD subdomain, SRP and CCP applied to services, god class problem and resolution via per-service domain models, bounded context = service boundary, Ubiquitous Language per service (Order vs Ticket vs Delivery)

- Ch. 5 (read directly from the book, pages 146–182): business logic organization patterns — transaction script (procedural, one method per system operation; this is what `TicketService`/`SagaJoinService` looked like pre-refactor) vs. domain model (behavior lives in the entities); DDD as a refinement of the domain model approach, not a third alternative. The three aggregate rules: (1) reference only the aggregate root, (2) inter-aggregate references use primary keys not object references (already true throughout this codebase, e.g. `Order.consumerId`), (3) one transaction creates/updates exactly one aggregate — the formal justification for *why* sagas (Ch.4) exist at all. Aggregate granularity trade-off: fine-grained (better concurrency, more sagas) vs. coarse (fewer sagas, more lock contention) — book recommends fine-grained by default. Domain events: the book's preferred mechanism is an aggregate method returning `List<DomainEvent>` to the calling service (over an `AbstractAggregateRoot`-style superclass, which couples aggregates to a base type); event enrichment (stuffing extra fields into events so consumers don't need a callback query) trades event stability for consumer simplicity. The book's own worked examples for this chapter are `Ticket` (Kitchen Service) and `Order` (Order Service) — the same two candidates already flagged in this project on 2026-07-18, now confirmed as the book's actual choice, not a coincidence.

### Needs more depth
- Ch. 5: the `Order` aggregate refactor itself — read and understood conceptually (state machine in Fig. 5.14, paired methods per saga step like `revise()`/`confirmRevision()`), but not yet applied to this codebase's `Order` (order-service).

### Open questions
- None remaining for `Ticket`. For `Order`: same pattern as `Ticket`, but a larger piece of work since `Order` is entangled with the saga orchestrator/choreography code (`CreateOrderSagaOrchestrator`, `OrderSagaService`) rather than being purely internal to one service — deferred to a future session.

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
| ftgo-order-service | Ch. 2–5 | `Order` is now a DDD aggregate (Ch.5) with the full create/cancel/revise state machine; Cancel Order saga participant (both modes); REST call + circuit breaker to restaurant-service (discovered via Eureka); publishes via outbox (polling or CDC); saga participant, both styles switchable via SAGA_MODE | Owns Order domain model (now with consumerId); `OrderStatus`: APPROVAL_PENDING → APPROVED/REJECTED, plus APPROVED ⇄ CANCEL_PENDING ⇄ CANCELLED and APPROVED ⇄ REVISION_PENDING; state-changing methods return class-per-event `OrderDomainEvent`s, published via `OrderDomainEventPublisher` onto a generalized `order.events` wire format; `POST /orders`, `POST /orders/{id}/cancel`, `POST /orders/{id}/revise`; choreography mode: `OrderSagaService` (create) + `OrderCancelSagaService` (cancel) react to `kitchen.events`/`accounting.events`; orchestration mode: `CreateOrderSagaOrchestrator` (persisted `CreateOrderSagaInstance`) for create, stateless `CancelOrderSagaOrchestrator` for cancel, both routed from one shared `saga.replies` listener via a `sagaType` field on `SagaReply` |
| ftgo-kitchen-service | Ch. 2, 4, 5 | `Ticket` is now a DDD aggregate (Ch.5); consumes order.events, creates Ticket (capacity-gated); Cancel Order saga participant (both modes); own outbox publishes TicketCreated/TicketCreationFailed/TicketConfirmed/TicketCancelled/TicketCancellationRejected/TicketAccepted/TicketPreparingStarted/TicketReadyForPickup/TicketPickedUp to kitchen.events; new REST API for restaurant staff | `TicketState` enum: CREATE_PENDING → AWAITING_ACCEPTANCE → ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP, or CANCELLED (only legal from the first three); state-changing methods return class-per-event `TicketDomainEvent`s, published via `TicketDomainEventPublisher`; reacts to accounting.events and consumer.events for confirmation/compensation, and to order.events'/kitchen.commands' cancel signals (`handleOrderCancelled`/`handleCancelTicketCommand`, both now replying/publishing the real outcome — a fix to a previously silent gap); deduped via processed_events ledger; `POST /tickets/{id}/accept|preparing|ready-for-pickup|picked-up` for restaurant staff |
| ftgo-accounting-service | Ch. 2, 4, 5 | Joins on ConsumerVerified + TicketCreated (either order), authorizes card, publishes CardAuthorized/Failed; `Authorization` is now a DDD aggregate (Ch.5); Cancel Order saga participant (both modes) | SagaJoinService + SagaJoinState (local join table, @Version optimistic locking) is the trickiest piece of the Create Order saga — resolves exactly once regardless of event arrival order; `AuthorizationStatus` enum (AUTHORIZED/DECLINED/REVERSED), guarded `authorize`/`decline`/`reverse` methods, `AuthorizationDomainEventPublisher`; new `AuthorizationCancelService` with two entry points — `reverseForChoreography` (kitchen's TicketCancelled event, publishes to accounting.events) and `reverseForCommand` (orchestration's ReverseAuthorization command, publishes a SagaReply to saga.replies) — this split matters: the two channels are not interchangeable, a bug caught during Docker e2e verification; own outbox + processed_events, topic accounting.events |
| ftgo-restaurant-service | Ch. 2 | GET /restaurants/{id} implemented | Restaurant/MenuItem JPA entities + seed data (2 restaurants); now registers with the Eureka service registry on startup (spring.application.name: ftgo-restaurant-service) |
| ftgo-service-registry | Ch. 3 | Eureka server, port 8761 | Standalone registry; restaurant-service registers on startup, order-service discovers it via @LoadBalanced RestClient |
| ftgo-delivery-service | Ch. 2 | Ready to scaffold | Uses Delivery not Order; separate bounded context |
| ftgo-api-gateway | Ch. 8 | Not started | |

### Architecture decisions made
- Hexagonal layers as Java packages (not Gradle sub-projects) — matches reference impl, simpler
- Shared `ftgo-common` module (2026-07-18) — extracted the generic outbox/dedup infrastructure (OutboxEvent, ProcessedEvent, their repositories, OutboxPublisher, KafkaProducerConfig) into a new Gradle module, `com.sanjay.ftgo.common.outbox`, depended on by all four saga services. The saga wire-format records (SagaReply, KitchenCommand, ConsumerVerificationEvent, etc.) remain per-service, copy-pasted into every producer/consumer — deliberately out of scope for this pass, since those carry business meaning specific to who produces/consumes them rather than being generic plumbing. Each service now carries a small `PersistenceConfig` class (rather than annotating the `@SpringBootApplication` class directly) with `@EntityScan`/`@EnableJpaRepositories`/`@ComponentScan` all pointed at both the service's own domain package and `com.sanjay.ftgo.common.outbox` — putting those annotations directly on the application class broke `@WebMvcTest` slice tests (order-service's `OrderControllerTest`), since annotations on the primary configuration class bypass slice-test type filtering in a way a separate `@Configuration` class doesn't.
- Minimal dependencies per service (web, jpa, mysql, test) — Eventuate Tram added in Ch. 3
- Single MySQL instance, one schema per service — lower local overhead, matches Richardson's Compose setup
- H2 in test scope with MODE=MySQL — allows contextLoads tests without a running DB

### Code patterns implemented
- DDD aggregate (2026-07-20) — `Ticket` (kitchen-service): `TicketState` enum with enforced legal transitions, class-per-event domain events behind a sealed `TicketDomainEvent` interface, returned from state-changing aggregate methods rather than hand-built inline in the service layer. `TicketDomainEventPublisher` translates typed domain events back into the pre-existing flat `KitchenEvent` wire record before writing to the outbox, preserving exact wire-format compatibility with `order-service`'s existing consumer (no new fields ever leak onto `kitchen.events`, since Jackson's default strict unknown-property handling would break deserialization there).

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
- [x] Saga — choreography (Ch. 4) — Create Order and Cancel Order sagas
- [x] Saga — orchestration (Ch. 4) — Create Order and Cancel Order sagas, switchable via SAGA_MODE, verified to reach identical outcomes to choreography

### Business logic
- [x] Domain model / DDD aggregates (Ch. 5) — `Ticket` (kitchen-service) and `Order` (order-service) both done
- [x] Domain events (Ch. 5) — class-per-event, returned from aggregate methods, published via `TicketDomainEventPublisher`/`OrderDomainEventPublisher`
- [x] Transaction script (Ch. 5) — understood and contrasted; the pre-refactor `TicketService`/`SagaJoinService` were the worked examples
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
*Last updated: 2026-07-21 — Ch. 5 `Order` aggregate merged; Cancel Order saga implemented (both modes); Revise Order saga remains*
