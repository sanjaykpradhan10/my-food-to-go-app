# Ch.10 Sub-Project 2: Component Tests — Design

**Status:** Approved
**Scope:** Chapter 10 of *Microservices Patterns* (§10.2, pp.335-347), sub-project 2 of 3 (component tests + component-test infrastructure). Sub-project 1 (consumer-driven contract tests) shipped in PR #21. Sub-project 3 (end-to-end tests) is separate and not addressed here.

## Goal

Add an out-of-process component test for order-service's Place Order flow, following the book's own worked example (§10.2) closely: the service runs as a real containerized process against real infrastructure (MySQL, Kafka), with its one synchronous external dependency (Restaurant Service) and its asynchronous saga participants (Consumer, Kitchen, Delivery, Accounting) stubbed, exercised through Gherkin scenarios via Cucumber.

## Why out-of-process (not in-process)

This project's existing test suite (Ch.9 unit tests, Ch.10 sub-project 1 contract tests) is entirely in-process: `@SpringBootTest` + WireMock/embedded Kafka, no Docker-in-test, no Cucumber. The book presents component testing as a genuine choice between in-process and out-of-process styles (p.339) and works its full example out-of-process specifically because it exercises the deployable artifact end-to-end rather than a slice of the Spring context. This sub-project follows the book's chosen style deliberately, as a second, complementary technique — not a replacement for the in-process patterns already in place.

## Architecture

**What runs for real (Docker Compose):**
- `order-service` itself, built and containerized (reuses the existing `ftgo-order-service/Dockerfile`)
- MySQL (`ftgo_order` schema)
- Kafka + Zookeeper

**What gets stubbed:**
- **Restaurant Service** — order-service's only synchronous REST dependency, called during `OrderService.createOrder()` to validate the menu. Stubbed with a WireMock container.
- **Consumer / Kitchen / Delivery / Accounting Services** — reached only asynchronously via Kafka saga commands/replies in orchestration mode. `CreateOrderSagaOrchestrator` fans out `VerifyConsumerCommand` (→ `consumer.commands`), `KitchenCommand`/CreateTicket (→ `kitchen.commands`), and `DeliveryCommand`/ScheduleDelivery (→ `delivery.commands`) in parallel at saga start, then sends `AccountingCommand`/AuthorizeCard (→ `accounting.commands`) only once all three of consumer-verified/ticket-created/delivery-scheduled have replied successfully. All four participants' replies are consumed from a single shared topic, `saga.replies` (`OrchestratorReplyListener`, active only when `saga.mode=orchestration`), routed by `SagaReply.sagaType()`. The component test therefore needs one "saga participant stub" helper that watches all four command topics and publishes replies back onto `saga.replies` on cue from step definitions — all four legs must succeed before the saga reaches the authorization step at all, so both the "authorized" and "rejected" scenarios drive the same three leading replies and differ only in the Accounting reply. This stub is a small dedicated class using a plain `KafkaConsumer`/`KafkaTemplate` — the existing `KafkaMessageVerifierSender`/`KafkaMessageVerifierReceiver` bridge from sub-project 1 is shaped around Spring Cloud Contract Verifier's one-shot, `YamlContract`-parameterized lifecycle and is not a fit for a long-running, Cucumber-driven stub reacting to arbitrary commands across topics; it is not reused here.

**Bypassing Eureka for service resolution:** order-service resolves its REST dependencies via `@LoadBalanced RestClient` against Eureka virtual hostnames (e.g. `http://ftgo-restaurant-service`, configured in `RestClientConfig`). Rather than running a real Eureka registry and making WireMock self-register in it, the component-test profile sets `eureka.client.enabled=false` and uses Spring Cloud LoadBalancer's `SimpleDiscoveryClient` (`spring.cloud.discovery.client.simple.instances.ftgo-restaurant-service[0].uri=http://wiremock:8080`) so the same `@LoadBalanced` code path resolves straight to the WireMock container. No Eureka container runs in this component-test compose stack.

**Saga mode:** runs with `SAGA_MODE=orchestration`. The book's own stub mechanism targets an orchestrator's command channel, and orchestration mode lets one saga-stub implementation drive both scenarios without also handling choreography's peer-to-peer event topology. Sub-project 1's contract tests already independently cover both saga styles' message shapes — this sub-project intentionally does not re-prove that in component-test form. Choreography-mode component-test coverage is deferred (see "Deferred to a future sub-project" below).

**Persistence mode:** `PERSISTENCE_MODE=jpa` only (order-service's default). The event-sourced path is out of scope here.

## Test Scenarios

Two scenarios, matching the book's own `PlaceOrder.feature` (Listing 10.11):

1. **Order authorized** — POST `/orders` with a consumer/restaurant/menu-item combination that WireMock's Restaurant Service stub accepts as valid → poll `GET /orders/{id}` (bounded retry loop, ~10s timeout) until status leaves `APPROVAL_PENDING` → assert `APPROVED` → assert the saga-stub observed the expected authorization command and that an order-approved domain event landed on `order.events`.
2. **Order rejected (expired credit card)** — same flow, but a `@Given` step configures the saga-stub to reply with a rejection instead of an approval → assert the order ends in `REJECTED` and the corresponding rejection event is published.

## Tooling

- **Cucumber** (JUnit Platform engine) executing Gherkin scenarios, per the book.
- **`com.avast.gradle.docker-compose` Gradle plugin** — the same plugin the book uses — to bring the compose stack up before the componentTest task and tear it down after, success or failure.
- **New `componentTest` Gradle source set + task** on `ftgo-order-service`, mirroring how `contractTest` was already wired as its own source set in sub-project 1. Kept out of the default `test` task since it requires Docker and is slow.

### File layout

- `compose-component-test.yml` (new, project root) — slimmed-down compose stack: order-service, mysql, kafka, zookeeper, wiremock. No service-registry, no other business services.
- `ftgo-order-service/src/componentTest/resources/features/PlaceOrder.feature`
- `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/ComponentTestRunner.java` — Cucumber JUnit Platform runner
- `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java`
- `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/SagaParticipantStub.java`
- `ftgo-order-service/build.gradle` — new `componentTest` source set/task, docker-compose plugin config, Cucumber deps

## Teardown

The docker-compose Gradle plugin's `composeDown` runs after the Cucumber task regardless of pass/fail. MySQL and Kafka data are container-local (fresh volumes each run) and discarded with the containers — no manual cleanup step.

## Deferred to a future sub-project

The following are explicitly out of scope here and are **not silently dropped** — they're deferred as a follow-up sub-project to be brainstormed and scoped separately when picked up:

- Component-test coverage for Revise Order and Cancel Order sagas (only Place Order is covered here)
- Choreography-mode component-test coverage (this sub-project covers orchestration mode only)
- Component tests for other services (Kitchen, Restaurant, Accounting, Delivery, order-history-service, the gateways) — this sub-project covers order-service only, matching the book's own single-service worked example
- Event-sourced persistence mode (`PERSISTENCE_MODE=eventsourcing`) component coverage

## Testing

- `./gradlew :ftgo-order-service:componentTest` — brings up the compose stack, runs both Cucumber scenarios, tears down. This is the sub-project's own verification step; no changes to any other module's test suite are expected.
