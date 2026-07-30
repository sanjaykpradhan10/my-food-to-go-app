# Ch.10 sub-project 1 — Consumer-driven contract tests — design

**Date:** 2026-07-30
**Status:** Approved, pending plan

## Background

Chapter 10 of *Microservices Patterns* ("Testing microservices: Part 2") covers
three layers of the test pyramid above unit tests: §10.1 integration tests
(persistence, REST, pub/sub, and async request/response, the latter three built
around Spring Cloud Contract's consumer-driven contract testing), §10.2
component tests (Gherkin/Cucumber acceptance tests with stubbed dependencies),
and §10.3 end-to-end tests. Given the size, the chapter is being worked as
three sequential sub-projects, each independently brainstormed/spec'd/planned/
implemented: (1) consumer-driven contract tests, (2) component tests, (3)
end-to-end tests. This spec covers sub-project 1 only.

## Audit findings

Reading the book chapter and auditing the existing FTGO suite (same
audit-first approach used for Ch.9) found:

- **Persistence integration tests (§10.1.1):** already solid and matches the
  book closely — `@DataJpaTest`-style tests exist across order-service,
  restaurant-service, and order-history-service (`OrderRepositoryTest`,
  `RestaurantRepositoryTest`, `OrderViewPersistenceTest`, plus event-sourcing
  repo tests). No gap; out of scope for this sub-project.
- **REST consumer-side tests:** `RestaurantServiceProxyTest`,
  `KitchenServiceProxyTest`, `DeliveryServiceProxyTest`,
  `AccountingServiceProxyTest` already use WireMock, but with hand-written
  stubs — no contract shared with the provider side, so nothing guarantees
  the stub matches the real service's actual behavior.
- **No consumer-driven contract testing exists at all:** no Spring Cloud
  Contract dependency, no `contracts` module anywhere in the repo. This is
  the chapter's headline technique and the largest real gap.
- **No pub/sub integration tests, no async request/response contract
  tests** — real gaps.
- **Tooling mismatch with the book:** this project uses Spring Cloud
  2025.0.3 and Kafka directly via `spring-kafka` (`@KafkaListener`,
  `KafkaTemplate`, hand-serialized JSON, hand-rolled outbox) — not Eventuate
  Tram, which the book's pub/sub and async-request/response Spring Cloud
  Contract examples are built on. Spring Cloud Contract's built-in Kafka
  messaging support otherwise assumes Spring Cloud Stream's Kafka binder,
  which this project also doesn't use. Making Spring Cloud Contract work for
  the two messaging interactions therefore requires implementing a custom
  `ContractVerifierMessaging<Message<String>>` bridge — real infrastructure
  work, not a copy of the book's example.

## Scope

Three consumer-driven contracts, one per interaction style, mapped to real
code already in the repo:

| Style | Consumer | Provider | Contract |
|---|---|---|---|
| REST | `ftgo-mobile-gateway`'s `OrderDetailsHandler` (via its `orderServiceClient` `WebClient`) | `ftgo-order-service`'s `OrderController.getOrder()` (`GET /orders/{id}`) | HTTP request/response |
| Pub/sub | `ftgo-order-history-service`'s `OrderEventListener` (`@KafkaListener("order.events")`) | `ftgo-order-service`'s domain event publisher (outbox → Kafka `order.events` topic) | `OrderCreated` domain event JSON |
| Async req/resp | `ftgo-order-service`'s `OutboxSagaCommandPublisher` (sends `CreateTicket` to `kitchen.commands`, awaits reply) | `ftgo-kitchen-service`'s `KitchenCommandListener` (`@KafkaListener("kitchen.commands")`) | Command message + reply message |

### 1. REST contract (§10.1.2)

Near-identical to the book's worked example:

- Add `spring-cloud-contract-verifier` to `ftgo-order-service` (provider) and
  `spring-cloud-contract-wiremock` + `@AutoConfigureStubRunner` test
  dependencies to `ftgo-mobile-gateway` (consumer).
- New module `ftgo-order-service-contracts` (mirrors the book's
  `ftgo-order-service:contracts` artifact) containing one Groovy contract:
  `GET /orders/{id}` → `200` + JSON body matching `OrderResponse`'s shape.
  Published to the local Maven repo (`mavenLocal()`) so the consumer's
  `@AutoConfigureStubRunner` can resolve it — matches this project's existing
  multi-module Gradle conventions (e.g. `ftgo-gateway-common`).
- Provider side: Spring Cloud Contract code-generates a
  `RestAssuredMockMvc`-based test against `OrderController`, following the
  same `HttpBase` abstract-class pattern the book uses (§10.1.2).
- Consumer side: a new WireMock-backed integration test asserting
  `OrderDetailsHandler.fetchOrderDetails()` correctly parses the contract's
  response for the `orderService` section of the composed `OrderDetails`.

### 2. Pub/sub contract (§10.1.3)

- New Groovy contract in `ftgo-order-service-contracts` describing the
  `OrderCreated` domain event: channel `order.events`, JSON body, headers.
- **Messaging bridge:** implement a `ContractVerifierMessaging<Message<String>>`
  bean (Spring Cloud Contract's documented custom-middleware extension
  point) backed by `spring-kafka-test`'s `EmbeddedKafkaBroker` — added as a
  new test dependency to both `ftgo-order-service` and
  `ftgo-order-history-service`. One shared adapter module/class pair
  (send-side, receive-side) reused by both this contract and the async
  request/response contract below, so the Kafka-bridging work is done once.
- Provider-side test (order-service): triggers publication of an
  `OrderCreated` event and verifies, via the embedded broker, that the
  published message matches the contract.
- Consumer-side test (order-history-service): publishes the contract's
  example message to the embedded broker's `order.events` topic and verifies
  `OrderEventListener` correctly invokes `OrderViewService`.

### 3. Async request/response contract (§10.1.4)

- New Groovy contract in a new `ftgo-kitchen-service-contracts` module (the
  book models this contract from the provider's perspective, i.e. Kitchen
  Service): input message (`CreateTicket` command on `kitchen.commands`),
  output message (success reply).
- Reuses the same embedded-Kafka `ContractVerifierMessaging` bridge from the
  pub/sub contract above — no new bridging infrastructure.
- Provider-side test (kitchen-service): feeds the contract's input command
  through the embedded broker and verifies `KitchenCommandListener` triggers
  `TicketService` correctly and that the resulting reply message matches the
  contract's output.
- Consumer-side test (order-service): verifies `OutboxSagaCommandPublisher`
  publishes a `CreateTicket` command matching the contract's input, and that
  it correctly handles a reply message matching the contract's output.

## Out of scope

- Component tests and end-to-end tests — sub-projects 2 and 3, separate
  spec/plan/implementation cycles.
- Any interaction pairs beyond the three listed above (e.g. the other saga
  participants — Accounting Service, Delivery Service — follow the same
  async-request/response pattern as Kitchen Service and could get contracts
  later, but aren't needed to demonstrate the technique).
- Persistence integration tests — already adequately covered, per the audit.
- Any production code changes. This is test/build-infrastructure only, same
  discipline as Ch.9: the existing implementation is already correct: only
  test coverage is being strengthened.
- Migrating the project's messaging from raw `spring-kafka` to Spring Cloud
  Stream, or introducing Eventuate Tram — the custom
  `ContractVerifierMessaging` bridge exists specifically so this isn't
  needed.

## Verification

- `./gradlew build` (or the relevant per-module test tasks) across
  `ftgo-order-service`, `ftgo-order-service-contracts`,
  `ftgo-mobile-gateway`, `ftgo-order-history-service`, `ftgo-kitchen-service`,
  and `ftgo-kitchen-service-contracts` — all new and existing tests must
  pass.
- For each of the three contracts: confirm the provider-side generated test
  fails if the provider's actual behavior is deliberately broken (e.g. a
  wrong field value), and the consumer-side test fails if the contract's
  example is deliberately mismatched with what the consumer expects — left
  to plan-time judgment on whether to do this for all three contracts or a
  representative subset, same as Ch.9's approach to load-bearing-assertion
  verification.
