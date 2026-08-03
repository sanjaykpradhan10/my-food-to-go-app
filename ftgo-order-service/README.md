# ftgo-order-service

**Port:** 8082
**Bounded context:** Order lifecycle / saga coordination

## Role

Owns the `Order` aggregate — the entry point for placing an order and the service whose status field records the outcome of every saga this order participates in: `APPROVAL_PENDING → APPROVED`/`REJECTED` (Create Order), `APPROVED ⇄ CANCEL_PENDING ⇄ CANCELLED` (Cancel Order), and `APPROVED ⇄ REVISION_PENDING` (Revise Order). It plays two different roles depending on `SAGA_MODE`: in **choreography** it's just one more participant reacting to events published by the other services; in **orchestration** it's the coordinator for all three sagas, driving kitchen-service and accounting-service via explicit commands and tracking each saga's progress. Both implementations live in the codebase simultaneously, gated by Spring's `@ConditionalOnProperty`. Independently of `SAGA_MODE`, `Order` can be persisted either via JPA or via a hand-rolled event store, switchable via `PERSISTENCE_MODE` (see "Persistence" below) — all four combinations of the two switches are supported.

It also validates every order against restaurant-service before creating it, via a synchronous REST call wrapped in a circuit breaker (the Ch.3 RPI pattern) — this part of the service is identical regardless of saga mode.

## API

All endpoints require a bearer JWT (Ch.11, §11.1) issued by `ftgo-authorization-server`, validated by this service as an OAuth2 resource server. `GET /orders/{id}` and `GET /orders/{id}/view` additionally enforce an instance-based ACL (`OrderAccessControl`): `ADMIN` unconditionally, or `CONSUMER` only when the JWT's `sub` matches the order's `consumerId` — anyone else gets `403`. `POST /orders` derives `consumerId` from the JWT `sub` rather than trusting the request body's `consumerId` field, so a consumer can't place orders on another consumer's behalf by forging it (the field is still required for request-shape/contract-test stability).

**`GET /orders/{id}`** — **Auth:** any authenticated user; `CONSUMER` restricted to their own order, `ADMIN` unrestricted (see above).

**`POST /orders`** — **Auth:** `CONSUMER` or `ADMIN`.

Request:
```json
{"consumerId": 1, "restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 2}]}
```

Response (`201 Created`):
```json
{"id": 1, "consumerId": 1, "restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 2}], "status": "APPROVAL_PENDING"}
```

| Condition | Status |
|---|---|
| `consumerId`/`restaurantId` missing, or `lineItems` empty | `400` |
| Restaurant or menu item not found | `404` |
| Restaurant-service circuit open / unreachable | `503` |

The order is always created in `APPROVAL_PENDING` — the Create Order saga (either style) transitions it asynchronously afterward.

**`POST /orders/{id}/cancel`** — **Auth:** `CONSUMER` or `ADMIN`.

No request body. Legal only from `APPROVED` — moves the order to `CANCEL_PENDING` and triggers the Cancel Order saga (either style).

| Condition | Status |
|---|---|
| Order not found | `404` |
| Order not in `APPROVED` | `409` |

**`POST /orders/{id}/revise`** — **Auth:** `CONSUMER` or `ADMIN`.

Request:
```json
{"lineItems": [{"menuItemId": 1, "quantity": 5}]}
```

Legal only from `APPROVED` — moves the order to `REVISION_PENDING`, records the proposed line items (`pendingRevisedLineItems`), and triggers the Revise Order saga (either style). The order's *current* `lineItems` are unchanged in the response — they only update once the saga confirms the revision.

| Condition | Status |
|---|---|
| Order not found | `404` |
| Order not in `APPROVED` | `409` |

**`GET /orders/{id}/view`** (API composition, Ch.7) — **Auth:** same instance-based ACL as `GET /orders/{id}` above (see the note at the top of this section).

A composite read: assembles a single response for the order-detail screen from data owned by four different services, fanned out in parallel rather than sequentially.

Response (`200 OK`):
```json
{
  "order": {"id": 1, "status": "APPROVED", "consumerId": 1, "restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 2}]},
  "restaurant": {"data": {"id": 1, "name": "Ajanta", "menuItems": [...]}},
  "ticket": {"data": {"id": 1, "orderId": 1, "status": "AWAITING_ACCEPTANCE", "readyBy": null}},
  "authorization": {"data": {"id": 1, "orderId": 1, "status": "AUTHORIZED"}},
  "delivery": {"reason": "..."}
}
```

`order` (`OrderSummary`) comes from this service's own `Order` — no remote call needed. The other four sections (`restaurant`/`ticket`/`authorization`/`delivery`) are each a `SectionResult<T>` — a sealed interface with exactly three cases, so a downstream problem degrades one section instead of failing the whole response. No Jackson type discriminator is configured, so each case serializes as its own record shape (`{"data": ...}`, `{}`, or `{"reason": ...}`) — a client distinguishes them structurally, not by a `type` field:

- `Found<T>(data)` — the remote call succeeded. Serializes as `{"data": {...}}`.
- `NotFound<T>()` — the remote service responded `404` (e.g. no ticket exists yet for this order). Serializes as `{}`.
- `Unavailable<T>(reason)` — the remote call failed for any other reason (timeout, connection refused, open circuit). Serializes as `{"reason": "..."}` — carries the exception message, not a generic string, since this is a debugging aid, not user-facing copy.

`OrderViewController.view()` fires all four downstream lookups (`restaurantServicePort.findRestaurantForView`, `kitchenServicePort.findTicket`, `accountingServicePort.findAuthorization`, `deliveryServicePort.findDelivery`) concurrently via `CompletableFuture.supplyAsync(..., orderViewExecutor)` on a dedicated virtual-thread-per-task `ExecutorService` (`VirtualThreadExecutorConfig`), then joins all four before assembling the response — the four are independent, degradable sections, so none should block the others, and virtual threads make the 4-way fan-out cheap without a fixed pool-size decision to justify.

| Condition | Status |
|---|---|
| Order not found | `404` |
| Every downstream call succeeds, fails, or times out | `200` — a downstream problem never fails this endpoint; it shows up as `NotFound`/`Unavailable` in the corresponding section |

## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only (no custom
`HealthIndicator` code). Reports:
- `db` — MySQL reachability via the service's `DataSource`.
- `discoveryComposite` — Eureka registration status.

There is no `kafka` component: Spring Boot's actuator-autoconfigure no longer ships a Kafka
health contributor as of this project's Spring Boot version (3.5.16) — verified directly against
the built jars (`spring-boot-actuator-autoconfigure` retains only `KafkaMetricsAutoConfiguration`
under `actuate.autoconfigure.kafka`; `spring-kafka` ships no health-indicator class either) — and
adding a custom one is out of scope for this sub-project.

`management.endpoint.health.show-details: always` — safe here since these ports aren't exposed
to untrusted clients in this project; full component detail is the point of exercising this
pattern. Verified against the real, running stack by `ftgo-end-to-end-test`'s
`AllServicesReportHealthy.feature`.

## Restaurant/kitchen/accounting/delivery service integration

`RestaurantServiceProxy`, `KitchenServiceProxy`, `AccountingServiceProxy`, and `DeliveryServiceProxy` each call their respective service via a `@LoadBalanced RestClient` (base URLs `http://ftgo-restaurant-service`/`http://ftgo-kitchen-service`/`http://ftgo-accounting-service`/`http://ftgo-delivery-service`, all resolved dynamically through Eureka), each wrapped in its own Resilience4j circuit breaker instance (`restaurantService`/`kitchenService`/`accountingService`/`deliveryService`) — all four instances share the exact same settings: sliding window 5, failure-rate threshold 50%, 5s wait-duration-in-open-state, 3 permitted calls in half-open. `RestaurantNotFoundException` is excluded from `restaurantService`'s failure count (a 404 isn't a service health signal) — the other three proxies don't need an equivalent exclusion, since their `findXForView`-style methods return `SectionResult.NotFound` directly on a `404` rather than throwing.

Each `findX`/`findXForView` method has a `@CircuitBreaker`-annotated fallback method returning `Unavailable<>(throwable.getMessage())` — this is what turns a timeout or an open circuit into a degraded section instead of a failed request. `RestaurantServiceProxy` alone carries two methods against the same `restaurantService` circuit breaker instance: the pre-existing `findRestaurant` (throws, used by `POST /orders`'s order-creation validation) and the new `findRestaurantForView` (returns `SectionResult`, used only by `GET /orders/{id}/view`) — same remote endpoint, two different failure-handling contracts for two different callers.

## Events

### Publishes (`order.events`, choreography only)

| eventType | When |
|---|---|
| `OrderCreated` | On every order creation |
| `OrderApproved` / `OrderRejected` | Create Order saga resolves |
| `OrderCancelled` | `/cancel` called (`CANCEL_PENDING`) |
| `OrderCancelConfirmed` / `OrderCancelRejected` | Cancel Order saga resolves |
| `OrderRevisionProposed` | `/revise` called (`REVISION_PENDING`) — carries the proposed line items |
| `OrderRevised` | Revise Order saga confirms — carries the applied line items |
| `OrderRevisionRejected` | Revise Order saga rejects (either outright, or after compensation finalizes) |
| `OrderRevisionCompensationRequested` | Revise Order saga's compensation trigger only — **not** a real `Order` state transition (status stays `REVISION_PENDING`); carries the original, untouched line items so kitchen knows what to revert to. Wire-only in both persistence modes: written straight to the outbox in JPA mode, and written to `order_events` with `replayable=false` in event-sourcing mode (see "Persistence" below) — it must never be fed back into `OrderAggregate.apply()` |

### Publishes (orchestration mode)

| Topic | Command/reply | When |
|---|---|---|
| `consumer.commands` | `VerifyConsumerCommand` | Create Order saga start |
| `kitchen.commands` | `KitchenCommand{commandType=CreateTicket\|ConfirmTicket\|CancelTicket\|ReviseTicket\|UndoReviseTicket}` | Depending on which saga/step |
| `delivery.commands` | `DeliveryCommand{commandType=ScheduleDelivery\|ReleaseDelivery}` | Create Order saga start (`ScheduleDelivery`, parallel with `VerifyConsumerCommand`/`CreateTicket`); Create Order compensation or Cancel Order's delivery-release step (`ReleaseDelivery`, `sagaType` distinguishes which) |
| `accounting.commands` | `AccountingCommand{commandType=AuthorizeCard\|ReverseAuthorization\|ReviseAuthorization}` | Depending on which saga/step |

Every command/reply carries a `sagaType` (`CreateOrder`/`CancelOrder`/`ReviseOrder`) so the three sagas can safely share these topics — see the root [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md#multi-saga-routing-sagatype).

### Consumes

| Mode | Topic | Reacts to |
|---|---|---|
| Choreography | `consumer.events` | `ConsumerVerificationFailed` → reject (Create Order) |
| Choreography | `kitchen.events` | `TicketConfirmed`/`TicketCreationFailed` (Create Order); `TicketCancellationRejected` (Cancel Order); `TicketRevisionRejected`/`TicketRevisionUndone` (Revise Order) |
| Choreography | `delivery.events` | `DeliverySchedulingFailed` → reject (Create Order). `DeliveryScheduled` doesn't move `Order` — only the failure case triggers a saga transition here, mirroring `kitchen.events` only reacting to `TicketCreationFailed`, not `TicketCreated` |
| Choreography | `accounting.events` | `CardAuthorizationFailed` (Create Order); `AuthorizationReversed` (Cancel Order); `AuthorizationRevised`/`AuthorizationRevisionRejected` (Revise Order) |
| Orchestration | `saga.replies` | All participants' replies for all 3 sagas, routed by `sagaType` then dispatched to the matching orchestrator |

## Saga participants

- **Create Order** — choreography: `OrderSagaService` (three thin listeners, approve/reject guarded on `APPROVAL_PENDING`). Orchestration: `CreateOrderSagaOrchestrator` + persisted `CreateOrderSagaInstance` (**3-way parallel join** — consumer verification, ticket creation, and delivery scheduling all run in parallel; accounting authorization only fires once all three flags (`consumerVerified`/`ticketCreated`/`deliveryScheduled`) are set, `@Version` optimistic locking). A failure on any one of the three legs rejects the order and compensates whichever of the other two legs already succeeded (`sendCancelTicket`/`sendReleaseDelivery`), including late-arriving success replies that land after the instance is already marked failed.
- **Cancel Order** — choreography: `OrderCancelSagaService`. Orchestration: stateless `CancelOrderSagaOrchestrator`. Sequential 3-step chain — kitchen ticket cancellation → delivery release → accounting authorization reversal. `Ticket.cancel()` can legitimately fail (ticket already `READY_FOR_PICKUP`+), in which case `Order.undoCancel()` fires immediately and neither delivery nor accounting is ever contacted.
- **Revise Order** — choreography: `OrderReviseSagaService`. Orchestration: stateless `ReviseOrderSagaOrchestrator`. Same kitchen-gates-accounting shape as Cancel Order, but kitchen *provisionally applies* the revised quantity before accounting is asked (since re-authorization is a real threshold check accounting can decline, unlike Cancel Order's unconditional reversal) — `compensateRevision`/`sendUndoReviseTicket` trigger kitchen to revert if accounting declines, and `Order` stays `REVISION_PENDING` until that reversion is confirmed.

Each saga's trigger from the REST layer is a small `OrderXSagaTrigger` interface (`OrderCancellationSagaTrigger`, `OrderRevisionSagaTrigger`) with a choreography impl (publishes domain events directly) and an orchestration impl (calls the orchestrator's `start()`), selected by `@ConditionalOnProperty(saga.mode=...)` — `OrderController` depends only on the interface, never on which mode is active.

See [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) for the full side-by-side sequence diagrams (all three sagas, both styles, happy path + every compensation case).

## Domain model

- `Order` — `id`, `consumerId`, `restaurantId`, `lineItems`, `status` (`OrderStatus`), `pendingRevisedLineItems` (only populated between `revise()` and `confirmRevision()`/`rejectRevision()`).
- `OrderStatus`: `APPROVAL_PENDING`, `APPROVED`, `REJECTED`, `CANCEL_PENDING`, `CANCELLED`, `REVISION_PENDING`.
- 8 guarded state-changing methods (`noteApproved`/`noteRejected`/`cancel`/`noteCancelled`/`undoCancel`/`revise`/`confirmRevision`/`rejectRevision`), each returning a `List<OrderDomainEvent>` (class-per-event, sealed interface) rather than being hand-built inline by callers.
- `CreateOrderSagaInstance` (orchestration mode only) — `orderId` PK, `consumerVerified`/`ticketCreated`/`failed` flags, `totalQuantity`, `@Version`. Cancel Order and Revise Order have no equivalent table (both orchestrators are stateless).
- `OrderAggregate` (`event-sourcing` mode only) — the same state machine as `Order`, re-expressed as `process(OrderCommand) -> List<OrderDomainEvent>` / `apply(OrderDomainEvent)` for event-sourced replay; `OrderEventStore` translates between it and the persisted `Order` used everywhere else via `EventSourcedOrderTransitions`.

## Persistence

`Order` supports two persistence paths, switchable via `PERSISTENCE_MODE` (env var, default `jpa`, alternate `event-sourcing`):

- **`jpa`** — a mutable `orders` row, updated in place, `@Version` optimistic locking. Unchanged from Ch.5.
- **`event-sourcing`** — `Order`'s full history stored as an append-only sequence in `order_events`, current state derived by replay. Implemented by `OrderAggregate` (the book's `process(Command)`/`apply(Event)` split) and `OrderEventStore` (the hand-rolled event store: append, replay from a snapshot plus the tail since it, and a dedicated `order_aggregate_version` table for optimistic locking rather than deriving a version from event count). Snapshots (`order_snapshots`) are written every 5 events, purely as a replay-cost optimization with no effect on correctness.

Every call site (`OrderController`, `OrderService`, all three choreography saga services, all three orchestration saga orchestrators) depends on an `OrderTransitions` facade rather than `OrderRepository` directly, so neither persistence mode leaks into business logic. A parallel `SagaCommandPublisher` facade does the same for orchestration-mode outbound saga commands. In event-sourcing mode, those commands are written to a table deliberately separate from `order_events` — `order_saga_command_requests` (`OrderSagaCommandRequest`), polled by its own `SagaCommandRequestPublisher` — so the Debezium connector that watches `order_events` (see below) never leaks a saga command meant for `kitchen.commands`/`accounting.commands`/`consumer.commands` onto `order.events`.

Choreography-mode publishing in event-sourcing mode reuses the existing Ch.3 Debezium/Kafka Connect connector rather than a new pipeline: `order_events`' columns (`event_id`/`event_type`/`order_id`/`payload`) are deliberately named to match `outbox_events`', so the same connector's `table.include.list` covers both tables and routes both to `order.events` unchanged.

Full mechanics, sequence diagrams, and the wire-only-pseudo-event gotcha this surfaced (`OrderEventEntity.replayable`) are documented in the root [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md#event-sourcing--order-aggregate-ch6).

## Idempotency & reliability

Every Kafka-driven state change (all 3 sagas, both modes) is guarded by the `processed_events` dedup ledger — insert-then-act in one local transaction, so at-least-once Kafka delivery can't double-process. All outbound events go through a transactional outbox (`OutboxEvent`, carrying a per-row `topic` column since this service fans out to up to 4 different topics depending on mode) written in the same transaction as the business change, published by a separate `@Scheduled` poller. `OutboxEvent`/`OutboxPublisher`/`KafkaProducerConfig` themselves live in the shared `ftgo-common` module (see the root `docs/ARCHITECTURE.md`), not this service's own source tree.

## Running standalone

```bash
./gradlew :ftgo-order-service:test
```

Runs against H2 in-memory (`MODE=MySQL`) — no Docker needed for unit tests. To run live, start the full stack (`docker compose up -d`) — this service needs MySQL, Kafka, service-registry, and restaurant-service to actually serve traffic.

## Component tests (Ch.10)

```bash
./gradlew :ftgo-order-service:componentTest
```

Requires Docker running locally. An out-of-process Cucumber suite (`src/componentTest`) that drives the real, packaged order-service over its actual HTTP API (`http://localhost:8082`) against a slimmed Docker Compose stack (root `compose-component-test.yml`): MySQL, Zookeeper, Kafka, WireMock, and order-service itself all run as real containers. What's stubbed:

- **restaurant-service** — a static WireMock mapping (`src/componentTest/resources/wiremock/mappings/find-restaurant.json`) stands in for `GET /restaurants/{id}`, reached via Spring Cloud LoadBalancer's `SimpleDiscoveryClient` (Eureka disabled) under the `componenttest` Spring profile.
- **consumer-service, kitchen-service, delivery-service, accounting-service** — none run as containers; a single plain KafkaConsumer/KafkaProducer stub (`SagaParticipantStub`) watches all four saga command topics (`consumer.commands`/`kitchen.commands`/`delivery.commands`/`accounting.commands`) and replies on `saga.replies`, standing in for all four services' saga-participant roles at once.

Scope: Place Order (Create Order saga) only, orchestration mode only, JPA persistence mode only — 2 scenarios (`src/componentTest/resources/features/PlaceOrder.feature`), order authorized and order rejected due to expired credit card. Choreography mode, the Cancel/Revise Order sagas, other services' own component tests, and event-sourced persistence are deferred — see the "Deferred to a future sub-project" list in [`docs/superpowers/specs/2026-07-31-ch10-component-tests-design.md`](../docs/superpowers/specs/2026-07-31-ch10-component-tests-design.md) for the full design rationale. This task is kept out of the default `test`/`check` task graph (it's slow and requires Docker) — run it explicitly.
