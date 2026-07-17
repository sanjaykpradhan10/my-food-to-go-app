# ftgo-order-service

**Port:** 8082
**Bounded context:** Order lifecycle / saga coordination

## Role

Owns the `Order` aggregate — the entry point for placing an order and the service whose status field (`APPROVAL_PENDING` → `APPROVED`/`REJECTED`) records the outcome of the whole Create Order saga. It plays two different roles depending on `SAGA_MODE`: in **choreography** it's just one more participant reacting to events published by the other three services; in **orchestration** it's the coordinator, driving the other three services via explicit commands and tracking progress itself. Both implementations live in the codebase simultaneously, gated by Spring's `@ConditionalOnProperty`.

It also validates every order against restaurant-service before creating it, via a synchronous REST call wrapped in a circuit breaker (the Ch.3 RPI pattern) — this part of the service is identical regardless of saga mode.

## API

**`POST /orders`**

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

The order is always created in `APPROVAL_PENDING` — the saga (either style) transitions it asynchronously afterward.

## Restaurant service integration

`RestaurantServiceProxy` calls restaurant-service via a `@LoadBalanced RestClient` (base URL `http://ftgo-restaurant-service`, resolved dynamically through Eureka), wrapped in a Resilience4j circuit breaker (`restaurantService` instance): sliding window 5, failure-rate threshold 50%, 5s wait-duration-in-open-state, 3 permitted calls in half-open. `RestaurantNotFoundException` is excluded from the failure count (a 404 isn't a service health signal).

## Events

### Publishes

| Mode | Topic | Event type(s) | When |
|---|---|---|---|
| Choreography | `order.events` | `OrderCreated` | On every order creation |
| Orchestration | `consumer.commands` | `VerifyConsumerCommand` | Saga start |
| Orchestration | `kitchen.commands` | `CreateTicket`, `ConfirmTicket`, `CancelTicket` | Saga start, on `CardAuthorized`, on any failure |
| Orchestration | `accounting.commands` | `AuthorizeCard` | Once both `ConsumerVerified` and `TicketCreated` replies are in |

### Consumes

| Mode | Topic | Reacts to | Ignores |
|---|---|---|---|
| Choreography | `consumer.events` | `ConsumerVerificationFailed` → reject | `ConsumerVerified` |
| Choreography | `kitchen.events` | `TicketConfirmed` → approve, `TicketCreationFailed` → reject | `TicketCreated`, `TicketCancelled` |
| Choreography | `accounting.events` | `CardAuthorizationFailed` → reject | `CardAuthorized` (waits for kitchen's confirmation instead) |
| Orchestration | `saga.replies` | All three participants' replies, dispatched by the `participant` field | — |

## Saga modes

This is the one service where the two saga styles look most different:

- **Choreography** (`OrderSagaService`): three thin `@KafkaListener`s each call `approve()`/`reject()` directly. The transition is guarded on current status (`APPROVAL_PENDING` only) plus a `processed_events` dedup check, so redelivery can't double-apply. Approval only happens on kitchen's `TicketConfirmed` — an indirect signal that accounting already authorized the card.
- **Orchestration** (`CreateOrderSagaOrchestrator` + `CreateOrderSagaInstance`): a single stateful coordinator persists saga progress (`consumerVerified`/`ticketCreated`/`failed` flags, with `@Version` optimistic locking since two Kafka consumer threads can race on the same order's row) and drives the flow via commands. It approves the order directly on `CardAuthorized`, without waiting for any downstream confirmation — since it already knows authorization succeeded.

See [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) for the full side-by-side sequence diagrams (happy path + all 3 compensation cases, both styles).

## Domain model

- `Order` / `OrderLineItem` / `OrderStatus` (`APPROVAL_PENDING`, `APPROVED`, `REJECTED`).
- `CreateOrderSagaInstance` (orchestration mode only) — `orderId` PK, `consumerVerified`/`ticketCreated`/`failed` flags, `totalQuantity`, `@Version`.

## Idempotency & reliability

Every Kafka-driven state change (both saga modes) is guarded by the `processed_events` dedup ledger — insert-then-act in one local transaction, so at-least-once Kafka delivery can't double-process. All outbound events go through a transactional outbox (`OutboxEvent`, now carrying a `topic` column per row since this service fans out to up to 4 different topics depending on mode) written in the same transaction as the business change, published by a separate `@Scheduled` poller.

## Running standalone

```bash
./gradlew :ftgo-order-service:test
```

Runs against H2 in-memory (`MODE=MySQL`) — no Docker needed for unit tests. To run live, start the full stack (`docker compose up -d`) — this service needs MySQL, Kafka, service-registry, and restaurant-service to actually serve traffic.
