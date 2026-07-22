# ftgo-order-service

**Port:** 8082
**Bounded context:** Order lifecycle / saga coordination

## Role

Owns the `Order` aggregate — the entry point for placing an order and the service whose status field records the outcome of every saga this order participates in: `APPROVAL_PENDING → APPROVED`/`REJECTED` (Create Order), `APPROVED ⇄ CANCEL_PENDING ⇄ CANCELLED` (Cancel Order), and `APPROVED ⇄ REVISION_PENDING` (Revise Order). It plays two different roles depending on `SAGA_MODE`: in **choreography** it's just one more participant reacting to events published by the other services; in **orchestration** it's the coordinator for all three sagas, driving kitchen-service and accounting-service via explicit commands and tracking each saga's progress. Both implementations live in the codebase simultaneously, gated by Spring's `@ConditionalOnProperty`.

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

The order is always created in `APPROVAL_PENDING` — the Create Order saga (either style) transitions it asynchronously afterward.

**`POST /orders/{id}/cancel`**

No request body. Legal only from `APPROVED` — moves the order to `CANCEL_PENDING` and triggers the Cancel Order saga (either style).

| Condition | Status |
|---|---|
| Order not found | `404` |
| Order not in `APPROVED` | `409` |

**`POST /orders/{id}/revise`**

Request:
```json
{"lineItems": [{"menuItemId": 1, "quantity": 5}]}
```

Legal only from `APPROVED` — moves the order to `REVISION_PENDING`, records the proposed line items (`pendingRevisedLineItems`), and triggers the Revise Order saga (either style). The order's *current* `lineItems` are unchanged in the response — they only update once the saga confirms the revision.

| Condition | Status |
|---|---|
| Order not found | `404` |
| Order not in `APPROVED` | `409` |

## Restaurant service integration

`RestaurantServiceProxy` calls restaurant-service via a `@LoadBalanced RestClient` (base URL `http://ftgo-restaurant-service`, resolved dynamically through Eureka), wrapped in a Resilience4j circuit breaker (`restaurantService` instance): sliding window 5, failure-rate threshold 50%, 5s wait-duration-in-open-state, 3 permitted calls in half-open. `RestaurantNotFoundException` is excluded from the failure count (a 404 isn't a service health signal).

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
| `OrderRevisionCompensationRequested` | Revise Order saga's compensation trigger only — **not** a real `Order` state transition (status stays `REVISION_PENDING`); carries the original, untouched line items so kitchen knows what to revert to |

### Publishes (orchestration mode)

| Topic | Command/reply | When |
|---|---|---|
| `consumer.commands` | `VerifyConsumerCommand` | Create Order saga start |
| `kitchen.commands` | `KitchenCommand{commandType=CreateTicket\|ConfirmTicket\|CancelTicket\|ReviseTicket\|UndoReviseTicket}` | Depending on which saga/step |
| `accounting.commands` | `AccountingCommand{commandType=AuthorizeCard\|ReverseAuthorization\|ReviseAuthorization}` | Depending on which saga/step |

Every command/reply carries a `sagaType` (`CreateOrder`/`CancelOrder`/`ReviseOrder`) so the three sagas can safely share these topics — see the root [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md#multi-saga-routing-sagatype).

### Consumes

| Mode | Topic | Reacts to |
|---|---|---|
| Choreography | `consumer.events` | `ConsumerVerificationFailed` → reject (Create Order) |
| Choreography | `kitchen.events` | `TicketConfirmed`/`TicketCreationFailed` (Create Order); `TicketCancellationRejected` (Cancel Order); `TicketRevisionRejected`/`TicketRevisionUndone` (Revise Order) |
| Choreography | `accounting.events` | `CardAuthorizationFailed` (Create Order); `AuthorizationReversed` (Cancel Order); `AuthorizationRevised`/`AuthorizationRevisionRejected` (Revise Order) |
| Orchestration | `saga.replies` | All participants' replies for all 3 sagas, routed by `sagaType` then dispatched to the matching orchestrator |

## Saga participants

- **Create Order** — choreography: `OrderSagaService` (three thin listeners, approve/reject guarded on `APPROVAL_PENDING`). Orchestration: `CreateOrderSagaOrchestrator` + persisted `CreateOrderSagaInstance` (parallel join, `@Version` optimistic locking).
- **Cancel Order** — choreography: `OrderCancelSagaService`. Orchestration: stateless `CancelOrderSagaOrchestrator`. Sequential, kitchen-gates-accounting — `Ticket.cancel()` can legitimately fail (ticket already `READY_FOR_PICKUP`+), in which case `Order.undoCancel()` fires immediately and accounting is never contacted.
- **Revise Order** — choreography: `OrderReviseSagaService`. Orchestration: stateless `ReviseOrderSagaOrchestrator`. Same kitchen-gates-accounting shape as Cancel Order, but kitchen *provisionally applies* the revised quantity before accounting is asked (since re-authorization is a real threshold check accounting can decline, unlike Cancel Order's unconditional reversal) — `compensateRevision`/`sendUndoReviseTicket` trigger kitchen to revert if accounting declines, and `Order` stays `REVISION_PENDING` until that reversion is confirmed.

Each saga's trigger from the REST layer is a small `OrderXSagaTrigger` interface (`OrderCancellationSagaTrigger`, `OrderRevisionSagaTrigger`) with a choreography impl (publishes domain events directly) and an orchestration impl (calls the orchestrator's `start()`), selected by `@ConditionalOnProperty(saga.mode=...)` — `OrderController` depends only on the interface, never on which mode is active.

See [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) for the full side-by-side sequence diagrams (all three sagas, both styles, happy path + every compensation case).

## Domain model

- `Order` — `id`, `consumerId`, `restaurantId`, `lineItems`, `status` (`OrderStatus`), `pendingRevisedLineItems` (only populated between `revise()` and `confirmRevision()`/`rejectRevision()`).
- `OrderStatus`: `APPROVAL_PENDING`, `APPROVED`, `REJECTED`, `CANCEL_PENDING`, `CANCELLED`, `REVISION_PENDING`.
- 8 guarded state-changing methods (`noteApproved`/`noteRejected`/`cancel`/`noteCancelled`/`undoCancel`/`revise`/`confirmRevision`/`rejectRevision`), each returning a `List<OrderDomainEvent>` (class-per-event, sealed interface) rather than being hand-built inline by callers.
- `CreateOrderSagaInstance` (orchestration mode only) — `orderId` PK, `consumerVerified`/`ticketCreated`/`failed` flags, `totalQuantity`, `@Version`. Cancel Order and Revise Order have no equivalent table (both orchestrators are stateless).

## Idempotency & reliability

Every Kafka-driven state change (all 3 sagas, both modes) is guarded by the `processed_events` dedup ledger — insert-then-act in one local transaction, so at-least-once Kafka delivery can't double-process. All outbound events go through a transactional outbox (`OutboxEvent`, carrying a per-row `topic` column since this service fans out to up to 4 different topics depending on mode) written in the same transaction as the business change, published by a separate `@Scheduled` poller. `OutboxEvent`/`OutboxPublisher`/`KafkaProducerConfig` themselves live in the shared `ftgo-common` module (see the root `docs/ARCHITECTURE.md`), not this service's own source tree.

## Running standalone

```bash
./gradlew :ftgo-order-service:test
```

Runs against H2 in-memory (`MODE=MySQL`) — no Docker needed for unit tests. To run live, start the full stack (`docker compose up -d`) — this service needs MySQL, Kafka, service-registry, and restaurant-service to actually serve traffic.
