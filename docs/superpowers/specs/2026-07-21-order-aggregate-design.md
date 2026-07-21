# Design: Refactor `Order` into a DDD aggregate (Ch. 5, sub-project 1 of 3)

**Date**: 2026-07-21
**Status**: Approved

## Goal

Apply Ch. 5's Aggregate and Domain event patterns to order-service's `Order`, the book's other worked example for this chapter (already applied to kitchen-service's `Ticket` on 2026-07-20). Today `Order` is a near-anemic JPA entity: a `status` field with two unguarded mutators (`markApproved()`, `markRejected()`), each called from two separate ad hoc call sites (`OrderSagaService` for choreography, `CreateOrderSagaOrchestrator` for orchestration) that duplicate the same `if (status != APPROVAL_PENDING) return;` guard inline.

Two things change together, matching the decision made during brainstorming to pursue the book's full Fig. 5.14 state machine rather than only the create-order lifecycle that exists today:

1. **Pattern refactor** — `Order` becomes a real aggregate: paired guarded methods (`noteApproved()`/`noteRejected()`) replacing the unguarded mutators, returning `List<OrderDomainEvent>` instead of being called and separately, manually checked by every caller.
2. **New capability** — the full Fig. 5.14 state machine, including `cancel()`/`revise()`/`confirmRevision()`/etc. and the `CANCEL_PENDING`/`REVISION_PENDING`/`CANCELLED` states the book models but this app has never had. `POST /orders/{id}/cancel` and `POST /orders/{id}/revise` REST endpoints are added now, ahead of the sagas that will resolve those pending states.

**A known, deliberate, temporary gap**: no saga yet resolves `CANCEL_PENDING`/`REVISION_PENDING` back to a terminal state. Calling `/cancel` or `/revise` today will strand an order in a pending state until sub-project 2 (Cancel Order saga) and sub-project 3 (Revise Order saga) land — both are separate, already-scoped future sessions, sequenced deliberately after this one since they depend on the aggregate methods this spec introduces. This is a conscious choice (weighed against deferring REST entirely until the sagas exist), made to allow early manual testing of the aggregate's transition logic.

## Current state (what's being replaced)

- `Order` (`ftgo-order-service/.../domain/Order.java`): `@Id`, `consumerId`, `restaurantId`, `lineItems`, `status` (`OrderStatus`), two mutators (`markApproved()`, `markRejected()`) with **no guard** — either can be called from any status.
- `OrderStatus`: 3 values (`APPROVAL_PENDING`, `APPROVED`, `REJECTED`).
- `OrderSagaService` (choreography): `approve(orderId, eventId)`/`reject(orderId, eventId)`, each doing dedup-check → find `Order` → manual `if (status != APPROVAL_PENDING) return;` guard → `order.markApproved()`/`markRejected()` → save. No events published — these two methods produce no outbox rows at all today.
- `CreateOrderSagaOrchestrator` (orchestration): `handleAccountingReply()`/`fail()`/`rejectOrder()` duplicate the identical guard-then-mutate pattern inline, also producing no domain events for the approve/reject transitions (only the saga's own command/reply traffic is published).
- `ChoreographyOrderCreationSagaTrigger`: publishes `OrderCreatedEvent` (a dedicated, non-generic wire record) to `order.events` on creation — the only event type `order.events` carries today.
- There are **two** other consumers of `order.events` in the system, both with the identical bug and both now fixed with the same `eventType` guard: `ftgo-kitchen-service`'s `OrderEventListener` (`ftgo-kitchen-service/.../infrastructure/OrderEventListener.java`) and `ftgo-consumer-service`'s `OrderEventListener` (`ftgo-consumer-service/.../infrastructure/OrderEventListener.java`). Each deserialized **every** `order.events` message directly as `OrderCreatedEvent` and called its respective service (`ticketService.handleOrderCreated(event)` / `consumerVerificationService.handleOrderCreated(event)`) unconditionally — **no `eventType` check** — which a final whole-branch review caught as an active regression for consumer-service before this branch merged.

## State machine

New `OrderStatus` enum values (existing 3 plus 3 new):

```
APPROVAL_PENDING → APPROVED → CANCEL_PENDING → CANCELLED
       ↓              ↓              ↑ (undoCancel, revert)
   REJECTED     REVISION_PENDING ────┘
                       ↑ (confirmRevision / rejectRevision, revert)
```

- `APPROVAL_PENDING → APPROVED` via `noteApproved()`, returns `OrderApprovedEvent`.
- `APPROVAL_PENDING → REJECTED` via `noteRejected()`, returns `OrderRejectedEvent`.
- `APPROVED → CANCEL_PENDING` via `cancel()`, returns `OrderCancelledEvent` (a *proposal*, not a confirmation — named to match the book; sub-project 2's saga is what actually cancels the ticket/reverses payment). Illegal from any other state → `OrderCannotBeCancelledException` (mirrors `Ticket`'s two-tier exception split — this is the foreseeable "already resolved" business case, not a bug).
- `CANCEL_PENDING → CANCELLED` via `noteCancelled()`, returns `OrderCancelConfirmedEvent` — saga-internal, called by sub-project 2's future saga once kitchen/accounting confirm.
- `CANCEL_PENDING → APPROVED` via `undoCancel()`, returns `OrderCancelRejectedEvent` — saga-internal compensating path, called if sub-project 2's future saga can't complete the cancellation (e.g. kitchen already started preparing).
- `APPROVED → REVISION_PENDING` via `revise(OrderRevision)`, returns `OrderRevisionProposedEvent`. Illegal from any other state → `UnsupportedStateTransitionException`.
- `REVISION_PENDING → APPROVED` (with updated line items applied) via `confirmRevision(OrderRevision)`, returns `OrderRevisedEvent` — saga-internal, called by sub-project 3's future saga.
- `REVISION_PENDING → APPROVED` (unchanged, reverted) via `rejectRevision()`, returns `OrderRevisionRejectedEvent` — saga-internal compensating path.

`OrderRevision` is a small new value type (`List<OrderLineItem> revisedLineItems`) carried by `revise()`/`confirmRevision()` — no persistence of its own; it's a method parameter, not an entity.

## Domain events

One class per event, all implementing a new `OrderDomainEvent` marker interface (same pattern as `TicketDomainEvent`):

`OrderApprovedEvent`, `OrderRejectedEvent`, `OrderCancelledEvent`, `OrderCancelConfirmedEvent`, `OrderCancelRejectedEvent`, `OrderRevisionProposedEvent`, `OrderRevisedEvent`, `OrderRevisionRejectedEvent`.

`OrderCreatedEvent` is **not** converted to this pattern — `Order` creation stays exactly as it is today (`OrderService.createOrder()` constructs the entity directly and calls `orderCreationSagaTrigger.onOrderCreated(order, eventId)`); only the post-creation state-changing methods return `OrderDomainEvent`s. This matches `Ticket`'s precedent, where `Ticket.createTicket(...)` also stayed a static factory outside the `TicketDomainEvent` list.

## Publisher and wire format

New `OrderDomainEventPublisher` (mirroring `TicketDomainEventPublisher`), wrapping `ftgo-common`'s `OutboxEventRepository`, publishing to topic `order.events` (unchanged).

`order.events`'s wire format changes from a single dedicated `OrderCreatedEvent` record to a **generic flat record** (matching `KitchenEvent`'s shape — one record, `eventType` string discriminator, fields nullable depending on type) so the same topic can carry `OrderCreated` alongside the 8 new event types without a record-per-type wire schema. `OrderCreated`'s own fields (`consumerId`, `restaurantId`, `lineItems`) are preserved exactly as they serialize today — existing consumers of that specific event type see no change. `ChoreographyOrderCreationSagaTrigger` is updated to publish through `OrderDomainEventPublisher` too, so there's one single publishing path for everything order-service puts on `order.events`, instead of two.

**Required fix — `ftgo-kitchen-service`'s `OrderEventListener`**: today it deserializes every `order.events` payload as `OrderCreatedEvent` and calls `handleOrderCreated` with no `eventType` check. Once `order.events` carries other event types, this listener would misinterpret e.g. an `OrderApproved` message as a new order (with `consumerId`/`restaurantId`/`lineItems` all null) and create a bogus `Ticket`. Fixed by adding an `eventType` switch — the same discriminator-first pattern order-service's own `KitchenEventListener`/`AccountingEventListener` already use — so it only acts on `"OrderCreated"` and silently ignores everything else. This is a necessary correctness fix, not new behavior, and is in scope for this change since it's what makes publishing the new event types safe.

## Saga integration changes

- `OrderSagaService.approve()`/`reject()`: replace `if (status != APPROVAL_PENDING) return;` + `order.markApproved()`/`markRejected()` with a call to `order.noteApproved()`/`noteRejected()` wrapped in a try/catch for `UnsupportedStateTransitionException` (illegal transition — e.g. a duplicate/late event after the order already resolved) — caught and logged, preserving today's silent-ignore behavior for that case, since it's a legitimate replay/race outcome, not an error to surface. Returned events are published via `OrderDomainEventPublisher`.
- `CreateOrderSagaOrchestrator.handleAccountingReply()`/`fail()`/`rejectOrder()`: same replacement pattern, same exception handling, same publishing.
- Both remain exactly as dual-mode-split as they are today; `Order`'s methods have no awareness of saga mode, matching `Ticket`'s precedent.

## REST API (new)

`OrderController` gains two customer-initiated endpoints:

| Method | Path | Body | Aggregate call |
|---|---|---|---|
| POST | `/orders/{id}/cancel` | — | `order.cancel()` |
| POST | `/orders/{id}/revise` | `{ "lineItems": [...] }` | `order.revise(orderRevision)` |

Each handler: `OrderRepository.findById(id)` → throw `OrderNotFoundException` if absent → call the aggregate method → save → publish returned events via `OrderDomainEventPublisher`.

Error mapping (`@ExceptionHandler`s on `OrderController`, matching its existing style):
- `OrderNotFoundException` → 404
- `OrderCannotBeCancelledException`, `UnsupportedStateTransitionException` → 409

No REST endpoints for `noteCancelled()`, `undoCancel()`, `confirmRevision()`, `rejectRevision()` — these are saga-internal, to be called by sub-project 2/3's future Kafka listeners, not by users.

## Data model

No schema changes beyond the `OrderStatus` enum gaining 3 new values (`@Enumerated(EnumType.STRING)` on the existing `status` column, same as `Ticket`). No new columns for `OrderRevision` — revised line items are applied directly onto `Order.lineItems` by `confirmRevision()`, not stored separately.

## Testing

TDD, per this project's established convention:
- **`Order` unit tests**: every legal transition (state changes correctly, correct event(s) returned) and every illegal transition (correct exception type) for all 8 methods/transitions in the diagram above.
- **`OrderDomainEventPublisher` tests**: one per event type, asserting exact wire-format shape — including a regression test that `OrderCreated`'s serialized shape is byte-for-byte unchanged from today.
- **Updated `OrderServiceTest`, `OrderSagaServiceTest`, `CreateOrderSagaOrchestratorTest`**: same behavioral coverage as today, adapted to the new aggregate-method call sites and exception handling.
- **New `OrderControllerTest`** (`@WebMvcTest`): happy path for both new endpoints, 404 (unknown order), 409 (illegal transition, e.g. `/cancel` on an `APPROVAL_PENDING` order).
- **`ftgo-kitchen-service`**: updated `OrderEventListenerTest` (or new test if none exists) confirming non-`OrderCreated` event types on `order.events` are ignored, not misfired into `handleOrderCreated`.
- **Manual e2e verification via Docker**, matching this project's established pattern:
  1. Choreography happy path — confirm `Order` still reaches `APPROVED` via the saga.
  2. Orchestration happy path — same, confirming `CreateOrderSagaOrchestrator`'s translation still produces identical end states.
  3. One compensation case (e.g. consumer-verification failure) — confirms `noteRejected()`'s new invariant doesn't break the existing compensation path.
  4. Redelivery/idempotency spot-check.
  5. Manual `/cancel` and `/revise` calls — confirm the order transitions to `CANCEL_PENDING`/`REVISION_PENDING` and the corresponding proposal event lands on `order.events`, and confirm kitchen-service does **not** create a spurious ticket in response (validates the `OrderEventListener` fix).

## Deferred (not in this pass)

- **Cancel Order saga** (sub-project 2) — wiring `cancel()`'s proposal event to a real cross-service saga (kitchen ticket cancellation, accounting refund/reversal) in both choreography and orchestration modes, resolving `CANCEL_PENDING` to `CANCELLED`/`APPROVED`.
- **Revise Order saga** (sub-project 3) — wiring `revise()`'s proposal event to a real cross-service saga (kitchen capacity re-check, accounting re-authorization delta) in both modes, resolving `REVISION_PENDING`.
- Any UI/notification for the "stuck in pending" gap noted in Goal — out of scope; this app has no consumer-facing UI at all.
