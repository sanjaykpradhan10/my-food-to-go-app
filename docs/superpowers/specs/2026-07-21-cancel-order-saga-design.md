# Design: Cancel Order saga (Ch.5/Ch.4 crossover, sub-project 2 of 3)

**Date**: 2026-07-21
**Status**: Approved

## Goal

Resolve `Order.CANCEL_PENDING` — a state the `Order` DDD aggregate (PR #12) can already enter via `POST /orders/{id}/cancel`, but nothing yet moves it out of. This sub-project wires a real cross-service saga so a cancel request actually cancels the kitchen ticket and reverses the accounting authorization, in both saga modes (choreography and orchestration), matching this project's existing dual-mode-switchable pattern for the Create Order saga.

**Sequenced dependency on PR #12**: uses `Order.cancel()` (already merged), `Order.noteCancelled()`/`undoCancel()` (already merged, previously saga-internal and unused), and `OrderDomainEventPublisher` (already merged, unchanged).

**Deferred to sub-project 3**: the Revise Order saga, a separate future session.

## Scope decisions made during brainstorming

1. **Sequential, not parallel.** `Order.cancel()` is only legal from `APPROVED`, meaning by the time a cancel is requested the `Ticket` has already been confirmed and may be anywhere from `AWAITING_ACCEPTANCE` through `PICKED_UP` — `Ticket.cancel()` may legitimately reject (`TicketCannotBeCancelledException` from `READY_FOR_PICKUP`, `UnsupportedStateTransitionException` from `PREPARING`/`PICKED_UP`). The saga asks kitchen first; accounting's authorization reversal is only triggered if kitchen confirms cancellable. If kitchen rejects, `Order.undoCancel()` fires immediately and accounting is never contacted — no compensating "re-authorize" step is ever needed, since accounting never touched anything it would need to un-reverse.
2. **`Authorization` (accounting-service) becomes a DDD aggregate too**, going beyond Ch.5's two book-named worked examples (`Ticket`, `Order`). Chosen so "reverse the authorization" has a real guarded transition (`AUTHORIZED → REVERSED`) rather than an unguarded status string mutation, consistent with how this saga's other two participants (`Order`, `Ticket`) already work.
3. **No new join-instance table for orchestration mode.** Unlike Create Order saga (which persists `CreateOrderSagaInstance` to join two *parallel* replies), Cancel Order saga is a strict linear pipeline — kitchen replies, then conditionally accounting replies. `CancelOrderSagaOrchestrator` is stateless, driven purely by incoming `saga.replies`, using `Order`'s own status as the implicit saga state.
4. **`SagaReply` gains a `sagaType` field** (`"CreateOrder"` / `"CancelOrder"`), so `order-service`'s shared `OrchestratorReplyListener` can route a reply to the correct orchestrator now that two saga types share the same `orderId` keyspace. Each participant's handler method supplies the literal at its `publishReply(...)` call site — no change needed to inbound command records, since a handler already knows which saga it's for.

## Required fix bundled into this sub-project (not new capability)

`ftgo-kitchen-service`'s `TicketService.handleCancelTicketCommand` currently does `ticket.cancel(); ticketRepository.save(ticket);` with **no exception handling and no reply published** — a gap already flagged (non-blocking) in the `Ticket` aggregate PR review, since today's only caller (Create Order saga's compensation path) never inspects the outcome. This sub-project's orchestration mode needs a real reply to know whether cancellation succeeded, so the fix (try/catch around `ticket.cancel()`, publish `"TicketCancelled"` or `"TicketCancellationRejected"` on `saga.replies`) is now required, not optional.

## Choreography flow

```
Order.cancel() -> CANCEL_PENDING, publishes OrderCancelledEvent to order.events (already done, PR #12)
  kitchen-service's OrderEventListener (extended eventType switch): reacts to "OrderCancelled"
    -> TicketService.handleOrderCancelled(eventId, orderId)
       -> ticket.cancel()
          success  -> TicketCancelledEvent -> kitchen.events ("TicketCancelled")
          rejected -> TicketCancellationRejectedEvent -> kitchen.events ("TicketCancellationRejected")
  accounting-service's KitchenEventListener (extended RELEVANT_EVENT_TYPES): reacts to "TicketCancelled" only
    -> AuthorizationCancelService.reverse(eventId, orderId)
       -> authorization.reverse() -> AuthorizationReversedEvent -> accounting.events ("AuthorizationReversed")
  order-service:
    KitchenEventListener (extended): "TicketCancellationRejected" -> OrderCancelSagaService.rejectCancel(orderId, eventId) -> Order.undoCancel()
    AccountingEventListener (extended): "AuthorizationReversed" -> OrderCancelSagaService.confirmCancel(orderId, eventId) -> Order.noteCancelled()
```

`"TicketCancelled"` is never order-service's own signal to finish — only `"AuthorizationReversed"` is, since accounting confirming the reversal is the last real step. `"TicketCancelled"` is consumed by accounting-service, not order-service.

## Orchestration flow

New `CancelOrderSagaOrchestrator` (separate class from `CreateOrderSagaOrchestrator`, no persisted instance — see scope decision 3):

```
OrderController.cancel() -> Order.cancel() -> CANCEL_PENDING
  -> CancelOrderSagaOrchestrator.start(order): sends "CancelTicket" command to kitchen.commands
  kitchen-service's KitchenCommandListener (existing "CancelTicket" case, unchanged dispatch):
     -> TicketService.handleCancelTicketCommand (fixed per above)
     -> replies "TicketCancelled" (sagaType=CancelOrder) or "TicketCancellationRejected" on saga.replies
  orchestrator.handleReply(sagaType=CancelOrder, "kitchen", ...):
     TicketCancelled            -> sends "ReverseAuthorization" command to accounting.commands
     TicketCancellationRejected -> Order.undoCancel(), saga done (no accounting step)
  accounting-service: AuthorizeCardCommandListener (extended commandType switch) handles "ReverseAuthorization"
     -> replies "AuthorizationReversed" (sagaType=CancelOrder) on saga.replies
  orchestrator.handleReply(sagaType=CancelOrder, "accounting", "AuthorizationReversed") -> Order.noteCancelled()
```

## Reply routing (`SagaReply`)

`SagaReply(eventId, participant, eventType, orderId, reason)` gains a 6th field, `sagaType`, in all 4 services' independent copies of the record (order/kitchen/consumer/accounting). Every existing `publishReply(...)` call site across all 4 services is updated to pass `sagaType="CreateOrder"` (the only saga type today); the two new Cancel Order reply sites (`handleCancelTicketCommand`, the new `ReverseAuthorization` handler) pass `sagaType="CancelOrder"`.

`order-service`'s `OrchestratorReplyListener` (the shared `saga.replies` consumer) is updated to inject both `CreateOrderSagaOrchestrator` and `CancelOrderSagaOrchestrator`, switching on `reply.sagaType()` to route to the correct one. This is a required, behavior-preserving change to `CreateOrderSagaOrchestratorTest`'s call sites (the extra field) but no logic change to the Create Order saga itself.

## `Authorization` aggregate (accounting-service)

New `AuthorizationStatus` enum: `AUTHORIZED, DECLINED, REVERSED`.

`Authorization` gains:
- Static factories `Authorization.authorize(orderId)` / `Authorization.decline(orderId, reason)`, returning `(Authorization, List<AuthorizationDomainEvent>)` — replacing `SagaJoinService`'s/`AuthorizeCardCommandListener`'s direct `new Authorization(orderId, "AUTHORIZED")`/`"DECLINED"` construction. Events: `CardAuthorizedEvent`, `CardAuthorizationDeclinedEvent` (wire types unchanged: `"CardAuthorized"`, `"CardAuthorizationFailed"` — matching the existing `accounting.events` vocabulary exactly, only the in-memory representation changes).
- Instance method `reverse()`: legal only from `AUTHORIZED`, transitions to `REVERSED`, returns `AuthorizationReversedEvent`. Illegal from `DECLINED`/`REVERSED` → `UnsupportedStateTransitionException` (new exception type for accounting-service, matching the `Ticket`/`Order` naming precedent).

New `AuthorizationDomainEventPublisher`, same shape as `TicketDomainEventPublisher`/`OrderDomainEventPublisher`, publishing to `accounting.events` (unchanged topic).

`SagaJoinService.tryResolve`/`handleAuthorizeCardCommand` updated to call the new factories instead of constructing `Authorization` directly, and to publish through the new publisher instead of hand-building `AccountingEvent` inline.

## Kitchen-service changes

- `TicketDomainEvent` gains 2 new implementations: `TicketCancelledEvent` already exists (reused, no change) — only `TicketCancellationRejectedEvent` is new (`orderId`, no other fields).
- `TicketService.handleCancelTicketCommand`: fixed per the "Required fix" section above — try/catch around `ticket.cancel()`, publishes the correct saga reply.
- `TicketService.handleOrderCancelled(eventId, orderId)`: new method, same try/catch shape as the fixed command handler, publishes via `TicketDomainEventPublisher` to `kitchen.events` instead of a saga reply.
- `OrderEventListener` (kitchen-service): `eventType` switch gains `"OrderCancelled"` → `handleOrderCancelled`, alongside the existing `"OrderCreated"` case.

## Order-service changes

- New `OrderCancelSagaService` (choreography), mirroring `OrderSagaService`'s shape exactly: `confirmCancel(orderId, eventId)` → `order.noteCancelled()`; `rejectCancel(orderId, eventId)` → `order.undoCancel()`. Both dedup via `ProcessedEventRepository`, catch `UnsupportedStateTransitionException` (silent-ignore, same pattern as `OrderSagaService`), publish returned events via the existing `OrderDomainEventPublisher` (no changes needed there — it already handles all 8 `OrderDomainEvent` types from PR #12).
- `KitchenEventListener` (order-service): `eventType` switch gains `"TicketCancellationRejected"` → `OrderCancelSagaService.rejectCancel`.
- `AccountingEventListener` (order-service): `eventType` switch gains `"AuthorizationReversed"` → `OrderCancelSagaService.confirmCancel`.
- New `CancelOrderSagaOrchestrator` (orchestration), stateless per scope decision 3: `start(order)` sends `"CancelTicket"`; `handleReply(sagaType, participant, eventType, orderId, reason)` branches per the orchestration flow above, calling `orderRepository.findById` + the `Order` aggregate directly (no instance table).
- New `OrderCancellationSagaTrigger` interface (mirroring the existing `OrderCreationSagaTrigger`), with `ChoreographyOrderCancellationSagaTrigger` (delegates to `OrderDomainEventPublisher.publish`, i.e. the existing behavior — `OrderController.cancel()` already publishes today, this just moves it behind the trigger interface for symmetry and mode-awareness) and `OrchestrationOrderCancellationSagaTrigger` (calls `CancelOrderSagaOrchestrator.start(order)`).
- `OrderController.cancel()` updated to call the trigger instead of publishing directly.

## Data model

No schema changes. `authorizations.status` keeps its column name and existing string values (`AUTHORIZED`, `DECLINED`); `@Enumerated(EnumType.STRING)` adds a new legal value (`REVERSED`) on top, same pattern as `Ticket`'s/`Order`'s prior refactors. No new tables (scope decision 3 — no `CancelOrderSagaInstance`).

## Testing

TDD, per this project's established convention, across all 4 touched services:
- `Authorization` aggregate: unit tests for `authorize`/`decline`/`reverse`, every legal and illegal transition.
- `AuthorizationDomainEventPublisher`: one test per event type.
- `TicketService`: updated tests for the fixed `handleCancelTicketCommand` (success reply, rejection reply, both dedup paths) and new `handleOrderCancelled`.
- `OrderCancelSagaService` and `CancelOrderSagaOrchestrator`: full happy path plus the "kitchen rejects" branch, asserting accounting is never contacted (`verifyNoInteractions`-style) in that branch.
- Updated listener tests (kitchen's `OrderEventListener`, accounting's `KitchenEventListener`, order-service's `KitchenEventListener`/`AccountingEventListener`/`OrchestratorReplyListener`) wherever a new `eventType`/`sagaType` case was added.
- Manual Docker e2e verification, both saga modes: cancel an order early (ticket still `AWAITING_ACCEPTANCE`) → full success, `Authorization` row flips to `REVERSED`; cancel an order after driving its ticket to `READY_FOR_PICKUP` via the existing restaurant-worker REST API → rejection path, `Order` back to `APPROVED`, `Authorization` row confirmed untouched (still `AUTHORIZED`).

## Deferred (not in this pass)

- **Revise Order saga** (sub-project 3 of 3) — the book's trickiest saga, a separate future session.
- Any UI/notification layer — out of scope, this app has none.
