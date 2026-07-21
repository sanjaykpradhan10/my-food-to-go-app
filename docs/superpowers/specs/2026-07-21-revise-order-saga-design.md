# Design: Revise Order saga (Ch.5/Ch.4 crossover, sub-project 3 of 3)

**Date**: 2026-07-21
**Status**: Approved

## Goal

Resolve `Order.REVISION_PENDING` — a state the `Order` DDD aggregate (PR #12) can already enter via `POST /orders/{id}/revise`, but nothing yet moves it out of. This sub-project wires a real cross-service saga so a revision request actually re-checks kitchen capacity for the new quantity and re-authorizes accounting for it, in both saga modes (choreography and orchestration), matching this project's existing dual-mode-switchable pattern.

**Sequenced dependency on PR #12**: uses `Order.revise()`/`confirmRevision()`/`rejectRevision()` (already merged), `OrderRevision`/`OrderLineItem` (already merged), and `OrderDomainEventPublisher` (already merged, unchanged).

**Sequenced dependency on PR #13 (Cancel Order saga)**: reuses the existing generic `KitchenCommand(eventId, commandType, orderId, totalQuantity, sagaType)` / `AccountingCommand(eventId, commandType, orderId, totalQuantity, sagaType)` / `SagaReply(eventId, participant, eventType, orderId, reason, sagaType)` record shapes as-is — no field additions needed there, since `sagaType` already exists as a discriminator for exactly this kind of multi-saga sharing.

This is the last of the three `Order` sub-projects (`Order` aggregate → Cancel Order saga → Revise Order saga).

## Scope decisions made during brainstorming

1. **Sequential, kitchen-gates-accounting — same shape as Cancel Order.** Kitchen re-checks capacity for the revised total quantity first; accounting only re-checks its authorization threshold if kitchen confirms. If kitchen rejects (over capacity), `Order.rejectRevision()` fires immediately and accounting is never contacted.
2. **Kitchen applies the revised quantity provisionally, before accounting is asked — this introduces a genuinely new compensation case Cancel Order never needed.** Cancel Order's `Authorization.reverse()` is unconditional (no threshold check), so nothing downstream of accounting ever needed undoing there. Revise Order's re-authorization is a real guarded threshold check (`AUTHORIZATION_QUANTITY_LIMIT = 10`), so accounting *can* decline it — but only after kitchen has already committed the new quantity to `Ticket`. So: kitchen confirms + applies revised quantity → accounting declines → a compensating command carrying the original quantity is sent back to kitchen, which reverts via a new guarded `Ticket.undoRevision(originalTotalQuantity)` method, and only once that undo is confirmed does `Order.rejectRevision()` fire.
3. **Both `Ticket` and `Authorization` gain a persisted `totalQuantity` field.** Today, `Ticket` never stores its quantity (only ever passed transiently into `TicketCreatedEvent` at creation) and `Authorization` doesn't store quantity at all (the creation-time threshold check happens transiently in `SagaJoinService`, nothing persisted). Revision needs a stored "current committed quantity" on both, so a later revise/undo has something concrete to check against and revert to.
4. **`Order` computes both `originalTotalQuantity` and `newTotalQuantity` at `revise()` time and both travel through the saga.** `Order.revise()` already has the pre-revision line items (untouched until `confirmRevision()` replaces them) and the proposed revision, so it's the natural place to compute both values once. Both commands/events carry both quantities — the "original" value is what a compensating undo reverts to; no aggregate needs to remember its own prior value.
5. **No new join-instance table for orchestration mode**, same as Cancel Order (scope decision 3 there) — this is a strict linear pipeline, not a parallel join. `ReviseOrderSagaOrchestrator` is stateless, driven purely by incoming `saga.replies`, using `Order`'s own status as the implicit saga state.
6. **Kitchen's revision-legal states mirror `cancel()`'s existing restriction exactly** — legal from `CREATE_PENDING`/`AWAITING_ACCEPTANCE`/`ACCEPTED`/`PREPARING`, illegal from `READY_FOR_PICKUP`/`PICKED_UP`/`CANCELLED` (same `TicketCannotBeCancelledException`-style boundary: once a ticket is ready or picked up, its quantity can no longer change). `undoRevision()` has no such restriction — reverting to a previously-valid quantity is always legal, since it's undoing a change this same saga just made.

## Choreography flow

```
Order.revise(revision) -> REVISION_PENDING, publishes OrderRevisionProposedEvent
  (orderId, originalTotalQuantity, newTotalQuantity) to order.events (already done, PR #12 — only the eventType/payload need a new listener case)
  kitchen-service's OrderEventListener (extended eventType switch): reacts to "OrderRevisionProposed"
    -> TicketService.handleOrderRevisionProposed(eventId, orderId, newTotalQuantity, originalTotalQuantity)
       -> isWithinCapacity(newTotalQuantity)?
          yes -> ticket.reviseQuantity(newTotalQuantity) -> TicketQuantityRevisedEvent
                 -> kitchen.events ("TicketQuantityRevised", carries originalTotalQuantity for downstream compensation)
          no  -> TicketRevisionRejectedEvent -> kitchen.events ("TicketRevisionRejected") — Ticket unchanged
  accounting-service's KitchenEventListener (extended RELEVANT_EVENT_TYPES): reacts to "TicketQuantityRevised" only
    -> AuthorizationReviseService.reviseForChoreography(eventId, orderId, newTotalQuantity, originalTotalQuantity)
       -> isAuthorized(newTotalQuantity)?
          yes -> authorization.reviseAuthorization(newTotalQuantity) -> AuthorizationRevisedEvent
                 -> accounting.events ("AuthorizationRevised")
          no  -> AuthorizationRevisionRejectedEvent -> accounting.events ("AuthorizationRevisionRejected", carries originalTotalQuantity) — Authorization unchanged
  order-service:
    KitchenEventListener (extended): "TicketRevisionRejected" -> OrderReviseSagaService.rejectRevision(orderId, eventId) -> Order.rejectRevision()
    AccountingEventListener (extended):
      "AuthorizationRevised"         -> OrderReviseSagaService.confirmRevision(orderId, eventId) -> Order.confirmRevision()
      "AuthorizationRevisionRejected" -> OrderReviseSagaService.compensateRevision(orderId, eventId, originalTotalQuantity)
                                          -> publishes OrderRevisionRejectedEvent to order.events (new eventType, carries originalTotalQuantity)
                                          -- Order stays REVISION_PENDING until kitchen confirms the undo (see below)
  kitchen-service's OrderEventListener: reacts to "OrderRevisionRejected"
    -> TicketService.handleOrderRevisionRejected(eventId, orderId, originalTotalQuantity)
       -> ticket.undoRevision(originalTotalQuantity) -> TicketRevisionUndoneEvent -> kitchen.events ("TicketRevisionUndone")
  order-service's KitchenEventListener (extended): "TicketRevisionUndone" -> OrderReviseSagaService.finalizeRejectedRevision(orderId, eventId) -> Order.rejectRevision()
```

`"TicketQuantityRevised"` is never order-service's own signal to finish — only `"AuthorizationRevised"` is (accounting confirming re-authorization is the last real step on the happy path). The compensation path is the one place order-service reacts twice for a single revision (once to trigger kitchen's undo, once to actually reject the order after the undo is confirmed) — a deliberate choice so `Order.rejectRevision()` only fires once every participant's state is fully consistent, matching this project's existing "don't finalize until the compensating action is confirmed" convention (see `CancelOrderSagaOrchestrator`'s `undoCancel` handling).

## Orchestration flow

New `ReviseOrderSagaOrchestrator` (stateless, no persisted instance — scope decision 5):

```
OrderController.revise() -> Order.revise(revision) -> REVISION_PENDING
  -> ReviseOrderSagaOrchestrator.start(order, originalTotalQuantity, newTotalQuantity):
     sends KitchenCommand(commandType="ReviseTicket", orderId, totalQuantity=newTotalQuantity, sagaType="ReviseOrder") to kitchen.commands
  kitchen-service's KitchenCommandListener (extended commandType switch): "ReviseTicket"
     -> TicketService.handleReviseTicketCommand(eventId, orderId, newTotalQuantity)
        -> isWithinCapacity(newTotalQuantity)?
           yes -> ticket.reviseQuantity(newTotalQuantity) -> replies "TicketQuantityRevised" (sagaType=ReviseOrder) on saga.replies
           no  -> replies "TicketRevisionRejected" (sagaType=ReviseOrder) on saga.replies
  orchestrator.handleReply(sagaType=ReviseOrder, "kitchen", ...):
     TicketQuantityRevised   -> sends AccountingCommand(commandType="ReviseAuthorization", orderId, totalQuantity=newTotalQuantity, sagaType="ReviseOrder") to accounting.commands
     TicketRevisionRejected  -> Order.rejectRevision(), saga done (no accounting step)
  accounting-service's AccountingCommandListener (extended commandType switch): "ReviseAuthorization"
     -> AuthorizationReviseService.reviseForCommand(eventId, orderId, newTotalQuantity, sagaType)
        -> isAuthorized(newTotalQuantity)?
           yes -> authorization.reviseAuthorization(newTotalQuantity) -> replies "AuthorizationRevised" (sagaType=ReviseOrder) on saga.replies
           no  -> replies "AuthorizationRevisionRejected" (sagaType=ReviseOrder) on saga.replies — Authorization unchanged
  orchestrator.handleReply(sagaType=ReviseOrder, "accounting", ...):
     AuthorizationRevised          -> Order.confirmRevision(), saga done
     AuthorizationRevisionRejected -> sends KitchenCommand(commandType="UndoReviseTicket", orderId, totalQuantity=originalTotalQuantity, sagaType="ReviseOrder") to kitchen.commands
                                       -- Order stays REVISION_PENDING until kitchen confirms the undo
  kitchen-service's KitchenCommandListener: "UndoReviseTicket"
     -> TicketService.handleUndoReviseTicketCommand(eventId, orderId, originalTotalQuantity)
        -> ticket.undoRevision(originalTotalQuantity) -> replies "TicketRevisionUndone" (sagaType=ReviseOrder) on saga.replies
  orchestrator.handleReply(sagaType=ReviseOrder, "kitchen", "TicketRevisionUndone") -> Order.rejectRevision()
```

Being stateless, the orchestrator can't carry `originalTotalQuantity` in memory across the round trip to accounting, and neither `AccountingCommand` nor `SagaReply` has a field free to carry it back. Instead, if compensation is needed, the orchestrator recomputes `originalTotalQuantity` by loading `Order` fresh (`orderRepository.findById(orderId)`) and reading its still-untouched pre-revision `lineItems` (unchanged until `confirmRevision()` replaces them) — no new field on any wire record, and the orchestrator stays genuinely stateless.

## Reply/command routing

No new fields needed on `KitchenCommand`, `AccountingCommand`, or `SagaReply` — `sagaType="ReviseOrder"` is simply a new value alongside the existing `"CreateOrder"`/`"CancelOrder"`.

- `order-service`'s `OrchestratorReplyListener` (shared `saga.replies` consumer) is extended to also inject `ReviseOrderSagaOrchestrator`, routing on `reply.sagaType()` alongside the existing two.
- `kitchen-service`'s `KitchenCommandListener` `commandType` switch gains `"ReviseTicket"` and `"UndoReviseTicket"` cases, alongside existing `"CreateTicket"`/`"ConfirmTicket"`/`"CancelTicket"`.
- `accounting-service`'s `AccountingCommandListener` `commandType` switch gains `"ReviseAuthorization"`, alongside existing `"AuthorizeCard"`/`"ReverseAuthorization"`.

## `Ticket` aggregate changes (kitchen-service)

- New persisted field `totalQuantity` (int), set at `createTicket()` (already computed there, just not stored today) and updated by `reviseQuantity()`/`undoRevision()`.
- New method `reviseQuantity(int newTotalQuantity)`: legal from `CREATE_PENDING`/`AWAITING_ACCEPTANCE`/`ACCEPTED`/`PREPARING` (same boundary as `cancel()` — scope decision 6); illegal from `READY_FOR_PICKUP`/`PICKED_UP` (`TicketCannotBeCancelledException`-style, reusing the existing exception type) and `CANCELLED` (`UnsupportedStateTransitionException`). Sets `totalQuantity`, returns `TicketQuantityRevisedEvent(orderId, newTotalQuantity)`.
- New method `undoRevision(int originalTotalQuantity)`: always legal regardless of state (scope decision 6) except `CANCELLED` (`UnsupportedStateTransitionException` — nothing to undo on a cancelled ticket, this shouldn't happen in practice since a revision can't have been in flight against an already-cancelled ticket, but guarded defensively). Sets `totalQuantity` back, returns `TicketRevisionUndoneEvent(orderId, originalTotalQuantity)`.
- Capacity checking (`isWithinCapacity`, reusing the existing `KITCHEN_CAPACITY_LIMIT = 20` constant) stays in `TicketService`, not the aggregate — same pattern as `createTicket()`, which also doesn't self-reject.
- `TicketService` gains `handleOrderRevisionProposed` (choreography), `handleReviseTicketCommand`/`handleUndoReviseTicketCommand` (orchestration commands), and `handleOrderRevisionRejected` (choreography compensation trigger) — all following the existing try/catch-then-reply/publish shape already used by `handleCancelTicketCommand`/`handleOrderCancelled`.
- `OrderEventListener` (kitchen-service) `eventType` switch gains `"OrderRevisionProposed"` and `"OrderRevisionRejected"`, alongside existing `"OrderCreated"`/`"OrderCancelled"`.

## `Authorization` aggregate changes (accounting-service)

- New persisted field `totalQuantity` (int), set by the static factories `authorize()`/`decline()` (already computed by the caller today, just not stored) and updated by `reviseAuthorization()`.
- New method `reviseAuthorization(int newTotalQuantity)`: legal only from `AUTHORIZED` (`UnsupportedStateTransitionException` otherwise — mirrors `reverse()`'s existing guard exactly). Sets `totalQuantity`, returns `AuthorizationRevisedEvent(orderId, newTotalQuantity)`.
- Threshold checking (`isAuthorized`, reusing the existing `AUTHORIZATION_QUANTITY_LIMIT = 10` constant, currently private to `SagaJoinService`) stays in the service layer — extracted into a small shared method (or reused directly if already a standalone method) so the new `AuthorizationReviseService` doesn't duplicate the threshold constant.
- New `AuthorizationReviseService`, mirroring `AuthorizationCancelService`'s existing choreography/command split exactly (the same split that fixed the orchestration-mode bug found during Cancel Order's Docker verification — see `docs/session-2026-07-21b.md`): `reviseForChoreography(eventId, orderId, newTotalQuantity, originalTotalQuantity)` publishes to `accounting.events`; `reviseForCommand(eventId, orderId, newTotalQuantity, sagaType)` publishes a `SagaReply` to `saga.replies`. Both check `isAuthorized(newTotalQuantity)` first and only call `authorization.reviseAuthorization()` if within threshold; on rejection, `Authorization` is left unchanged (no aggregate method call at all — matching `Ticket.reviseQuantity()`'s "service decides, aggregate doesn't self-reject" pattern).

## Order-service changes

- New `OrderReviseSagaService` (choreography), mirroring `OrderCancelSagaService`'s shape: `confirmRevision(orderId, eventId)` → `order.confirmRevision(revision)`; `rejectRevision(orderId, eventId)` → `order.rejectRevision()`; `compensateRevision(orderId, eventId, originalTotalQuantity)` → publishes `OrderRevisionRejectedEvent` (new event, carries `originalTotalQuantity`, triggers kitchen's undo — does **not** itself call `Order.rejectRevision()`, since `Order` must stay `REVISION_PENDING` until the undo is confirmed); `finalizeRejectedRevision(orderId, eventId)` → `order.rejectRevision()` (called only after kitchen confirms `"TicketRevisionUndone"`). All dedup via `ProcessedEventRepository`, same pattern as every other saga participant.

  `confirmRevision` needs the revised line items to call `Order.confirmRevision(revision)` — these aren't in the `AuthorizationRevised` reply (which only carries `newTotalQuantity`, a scalar). Resolution: `Order` itself already holds the proposed revision's line items in memory from the initial `revise()` call within the same request... but that's a *different* HTTP request/transaction than the one processing the async reply. So `Order` needs a persisted field to carry the pending revision's line items across the async round trip — a new `pendingRevisedLineItems` column on `Order`, set by `revise()` (already computed there) and cleared by `confirmRevision()`/`rejectRevision()`. This is a small necessary addition to the `Order` aggregate itself (not just wiring), flagged here since it wasn't anticipated in the original `Order` aggregate design (PR #12), which returned the revision in the same call rather than needing to persist it for a later async resume.
- `KitchenEventListener` (order-service) `eventType` switch gains `"TicketRevisionRejected"` → `rejectRevision`, `"TicketRevisionUndone"` → `finalizeRejectedRevision`.
- `AccountingEventListener` (order-service) `eventType` switch gains `"AuthorizationRevised"` → `confirmRevision`, `"AuthorizationRevisionRejected"` → `compensateRevision`.
- New `ReviseOrderSagaOrchestrator` (orchestration), stateless per scope decision 5, recomputing `originalTotalQuantity` from `Order`'s current (pre-revision) line items when compensation is needed, per the orchestration flow section above.
- New `OrderRevisionSagaTrigger` interface (mirroring `OrderCancellationSagaTrigger`), with `ChoreographyOrderRevisionSagaTrigger` (delegates to `OrderDomainEventPublisher.publish`, i.e. existing behavior moved behind the trigger for symmetry) and `OrchestrationOrderRevisionSagaTrigger` (calls `ReviseOrderSagaOrchestrator.start(order, originalTotalQuantity, newTotalQuantity)`).
- `OrderController.revise()` updated to call `revisionSagaTrigger.onOrderRevised(order, originalTotalQuantity, newTotalQuantity, events)` instead of publishing directly.

## Data model

- `tickets` gains a `total_quantity` column (int, not null — backfilled implicitly since existing rows only exist in dev/test data, no production migration concern for this learning project).
- `authorizations` gains a `total_quantity` column (int, not null, same backfill note).
- `orders` gains a `pending_revised_line_items` column (same `@ElementCollection`/embeddable mapping style already used for `Order.lineItems`, nullable — populated only while `REVISION_PENDING`).
- No new saga-instance tables (scope decision 5).

## Testing

TDD, per this project's established convention, across all 3 touched services (order/kitchen/accounting):
- `Ticket` aggregate: unit tests for `reviseQuantity`/`undoRevision`, every legal and illegal transition, including the `READY_FOR_PICKUP` boundary reused from `cancel()`.
- `Authorization` aggregate: unit tests for `reviseAuthorization`, legal (`AUTHORIZED`) and illegal (`DECLINED`/`REVERSED`) source states.
- `Order` aggregate: test that `revise()` persists `pendingRevisedLineItems` and both `confirmRevision()`/`rejectRevision()` clear it.
- `TicketService`/`AuthorizationReviseService`: capacity/threshold check both ways (within and over limit), dedup paths.
- `OrderReviseSagaService` and `ReviseOrderSagaOrchestrator`: full happy path; kitchen-rejects branch (asserting accounting is never contacted); kitchen-confirms-then-accounting-declines branch (asserting kitchen's undo is triggered and `Order` only reaches `APPROVED`/rejected state after the undo reply, not before).
- Updated listener tests (kitchen's `OrderEventListener`/`KitchenCommandListener`, accounting's `KitchenEventListener`/`AccountingCommandListener`, order-service's `KitchenEventListener`/`AccountingEventListener`/`OrchestratorReplyListener`) wherever a new `eventType`/`commandType`/`sagaType` case was added.
- Manual Docker e2e verification, both saga modes:
  1. Happy path: revise an order's line items to a still-within-limits quantity → `Order.APPROVED` with new line items, `Ticket.totalQuantity`/`Authorization.totalQuantity` both updated.
  2. Kitchen-rejects path: revise to a quantity exceeding `KITCHEN_CAPACITY_LIMIT` (20) → `Order` back to `APPROVED` with original line items, `Ticket`/`Authorization` both unchanged, accounting never contacted.
  3. Kitchen-confirms-then-accounting-declines path: revise to a quantity within kitchen capacity but exceeding `AUTHORIZATION_QUANTITY_LIMIT` (10) → `Ticket.totalQuantity` observed to first change then revert (or settle at original), `Authorization.totalQuantity` unchanged, `Order` back to `APPROVED` with original line items.

## Deferred (not in this pass)

- Any UI/notification layer — out of scope, this app has none.
- Concurrent revise+cancel races against the same order — not handled today for cancel either, consistent with existing scope boundaries.
