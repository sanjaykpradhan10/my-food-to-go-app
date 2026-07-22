# Session — 2026-07-22 (Revise Order saga)

**Tool:** Claude Code
**Duration:** Full brainstorm → spec → plan → subagent-driven-development cycle (13 planned tasks, 11 dispatched — 2 absorbed into earlier tasks), a final whole-branch review, and a full manual Docker e2e verification pass (both saga modes) that caught and fixed a real pre-existing production bug
**Repo:** https://github.com/sanjaykpradhan10/my-food-to-go-app
**Branch:** `worktree-revise-order-saga`
**Spec:** `docs/superpowers/specs/2026-07-21-revise-order-saga-design.md`
**Plan:** `docs/superpowers/plans/2026-07-21-revise-order-saga.md`

Continuation of `docs/session-2026-07-21b.md` (the Cancel Order saga session, PR #13, merged the same day this session's design work started).

---

## What we did

### Scope: Revise Order saga, sub-project 3 of 3 — the last piece of Ch. 5

`Order.REVISION_PENDING` — reachable via `POST /orders/{id}/revise` since PR #12 — had nothing to resolve it. This session built the real cross-service saga, completing all three `Order` sub-projects from Ch. 5 (the aggregate refactor, Cancel Order saga, Revise Order saga).

**Key design decisions from brainstorming:**
1. **Sequential, kitchen-gates-accounting — same shape as Cancel Order.** Kitchen re-checks capacity for the revised quantity first; accounting only re-checks its authorization threshold if kitchen confirms.
2. **A genuinely new compensation case Cancel Order never needed.** Cancel Order's `Authorization.reverse()` is unconditional, so nothing downstream of accounting ever needed undoing. Revise Order's re-authorization is a real guarded threshold check accounting *can* decline — but only after kitchen has already provisionally applied the new quantity. So: kitchen confirms + applies revised quantity → accounting declines → a compensating command/event carrying the original quantity reverts kitchen's `Ticket` via a new `undoRevision()` method, and only then does `Order.rejectRevision()` fire.
3. **Both `Ticket` and `Authorization` gain a persisted `totalQuantity` field** — neither stored it before this session.
4. **`Order.confirmRevision()` changes from taking an `OrderRevision` parameter to reading a new persisted `pendingRevisedLineItems` field** — set by `revise()`, cleared by `confirmRevision()`/`rejectRevision()`, since the confirming reply arrives in a different transaction than the original `/revise` request.
5. **No new saga-instance table for orchestration mode**, same as Cancel Order — `ReviseOrderSagaOrchestrator` is stateless, recomputing both the pending revised quantity and the original quantity from `Order`'s own fields rather than threading them through the reply chain.

### A design correction made during spec review, before any code was written

The choreography compensation trigger was initially conflated with the real terminal `OrderRevisionRejectedEvent`. Fixed before planning began: the compensation trigger is a distinct wire event, `"OrderRevisionCompensationRequested"`, published directly (bypassing the `OrderDomainEvent` sealed interface, since `Order` doesn't transition here) rather than reusing the event that fires on the real terminal rejection — conflating them would have made kitchen try to undo a revision that was rejected outright, with nothing to undo.

### Subagent-driven-development execution

13 planned tasks, 11 dispatched implementer rounds. Twice, Java's sealed-interface exhaustiveness forced a later task's file into an earlier one's commit (adding new record types to a sealed interface breaks any other file's exhaustive `switch` over it, with no `default` branch to fall back on) — Task 4 (`TicketDomainEventPublisher` wiring) got absorbed into Task 3 (the `Ticket` aggregate change that added new event types), and Task 8 (`AuthorizationDomainEventPublisher` wiring) got absorbed into Task 7 (the `Authorization` aggregate change), both verified with full test coverage rather than left as an untested necessity.

Per-task review caught and fixed 3 real gaps:
- Missing test coverage for the necessarily-absorbed `TicketDomainEventPublisher` switch cases (Task 3).
- A saga-hang risk: `TicketService.handleUndoReviseTicketCommand` silently returned instead of replying when its quantity argument was null, which could leave an orchestrator waiting forever on a reply that never arrives — fixed to reply `TicketRevisionRejected` instead (Task 5).
- A missing exception-path test for `OrderReviseSagaService.confirmRevision`, closing a parity gap against its sibling `OrderCancelSagaService` (Task 11).

### The final whole-branch review caught a real runtime bug invisible to any task-scoped review

`Order.confirmRevision()` re-homed the managed `pendingRevisedLineItems` `@ElementCollection` directly into the `lineItems` field — a Hibernate "shared collection reference" hazard that only manifests when `Order` is loaded via the repository (every real saga-participant call path), never when constructed directly (every unit test's fixture). Fixed with a defensive copy, plus a new `@DataJpaTest` (`OrderRepositoryTest`) that actually persists an order through `revise()` → `confirmRevision()` to lock this down.

**Building that regression test surfaced a second, more severe bug, independently reproduced before fixing:** `Order.revise()` had the identical exposure with the *immutable* list `OrderController.revise()` actually constructs (`Stream.toList()`). This meant the real `POST /orders/{id}/revise` endpoint would throw `UnsupportedOperationException` on every single call once merged — confirmed via a standalone repro test before applying the same defensive-copy fix.

### Docker e2e verification caught a third, pre-existing bug no code review of any kind could have found

`ftgo-kitchen-service`'s `AccountingEventListener`/`TicketService.handleAccountingEvent`, written back when `accounting.events` only ever carried `"CardAuthorized"`/`"CardAuthorizationFailed"` (the Create Order saga's only two outcomes), treated *any other* message on that shared topic as an authorization failure and cancelled the ticket. This feature's new `"AuthorizationRevised"`/`"AuthorizationRevisionRejected"` events on the same topic triggered spurious ticket cancellation and authorization reversal on every successful choreography-mode revision — a real order revision would silently corrupt into `Ticket.CANCELLED`/`Authorization.REVERSED` instead of the correct revised state. Fixed by switching the handler to an explicit `eventType` match, ignoring everything it doesn't specifically handle; re-verified clean via Docker after the fix.

This is the same class of bug the Cancel Order session's own Docker verification caught (`AuthorizationCancelService.reverse()`'s wrong-channel bug) — a genuine cross-service interaction that mocked unit tests, per-task reviews, and even a careful whole-branch diff review cannot structurally see, since it depends on how a *pre-existing, untouched* listener reacts to a *new* message shape on a topic it already subscribed to.

### Docker e2e verification (full results)

All verified via `docker compose`, both saga modes, all 3 scenarios each, with the full event sequence traced through each service's outbox table (not just end-state spot checks):

1. **Happy path** (quantity 2 → 5, within both limits): `Order.APPROVED` with revised line items, `Ticket.AWAITING_ACCEPTANCE`/`totalQuantity=5`, `Authorization.AUTHORIZED`/`totalQuantity=5`.
2. **Kitchen-rejects** (5 → 25, exceeds `KITCHEN_CAPACITY_LIMIT=20`): `Order` back to `APPROVED` with the original quantity, `Ticket`/`Authorization` both unchanged, accounting never contacted.
3. **Kitchen-confirms-then-accounting-declines** (5 → 15, within kitchen's limit but exceeds `AUTHORIZATION_QUANTITY_LIMIT=10`): full compensation sequence confirmed via outbox — `TicketQuantityRevised` → `AuthorizationRevisionRejected` → `OrderRevisionCompensationRequested` (choreography) / `UndoReviseTicket` command (orchestration) → `TicketRevisionUndone` → `OrderRevisionRejected` — `Order`/`Ticket`/`Authorization` all correctly reverted to the pre-revision quantity.

Orchestration mode's compensation path confirmed routed via `kitchen.commands`/`accounting.commands`/`saga.replies` with `sagaType="ReviseOrder"`, distinct from choreography's domain-event topics — not a coincidental match to the choreography end state.

Redelivery/idempotency: kitchen-service restarted mid-flow, `processed_events`/`tickets` counts and `Ticket` state unchanged after restart.

---

## What's implemented now (full picture)

- **ftgo-order-service**: `Order.confirmRevision()`/`revise()` both defensively copy their line-item lists (JPA-safety fix). `OrderReviseSagaService` (choreography) and stateless `ReviseOrderSagaOrchestrator` (orchestration) resolve `Order.REVISION_PENDING`. `OrderController.revise()` routed through a new saga-mode-aware `OrderRevisionSagaTrigger`. `OrchestratorReplyListener` now routes 3-way (`CreateOrder`/`CancelOrder`/`ReviseOrder`) by `SagaReply.sagaType()`.
- **ftgo-kitchen-service**: `Ticket` gained a persisted `totalQuantity`, `reviseQuantity()`/`undoRevision()` guarded methods, and 3 new domain events. `TicketService` gained 4 new handlers for the choreography/orchestration/compensation paths. `AccountingEventListener`/`handleAccountingEvent` fixed to stop mis-cancelling tickets on any non-`CardAuthorized`/`CardAuthorizationFailed` message (a real pre-existing bug this session's new event types exposed).
- **ftgo-accounting-service**: `Authorization` gained a persisted `totalQuantity` and a guarded `reviseAuthorization()` method plus 2 new domain events. New `AuthorizationReviseService` mirrors `AuthorizationCancelService`'s choreography/command split.
- **Everything else** (`Ticket`'s/`Order`'s/`Authorization`'s other existing behavior, restaurant-service, service-registry, ftgo-common): unchanged this session.

## Ch. 5 status: all three `Order` sub-projects done

- [x] `Order` DDD aggregate (sub-project 1) — merged, PR #12.
- [x] Cancel Order saga (sub-project 2) — merged, PR #13.
- [x] **Revise Order saga (sub-project 3)** — this session.

Chapter 5 (Designing business logic in a microservice architecture) is now fully done. Next: Chapter 6, event sourcing.

---

## Project state at end of session

```
Git log (worktree-revise-order-saga, HEAD):
2e4aa96 fix(kitchen-service): stop mis-cancelling tickets on non-CardAuthorized accounting.events
33eff20 fix(order-service): defensively copy revised line items in Order.revise()
adf9df8 fix: prevent JPA shared-collection exception in Order.confirmRevision; cleanup
a8c3f04 feat(order-service): wire OrderController.revise() through a saga-mode-aware OrderRevisionSagaTrigger
d041eeb feat(order-service): add stateless ReviseOrderSagaOrchestrator and route saga.replies by sagaType
3fae7c5 test(order-service): cover OrderReviseSagaService.confirmRevision's already-resolved-order path
c3baa87 feat(order-service): add OrderReviseSagaService for choreography
09e3318 feat(accounting-service): dispatch revision events/commands to AuthorizationReviseService
3371e83 feat(accounting-service): add AuthorizationReviseService for choreography and orchestration
a2f50df feat(accounting-service): persist Authorization.totalQuantity, add reviseAuthorization
27d1820 feat(kitchen-service): dispatch revision events/commands to TicketService
c0cfca1 fix(kitchen-service): reply TicketRevisionRejected on null quantity in undo command; cover null-guard branches
3c56b94 feat(kitchen-service): add revision handlers for choreography, orchestration, and compensation
4222bca test(kitchen-service): cover TicketQuantityRevised/TicketRevisionRejected/TicketRevisionUndone wire mapping
d3bbc43 feat(kitchen-service): persist Ticket.totalQuantity, add reviseQuantity/undoRevision
e304b90 feat(order-service): add publishRevisionCompensationRequested
54be11f feat(order-service): persist pendingRevisedLineItems, change confirmRevision to no-arg
```

PR #14 (https://github.com/sanjaykpradhan10/my-food-to-go-app/pull/14) reviewed and merged to `main` via merge commit `b37d8b6`. Worktree left in place (not superpowers-created — harness-managed).

## Next actions

- [ ] Still-deferred from earlier sessions: consider a Spring Boot 4.x migration now that 3.5.x is permanently frozen (no more OSS patches). Not part of any recent session's scope.
- [ ] Chapter 6 (event sourcing) — not started, no session yet.
- [ ] Chapter 6 (event sourcing) — not started, no session yet.

---

## Resuming in a new session

### In Claude Code
Open the project and say:
> "Read CONTEXT.md. Let's start Chapter 6 — event sourcing."

### In Claude Chat
Paste `CONTEXT.md`, then say:
> "I'm working through Microservices Patterns. Chapter 5 (designing business logic) is fully done — all three `Order`/`Ticket`/`Authorization` DDD aggregates and all three sagas (Create Order, Cancel Order, Revise Order) implemented in both saga modes. Ready to move to Chapter 6, event sourcing."
