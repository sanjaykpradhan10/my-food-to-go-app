# Design: Delivery aggregate + Create/Cancel Order saga participation (Ch.7 prerequisite, sub-project 1 of 3)

**Date**: 2026-07-24
**Status**: Approved

## Goal

Stand up `ftgo-delivery-service` as a real saga participant — today it is an empty stub with no domain model, no events, no saga involvement. This is prerequisite work for Ch.7 (implementing queries): the chapter's composite `findOrder`-style query is only a meaningful exercise once there is real delivery data to compose into the view. This sub-project is scoped to the delivery domain and its saga participation only — the query patterns themselves (API composition, CQRS) are sub-projects 2 and 3, future sessions.

## Scope decisions made during brainstorming

1. **Fuller delivery domain, not a placeholder.** `Delivery` becomes a real saga participant in the Create Order saga (parallel join, alongside consumer verification and ticket creation) and the Cancel Order saga (courier release), comparable in size to the Ch.5/6 `Ticket`/`Authorization` work — not just a hardcoded-always-succeeds stub. This is what makes "no courier available" a real, testable decline path, and keeps the domain consistent (no cancelled order left holding a scheduled courier).
2. **Courier matching stays simple.** A small fixed, seeded `Courier` pool (id, name, `available` boolean) — no geo/routing/ETA logic. `DeliveryService.schedule()` picks the first available courier; if none are free, it declines. This is enough to make the decline path real without pulling in matching-algorithm scope that isn't this project's point.
3. **No `Delivery` row on decline.** Matches kitchen-service's existing `TicketCreationFailed` precedent — capacity/availability failures never persist an entity, only a decline event. Nothing to compensate for that leg later.
4. **Create Order saga becomes a 3-way parallel join**, not sequential-after-authorization. `accounting-service`'s existing `SagaJoinState` (currently 2 legs: `consumerVerified`, `ticketCreated`) gains a 3rd leg, `deliveryScheduled`, and only authorizes once all three resolve. Maximizes parallelism and fits this project's existing saga philosophy (Create Order already runs consumer verification and ticket creation in parallel).
5. **Cancel Order saga gains a `delivery-release` step**, inserted between kitchen and accounting: kitchen confirms cancellable → release the courier → reverse the authorization. Release is unconditional once kitchen confirms cancellable (mirrors accounting's existing unconditional `reverse()` — no decline path for releasing a courier).
6. **Revise Order saga is untouched.** Delivery only cares about pickup location (`restaurantId`), which a quantity revision never changes.
7. **Infra pattern is identical to the other 4 saga services.** `delivery-service` depends on `ftgo-common` (outbox + `processed_events` dedup), owns its own MySQL schema, publishes to a new `delivery.events` topic (choreography) and consumes `delivery.commands` (orchestration), replies on the shared `saga.replies` topic like every other participant. Plain JPA persistence — event sourcing was a deliberate Ch.6 exercise scoped to `Order` only, not a pattern every aggregate needs.
8. **No REST read endpoint yet.** `POST /deliveries/{id}/picked-up` and `POST /deliveries/{id}/delivered` (courier-facing, mirrors `TicketController`) are the only endpoints this sub-project adds. `GET` composite views are sub-projects 2/3's job.

## Domain model (`ftgo-delivery-service`)

New `DeliveryStatus` enum: `PENDING, SCHEDULED, PICKED_UP, DELIVERED, CANCELLED`. `PENDING` exists only as a transient in-memory state during scheduling — a persisted `Delivery` row is only ever created already in `SCHEDULED` (decision 3), so `PENDING` never appears in the database. `CANCELLED` is legal only from `SCHEDULED` (mirrors `Ticket`'s cancellable-states rule — once `PICKED_UP`, delivery can no longer be cancelled).

`Delivery` fields: `id`, `orderId`, `restaurantId` (pickup location — the only pickup-relevant data available from the Create Order flow), `courierId`, `status`.

Guarded methods, each returning `List<DeliveryDomainEvent>` (sealed interface, same shape as `TicketDomainEvent`/`AuthorizationDomainEvent`):
- Static factory `Delivery.schedule(orderId, restaurantId, courierId)` → `SCHEDULED`, returns `DeliveryScheduledEvent`.
- `pickUp()`: legal only from `SCHEDULED` → `PICKED_UP`, returns `DeliveryPickedUpEvent`. Illegal elsewhere → `UnsupportedStateTransitionException` (existing exception-naming precedent from `Ticket`/`Order`/`Authorization`).
- `deliver()`: legal only from `PICKED_UP` → `DELIVERED`, returns `DeliveredEvent`.
- `cancel()`: legal only from `SCHEDULED` → `CANCELLED`, returns `DeliveryCancelledEvent`. Illegal from `PICKED_UP`/`DELIVERED` → `UnsupportedStateTransitionException` (no `DeliveryCannotBeCancelledException`-style distinction needed — unlike `Ticket.cancel()`, nothing here needs a *different* exception for a specific illegal-from state, since the saga only ever calls `cancel()` when it already knows the delivery was never picked up).

`Courier` entity (plain JPA, no state machine): `id`, `name`, `available` (boolean, default `true`). Seeded with 3 rows on startup (matches restaurant-service's seed-data precedent).

`DeliveryService` (domain service):
- `schedule(orderId, restaurantId)`: finds the first `Courier` with `available=true`; if found, flips it `false`, constructs `Delivery.schedule(...)`, saves, publishes `DeliveryScheduledEvent`. If none free, publishes `DeliverySchedulingFailedEvent` directly (no `Delivery` entity involved) with no wire-record change needed beyond the existing event/reply plumbing.
- `release(deliveryId)`: loads the `Delivery`, calls `cancel()`, saves, flips its `courierId`'s `Courier.available` back to `true`, publishes `DeliveryCancelledEvent`.

New `DeliveryDomainEventPublisher`, same shape as `TicketDomainEventPublisher`/`AuthorizationDomainEventPublisher`, publishing to `delivery.events`.

## Create Order saga — choreography

```
order-service: OrderCreatedEvent -> order.events ("OrderCreated")   (already exists, unchanged)
  delivery-service's OrderEventListener (new, same shape as kitchen's): reacts to "OrderCreated"
    -> DeliveryService.schedule(orderId, restaurantId)
       success -> DeliveryScheduledEvent      -> delivery.events ("DeliveryScheduled")
       no courier -> DeliverySchedulingFailedEvent -> delivery.events ("DeliverySchedulingFailed")
  accounting-service's listener(s) (extended): react to "ConsumerVerified" / "TicketCreated" / "DeliveryScheduled"
    -> SagaJoinState gains deliveryScheduled leg; SagaJoinService.tryResolve only authorizes once all 3 are true
    -> if any leg reports failure ("ConsumerVerificationFailed" / "TicketCreationFailed" / "DeliverySchedulingFailed"),
       decline immediately without waiting for the other legs, as today's 2-leg logic already does
  order-service (extended KitchenEventListener-equivalent, new DeliveryEventListener):
    "DeliverySchedulingFailed" -> compensate: cancel ticket if already created, Order.noteRejected()
```

`SagaJoinState` (accounting-service) gains a `deliveryScheduled` boolean column alongside the existing `consumerVerified`/`ticketCreated`, `@Version` optimistic locking unchanged. `tryResolve` extends its existing "both flags true" check to "all three flags true"; a failure on any leg still short-circuits to decline immediately (today's behavior for a 2-leg failure fast-path, generalized to 3).

**Compensation matrix** (whichever of {ticket, delivery} already succeeded gets compensated on any other leg's failure or on accounting's decline):

| Failing leg | Ticket compensation | Delivery compensation |
|---|---|---|
| Consumer inactive | Cancel ticket if created | Release delivery if scheduled |
| Kitchen capacity exceeded | (n/a — never created) | Release delivery if scheduled |
| No courier available | Cancel ticket if created | (n/a — never created) |
| Accounting declines | Cancel ticket (existing) | Release delivery (new) |

This generalizes the existing 2-leg matrix (today: kitchen failure and accounting-decline compensations only had ticket to worry about) — no new compensation *mechanism*, just one more leg width.

## Create Order saga — orchestration

`CreateOrderSagaOrchestrator` (order-service) sends a 3rd command in parallel with the existing two: `ScheduleDeliveryCommand`-equivalent (a new `DeliveryCommand(eventId, commandType, orderId, restaurantId, sagaType)` record, on `delivery.commands`, `commandType="ScheduleDelivery"`). `CreateOrderSagaInstance` gains a `deliveryScheduled` boolean column (or equivalent join tracking) alongside its existing 2-leg tracking, extended the same way as choreography's `SagaJoinState`.

`delivery-service`'s new `DeliveryCommandListener` handles `"ScheduleDelivery"` → `DeliveryService.schedule(...)` → replies `"DeliveryScheduled"` / `"DeliverySchedulingFailed"` on `saga.replies` (`sagaType="CreateOrder"`).

On any leg's failure reply, the orchestrator compensates whichever of {ticket, delivery} it already has confirmation for — same matrix as choreography, just triggered by `handleReply` branches instead of independent listeners. `DeliveryCommand` also carries `commandType="ReleaseDelivery"` for this compensation send (reusing the same wire record rather than inventing a parallel one, consistent with how `KitchenCommand` already carries both `"CreateTicket"` and `"CancelTicket"`).

## Cancel Order saga extension (both modes)

Extends the existing kitchen → accounting pipeline to kitchen → delivery-release → accounting:

```
(choreography)
  kitchen-service: "TicketCancelled" -> kitchen.events   (already exists, unchanged)
  delivery-service's KitchenEventListener (new): reacts to "TicketCancelled"
    -> DeliveryService.release(deliveryId-for-orderId) -> DeliveryCancelledEvent -> delivery.events ("DeliveryCancelled")
  accounting-service's DeliveryEventListener (new): reacts to "DeliveryCancelled"
    -> AuthorizationCancelService.reverse(...)   (already exists, trigger source changes from "TicketCancelled" to "DeliveryCancelled")

(orchestration)
  CancelOrderSagaOrchestrator.handleReply(sagaType=CancelOrder, "kitchen", "TicketCancelled")
    -> sends DeliveryCommand(commandType="ReleaseDelivery", sagaType="CancelOrder") to delivery.commands
  delivery-service's DeliveryCommandListener: "ReleaseDelivery" -> DeliveryService.release(...)
    -> replies "DeliveryCancelled" (sagaType=CancelOrder) on saga.replies
  orchestrator.handleReply(sagaType=CancelOrder, "delivery", "DeliveryCancelled")
    -> sends AccountingCommand("ReverseAuthorization", ...)   (existing next step, trigger source changes)
```

`Delivery` lookup by `orderId` (not `deliveryId`, which the kitchen-cancellation event doesn't carry) needs a `findByOrderId` query — the only new repository method this sub-project requires beyond the standard CRUD `JpaRepository`.

Accounting's reversal trigger moves from "kitchen confirmed cancellable" to "delivery confirmed released" — one hop further down the pipeline, but the same unconditional-reverse logic (`AuthorizationCancelService.reverseForChoreography`/`reverseForCommand`, already split by channel per the Revise Order saga's established precedent) is otherwise unchanged.

## REST API (`ftgo-delivery-service`)

`DeliveryController`, courier-facing, mirrors `TicketController`'s shape:
- `POST /deliveries/{id}/picked-up` → `Delivery.pickUp()`.
- `POST /deliveries/{id}/delivered` → `Delivery.deliver()`.

No `GET` endpoint in this sub-project (decision 8).

## Data model

New `delivery-service` schema: `deliveries` table (id, order_id, restaurant_id, courier_id, status), `couriers` table (id, name, available), plus the standard shared `outbox_events`/`processed_events` tables via `ftgo-common`.

`accounting-service`: `saga_join_state` gains a `delivery_scheduled` boolean column (default `false`), alongside existing `consumer_verified`/`ticket_created`.

`order-service` (orchestration mode only): `create_order_saga_instance` gains a `delivery_scheduled` boolean column, same pattern.

## Kafka topics

New: `delivery.events` (choreography, producer: delivery-service; consumers: accounting-service, order-service), `delivery.commands` (orchestration, producer: order-service; consumer: delivery-service). Both added to `docs/ARCHITECTURE.md`'s Kafka topic catalog and, if choreography mode also needs CDC support consistent with the other topics, the Debezium connector's `table.include.list` (delivery-service's own `outbox_events` table, matching the existing per-service pattern — no change to the connector's *mechanism*, just a new table to include if delivery-service enables CDC mode).

## Testing

TDD, per this project's established convention:
- `Delivery`/`Courier`: unit tests for every legal and illegal transition (`schedule`, `pickUp`, `deliver`, `cancel`), courier availability flip on schedule/release.
- `DeliveryDomainEventPublisher`: one test per event type.
- `DeliveryService`: schedule-success, schedule-no-courier-available, release — including the courier-availability side effects.
- `DeliveryController`: `@WebMvcTest` tests for both endpoints, legal and illegal-state 409 cases (matching `TicketController`'s existing test shape).
- `SagaJoinState`/`SagaJoinService` (accounting-service): updated tests covering all orderings of the now-3-leg join, and the fast-fail-on-any-leg-failure path.
- `CreateOrderSagaOrchestrator`/`CancelOrderSagaOrchestrator` (order-service): updated tests for the 3-way join and the new delivery-release step.
- New listener tests wherever a new `eventType`/`commandType`/`sagaType` case was added (delivery-service's `OrderEventListener`/`KitchenEventListener`/`DeliveryCommandListener`; accounting-service's join listeners and new `DeliveryEventListener`; order-service's new `DeliveryEventListener`).
- Manual Docker e2e verification, both saga modes: Create Order happy path (courier assigned, `available` flipped false, `Authorization` only authorized after all 3 legs), no-courier-available decline (ticket compensated, consumer never authorized), the 3 pre-existing decline paths re-verified with delivery now in the mix (consumer inactive, kitchen capacity exceeded, accounting declines — each now also releasing the courier if one was assigned), Cancel Order releasing the courier end-to-end (`available` flipped back true, `Authorization` reversed only after delivery confirms released).

## Docs

Per-change, landing in the same PR (not a chapter-completion sweep — Ch.7 as a whole isn't done until sub-projects 2–3 land too):
- `ftgo-delivery-service/README.md` — full rewrite from stub to real API/events/domain-model treatment, matching the depth of `ftgo-kitchen-service/README.md`.
- `ftgo-order-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-kitchen-service/README.md` — updated wherever the saga sequence changed (Create Order's 3-way join, Cancel Order's new delivery-release step).
- `docs/ARCHITECTURE.md` — Kafka topic catalog gains `delivery.events`/`delivery.commands`; Create Order and Cancel Order saga sequence-diagram sections updated to show the 3rd participant.
- `CONTEXT.md` — services table (`ftgo-delivery-service` row updated from stub), session log entry.

## Deferred (not in this pass)

- **Sub-project 2**: API composition — `GET /orders/{id}/view` composing Order/Ticket/Authorization/Restaurant/Delivery via synchronous REST calls.
- **Sub-project 3**: CQRS read model — a dedicated read-side service/table fed by Kafka events from all five services.
- Any courier-facing UI/notification layer, geo/routing/ETA logic, or delivery-time estimation — out of scope, not this book chapter's teaching point.
