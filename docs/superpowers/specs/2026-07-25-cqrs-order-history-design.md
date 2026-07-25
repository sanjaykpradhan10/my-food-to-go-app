# Design: CQRS read model — `ftgo-order-history-service` (Ch.7, sub-project 3 of 3)

**Date**: 2026-07-25
**Status**: Approved

## Goal

Implement the book's CQRS pattern (Ch.7) as a dedicated, event-driven read-side service: `ftgo-order-history-service` maintains a denormalized `order_views` table kept in sync purely by consuming Kafka events from `order-service`/`kitchen-service`/`accounting-service`/`delivery-service`, and exposes `GET /order-views/{orderId}` — a single-query read with no synchronous fan-out at request time. This is the last of Ch.7's three sub-projects; sub-project 1 (Delivery aggregate + saga participation, PR #16) and sub-project 2 (API composition, `GET /orders/{id}/view`, PR #17) are both merged.

**Deliberately coexists with, does not replace, sub-project 2's API-composition endpoint.** Ch.7's whole teaching point is contrasting the two query patterns side by side — `order-service`'s `GET /orders/{id}/view` (synchronous fan-out, always fresh, pays request-time latency/failure-coupling) stays untouched; this sub-project adds the CQRS alternative (eventually consistent, near-instant reads, no request-time coupling to 4 other services' availability).

## Scope decisions made during brainstorming

1. **A new, dedicated standalone service** (`ftgo-order-history-service`, port 8088 — next free port), not a module bolted onto `order-service`. A real CQRS split keeps the read side physically separate with its own failure/scaling characteristics, matching the book's actual pattern rather than blurring it with sub-project 2's approach.
2. **Restaurant data is deliberately omitted from the read model.** `restaurant-service` never publishes Kafka events (it's purely a REST-queried reference-data service today), so it structurally can't be kept in sync the way order/ticket/authorization/delivery can. The view carries `restaurantId` only, as an opaque foreign key — no synchronous REST fallback, no menu/name denormalization. This keeps the sub-project purely event-driven, which is the actual point of building it; a caller needing the restaurant name makes a separate call to `restaurant-service` directly, same as any CQRS read model's caller would in the book's own treatment.
3. **Status columns are derived via a direct `eventType` → status string mapping**, not by reimplementing each aggregate's guarded state machine. The read side trusts the write-side aggregates already enforced correctness; it just mirrors outcomes. Reimplementing state machines here would duplicate business rules on the read side, which fights CQRS's own philosophy (the read side should be a thin projection).
4. **Every Kafka listener upserts (find-or-create) by `orderId`,** not just the `OrderCreated` handler. Kafka gives no ordering guarantee across different topics, so a ticket/authorization/delivery event could arrive before this service has processed the order's own `OrderCreated` event. Whichever event arrives first creates the row with just the fields that event provides; later events (including a later-arriving `OrderCreated`) fill in the rest. This avoids a class of silent, permanent data loss that a "only `OrderCreated` creates the row, everything else no-ops if missing" design would have, without needing a separate race-tracking table (the row itself is the accumulator).
5. **No Eureka registration for `ftgo-order-history-service`.** Nothing in this codebase calls it synchronously via service discovery — clients (a human, a future gateway) hit its REST API directly, the same way `restaurant-service` was originally queried before Ch.3 introduced discovery for order-service's specific need to find it dynamically. No analogous need exists here.
6. **Own MySQL schema, `ftgo_order_history`**, no shared database — matches every other service's database-per-service convention.

## Domain model / schema

```
order_views:
  order_id BIGINT PRIMARY KEY
  consumer_id BIGINT
  restaurant_id BIGINT
  order_status VARCHAR
  ticket_status VARCHAR NULL
  authorization_status VARCHAR NULL
  delivery_status VARCHAR NULL
  courier_id BIGINT NULL

order_view_line_items:   (@ElementCollection, mirrors Order's own line-item storage)
  order_id BIGINT (FK)
  menu_item_id BIGINT
  quantity INT
```

`OrderView` entity (JPA), fields as above, no guarded transitions — plain getters/setters, since the read side doesn't enforce business rules (decision 3). `OrderViewRepository extends JpaRepository<OrderView, Long>` — `order_id` is the primary key, so `findById`/`save` (upsert via `save` on an entity constructed with the known ID, or `findById(...).orElseGet(...)`) cover every access pattern needed; no `findByOrderId` needed since the PK already is the order ID.

## Kafka listeners (choreography-only — no saga mode split, this service has no orchestration role)

Each listener has its own trimmed wire-record copy, per this codebase's established per-service convention (matches `KitchenCommand`/`DeliveryEvent` etc. already having independent per-consumer copies elsewhere):

**`OrderEventListener`** (`order.events`, wire record `OrderEvent(eventId, eventType, orderId, consumerId, restaurantId, lineItems)`):
- `"OrderCreated"` → upsert full row: `consumerId`, `restaurantId`, `lineItems`, `orderStatus = "APPROVAL_PENDING"`.
- Every other `eventType` → upsert (find-or-create by `orderId`) and set `orderStatus` via direct mapping. Verified exhaustively against `OrderDomainEvent`'s real sealed-interface `permits` list (`OrderCreatedEvent, OrderApprovedEvent, OrderRejectedEvent, OrderCancelledEvent, OrderCancelConfirmedEvent, OrderCancelRejectedEvent, OrderRevisionProposedEvent, OrderRevisedEvent, OrderRevisionRejectedEvent`) plus the one wire-only pseudo-event `OrderDomainEventPublisher.publishRevisionCompensationRequested` produces outside that interface: `OrderApproved→APPROVED`, `OrderRejected→REJECTED`, `OrderCancelled→CANCEL_PENDING`, `OrderCancelConfirmed→CANCELLED`, `OrderCancelRejected→APPROVED`, `OrderRevisionProposed→REVISION_PENDING`, `OrderRevised→APPROVED`, `OrderRevisionRejected→APPROVED`, `OrderRevisionCompensationRequested` → no `orderStatus` change (the order itself stays `REVISION_PENDING` at this point — this pseudo-event only signals kitchen to compensate, per the Ch.6 event-sourcing design's own documentation of it).

**`KitchenEventListener`** (`kitchen.events`, wire record `KitchenEvent(eventId, eventType, orderId, ticketId, totalQuantity, reason)`):
- Upsert `ticketStatus` via direct mapping. Verified exhaustively against `TicketDomainEvent`'s real sealed-interface `permits` list (`TicketCreatedEvent, TicketCreationFailedEvent, TicketConfirmedEvent, TicketCancelledEvent, TicketCancellationRejectedEvent, TicketAcceptedEvent, TicketPreparingStartedEvent, TicketReadyForPickupEvent, TicketPickedUpEvent, TicketQuantityRevisedEvent, TicketRevisionRejectedEvent, TicketRevisionUndoneEvent`): `TicketCreated→CREATE_PENDING`, `TicketConfirmed→AWAITING_ACCEPTANCE`, `TicketAccepted→ACCEPTED`, `TicketPreparingStarted→PREPARING`, `TicketReadyForPickup→READY_FOR_PICKUP`, `TicketPickedUp→PICKED_UP`, `TicketCancelled→CANCELLED`. `TicketCreationFailed` (no ticket was ever created — no `ticketStatus` write, since decision 3's whole point is mirroring what actually happened, and nothing did), `TicketCancellationRejected`/`TicketRevisionRejected`/`TicketRevisionUndone`/`TicketQuantityRevised` → no `ticketStatus` change (none of these represent a new lifecycle state on their own — `TicketQuantityRevised` changes the ticket's quantity, not its status, and this read model doesn't track quantity at all per its scope).

**`AccountingEventListener`** (`accounting.events`, wire record `AccountingEvent(eventId, eventType, orderId, reason)`):
- Upsert `authorizationStatus` via direct mapping: `CardAuthorized→AUTHORIZED`, `CardAuthorizationFailed→DECLINED`, `AuthorizationReversed→REVERSED`, `AuthorizationRevised→AUTHORIZED` (still authorized, just at a new quantity — the read model doesn't track quantity separately), `AuthorizationRevisionRejected` → no status change (decline of a revision attempt, not a new authorization state).

**`DeliveryEventListener`** (`delivery.events`, wire record `DeliveryEvent(eventId, eventType, orderId, deliveryId, courierId, reason)`):
- Upsert `deliveryStatus` + `courierId` via direct mapping: `DeliveryScheduled→SCHEDULED` (+ set `courierId`), `DeliveryPickedUp→PICKED_UP`, `DeliveryDelivered→DELIVERED`, `DeliveryCancelled→CANCELLED` (+ clear `courierId` to null, since the courier is no longer assigned). `DeliverySchedulingFailed` → no status change (nothing was ever scheduled).

All four listeners dedup via `ftgo-common`'s `ProcessedEventRepository` (`existsById`/`save`), matching every other consumer in this codebase.

## Query API

`OrderHistoryController`: `GET /order-views/{orderId}` → `OrderViewResponse` (mirrors the entity's fields, including line items) or `404` if the order has never been seen by this service.

## Infra

- `build.gradle`: depends on `ftgo-common` only (no Eureka client, no Resilience4j, no `spring-cloud-starter-loadbalancer` — this service makes no outbound synchronous calls to anything).
- `application.yml`: standard `spring.datasource`/`spring.jpa`/`spring.kafka` block, `server.port: 8088`. No `saga.mode` property (irrelevant — no saga participation).
- `compose.yml`: new `order-history-service` block, `depends_on: mysql (healthy), kafka (started)` — no `service-registry` dependency (decision 5).
- `infrastructure/mysql/init.sql`: add `CREATE DATABASE IF NOT EXISTS ftgo_order_history;` + matching `GRANT` line, mirroring every other service's entry.
- `settings.gradle`: add `include 'ftgo-order-history-service'`.
- `Dockerfile`: standard 2-stage build, mirrors every other service's Dockerfile exactly (port 8088).

## Testing

TDD, per this project's established convention:
- `OrderView` entity: no dedicated unit tests needed beyond what JPA repository tests exercise (it's a plain data holder, no guarded behavior — consistent with decision 3).
- `OrderViewService`: unit tests per listener-handler method, covering the create case (`OrderCreated`/first-seen-for-any-topic) and the update-existing case, for each of the 4 event sources. Explicitly test the upsert race: an update-type event (e.g. `TicketCreated`) arriving for an `orderId` with no existing row creates a stub row with only `ticketStatus` set, `orderStatus` etc. left at their JPA defaults (null/unset) until `OrderCreated` arrives later.
- 4 listener tests (one per Kafka topic), mirroring this codebase's established listener-test shape (mocked service, malformed-payload handling, dedup).
- `OrderHistoryController`: `@WebMvcTest`, found (200) and not-found (404) cases.
- Manual Docker e2e: place a real order, poll `GET /order-views/{id}` across the order's real lifecycle (immediately after creation, mid-saga, full happy-path completion, after a decline/compensation) and verify the denormalized view matches each write-side aggregate's real state at that moment — cross-check directly against `GET /orders/{id}/view` (sub-project 2's endpoint) for the same order at the same moment, confirming both patterns agree once the read model has caught up (allow a couple seconds for Kafka consumption lag). Also verify the cross-topic race case for real: is there a way to force one, e.g. by observing consumer-group lag/ordering under load, or is this only practically testable via the unit test in the prior bullet — decide during planning; if not practically forceable in a Docker e2e pass, the unit test is the primary evidence for this scope decision and that's acceptable.

## Docs

Per-change, landing in the same PR:
- New `ftgo-order-history-service/README.md` — full treatment (Role/API/Events consumed/Domain model/Idempotency/Running standalone), matching the depth of `ftgo-delivery-service/README.md`.
- `docs/ARCHITECTURE.md` — new "CQRS" subsection alongside the existing "API composition" section, contrasting the two patterns directly (latency/freshness/failure-coupling tradeoffs), Kafka topic catalog table gains `ftgo-order-history-service` as a 4th consumer of `order.events`/`kitchen.events`/`accounting.events`/`delivery.events` (add columns/rows as needed to the existing catalog format).
- `CONTEXT.md` — new services-table row for `ftgo-order-history-service`, session log entry, `[x] CQRS (Ch.7)` checked off in the Querying section of the patterns reference. **This is the point where Ch.7 as a whole flips to Done** (both API composition and CQRS sub-projects complete) — per this project's `CLAUDE.md`, this triggers the full documentation sweep rule: `docs/ARCHITECTURE.md` and every touched service's `README.md` need a full pass, not just the per-change updates listed above. Scope that sweep during planning once the core feature is built, per the established convention from prior chapter completions.

## Deferred (not in this pass)

- Any query beyond single-order-by-id (no listing, filtering, pagination, or consumer-scoped "my orders" query) — out of scope, not this sub-project's teaching point.
- Restaurant name/menu denormalization (decision 2).
- Any caching, read replicas, or eventual-consistency SLA tracking.
