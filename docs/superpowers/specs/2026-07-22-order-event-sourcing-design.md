# Order Event Sourcing — Design Spec

**Date:** 2026-07-22
**Status:** Approved (brainstorming), pending implementation plan
**Chapter:** Ch. 6 — Developing business logic with event sourcing (Microservices Patterns, Chris Richardson, pp. 183–219)

## Context

Chapter 5 refactored `Order` (order-service), `Ticket` (kitchen-service), and `Authorization`
(accounting-service) into DDD aggregates whose state-changing methods return class-per-event
domain events, persisted via JPA. Chapter 6 builds on that by replacing an aggregate's
*persistence* mechanism with event sourcing: instead of storing current state as rows in an
`ORDER`/`ORDER_LINE_ITEM`-style table, the aggregate is persisted as the sequence of domain
events that produced its current state, and reconstructed by replaying them.

This project has hand-rolled every other pattern from the book (transactional outbox, sagas,
circuit breaker, service discovery, transaction log tailing) rather than adopting the book's own
reference framework (Eventuate Tram/Client/Local). This spec follows that same precedent: no new
external event-sourcing framework, built from the book's described mechanics using this
project's existing infrastructure (H2/MySQL, the Ch.3 Debezium CDC pipeline).

## Scope

Convert **`Order`** (order-service) — the book's own worked example, and the most fully-built
aggregate in this codebase — to event sourcing, covering its full state machine: create, cancel,
and revise (all three existing sagas, both saga modes). `Ticket` and `Authorization` are
explicitly out of scope for this pass; the JPA-based versions of those aggregates are unaffected.

The event-sourced `Order` runs as a **switchable mode**, not a replacement:

```
PERSISTENCE_MODE=jpa            # default — current behavior, completely unchanged
PERSISTENCE_MODE=event-sourcing # new path described in this spec
```

This mirrors the precedent already set by `SAGA_MODE` (choreography/orchestration) and
`OUTBOX_PUBLISH_MODE` (polling/CDC) — switchable modes for side-by-side pattern comparison, which
is valuable in a study project even though a real migration would only go one direction.

`PERSISTENCE_MODE` is a service-instance-level setting, not a per-request one. An `Order` created
under `jpa` mode has no `order_events` rows and is invisible to the event-sourcing path, and vice
versa — there is no live migration tooling between modes, and none is planned.

## Architecture

### New event-sourcing path (package `com.sanjay.ftgo.orderservice.eventsourcing`)

- **`OrderAggregate`** — a plain (non-JPA) class holding the same fields as today's `Order`
  entity, restructured into the book's `process(Command) → List<OrderDomainEvent>` (pure,
  validates and decides, never mutates) / `apply(OrderDomainEvent)` (mutates state, never
  validates, cannot fail) split. One command class and one `process`/`apply` pair per existing
  state-changing method:

  | Existing `Order` method | New command | Emits |
  |---|---|---|
  | (factory) create | `CreateOrderCommand` | `OrderCreatedEvent` |
  | `noteApproved` | `ApproveOrderCommand` | `OrderApprovedEvent` |
  | `noteRejected` | `RejectOrderCommand` | `OrderRejectedEvent` |
  | `cancel` | `CancelOrderCommand` | `OrderCancelPendingEvent` |
  | `noteCancelled` | `NoteOrderCancelledCommand` | `OrderCancelledEvent` |
  | `undoCancel` | `UndoCancelCommand` | `OrderCancelUndoneEvent` |
  | `revise` | `ReviseOrderCommand` | `OrderRevisionProposedEvent` |
  | `confirmRevision` | `ConfirmRevisionCommand` | `OrderRevisedEvent` |
  | `rejectRevision` | `RejectRevisionCommand` | `OrderRevisionRejectedEvent` |

  (Exact event class names match whatever the existing `OrderDomainEvent` sealed hierarchy
  already uses — this table is illustrative, not a rename.)

  `OrderAggregate` **reuses the existing `OrderDomainEvent` sealed interface and its
  implementations unchanged** — same event types, same fields — since Ch.5's aggregate methods
  already return exactly this shape; event sourcing only requires also being able to fold them
  back into state via `apply()`, which today's JPA-based `Order` doesn't need to do.

- **`order_events`** table — `event_id`, `event_type`, `entity_id` (order ID), `event_data`
  (JSON), `triggering_event` (inbound message ID, for idempotency) — the book's `events` table
  (§6.2.1).
- **`order_aggregate_version`** table — `aggregate_id`, `version` — dedicated optimistic-locking
  table (the book's `entities` table), per the decision to use a dedicated table over deriving
  version from event count.
- **`order_snapshots`** table — `aggregate_id`, `version`, `snapshot_json` — included for
  completeness against the full chapter material, even though `Order`'s short per-instance
  lifecycle (a handful of transitions at most) doesn't strictly need it per the book's own
  guidance in §6.1.5. A configurable event-count threshold (e.g. every N events) triggers a
  snapshot write.
- **`OrderEventStore`** — hand-rolled equivalent of the book's `AggregateRepository`:
  - `save(command)` — new `OrderAggregate()`, `process()`, fold `apply()`, persist events +
    initial version row.
  - `find(orderId)` — load latest snapshot (if any) + events after it, replay via `apply()`.
  - `update(orderId, command)` — same load-and-replay as `find()`, then `process(command)`
    against the reconstructed state, fold new events via `apply()`, persist with a
    version-guarded conditional update (fails if `order_aggregate_version` moved since load —
    same observable conflict semantics as today's JPA `@Version`).

### Publishing

`order_events` inserts are picked up by the **existing Ch.3 Debezium/Kafka-Connect CDC
pipeline** (Outbox Event Router SMT), reconfigured to also tail `order_events` and republish rows
onto `order.events` in `event-sourcing` mode. Wire format is unchanged — still the `OrderEvent`
record kitchen-service and accounting-service already deserialize — so **no consumer-side changes
are needed in either service**. In `jpa` mode, publishing is entirely unchanged (existing
`outbox_events` table, existing `OutboxPublisher`/CDC per `OUTBOX_PUBLISH_MODE`).

### Sagas

- **Choreography** — no changes needed. Choreography participants only ever react to
  `order.events`/`kitchen.events`/`accounting.events`, which are unaffected by which persistence
  mode produced the `order.events` messages.
- **Orchestration** — per the decision to build the book's full pseudo-event mechanism (§6.3.3,
  §6.3.4) rather than take the same-transaction shortcut available since `order_events` and the
  saga orchestrator tables share one MySQL schema:
  - Each saga step's `update()` call, alongside real domain events, also appends a
    `SagaCommandRequestedEvent` pseudo event to `order_events` for each outbound command the
    orchestrator needs to send next (e.g., after `OrderCreatedEvent`, a pseudo event carrying the
    `VerifyConsumerCommand` payload and destination topic).
  - A new event handler subscribed to `SagaCommandRequestedEvent` sends the actual Kafka command
    message, reusing the pseudo event's own `event_id` as the outbound message ID so a redelivery
    is caught by the saga participant's existing dedup.
  - `CreateOrderSagaInstance`'s own state transitions (today a separate JPA row) fold through the
    same `process`/`apply`/pseudo-event cycle as `OrderAggregate` itself, rather than being
    persisted as an independent table update inside the same transaction.
  - The stateless Cancel/Revise orchestrators get the equivalent treatment for their own
    command-sending steps.
  - Saga replies arriving on `saga.replies` are consumed exactly as today (`SagaReply` records) —
    only the *sending* side changes.

## Data flow

**Write (event-sourcing mode):**
1. `OrderController` (endpoints unchanged) calls a mode-selected repository facade instead of
   `OrderRepository` directly.
2. `OrderEventStore.save(CreateOrderCommand)` / `.update(orderId, command)` as described above.
3. If the event count since the last snapshot crosses the configured threshold, `OrderEventStore`
   also writes a fresh `order_snapshots` row.
4. CDC tails the `order_events` insert(s) and republishes as `OrderEvent` on `order.events`.
5. (Orchestration mode only) the `SagaCommandRequestedEvent` handler sends the real command
   message to the saga participant's topic.

**Read:** `OrderEventStore.find(orderId)` — load snapshot (if any) + events since, replay via
`apply()` — used by the controller's reads and by saga listeners that need current state.

## Error handling & edge cases

- **Concurrent updates** — `OrderEventStore.update()`'s version-guarded write fails (equivalent
  to `OptimisticLockException`) if `order_aggregate_version` moved since load — same observable
  behavior as today's JPA `@Version` conflicts.
- **Idempotent processing (producer side)** — `order_events.triggering_event` records the inbound
  message ID that caused each write; a redelivered saga command/event is detected before any new
  events are appended (book §6.1.6, RDBMS-based approach).
- **Idempotent processing (consumer side)** — unaffected; kitchen/accounting-service's existing
  `processed_events` ledgers consume `order.events`, not `order_events`, directly.
- **Saga pseudo-event duplication** — at-least-once CDC delivery could redeliver a
  `SagaCommandRequestedEvent`; its handler reuses the pseudo event's `event_id` as the outbound
  message ID so the participant's existing dedup absorbs the duplicate (book §6.3.4).
- **Snapshot/replay consistency** — a snapshot is only ever read together with events *after* its
  recorded version, never instead of them; a snapshotting bug can at worst cause more replay than
  necessary, never a dropped event.
- **No cross-mode migration** — explicitly out of scope; see Scope section.

## Testing plan

- **Aggregate-level unit tests** — one test group per `process()`/`apply()` pair (9 pairs),
  matching the style of the existing ~19 `Ticket`/`Order` state-machine tests: `process()`
  returns the right events without mutating state; `apply()` produces the right state from a
  given event, independent of persistence.
- **`OrderEventStore` tests** — save/find/update round-trips against H2 (MODE=MySQL, matching
  this project's existing test setup), an explicit optimistic-lock-conflict test (two concurrent
  `update()` calls on the same aggregate), and a snapshot-write-then-replay test.
- **CDC/publishing** — verified manually via Docker (confirm `order.events` messages match wire
  format), consistent with how transaction log tailing was originally verified in Ch.3; no new
  automated test for the CDC path itself.
- **Saga regression via Docker e2e** — both saga modes × all three sagas (create, cancel,
  revise), each run with `PERSISTENCE_MODE=event-sourcing`, reusing the exact manual verification
  scenarios already used for the JPA version (happy path, rejection/compensation cases,
  redelivery/idempotency). This is the primary confidence check given how much saga-integration
  surface is touched.
- **Existing `jpa`-mode tests** — must stay green unchanged, proving the new mode is genuinely
  additive.

## Out of scope

- `Ticket` and `Authorization` event sourcing conversions.
- CQRS / read-side query implementation (Ch. 7).
- Live migration tooling between `PERSISTENCE_MODE` values.
- Event schema evolution / upcasting (book §6.1.7) — no event schema changes are introduced by
  this work, so there's nothing to upcast yet; revisit if a later chapter's changes require
  evolving an `OrderDomainEvent` shape.
- GDPR-style deletion/pseudonymization (book §6.1.9) — no regulatory requirement drives this
  study project.
