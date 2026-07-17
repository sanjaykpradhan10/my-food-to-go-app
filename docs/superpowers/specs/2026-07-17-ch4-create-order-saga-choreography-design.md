# Design: Create Order Saga — Choreography (Ch. 4)

**Date**: 2026-07-17
**Status**: Approved

## Goal

Implement the Create Order saga from Ch. 4 using the choreography style: no
central coordinator, each participating service reacts to events published
by the others and publishes its own in turn. The saga spans four services —
order, consumer, kitchen, accounting — and ends with `Order` in either
`APPROVED` or `REJECTED` state, demonstrating data consistency across
service boundaries without a distributed transaction.

Orchestration (a `CreateOrderSaga` coordinator driving the same participants
explicitly) is a deliberate follow-up exercise, not part of this pass — see
"Deferred" below.

## Scope decisions

- **New services**: `consumer-service` and `accounting-service` (currently
  empty Spring Boot stubs) get their first real code. `delivery-service`
  stays out of scope — the book's Create Order saga doesn't involve
  delivery.
- **Verification/authorization rules are mocked, not real**:
  consumer-service checks a seeded `consumers` table for an
  active/non-blacklisted consumer; accounting-service "authorizes" a card by
  a simple rule (e.g. order total under a fixed threshold succeeds). No real
  payment gateway or KYC integration — the point is the saga's coordination
  and compensation, not fraud/payment logic.
- **Event topology**: one Kafka topic per service (`order.events`,
  `consumer.events`, `kitchen.events`, `accounting.events`), each backed by
  that service's own outbox table, extending the Ch. 3 transactional outbox
  pattern uniformly to all four services (kitchen-service and order-service
  already partially follow this; consumer/accounting-service are built this
  way from the start).
- **Compensation**: full matrix — all three failure points (consumer
  verification, ticket creation, card authorization) get a real compensating
  transaction, not just the happy path.
- **Idempotency**: every consumer reuses Ch. 3's `processed_events` dedup
  ledger pattern (insert-then-act in one local transaction).

## Architecture

```
                         ┌──────────────────┐
                         │  order-service    │
                         │  POST /orders      │
                         │  Order:            │
                         │  APPROVAL_PENDING  │
                         └─────────┬─────────┘
                                   │ OrderCreated
                                   │ (order.events)
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
         ┌────────────────────┐         ┌────────────────────┐
         │  consumer-service   │         │  kitchen-service    │
         │  verify consumer    │         │  Ticket:             │
         └──────────┬──────────┘         │  CREATE_PENDING      │
                     │                    └──────────┬──────────┘
        ConsumerVerified /                TicketCreated /
        ConsumerVerificationFailed        TicketCreationFailed
        (consumer.events)                 (kitchen.events)
                     │                              │
                     └──────────────┬───────────────┘
                                    ▼
                         ┌────────────────────┐
                         │  accounting-service  │
                         │  joins on orderId:    │
                         │  waits for BOTH        │
                         │  events before acting  │
                         │  authorize card         │
                         └──────────┬─────────────┘
                                    │
                CardAuthorized / CardAuthorizationFailed
                          (accounting.events)
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                                ▼
         ┌────────────────────┐          ┌────────────────────┐
         │  kitchen-service     │          │  order-service       │
         │  Ticket:              │          │  Order:               │
         │  AWAITING_ACCEPTANCE  │          │  APPROVED             │
         │  (or CANCELLED on     │          │  (or REJECTED on any  │
         │   any upstream        │          │   upstream failure    │
         │   failure event)      │          │   event, direct)      │
         └────────────────────┘          └────────────────────┘
```

order-service and kitchen-service also each directly consume the three
failure events (`ConsumerVerificationFailed`, `TicketCreationFailed`,
`CardAuthorizationFailed`) so rejection/compensation doesn't depend on a
chain of intermediate compensations completing first — see "Compensation
flows."

## Event flow — happy path

1. **order-service**: `POST /orders` → `Order{status=APPROVAL_PENDING}` +
   `OrderCreated` outbox row → `order.events`.
2. **consumer-service**: consumes `OrderCreated` → looks up the consumer,
   applies the verification rule → `ConsumerVerified` → `consumer.events`.
3. **kitchen-service**: consumes `OrderCreated` (independently and in
   parallel with step 2) → `Ticket{status=CREATE_PENDING}` →
   `TicketCreated` → `kitchen.events`.
4. **accounting-service**: consumes `ConsumerVerified` and `TicketCreated`,
   keyed by `orderId`, via a local join table (`saga_join_state`) — records
   whichever of the two arrives first, fires the authorization only once
   both are present. On firing: applies the authorization rule →
   `CardAuthorized` → `accounting.events`.
5. **kitchen-service**: consumes `CardAuthorized` →
   `Ticket{status=AWAITING_ACCEPTANCE}` → `TicketConfirmed` →
   `kitchen.events`.
6. **order-service**: consumes `TicketConfirmed` →
   `Order{status=APPROVED}`.

## Compensation flows

Three failure points. order-service listens to all three failure event
types directly — the first one to arrive is sufficient to reject the order,
with no need to wait on a chain of other services' compensations first.

**A. Consumer verification fails** — consumer-service emits
`ConsumerVerificationFailed` instead of `ConsumerVerified`.
- accounting-service: abandons the join for that `orderId`; never
  authorizes.
- kitchen-service: also consumes this event. If it already created a
  `CREATE_PENDING` ticket, compensates by transitioning it to `CANCELLED`
  and emits `TicketCancelled`. If no ticket exists yet — kitchen-service
  hasn't processed `OrderCreated` yet, a race since steps 2 and 3 run in
  parallel — it records "this `orderId` already failed" in a local
  `saga_join_state` row, so that when `OrderCreated` does arrive the
  ticket is created directly in `CANCELLED` state instead of
  `CREATE_PENDING`.
- order-service: consumes it directly → `Order{status=REJECTED}`.

**B. Kitchen can't create the ticket** — kitchen-service emits
`TicketCreationFailed` instead of `TicketCreated` (e.g. restaurant not
accepting orders).
- accounting-service: abandons the join.
- order-service: consumes it directly → `Order{status=REJECTED}`.
- consumer-service: nothing to compensate — verification is read-only.

**C. Card authorization fails** — accounting-service emits
`CardAuthorizationFailed` instead of `CardAuthorized` (only reachable after
the join already succeeded, i.e. both consumer verification and ticket
creation already succeeded).
- kitchen-service: compensates its `CREATE_PENDING` ticket →
  `CANCELLED`, emits `TicketCancelled`.
- order-service: consumes it directly → `Order{status=REJECTED}`.

**Idempotent rejection**: order-service guards the `REJECTED` transition on
current status (`APPROVAL_PENDING` → `REJECTED` only) — a defensive check,
not something the design above should trigger twice, but cheap insurance
against redelivery races.

## Data model (additions)

### consumer-service (new)

```sql
CREATE TABLE consumers (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  active     BOOLEAN NOT NULL DEFAULT TRUE   -- seed a mix of true/false
);

CREATE TABLE outbox_events ( -- same shape as order-service's, Ch.3 pattern
  id           BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id     VARCHAR(36) NOT NULL UNIQUE,
  event_type   VARCHAR(50) NOT NULL,
  payload      JSON NOT NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_at      TIMESTAMP NULL
);

CREATE TABLE processed_events (
  event_id     VARCHAR(36) PRIMARY KEY,
  processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### accounting-service (new)

```sql
CREATE TABLE authorizations (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id   BIGINT NOT NULL,
  status     VARCHAR(20) NOT NULL   -- "AUTHORIZED" / "DECLINED"
);

CREATE TABLE saga_join_state (
  order_id            BIGINT PRIMARY KEY,
  consumer_verified    BOOLEAN NOT NULL DEFAULT FALSE,
  ticket_created        BOOLEAN NOT NULL DEFAULT FALSE,
  failed                BOOLEAN NOT NULL DEFAULT FALSE,  -- either prerequisite failed
  resolved              BOOLEAN NOT NULL DEFAULT FALSE   -- authorization already fired
);

-- outbox_events, processed_events: same shape as above
```

### kitchen-service (extends Ch.3 schema)

```sql
ALTER TABLE tickets ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'CREATE_PENDING';
-- values used: CREATE_PENDING, AWAITING_ACCEPTANCE, CANCELLED

CREATE TABLE saga_join_state (
  order_id   BIGINT PRIMARY KEY,
  failed     BOOLEAN NOT NULL DEFAULT FALSE  -- an upstream failure arrived before ticket creation
);
```

### order-service (extends Ch.3 schema)

```sql
-- orders.status already exists; add REJECTED as a valid value alongside
-- APPROVAL_PENDING / APPROVED
```

## Component changes

### consumer-service (first real code)

- `Consumer` JPA entity + seed data (mix of active/inactive).
- `OutboxEvent` + `ProcessedEvent` entities (Ch.3 outbox/dedup pattern).
- `OrderEventListener`: `@KafkaListener(topics = "order.events")` →
  verifies consumer, writes `ConsumerVerified`/`ConsumerVerificationFailed`
  outbox row in the same transaction as the dedup-ledger insert.
- `OutboxPublisher`: `@Scheduled` poller, same shape as order-service's.

### accounting-service (first real code)

- `Authorization` JPA entity, `SagaJoinState` JPA entity.
- `OutboxEvent` + `ProcessedEvent` entities.
- Two listeners (`ConsumerEventListener` on `consumer.events`,
  `KitchenEventListener` on `kitchen.events`), both updating the same
  `SagaJoinState` row per `orderId` and firing authorization once both
  flags are true (or marking `failed` and short-circuiting on a failure
  event).
- `OutboxPublisher`.

### kitchen-service (extends Ch.3 code)

- `Ticket.status` field added; `OrderEventListener` sets
  `CREATE_PENDING` on create (or `CANCELLED` directly if
  `saga_join_state.failed` is already true for that `orderId`).
- New `AccountingEventListener` (`accounting.events`) →
  `CardAuthorized` → ticket to `AWAITING_ACCEPTANCE`, emits
  `TicketConfirmed`; `CardAuthorizationFailed` → ticket to `CANCELLED`,
  emits `TicketCancelled`.
- New `ConsumerEventListener` (`consumer.events`) →
  `ConsumerVerificationFailed` → cancels ticket if present, else marks
  `saga_join_state.failed = true` for that `orderId`.

### order-service (extends Ch.3 code)

- `Order.status` gains `REJECTED` as a valid value.
- New `SagaEventListener` subscribing to `consumer.events` (failure only),
  `kitchen.events` (`TicketCreationFailed`, `TicketConfirmed`), and
  `accounting.events` (failure only) → transitions `Order` to `APPROVED`
  or `REJECTED` accordingly, guarded on current status.

## Error handling

- **Outbox publish failure**: same as Ch.3 — row stays unsent, poller
  retries. No change to this mechanism.
- **Consumer crash mid-processing**: dedup-ledger insert and the
  business-state change happen in one local transaction per service, so a
  crash rolls back both — redelivery reprocesses correctly.
- **Out-of-order arrival at the join** (accounting-service or
  kitchen-service receiving events in either order): handled by the local
  join-state tables described above — order of arrival doesn't affect the
  outcome.
- **Malformed/unexpected event payload**: log and skip, consistent with
  Ch.3 — no dead-letter queue in this pass.

## Testing

- **Unit tests**:
  - consumer-service: verification rule (active/inactive consumer).
  - accounting-service: join-state logic — both event orders, and a
    failure event on either side aborting the join without authorizing.
  - kitchen-service, order-service: idempotent state-transition guards
    (no double-cancel, no double-reject; ticket created directly as
    `CANCELLED` when a failure preceded ticket creation).
- **Manual e2e verification via Docker** (same style as every Ch.3 pass):
  1. Happy path — order reaches `APPROVED`, ticket reaches
     `AWAITING_ACCEPTANCE`.
  2. Each of the three failure cases (A/B/C) — order reaches `REJECTED`,
     correct compensating transaction confirmed (ticket `CANCELLED` where
     applicable, no authorization recorded).
  3. Redelivery/idempotency check — force-redeliver one saga event (reset
     `sent_at`, same technique as Ch.3) and confirm no double-authorization
     or double-cancellation.
  4. Out-of-order arrival — confirm accounting-service's join fires
     correctly regardless of whether `ConsumerVerified` or `TicketCreated`
     lands first.

## Deferred (not in this pass)

- Orchestration-style Create Order saga (a `CreateOrderSaga` coordinator in
  order-service) — planned as a distinct follow-up exercise to compare
  directly against this choreography implementation.
- delivery-service and the later stages of order fulfillment (ticket
  acceptance, delivery pickup) — out of scope for the Create Order saga.
- Real payment gateway / KYC integration — verification and authorization
  stay mocked.
- Dead-letter queue for malformed events (consistent with Ch.3).
