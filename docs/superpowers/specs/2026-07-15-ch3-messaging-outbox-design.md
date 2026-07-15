# Design: Async Messaging + Transactional Outbox (Ch. 3)

**Date**: 2026-07-15
**Status**: Approved

## Goal

order-service reliably publishes an `OrderCreated` event when an order is
placed, and kitchen-service consumes it to create a `Ticket`. This
demonstrates the transactional outbox pattern (the event write can never be
lost or published without the business write actually committing) and
at-least-once delivery handled correctly via an idempotent consumer.

This closes two of the four remaining Ch. 3 IPC patterns: **async
messaging** and **transactional outbox**. Transaction log tailing (CDC) and
service discovery remain out of scope for this pass — see
`CONTEXT.md` for tracking.

## Scope decisions

- **Order persistence**: `Order` becomes a real JPA entity, persisted to
  MySQL for the first time (previously in-memory only, despite MySQL being
  configured). Required for the outbox pattern to have a real local
  transaction to piggyback on.
- **Outbox implementation**: hand-rolled (outbox table + `@Scheduled`
  polling publisher), not the Eventuate Tram framework. Chosen so the
  mechanics stay fully visible for learning purposes, at the cost of not
  matching the reference FTGO app's library choice.
- **Consumer**: kitchen-service (currently an empty stub) gets its first
  real code — a Kafka listener that creates a `Ticket` per order. This is
  scoped to "consume and create a ticket," not the full Ch. 4 saga
  (no reply event, no orchestration logic yet).
- **Idempotency**: kitchen-service dedupes by `event_id` via a
  `processed_events` ledger table, since outbox delivery is at-least-once.

## Architecture

```
order-service                                    kitchen-service
┌─────────────────────────┐    Kafka topic       ┌──────────────────────┐
│ POST /orders              │    "order.events"    │ @KafkaListener        │
│  └─ OrderService           │                      │  └─ TicketService     │
│      @Transactional        │                      │      - dedupe by      │
│      ├─ save Order         │                      │        event_id       │
│      └─ save OutboxEvent   │──publish (poller)──▶ │      └─ save Ticket   │
│                             │                      │      └─ save          │
│ OutboxPublisher             │                      │        processed_    │
│  (@Scheduled poller)        │                      │        event_id      │
│  reads unsent rows,         │                      └──────────────────────┘
│  publishes to Kafka,        │
│  marks sent                 │
└─────────────────────────┘
```

Both the `Order` write and the `OutboxEvent` write happen in one local DB
transaction in order-service — no distributed transaction, no dual-write
race. A separate poller reads unsent outbox rows on a fixed interval,
publishes them to Kafka, and marks each sent after a successful publish.

## Data model

### order-service — new tables

```sql
CREATE TABLE orders (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  restaurant_id BIGINT NOT NULL,
  status        VARCHAR(20) NOT NULL
);

CREATE TABLE order_line_items (
  order_id     BIGINT NOT NULL REFERENCES orders(id),
  menu_item_id BIGINT NOT NULL,
  quantity     INT NOT NULL
);

CREATE TABLE outbox_events (
  id           BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id     VARCHAR(36) NOT NULL UNIQUE,   -- UUID, becomes the dedup key downstream
  event_type   VARCHAR(50) NOT NULL,          -- "OrderCreated"
  payload      JSON NOT NULL,                 -- serialized event body
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_at      TIMESTAMP NULL                 -- null = not yet published
);
```

`Order` becomes a JPA entity (`@Entity`, `@OneToMany` line items) instead of
a plain in-memory class. `ddl-auto: update` (already configured) creates
these tables on boot, consistent with restaurant-service's existing setup.

### kitchen-service — new tables

```sql
CREATE TABLE tickets (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id   BIGINT NOT NULL,
  status     VARCHAR(20) NOT NULL   -- "CREATED"
);

CREATE TABLE processed_events (
  event_id     VARCHAR(36) PRIMARY KEY,
  processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

`processed_events` is the dedup ledger: before creating a `Ticket`, the
consumer checks whether `event_id` is already present; if so it skips. The
insert into `processed_events` and the `Ticket` save happen in the same
local transaction, so a crash mid-processing rolls back both together (a
redelivery is correctly reprocessed, not falsely skipped).

### Event payload (`OrderCreated`)

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "orderId": 123,
  "restaurantId": 1,
  "lineItems": [{"menuItemId": 1, "quantity": 2}]
}
```

## Component changes

### order-service

- `Order` domain class → JPA `@Entity` (id, restaurantId, status,
  `@OneToMany` `OrderLineItem` entities). `OrderRepository extends
  JpaRepository`.
- `OutboxEvent` JPA entity + `OutboxEventRepository`.
- `OrderService.createOrder(...)` becomes `@Transactional`: validates
  against restaurant-service (unchanged — still via the existing
  circuit-breaker proxy), then in one transaction saves the `Order` and an
  `OutboxEvent` row with the serialized `OrderCreated` payload (Jackson).
- New `OutboxPublisher`: `@Scheduled(fixedDelay = ...)` job that reads rows
  where `sent_at IS NULL` (oldest first, batch-limited), publishes each to
  Kafka topic `order.events` (key = `orderId`, for per-order partition
  ordering), then sets `sent_at` on success. Uses Spring Kafka's
  `KafkaTemplate`.
- Add `spring-kafka` dependency to `ftgo-order-service/build.gradle`.

### kitchen-service (first real code — currently an empty stub)

- `Ticket` JPA entity + `TicketRepository`.
- `ProcessedEvent` JPA entity + `ProcessedEventRepository` (dedup ledger).
- `OrderEventListener`: `@KafkaListener(topics = "order.events")`,
  deserializes `OrderCreated`, and in one `@Transactional` method: if
  `eventId` is already in `processed_events`, skip; otherwise insert into
  `processed_events` and save a new `Ticket`.
- Add `spring-kafka` dependency to `ftgo-kitchen-service/build.gradle`;
  dedicated consumer group `kitchen-service`.
- Add `ftgo_kitchen` datasource config (DB already exists per
  `infrastructure/mysql/init.sql`) — mirrors restaurant-service/
  order-service's `application.yml` pattern.

## Error handling

- **Outbox publish failure** (Kafka unreachable): row stays unsent
  (`sent_at` still null), poller retries next tick — no data loss,
  at-least-once by construction.
- **Consumer failure** after `processed_events` insert but before/during
  `Ticket` save: same transaction, so it's all-or-nothing — a crash
  mid-transaction rolls back both, so a redelivery is correctly
  re-processed rather than falsely skipped.
- **Malformed/unexpected event payload**: log and skip (don't crash the
  listener thread). No dead-letter queue in this pass — deferred.

## Testing

- **order-service**: unit test for `OrderService.createOrder` verifying
  both `Order` and `OutboxEvent` rows are persisted in one transaction
  (existing H2/MODE=MySQL test setup). Unit test for `OutboxPublisher`
  covering publish-success (marks `sent_at`) and publish-failure (row stays
  unsent) with a mocked `KafkaTemplate`.
- **kitchen-service**: unit test for `OrderEventListener` covering
  first-delivery (creates `Ticket` + `processed_events` row) and
  duplicate-delivery (event ID already processed → no second `Ticket`).
- **Manual e2e verification**: `docker compose up`, `POST /orders`, confirm
  a row lands in order-service's `outbox_events` (initially unsent),
  confirm it flips to sent within the poll interval, confirm a `Ticket`
  appears in kitchen-service's DB. Then manually reset `sent_at` to null on
  a sent row and let the poller re-send, confirming the consumer dedupes
  instead of creating a second ticket.

## Deferred (not in this pass)

- Transaction log tailing (CDC via Debezium) — alternative to outbox, not
  needed alongside it.
- Service discovery (client-side/server-side) — order-service continues to
  reach restaurant-service via the existing configured base URL.
- Dead-letter queue for malformed events.
- Full Ch. 4 saga orchestration/choreography (reply events, compensating
  transactions) — this pass only covers one-way event delivery.
