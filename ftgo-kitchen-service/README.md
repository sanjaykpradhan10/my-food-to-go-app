# ftgo-kitchen-service

**Port:** 8083
**Bounded context:** Ticket management (separate from Order — Ubiquitous Language: *Ticket*, not *Order*)

## Role

The kitchen's view of a customer order is not "an order" — it's a ticket to prepare. Per Ch.2's decompose-by-subdomain reasoning, Order and Ticket are deliberately separate aggregates in separate bounded contexts: order-service tracks a consumer-facing lifecycle (`APPROVAL_PENDING` → `APPROVED`/`REJECTED`), while kitchen-service tracks a kitchen-facing one (`CREATE_PENDING` → `AWAITING_ACCEPTANCE`/`CANCELLED`). They're correlated by `orderId`, not merged into one shared model.

kitchen-service never calls another service synchronously — every interaction is Kafka-driven, either reacting to another service's domain events (choreography) or to explicit commands from order-service's orchestrator (orchestration), selected per-deployment by `SAGA_MODE`.

## API

None. This service has no REST endpoints — it's a pure Kafka consumer/producer.

## Events

### Publishes

| Mode | Topic | eventType | When |
|---|---|---|---|
| Choreography | `kitchen.events` | `TicketCreated` | Ticket created within capacity |
| Choreography | `kitchen.events` | `TicketCreationFailed` | Order exceeds kitchen capacity |
| Choreography | `kitchen.events` | `TicketConfirmed` | Card authorized, ticket moved to `AWAITING_ACCEPTANCE` |
| Choreography | `kitchen.events` | `TicketCancelled` | Ticket compensated (card declined, or consumer verification failed) |
| Orchestration | `saga.replies` | `TicketCreated` / `TicketCreationFailed` | Reply to `CreateTicket` command only |

`ConfirmTicket`/`CancelTicket` commands are fire-and-forget in orchestration mode — no reply is published for them, since the orchestrator is already authoritative about the order's outcome by the time it sends them.

### Consumes

| Mode | Topic | Reacts to |
|---|---|---|
| Choreography | `order.events` | `OrderCreated` — creates the ticket |
| Choreography | `accounting.events` | `CardAuthorized` / `CardAuthorizationFailed` — confirm or cancel |
| Choreography | `consumer.events` | `ConsumerVerificationFailed` only (ignores `ConsumerVerified` — ticket creation doesn't wait on it) |
| Orchestration | `kitchen.commands` | `CreateTicket` / `ConfirmTicket` / `CancelTicket`, one listener (`KitchenCommandListener`) dispatching on `commandType` |

## Domain model

`Ticket` (`orderId`, `status`) moves `CREATE_PENDING` → `AWAITING_ACCEPTANCE` (confirmed) or `CANCELLED` (compensated). No dedicated status enum — status is a plain string, matching this service's original Ch.3 convention.

**Capacity check** (`TicketService.isWithinCapacity`, `KITCHEN_CAPACITY_LIMIT = 20`): since no pricing data flows through any saga event (order-service validates prices against restaurant-service but never persists or forwards them), total line-item quantity stands in for "order size" as the input to both this check and accounting-service's authorization threshold — a deliberate simplification, not a real capacity model.

**`FailedOrder`** (choreography only): records an `orderId` when `ConsumerVerificationFailed` arrives *before* kitchen has processed `OrderCreated` for that order — a genuine race, since consumer verification and ticket creation happen in parallel, reacting independently to the same event. When `OrderCreated` does eventually arrive, `handleOrderCreated` checks this table first and creates the ticket directly as `CANCELLED` instead of `CREATE_PENDING`. Orchestration mode has no equivalent table: the central orchestrator already knows the saga failed and sends an explicit `CancelTicketCommand` once the ticket exists, so the race is absorbed there instead.

## Idempotency & reliability

Every handler dedupes via a `processed_events` ledger (insert-then-act in one local transaction) before touching any other state — protects against Kafka's at-least-once redelivery.

This service's outbox/producer capability (`OutboxEvent`, `OutboxPublisher`, `KafkaProducerConfig` — now shared via the `ftgo-common` module, see the root `docs/ARCHITECTURE.md`) was added during the Ch.4 choreography pass — before that, kitchen-service only ever consumed `order.events`, it never published anything. `OutboxEvent` carries a `topic` column per row (not a fixed constant), since this service now writes to two different topics (`kitchen.events` in choreography mode, `saga.replies` in orchestration mode) from one outbox table.

## Running standalone

```bash
./gradlew :ftgo-kitchen-service:test
```

Needs the full docker-compose stack (MySQL, Kafka, and at least order-service publishing `order.events`/commands) to exercise live — see the root [README](../README.md) for `docker compose up`.
