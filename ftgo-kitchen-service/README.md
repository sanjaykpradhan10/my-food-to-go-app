# ftgo-kitchen-service

**Port:** 8083
**Bounded context:** Ticket management (separate from Order — Ubiquitous Language: *Ticket*, not *Order*)

## Role

The kitchen's view of a customer order is not "an order" — it's a ticket to prepare. Per Ch.2's decompose-by-subdomain reasoning, Order and Ticket are deliberately separate aggregates in separate bounded contexts, correlated by `orderId`, not merged into one shared model.

Most interaction is Kafka-driven, either reacting to another service's domain events (choreography) or to explicit commands from order-service's orchestrator (orchestration), selected per-deployment by `SAGA_MODE`. This service also exposes a small REST API for the restaurant-worker lifecycle (accept/prepare/ready/pick-up), which is unrelated to saga mode.

## API

**`POST /tickets/{id}/accept`**

Request: `{"readyBy": "2026-07-22T18:30:00Z"}`. Legal only from `AWAITING_ACCEPTANCE` — moves to `ACCEPTED`.

**`POST /tickets/{id}/preparing`** — legal only from `ACCEPTED` — moves to `PREPARING`.

**`POST /tickets/{id}/ready-for-pickup`** — legal only from `PREPARING` — moves to `READY_FOR_PICKUP`.

**`POST /tickets/{id}/picked-up`** — legal only from `READY_FOR_PICKUP` — moves to `PICKED_UP`.

All four: `404` if the ticket doesn't exist, `409` on an illegal transition.

## Events

### Publishes (`kitchen.events`, choreography)

| eventType | When |
|---|---|
| `TicketCreated` | Ticket created within capacity |
| `TicketCreationFailed` | Order exceeds kitchen capacity |
| `TicketConfirmed` | Card authorized, ticket moved to `AWAITING_ACCEPTANCE` |
| `TicketCancelled` | Ticket cancelled (Create Order compensation, or Cancel Order saga's primary flow) |
| `TicketCancellationRejected` | Cancel Order saga: cancellation attempted but the ticket is already too far along (`READY_FOR_PICKUP`+) |
| `TicketAccepted` / `TicketPreparingStarted` / `TicketReadyForPickup` / `TicketPickedUp` | Restaurant-worker REST API calls |
| `TicketQuantityRevised` | Revise Order saga: revised quantity applied (provisionally, before accounting is asked) |
| `TicketRevisionRejected` | Revise Order saga: revision rejected outright (over kitchen capacity) |
| `TicketRevisionUndone` | Revise Order saga: compensation — quantity reverted after accounting declined |

### Publishes (`saga.replies`, orchestration)

| Reply eventType | Reply to command | sagaType |
|---|---|---|
| `TicketCreated` / `TicketCreationFailed` | `CreateTicket` | `CreateOrder` |
| `TicketCancelled` / `TicketCancellationRejected` | `CancelTicket` | `CreateOrder` (compensation) or `CancelOrder` (primary flow) — the command's own `sagaType` is echoed back, since this handler can't infer which saga it's servicing from `commandType` alone |
| `TicketQuantityRevised` / `TicketRevisionRejected` | `ReviseTicket` | `ReviseOrder` |
| `TicketRevisionUndone` | `UndoReviseTicket` | `ReviseOrder` |

`ConfirmTicket` is fire-and-forget — no reply, since the orchestrator is already authoritative about the order's outcome by the time it sends it.

### Consumes

| Mode | Topic | Reacts to |
|---|---|---|
| Choreography | `order.events` | `OrderCreated` (create the ticket); `OrderCancelled` (Cancel Order primary flow); `OrderRevisionProposed` (Revise Order primary flow); `OrderRevisionCompensationRequested` (Revise Order compensation — **not** the terminal `OrderRevisionRejected`, which nothing here reacts to, since by then there's nothing left to undo) |
| Choreography | `accounting.events` | `CardAuthorized` / `CardAuthorizationFailed` **only** — every other message on this shared topic (`AuthorizationReversed`, `AuthorizationRevised`, `AuthorizationRevisionRejected`) is explicitly ignored. This topic used to be filtered by "not `CardAuthorized` ⇒ treat as failure," which broke once other event types started appearing on it (see "A bug worth knowing about" below) |
| Choreography | `consumer.events` | `ConsumerVerificationFailed` only (ignores `ConsumerVerified` — ticket creation doesn't wait on it) |
| Orchestration | `kitchen.commands` | `CreateTicket` / `ConfirmTicket` / `CancelTicket` / `ReviseTicket` / `UndoReviseTicket`, one listener (`KitchenCommandListener`) dispatching on `commandType` |

## A bug worth knowing about

`TicketService.handleAccountingEvent` originally treated *any* `accounting.events` message that wasn't `"CardAuthorized"` as an authorization failure and cancelled the ticket — safe only while that topic carried exactly two event types. Once the Cancel Order and Revise Order sagas added `AuthorizationReversed`/`AuthorizationRevised`/`AuthorizationRevisionRejected` to the same topic, this handler started mis-cancelling tickets on every successful revision (spuriously reacting to `AuthorizationRevised`). Found only via Docker end-to-end testing — no unit test round-trips a real message between two services — and fixed by switching to an explicit `eventType` match with everything else ignored. Worth remembering if a future saga adds a 5th event type to `accounting.events`: it needs its own explicit case here, or it's silently ignored (the safer failure mode, but still worth checking deliberately).

## Domain model

`Ticket` — `id`, `orderId`, `state` (`TicketState`), `readyBy`, `totalQuantity` (persisted).

`TicketState`: `CREATE_PENDING → AWAITING_ACCEPTANCE → ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP`, or `CANCELLED` (legal from any of the first four states, or — via `reviseQuantity`/`undoRevision` specifically — up through `PREPARING`). State-changing methods return class-per-event `TicketDomainEvent`s (sealed interface) rather than being hand-built inline in the service layer.

- `cancel()`: legal from `CREATE_PENDING`/`AWAITING_ACCEPTANCE`/`ACCEPTED`, throws `TicketCannotBeCancelledException` from `READY_FOR_PICKUP` (a real, expected business rejection) and generic `UnsupportedStateTransitionException` from `PREPARING`/`PICKED_UP`/`CANCELLED`.
- `reviseQuantity(int)`/`undoRevision(int)`: legal from `CREATE_PENDING`/`AWAITING_ACCEPTANCE`/`ACCEPTED`/`PREPARING` (a wider window than `cancel()`, since quantity can still change mid-prep); no two-tier exception split like `cancel()` — every illegal-state case throws `UnsupportedStateTransitionException` uniformly. Capacity checking (`isWithinCapacity`, `KITCHEN_CAPACITY_LIMIT = 20`) stays in `TicketService`, not the aggregate — same pattern as ticket creation, where the aggregate method itself never self-rejects.

**Capacity check**: since no pricing data flows through any saga event (order-service validates prices against restaurant-service but never persists or forwards them), total line-item quantity stands in for "order size" as the input to both this check and accounting-service's authorization threshold — a deliberate simplification, not a real capacity model.

**`FailedOrder`** (choreography only): records an `orderId` when `ConsumerVerificationFailed` arrives *before* kitchen has processed `OrderCreated` for that order — a genuine race, since consumer verification and ticket creation happen in parallel, reacting independently to the same event. When `OrderCreated` does eventually arrive, `handleOrderCreated` checks this table first and creates the ticket directly as `CANCELLED` instead of `CREATE_PENDING`. Orchestration mode has no equivalent table: the central orchestrator already knows the saga failed and sends an explicit `CancelTicketCommand` once the ticket exists, so the race is absorbed there instead.

## Idempotency & reliability

Every handler dedupes via a `processed_events` ledger (insert-then-act in one local transaction) before touching any other state — protects against Kafka's at-least-once redelivery.

This service's outbox/producer capability (`OutboxEvent`, `OutboxPublisher`, `KafkaProducerConfig` — now shared via the `ftgo-common` module, see the root `docs/ARCHITECTURE.md`) was added during the Ch.4 choreography pass — before that, kitchen-service only ever consumed `order.events`, it never published anything. `OutboxEvent` carries a `topic` column per row (not a fixed constant), since this service now writes to two different topics (`kitchen.events` in choreography mode, `saga.replies` in orchestration mode) from one outbox table.

## Running standalone

```bash
./gradlew :ftgo-kitchen-service:test
```

Needs the full docker-compose stack (MySQL, Kafka, and at least order-service publishing `order.events`/commands) to exercise live — see the root [README](../README.md) for `docker compose up`.
