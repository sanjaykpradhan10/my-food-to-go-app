# Design: Create Order Saga — Orchestration (Ch. 4, follow-up)

**Date**: 2026-07-17
**Status**: Approved

## Goal

Implement the Ch. 4 Create Order saga a second time, using the orchestration style, switchable against the existing choreography implementation via one `SAGA_MODE` env var per service (`choreography` default, or `orchestration`). A central `CreateOrderSagaOrchestrator` in order-service sends explicit commands to each participant and reacts to their replies, rather than participants reacting to each other's domain events directly. The goal is a direct, apples-to-apples comparison against the choreography implementation already built and merged — same observable outcomes (`Order`/`Ticket`/`Authorization` states) for the happy path and all three compensation cases, reached by a structurally different mechanism.

## Scope decisions

- **Mode switch**: `SAGA_MODE` env var, default `choreography`, alternate `orchestration`. Every existing choreography listener across all four services gets `@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)`; a parallel set of orchestration-only components activates under `havingValue = "orchestration"`. Both code paths live in the codebase simultaneously; only one is wired at a time. Mirrors the `OUTBOX_PUBLISH_MODE` convention from Ch. 3 exactly.
- **Channel topology**: four new Kafka topics, fully separate from choreography's domain-event topics — `consumer.commands`, `kitchen.commands`, `accounting.commands` (orchestrator → participant) and one shared `saga.replies` (participant → orchestrator). Chosen over reusing the existing four topics with mixed-in command/reply event types, to keep the two communication styles cleanly separable and directly comparable.
- **`OutboxEvent` generalization**: since order-service (the orchestrator) must publish to three different command topics from one outbox table, `OutboxEvent` gains a `topic` column in all four services, and each service's `OutboxPublisher` reads `event.getTopic()` per row instead of a hardcoded topic constant. Applied uniformly across all four services for consistency, even though only order-service strictly requires multi-topic fan-out. This is a mechanical change to existing Ch. 4 code — no behavior change for choreography, which continues writing its known topic literal into each row.
- **Saga state persistence**: a new `CreateOrderSagaInstance` table in order-service (`orderId` PK, `consumerVerified`, `ticketCreated`, `failed`, `totalQuantity`) tracks orchestration progress durably, so an order-service restart mid-saga doesn't silently strand an order with no record of what commands were already sent. This is the orchestration analogue of accounting-service's `SagaJoinState` from choreography — but here it's the *only* join point in the whole saga (see below).
- **No Eventuate Tram**, consistent with every prior pass in this project — the orchestrator, saga instance table, and command/reply plumbing are all hand-rolled, same as the outbox pattern.
- **Domain logic reuse**: each participant's command handler reuses the exact same verification/creation/authorization decision logic choreography already built (`ConsumerVerificationService`, `TicketService`, the authorization threshold check) — only the outbound event (which topic, which record shape) differs between the two modes. No new business rules, no new thresholds.
- **Compensation matrix must match choreography's observable outcomes exactly** (same `Order`/`Ticket`/`Authorization` end states for the happy path and cases A/B/C), reached via commands from the orchestrator rather than participants self-observing peer failures.

## Architecture

```
order-service (orchestration mode)
┌──────────────────────────────────────────────────────────┐
│ POST /orders → Order{APPROVAL_PENDING}                     │
│  └─ CreateOrderSagaOrchestrator.start(...)                 │
│       ├─ save CreateOrderSagaInstance                       │
│       └─ send VerifyConsumerCommand + CreateTicketCommand   │
│           (outbox rows, topic-per-row → consumer.commands / │
│            kitchen.commands)                                 │
│                                                                │
│ OrchestratorReplyListener (saga.replies)                      │
│  └─ CreateOrderSagaOrchestrator.handleReply(...)               │
│       ├─ both ConsumerVerified + TicketCreated →                │
│       │     send AuthorizeCardCommand (accounting.commands)      │
│       ├─ CardAuthorized → send ConfirmTicketCommand,               │
│       │     Order → APPROVED  (no wait for a confirmation echo)     │
│       └─ any failure reply → Order → REJECTED,                       │
│             send CancelTicketCommand if a ticket already exists       │
└──────────────────────────────────────────────────────────────────────┘
        │ commands                                    ▲ replies
        ▼                                              │
┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
│ consumer-service     │  │ kitchen-service      │  │ accounting-service   │
│ VerifyConsumerCommand │  │ CreateTicketCommand    │  │ AuthorizeCardCommand  │
│  → reuse verification  │  │  → reuse capacity-gated │  │  → reuse threshold      │
│    decision, reply to   │  │    creation, reply to    │  │    decision, reply to    │
│    saga.replies          │  │    saga.replies            │  │    saga.replies            │
│                            │  │ ConfirmTicketCommand /       │  │ (no join needed — the       │
│                            │  │ CancelTicketCommand            │  │  orchestrator already        │
│                            │  │  → transition ticket,             │  │  waited for both              │
│                            │  │    fire-and-forget, no reply         │  │  prerequisites)                │
└────────────────────┘  └────────────────────┘  └────────────────────┘
```

## Event flow — happy path

1. `POST /orders` → `Order{status=APPROVAL_PENDING}`. Instead of publishing `OrderCreated` for peers to react to, `CreateOrderSagaOrchestrator.start(...)` runs synchronously in the same transaction: creates a `CreateOrderSagaInstance` row and writes two outbox rows — `VerifyConsumerCommand` (→ `consumer.commands`) and `CreateTicketCommand` (→ `kitchen.commands`), carrying `orderId`, `consumerId`/`totalQuantity` as needed. These are dispatched in parallel, same topology as choreography's steps 2–3.
2. **consumer-service**: `VerifyConsumerCommandListener` consumes `consumer.commands`, reuses the existing verification decision, replies `ConsumerVerified` (or `ConsumerVerificationFailed`) to `saga.replies`.
3. **kitchen-service**: `CreateTicketCommandListener` consumes `kitchen.commands` (independently, in parallel with step 2), reuses the capacity-gated creation logic, replies `TicketCreated` (or `TicketCreationFailed`) to `saga.replies`.
4. **order-service**: `OrchestratorReplyListener` consumes both replies (either order), updates `CreateOrderSagaInstance`. Once both `consumerVerified` and `ticketCreated` are true, sends `AuthorizeCardCommand` to `accounting.commands`.
5. **accounting-service**: `AuthorizeCardCommandListener` consumes `accounting.commands`, reuses the same quantity-threshold decision, replies `CardAuthorized` (or `CardAuthorizationFailed`) to `saga.replies`. No join needed — accounting-service only ever receives this command once the orchestrator has already confirmed both prerequisites succeeded.
6. **order-service**: on `CardAuthorized`, sends `ConfirmTicketCommand` to `kitchen.commands` and marks `Order{status=APPROVED}` directly — it does not wait for any downstream confirmation, unlike choreography where order-service had to wait for kitchen's `TicketConfirmed` as an indirect signal that accounting had already succeeded.
7. **kitchen-service**: `ConfirmTicketCommandListener` transitions `Ticket{status=AWAITING_ACCEPTANCE}`. Fire-and-forget — no reply, since the orchestrator's own bookkeeping is already authoritative.

## Compensation flows

Three failure points, each ending in the same states choreography reaches, via explicit commands instead of self-observed peer events:

**A. Consumer verification fails** (`ConsumerVerificationFailed` reply)
- Orchestrator marks its `CreateOrderSagaInstance.failed = true`, `Order → REJECTED`.
- If `ticketCreated` is already true, sends `CancelTicketCommand` immediately. If not yet true, the orchestrator's own state already records `failed`, so when the (eventually arriving) `TicketCreated` reply is processed, the orchestrator recognizes the saga is already failed and sends `CancelTicketCommand` at that point instead of proceeding toward authorization. This is what absorbs the race choreography's kitchen-side `FailedOrder` table existed to handle — no analogous table is needed on the kitchen side in orchestration mode, because the orchestrator is the single source of truth.

**B. Kitchen can't create the ticket** (`TicketCreationFailed` reply)
- Orchestrator marks failed, `Order → REJECTED`. No compensating command needed for consumer-service (nothing to undo, matches choreography's Case B exactly).

**C. Card authorization declined** (`CardAuthorizationFailed` reply, only reachable after both prerequisites already succeeded)
- Orchestrator sends `CancelTicketCommand`, `Order → REJECTED`.

In all three cases, `Order` rejects on the *first* failure reply the orchestrator sees — it does not wait for a compensating command to complete before rejecting, same optimization choreography's order-service already used.

## Data model (additions)

### order-service — new table

```sql
CREATE TABLE create_order_saga_instances (
  order_id            BIGINT PRIMARY KEY,
  consumer_verified    BOOLEAN NOT NULL DEFAULT FALSE,
  ticket_created         BOOLEAN NOT NULL DEFAULT FALSE,
  failed                  BOOLEAN NOT NULL DEFAULT FALSE,
  total_quantity           INT NOT NULL
);
```

### All four services — `outbox_events` gains a column

```sql
ALTER TABLE outbox_events ADD COLUMN topic VARCHAR(50) NOT NULL DEFAULT '';
-- existing choreography call sites updated to pass their known topic literal explicitly;
-- default only exists to satisfy NOT NULL during the migration, never relied upon at runtime.
```

### Command/reply event shapes (new records, one per topic, duplicated per consuming service per the project's existing no-shared-module convention)

```java
// consumer.commands
public record VerifyConsumerCommand(String eventId, Long orderId, Long consumerId) {}

// kitchen.commands — one shape, discriminated by commandType
public record KitchenCommand(String eventId, String commandType, Long orderId, Integer totalQuantity) {}
// commandType: "CreateTicket" | "ConfirmTicket" | "CancelTicket"

// accounting.commands
public record AuthorizeCardCommand(String eventId, Long orderId, Integer totalQuantity) {}

// saga.replies — one shape, discriminated by participant + eventType
public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason) {}
// participant: "consumer" | "kitchen" | "accounting"
// eventType: "ConsumerVerified" | "ConsumerVerificationFailed" | "TicketCreated" | "TicketCreationFailed"
//          | "CardAuthorized" | "CardAuthorizationFailed"
```

## Component changes

### order-service
- `CreateOrderSagaInstance` entity + repository.
- `CreateOrderSagaOrchestrator` (domain): `start(...)`, `handleReply(...)` — the state machine described above.
- `OrchestratorReplyListener` (infra, `@KafkaListener(topics = "saga.replies", groupId = "order-service")`, gated on `saga.mode=orchestration`).
- `OrderService.createOrder`: small mode branch — choreography path unchanged (publishes `OrderCreated`); orchestration path delegates to `CreateOrderSagaOrchestrator.start(...)`.
- Existing `ConsumerEventListener`/`KitchenEventListener`/`AccountingEventListener` (choreography) gain `@ConditionalOnProperty(saga.mode=choreography, matchIfMissing=true)`.
- `OutboxEvent`/`OutboxPublisher`: add `topic` column/field; publisher reads per-row topic.

### consumer-service
- `ConsumerVerificationService`: verification decision extracted into a small shared method; existing `handleOrderCreated` (choreography) and new `handleVerifyConsumerCommand` (orchestration) both call it, each publishing to their own topic/shape.
- `VerifyConsumerCommandListener` (infra, `consumer.commands`, gated on `saga.mode=orchestration`).
- Existing `OrderEventListener` (choreography) gains the `saga.mode=choreography` gate.
- `OutboxEvent`/`OutboxPublisher`: `topic` column/field.

### kitchen-service
- `TicketService`: capacity-gated creation logic reused by new `handleCreateTicketCommand`; new `handleConfirmTicketCommand`/`handleCancelTicketCommand` reuse the existing `markAwaitingAcceptance()`/`markCancelled()` mutators.
- One `KitchenCommandListener` (infra, `kitchen.commands`, gated on `saga.mode=orchestration`) consumes all three `commandType` values (`CreateTicket`/`ConfirmTicket`/`CancelTicket`) and dispatches to the matching `TicketService` method — mirrors the existing convention of one listener per topic dispatching on a type field (e.g. choreography's `AccountingEventListener` in this same service already handles two event types in one listener), rather than splitting into multiple listener classes per topic.
- Existing `OrderEventListener`/`AccountingEventListener`/`ConsumerEventListener` (choreography) gain the `saga.mode=choreography` gate.
- `FailedOrder`/`FailedOrderRepository` are choreography-only — not used by the orchestration path (the orchestrator itself absorbs that race).
- `OutboxEvent`/`OutboxPublisher`: `topic` column/field.

### accounting-service
- Authorization threshold decision extracted into a small shared method reused by `SagaJoinService`'s existing `tryResolve` (choreography) and new `handleAuthorizeCardCommand` (orchestration).
- `AuthorizeCardCommandListener` (infra, `accounting.commands`, gated on `saga.mode=orchestration`).
- Existing `ConsumerEventListener`/`KitchenEventListener` (choreography) gain the `saga.mode=choreography` gate.
- `SagaJoinState`/`SagaJoinService`'s join logic are choreography-only — not used by the orchestration path.
- `OutboxEvent`/`OutboxPublisher`: `topic` column/field.

## Error handling

- Same idempotency approach as choreography throughout: every consumer (including the new command/reply listeners) dedupes via the existing `processed_events` ledger, insert-then-act in one local transaction.
- **Saga instance persistence**: `CreateOrderSagaInstance` is written transactionally alongside each command dispatch/reply handling, so a restart mid-saga leaves a durable record of exactly which flags were set — unlike an in-memory-only approach, which was explicitly rejected because it would silently orphan an order on a crash.
- Outbox publish failure / malformed payload handling: unchanged from Ch. 3/4 — row stays unsent and retries next poll; malformed payloads are logged and skipped, no dead-letter queue.

## Testing

- **Unit tests** (Mockito, matching existing convention):
  - `CreateOrderSagaOrchestrator`: both reply orderings (`ConsumerVerified` then `TicketCreated`, and reverse) resolve identically; all three failure branches; the direct-approve-on-`CardAuthorized` shortcut (no wait for a confirmation echo).
  - Each new command handler (`VerifyConsumerCommandListener`'s service method, `CreateTicketCommandListener`'s, `AuthorizeCardCommandListener`'s): correct reply on success/failure, reused decision logic behaves identically to its choreography counterpart.
- **Manual e2e verification via Docker**, `SAGA_MODE=orchestration`, reusing the exact same scenarios as the choreography pass for direct comparison:
  1. Happy path — same end states (`APPROVED`/`AWAITING_ACCEPTANCE`/`AUTHORIZED`).
  2. Cases A/B/C — same end states (`REJECTED`/`CANCELLED` or absent/`DECLINED`).
  3. Redelivery/idempotency check.

## Deferred (not in this pass)

- Running both `SAGA_MODE` values simultaneously against live traffic (feature-flag-per-request rather than per-deployment) — out of scope; this is a deployment-time switch only, matching `OUTBOX_PUBLISH_MODE`'s precedent.
- A shared module to de-duplicate the now-further-multiplied outbox/dedup/event-record infrastructure — still deferred, per the existing project-wide decision; noted again here since orchestration adds a second full set of duplicated command/reply records on top of choreography's domain-event records.
