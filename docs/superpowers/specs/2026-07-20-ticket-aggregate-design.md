# Design: Refactor `Ticket` into a DDD aggregate (Ch. 5)

**Date**: 2026-07-20
**Status**: Approved

## Goal

Apply Ch. 5's Aggregate and Domain event patterns to kitchen-service's `Ticket`, which today is a near-anemic JPA entity (a raw `String status` field with two unguarded mutator methods) driven by a transaction-script `TicketService`. This is the book's own worked example (Listing 5.10–5.13, pages 169–172), read directly from the book (`Microservices_Patterns.pdf`) rather than reconstructed from memory.

Two things change together:
1. **Pattern refactor** — `Ticket` becomes a real aggregate: a `TicketState` enum, enforced state-transition invariants, and state-changing methods that return `List<TicketDomainEvent>` instead of `TicketService` hand-constructing events inline.
2. **New capability** — the restaurant-worker-facing lifecycle (`accept`/`preparing`/`readyForPickup`/`pickedUp`) that the book's `Ticket` models but this app never had (kitchen-service currently has zero REST endpoints — only Kafka listeners). Deliberately chosen to add this now rather than defer it, since the book's own aggregate rules (only-one-legal-next-state) are best exercised against the full lifecycle, not just the three saga-driven states that existed before this change.

Explicitly **not** in scope: a manual staff-initiated `reject()` action. The book has one; this app's cancellation is saga-driven only (`CancelTicket` command, consumer-verification-failure, authorization-failure), and there's no existing caller or business policy for staff-initiated rejection outside that. Adding it would be a second, undefined new capability riding along with this change.

## Current state (what's being replaced)

- `Ticket` (`ftgo-kitchen-service/.../domain/Ticket.java`): `@Id`, `orderId`, `status` (`String`), two mutators (`markAwaitingAcceptance()`, `markCancelled()`) with **no guard** — any status can be overwritten from any other status.
- `TicketService`: six `@Transactional` methods (`handleOrderCreated`, `handleAccountingEvent`, `handleConsumerVerificationFailed`, `handleCreateTicketCommand`, `handleConfirmTicketCommand`, `handleCancelTicketCommand`), each doing dedup-check → find/mutate `Ticket` → save → hand-build an event/reply inline with a string literal (`publishEvent("TicketCreated", ...)`).
- `KitchenEvent`: one flat record with a string `eventType` field — not a class per event.
- Only 3 states exist today: `CREATE_PENDING`, `AWAITING_ACCEPTANCE`, `CANCELLED`.
- No REST controller exists in kitchen-service at all.

## State machine

New `TicketState` enum (`@Enumerated(EnumType.STRING)` on the existing `status` column — same column, same string values already stored today, no schema migration):

```
CREATE_PENDING → AWAITING_ACCEPTANCE → ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP
       ↓                  ↓               ↓
   CANCELLED          CANCELLED       CANCELLED
```

- `CREATE_PENDING`, `AWAITING_ACCEPTANCE`: existing saga-driven pre-acceptance states, meaning unchanged.
- `ACCEPTED`, `PREPARING`, `READY_FOR_PICKUP`, `PICKED_UP`: new, restaurant-worker-driven via the new REST API.
- `cancel()` rules (extending the book's own two-tier distinction in Listing 5.11 to this app's extra pre-accept state):
  - Legal from `CREATE_PENDING`, `AWAITING_ACCEPTANCE`, `ACCEPTED` → transitions to `CANCELLED`, returns `TicketCancelledEvent`.
  - From `READY_FOR_PICKUP` → throws `TicketCannotBeCancelledException` (a foreseeable business case: too late to cancel).
  - From `PREPARING`, `PICKED_UP`, `CANCELLED` → throws `UnsupportedStateTransitionException` (a bug, not a business case — nothing in this app should ever attempt these).
- Worker-progression methods (`accept`, `preparing`, `readyForPickup`, `pickedUp`) each accept exactly one legal predecessor state and throw `UnsupportedStateTransitionException` otherwise, matching `Ticket.preparing()` in Listing 5.11.

## Domain events

One class per event, all implementing a `TicketDomainEvent` marker interface (matching `OrderDomainEvent`/`DomainEvent` in Listing 5.1):

`TicketCreatedEvent`, `TicketCreationFailedEvent`, `TicketConfirmedEvent`, `TicketCancelledEvent`, `TicketAcceptedEvent`, `TicketPreparingStartedEvent`, `TicketReadyForPickupEvent`, `TicketPickedUpEvent`.

`TicketConfirmedEvent`/`TicketCancelledEvent` replace today's string-literal `"TicketConfirmed"`/`"TicketCancelled"` outbox rows with real types. Every state-changing `Ticket` method returns `List<TicketDomainEvent>` (the book's preferred style over an `AbstractAggregateRoot` superclass — Listing 5.9's own stated tradeoff: a superclass couples aggregates to a base type and complicates non-root methods registering events).

Two static factories on `Ticket` own all valid construction (no external code builds a `Ticket` directly):
- `Ticket.createTicket(orderId, lineItems)` → `TicketCreationResult(Ticket ticket, List<TicketDomainEvent> events)` — normal creation, returns a `TicketCreatedEvent`.
- `Ticket.createCancelled(orderId)` → for the existing "order already known-failed" bypass path (`FailedOrderRepository` hit) — still an explicit named factory, not `new Ticket(orderId, "CANCELLED")`. Returns no events (matches current behavior: this path never published anything).

Capacity rejection (`TicketCreationFailedEvent` when `totalQuantity` exceeds `KITCHEN_CAPACITY_LIMIT`) stays a **`TicketService`-level guard**, evaluated before calling `Ticket.createTicket(...)` — kitchen capacity is external, cross-cutting state (how many other tickets currently exist) that the aggregate has no access to and the book doesn't model. No `Ticket` instance exists in this path; `TicketService` constructs the `TicketCreationFailedEvent` directly, as it does today.

## Publisher

New `TicketDomainEventPublisher`, following the book's `AbstractAggregateDomainEventPublisher`/`TicketDomainEventPublisher` (Listing 5.6–5.9), wrapping the existing `ftgo-common` `OutboxEventRepository`:
- Resolves each event's outbox `eventType` string from its class (`TicketAcceptedEvent` → `"TicketAccepted"`, via `getClass().getSimpleName()` with the `Event` suffix stripped where the existing wire vocabulary doesn't have it — e.g. `TicketCreatedEvent` → `"TicketCreated"`, `TicketCancelledEvent` → `"TicketCancelled"` — matching the exact strings already in use today so no downstream consumer's `case "TicketCreated" ->` in `order-service`'s choreography listener needs to change).
- Publishes to topic `kitchen.events` (unchanged).
- All REST-driven worker actions and all choreography-mode transitions in `TicketService` publish through it.

## Service layer — choreography vs. orchestration unchanged in shape

`Ticket`'s methods only know domain events; they have no awareness of saga mode. The existing dual-mode split in `TicketService` stays exactly as structured today:
- **Choreography-mode call sites** (`handleOrderCreated`, `handleAccountingEvent`, `handleConsumerVerificationFailed`) publish the returned domain events directly via `TicketDomainEventPublisher` → `kitchen.events`.
- **Orchestration-mode call sites** (`handleCreateTicketCommand`, `handleConfirmTicketCommand`, `handleCancelTicketCommand`) keep translating the same returned events into `SagaReply` objects on `saga.replies` — a small event-class → reply-type-string mapping replaces today's hardcoded strings, but the translation step itself doesn't disappear (the two channels carry genuinely different envelopes: domain event vs. command reply).

## REST API (new)

`TicketController`, kitchen-service's first REST controller:

| Method | Path | Body | Aggregate call |
|---|---|---|---|
| POST | `/tickets/{ticketId}/accept` | `{ "readyBy": "<ISO datetime>" }` | `ticket.accept(readyBy)` |
| POST | `/tickets/{ticketId}/preparing` | — | `ticket.preparing()` |
| POST | `/tickets/{ticketId}/ready-for-pickup` | — | `ticket.readyForPickup()` |
| POST | `/tickets/{ticketId}/picked-up` | — | `ticket.pickedUp()` |

Each handler: `TicketRepository.findById(ticketId)` (existing `JpaRepository` method, no new repository method needed) → throw `TicketNotFoundException` if absent → call the aggregate method → save → publish returned events via `TicketDomainEventPublisher`.

Error mapping (`@ExceptionHandler` in the controller, or a small `@ControllerAdvice` if that fits the project's existing conventions better — no other service in this app has a REST error-handling precedent to match, so this is a fresh, minimal decision):
- `TicketNotFoundException` → 404
- `TicketCannotBeCancelledException`, `UnsupportedStateTransitionException` → 409

## Data model

No schema changes. `status` column keeps its name and existing string values; `@Enumerated(EnumType.STRING)` just adds compile-time-checked values on top of what's already stored. No new columns needed for the worker lifecycle — `Ticket` doesn't currently persist `readyBy`/timestamps per state transition, and adding that is out of scope (not required by any consumer; the book persists them in `Ticket` for the restaurant UI, which this app doesn't have).

## Testing

TDD, per this project's established convention:
- **`Ticket` unit tests**: every legal transition (state changes correctly, correct event(s) returned) and every illegal transition (correct exception type — `TicketCannotBeCancelledException` vs. `UnsupportedStateTransitionException`) for all 8 states/transitions in the diagram above.
- **`TicketServiceTest`** (existing file, 14 tests): updated for the new event types and `Ticket` factories, same behavioral coverage as today (dedup, capacity rejection, failed-order bypass, choreography vs. orchestration publishing).
- **New `TicketControllerTest`**: happy path for all 4 endpoints, 404 (unknown ticket), 409 (illegal transition e.g. `accept` on an already-`PICKED_UP` ticket).
- **Manual e2e verification via Docker**, matching this project's established pattern for anything touching Kafka/message wiring:
  1. Choreography happy path — confirm `Ticket` still reaches `AWAITING_ACCEPTANCE` via the saga, then manually drive it through `ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP` via the new REST API.
  2. Orchestration happy path — same saga-driven states, confirm `SagaReply` translation still produces identical `Order`/`Ticket`/`Authorization` end states as before this change.
  3. One compensation case (e.g. consumer-verification failure) — confirms `cancel()`'s new invariant doesn't break the existing compensation path.
  4. Redelivery/idempotency spot-check — confirms `ProcessedEvent` dedup still prevents double-processing with the new `Ticket` factories in place.

## Deferred (not in this pass)

- Staff-initiated `reject()` — no existing caller or business policy; see Goal section.
- Persisting `readyBy`/per-transition timestamps on `Ticket` — no consumer needs them yet.
- `Order` aggregate refactor (order-service) — the book's other worked example; a separate, larger piece of work (entangled with the saga orchestrator/choreography code) explicitly deferred to a future session.
