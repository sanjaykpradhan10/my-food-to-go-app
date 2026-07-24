# ftgo-delivery-service

**Port:** 8086
**Bounded context:** Delivery tracking (separate bounded context from Order — Ubiquitous Language: Delivery, not Order, per Ch.2 decomposition)

## Role

The delivery's view of a customer order is not "an order" — it's a delivery to schedule, assign a courier to, and track through pickup/drop-off, correlated by `orderId` but never sharing a model with `Order`. This service is the Create Order saga's **3rd parallel-join leg** (alongside consumer verification and ticket creation — accounting's authorization waits on all three) and the Cancel Order saga's **delivery-release step**, inserted between kitchen's ticket cancellation and accounting's authorization reversal.

Most interaction is Kafka-driven, either reacting to another service's domain events (choreography) or to explicit commands from order-service's orchestrator (orchestration), selected per-deployment by `SAGA_MODE`. This service also exposes a small REST API for the courier's own pickup/delivered lifecycle, which is unrelated to saga mode.

## API

**`POST /deliveries/{id}/picked-up`** — legal only from `SCHEDULED` — moves to `PICKED_UP`.

**`POST /deliveries/{id}/delivered`** — legal only from `PICKED_UP` — moves to `DELIVERED`.

Both: `404` if the delivery doesn't exist, `409` on an illegal transition.

## Events

### Publishes (`delivery.events`, choreography)

| eventType | When |
|---|---|
| `DeliveryScheduled` | `OrderCreated` received and a courier is available — delivery-service's own parallel-join leg |
| `DeliverySchedulingFailed` | `OrderCreated` received but no courier is available |
| `DeliveryPickedUp` | Courier REST API call (`/picked-up`) |
| `DeliveryDelivered` | Courier REST API call (`/delivered`) |
| `DeliveryCancelled` | Cancel Order saga's delivery-release step, or Create Order compensation (consumer/kitchen/accounting failure) |

`DeliveryPickedUp`/`DeliveryDelivered` publish to `delivery.events` unconditionally, regardless of `SAGA_MODE` — the courier REST API isn't saga-triggered, so there's no orchestration-mode reply equivalent for these two.

### Publishes (`saga.replies`, orchestration)

| Reply eventType | Reply to command | sagaType |
|---|---|---|
| `DeliveryScheduled` / `DeliverySchedulingFailed` | `ScheduleDelivery` | `CreateOrder` |
| `DeliveryCancelled` | `ReleaseDelivery` | `CreateOrder` (compensation) or `CancelOrder` (primary flow) — the command's own `sagaType` is echoed back, the same discriminator-forwarding pattern kitchen's `CancelTicket` handler already uses, since this handler can't infer which saga it's servicing from `commandType` alone |

`ReleaseDelivery` has no decline path — releasing a courier back to the pool always succeeds once a `Delivery` row exists, so `handleReleaseDeliveryCommand` never needs a rejection branch the way `ScheduleDelivery` does.

### Consumes

| Mode | Topic | Reacts to |
|---|---|---|
| Choreography | `order.events` | `OrderCreated` — schedule the delivery (this service's own parallel-join leg, not gated on consumer/kitchen) |
| Choreography | `consumer.events` | `ConsumerVerificationFailed` — release (Create Order compensation) |
| Choreography | `kitchen.events` | `TicketCreationFailed` (Create Order compensation) / `TicketCancelled` (Cancel Order's primary trigger) — both funnel into the same `release()` |
| Choreography | `accounting.events` | `CardAuthorizationFailed` only — release (Create Order compensation) |
| Orchestration | `delivery.commands` | `ScheduleDelivery` / `ReleaseDelivery`, one listener (`DeliveryCommandListener`) dispatching on `commandType` |

## Domain model

`Delivery` — `id`, `orderId`, `restaurantId`, `courierId`, `status` (`DeliveryStatus`) (persisted). No `Delivery` row is ever constructed outside `SCHEDULED` — a decline (no courier available) never persists a row at all, mirroring `Ticket.createTicket`/`TicketCreationFailed`, so there's no separate "pending" starting state to guard against.

`DeliveryStatus`: `SCHEDULED → PICKED_UP → DELIVERED`, or `CANCELLED` (legal only from `SCHEDULED` — cancellation after pickup isn't modeled, since Cancel Order can only reach an already-`APPROVED` order, and by then the delivery is still `SCHEDULED` in practice). State-changing methods (`pickUp()`, `deliver()`, `cancel()`) return class-per-event `DeliveryDomainEvent`s (sealed interface), same pattern as `Ticket`/`Order`/`Authorization`.

`Courier` — `id`, `name`, `available` (persisted). Seeded with a fixed pool of 3 (`Alex`/`Bailey`/`Casey`) by `CourierSeeder` (a `CommandLineRunner`, idempotent — skips seeding if any courier already exists). `DeliveryService.schedule`/`handleScheduleDeliveryCommand` pick the first available courier (`findFirstByAvailableTrue`), flip it unavailable, and assign it to the new `Delivery`; releasing a delivery (`release()`/`handleReleaseDeliveryCommand`) flips the assigned courier back to available. No courier-selection strategy beyond "first available" — this is a learning-exercise simplification, not a real dispatch algorithm.

**Capacity check**: unlike kitchen's quantity-based capacity limit or accounting's quantity-based authorization threshold, delivery scheduling has a binary decline condition — courier pool exhausted (all 3 unavailable) — with no quantity dimension at all, since a delivery either has an available courier or it doesn't.

## Idempotency & reliability

Every handler dedupes via a `processed_events` ledger (insert-then-act in one local transaction) before touching any other state — protects against Kafka's at-least-once redelivery.

**`FailedOrder`** (choreography only, same as kitchen's equivalent table): records an `orderId` when `release()` (triggered by `ConsumerVerificationFailed`, `TicketCreationFailed`, or `CardAuthorizationFailed`) is called for an order this service hasn't scheduled a delivery for yet — the same genuine race this codebase already solved for kitchen's `Ticket` (`handleOrderCreated`/`FailedOrder` in `ftgo-kitchen-service`): consumer verification, ticket creation, and delivery scheduling all happen in parallel, reacting independently to the same `OrderCreated` event, so a fast-failing sibling leg can race ahead of this service's own scheduling. When `OrderCreated` does eventually arrive, `handleOrderCreated` checks this table first and skips scheduling entirely — a narrower response than kitchen's, which creates the ticket directly as `CANCELLED` rather than skipping creation outright, since `Delivery` scheduling has no equivalent "create anyway, already cancelled" step. Orchestration mode has no equivalent table: `handleReleaseDeliveryCommand` is a no-op if no `Delivery` row exists yet, since the orchestrator only ever sends `ReleaseDelivery` after it already has confirmation the delivery was scheduled, so this race can't occur on that path.

This service's outbox/producer capability (`OutboxEvent`, `OutboxPublisher`, `KafkaProducerConfig` — shared via the `ftgo-common` module, see the root `docs/ARCHITECTURE.md`) is used from the start, unlike kitchen-service which only gained it partway through Ch.4. `OutboxEvent` carries a `topic` column per row, since this service writes to two different topics (`delivery.events` in choreography mode, `saga.replies` in orchestration mode) from one outbox table.

## Running standalone

```bash
./gradlew :ftgo-delivery-service:test
```

Needs the full docker-compose stack (MySQL, Kafka, and at least order-service publishing `order.events`/`delivery.commands`) to exercise live — see the root [README](../README.md) for `docker compose up`.

Key environment variables (see `application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `SPRING_DATASOURCE_URL` | `jdbc:mysql://localhost:3306/ftgo_delivery` | MySQL connection |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `SAGA_MODE` | `choreography` | `choreography` or `orchestration` — selects which `@ConditionalOnProperty`-gated listener set is active |
| `SERVER_PORT` | `8086` | HTTP port |
