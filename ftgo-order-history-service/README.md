# ftgo-order-history-service

**Port:** 8088
**Bounded context:** Order history / order view (read model — no aggregate, no write-side business logic)

## Role

This service is this project's second query pattern (Ch.7), and a deliberate contrast with the first (`GET /orders/{id}/view` API composition on order-service, see `docs/ARCHITECTURE.md`). Instead of composing a response at request time from four live synchronous calls, it maintains its own denormalized, pre-joined `order_views` table by consuming the same four domain-event topics every other saga participant already publishes to (`order.events`/`kitchen.events`/`accounting.events`/`delivery.events`), and answers `GET /order-views/{orderId}` straight out of that table — no downstream call of any kind at request time.

Consequently this service has **no Eureka registration** and **makes no synchronous call to anything** — order-service, kitchen-service, accounting-service, and delivery-service never even know it exists. It is a pure Kafka consumer plus a REST query endpoint, the simplest shape any service in this codebase has.

## API

**`GET /order-views/{orderId}`** — **Auth:** `ADMIN` (bearer JWT issued by `ftgo-authorization-server`, Ch.11 §11.1; validated by this service as an OAuth2 resource server). Returns `200` with an `OrderViewResponse` if a row exists for that `orderId`, `404` otherwise. There is no `POST`/`PUT`/`DELETE` — this service never originates a write, only reacts to events.

```json
{
  "orderId": 42,
  "consumerId": 7,
  "restaurantId": 3,
  "orderStatus": "APPROVED",
  "ticketStatus": "AWAITING_ACCEPTANCE",
  "authorizationStatus": "AUTHORIZED",
  "deliveryStatus": "SCHEDULED",
  "courierId": 2,
  "lineItems": [
    { "menuItemId": 101, "quantity": 2 }
  ]
}
```

`restaurantId` is carried through from `OrderCreated` unchanged and never resolved to a restaurant name/address — this service has no dependency on restaurant-service at all, deliberately: restaurant-service is the one service in this codebase that publishes no domain events, so there is no event this service could react to even if it wanted to enrich the view with restaurant details. `restaurantId` is opaque data here, exactly as it arrives on the wire.

## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only (no custom
`HealthIndicator` code). Reports:
- `db` — MySQL reachability via the service's `DataSource`.
- `discoveryComposite` — Eureka registration status.

There is no `kafka` component: Spring Boot's actuator-autoconfigure no longer ships a Kafka
health contributor as of this project's Spring Boot version (3.5.16) — verified directly against
the built jars (`spring-boot-actuator-autoconfigure` retains only `KafkaMetricsAutoConfiguration`
under `actuate.autoconfigure.kafka`; `spring-kafka` ships no health-indicator class either) — and
adding a custom one is out of scope for this sub-project.

`management.endpoint.health.show-details: always` — safe here since these ports aren't exposed
to untrusted clients in this project; full component detail is the point of exercising this
pattern. Verified against the real, running stack by `ftgo-end-to-end-test`'s
`AllServicesReportHealthy.feature`.

## Metrics (Ch.11, §11.3.4)

`GET /actuator/prometheus` — Micrometer `PrometheusMeterRegistry`, unauthenticated. Custom business
counter:

- `order_views_updated` — `OrderViewService`, on each order-view update.

Appears in the exposition output with a `_total` suffix (`order_views_updated_total`). Scraped
every 5s by the `prometheus` compose service.

## Tracing (Ch.11, §11.3.3)

Traces exported via OTLP/HTTP to Grafana Tempo (`http://tempo:4318/v1/traces`), 100% sampled
(`management.tracing.sampling.probability: 1.0`). HTTP and JDBC spans come free from Spring Boot's
autoconfiguration. This service publishes nothing, so only `spring.kafka.listener.observation-enabled: true`
is set — but that property alone isn't enough here: `KafkaConsumerConfig` hand-builds a
`ConcurrentKafkaListenerContainerFactory` bean (to get retry behavior for optimistic-lock races
across its four listeners — see below), and a hand-built factory bypasses Boot's property-driven
autoconfiguration of the default listener factory. The fix is an explicit
`factory.getContainerProperties().setObservationEnabled(true)` call in that same `@Bean` method.
Viewable in Grafana via the provisioned Tempo datasource, or queried directly against Tempo's
search API.

## Events consumed

One `@KafkaListener` per topic, all sharing Kafka consumer group `order-history-service`, all deserializing with Jackson and routing into one shared `OrderViewService`:

| Topic | Listener | eventType values handled |
|---|---|---|
| `order.events` | `OrderEventListener` | `OrderCreated`, `OrderApproved`, `OrderRejected`, `OrderCancelled`, `OrderCancelConfirmed`, `OrderCancelRejected`, `OrderRevisionProposed`, `OrderRevised`, `OrderRevisionRejected` |
| `kitchen.events` | `KitchenEventListener` | `TicketCreated`, `TicketConfirmed`, `TicketAccepted`, `TicketPreparingStarted`, `TicketReadyForPickup`, `TicketPickedUp`, `TicketCancelled` |
| `accounting.events` | `AccountingEventListener` | `CardAuthorized`, `CardAuthorizationFailed`, `AuthorizationReversed`, `AuthorizationRevised` |
| `delivery.events` | `DeliveryEventListener` | `DeliveryScheduled`, `DeliveryPickedUp`, `DeliveryDelivered`, `DeliveryCancelled` |

Every other `eventType` on these four topics (`TicketCreationFailed`, `TicketCancellationRejected`, `TicketRevisionRejected`, `TicketRevisionUndone`, `TicketQuantityRevised`, `AuthorizationRevisionRejected`, `DeliverySchedulingFailed`, and the wire-only `OrderRevisionCompensationRequested` pseudo-event from Ch.6's event-sourcing work) falls through each handler's `default -> { }` branch untouched — either because nothing was ever created/scheduled to begin with (a `*Failed` event), the event doesn't represent a new lifecycle state this read model tracks (`TicketQuantityRevised` — quantity isn't tracked here at all), or it's a rejection of an in-flight attempt that leaves the existing state unchanged.

Each listener does the minimum: deserialize the topic's flat wire-format record (`OrderEvent`/`KitchenEvent`/`AccountingEvent`/`DeliveryEvent` — the same per-service record shapes every other consumer of these topics already uses, copy-pasted here rather than shared, matching this codebase's existing convention of keeping wire-format records per-consumer), then hand `eventId`/`eventType`/`orderId` (plus whatever event-specific fields matter) to the matching `OrderViewService.handle*Event` method. A malformed payload is logged and skipped, never rethrown — one bad message on a shared topic must not stop this consumer's group from making progress on the rest.

## Domain model

`OrderView` (`@Entity`, table `order_views`, `@Id orderId`) is not a DDD aggregate — it has no invariants, no guarded state transitions, and no domain events of its own. It is a plain denormalized projection: `consumerId`, `restaurantId`, `orderStatus`, `ticketStatus`, `authorizationStatus`, `deliveryStatus`, `courierId`, plus an `@ElementCollection` of `OrderViewLineItem`s (`menuItemId`, `quantity`) in a separate `order_view_line_items` table. Every field is a plain setter target — `OrderViewService` is where all the logic lives, not the entity.

## The upsert-on-any-event pattern (and why)

`OrderViewService` has one method per topic (`handleOrderEvent`/`handleKitchenEvent`/`handleAccountingEvent`/`handleDeliveryEvent`), and every one of them follows the identical shape:

```java
OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));
switch (eventType) {
    case "SomeEventType" -> view.setSomeField(...);
    ...
}
orderViewRepository.save(view);
```

This is an **upsert, never a create-only insert** — any of the four handlers can be the one that creates the `order_views` row for a given `orderId`, not just `OrderCreated`. This is forced by a real constraint: Kafka gives no ordering guarantee *across* topics (only within a single topic-partition), and `order.events`/`kitchen.events`/`accounting.events`/`delivery.events` are four independent topics with four independent consumer offsets. A ticket, authorization, or delivery event can genuinely arrive and be processed before this service has seen `OrderCreated` for that same `orderId` — e.g. if this service's consumer group falls behind on `order.events` specifically, or restarts and catches up unevenly across partitions. If `handleKitchenEvent` assumed a row already existed, it would throw or silently drop the event. Instead, every handler creates a stub row (`new OrderView(orderId)`, every field left null/unset) if one doesn't exist yet, and fills in only the fields it owns — later events, from any of the four sources, in any order, keep filling in the rest.

The corollary: this table can be — and often briefly is — **incomplete**. A `GET /order-views/{orderId}` response the instant after `TicketCreated` arrives but before `OrderCreated` has been consumed will show `ticketStatus` populated with `consumerId`/`restaurantId`/`orderStatus`/`lineItems` still null. This is the same eventual-consistency tradeoff every CQRS read model makes (see `docs/ARCHITECTURE.md`'s CQRS section) — it resolves itself within one Kafka poll interval as the remaining events are consumed, never requiring any read-repair logic.

## Idempotency & reliability

Every handler dedupes via the same `processed_events` ledger (`ftgo-common`'s `ProcessedEventRepository`) every other consumer in this codebase already uses: `existsById(eventId)` check, insert, *then* act, all in one `@Transactional` method — protects against Kafka's at-least-once redelivery causing a double-apply (e.g. `TicketPickedUp` arriving twice would be harmless here since it's idempotent by nature, but `DeliveryScheduled` setting `courierId` twice on a redelivered message with a stale payload would not be, so the dedup ledger is applied uniformly rather than reasoned about per-event-type).

This service has no outbox of its own — it never publishes anything. It's a pure consumer, so `ftgo-common`'s `OutboxPublisher`/`KafkaProducerConfig` auto-configuration is present (since it depends on `ftgo-common` the same way every other service does) but has nothing to poll, since no service code ever writes an `OutboxEvent` row here.

### Concurrent writers and optimistic locking

`order_views` has an externally-assigned `@Id` (`orderId`), so every `save()` goes through `EntityManager.merge()` rather than a true INSERT-then-dirty-checking path — `merge()` writes every column, not just the ones the current handler touched. Because the four `@KafkaListener`s run on independent consumer threads, two of them can race to update the same row (e.g. `OrderCreated` and `TicketCreated` for the same `orderId` landing close together); without protection, whichever transaction commits second would silently overwrite the first's columns with its own stale read snapshot. `OrderView` carries a `@Version` field (the same optimistic-locking pattern `ftgo-accounting-service`'s `SagaJoinState` already uses) so the losing writer's `merge()` throws `OptimisticLockingFailureException` instead. `KafkaConsumerConfig` configures the listener container factory's `DefaultErrorHandler` with a short `FixedBackOff` (3 retries, 4 attempts total, 200ms apart) so that exception triggers a retry of the whole listener invocation — since each `OrderViewService.handle*Event` method re-reads via `findById` at the top of its own `@Transactional` call, the retry sees the winner's already-committed write rather than reusing a stale entity.

## Running standalone

```bash
./gradlew :ftgo-order-history-service:test
```

Needs the full docker-compose stack (MySQL, Kafka, and at least order-service/kitchen-service/accounting-service/delivery-service publishing to their respective topics) to exercise live — see the root [README](../README.md) for `docker compose up`.

Key environment variables (see `application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `SPRING_DATASOURCE_URL` | `jdbc:mysql://localhost:3306/ftgo_order_history` | MySQL connection |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `SERVER_PORT` | `8088` | HTTP port |

No `SAGA_MODE` and no `eureka.client.*` config — this service isn't a saga participant in either style, and isn't discoverable or discovering anything.
