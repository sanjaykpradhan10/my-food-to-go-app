# Order Event Sourcing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the `Order` aggregate (`ftgo-order-service`) to event sourcing as a switchable
`PERSISTENCE_MODE=jpa|event-sourcing` mode, covering the full state machine (create, cancel,
revise) and both saga modes (choreography, orchestration), without touching the existing
`jpa`-mode behavior.

**Architecture:** A new non-JPA `OrderAggregate` class in package
`com.sanjay.ftgo.order.eventsourcing` implements the book's `process(Command)` (pure,
validates) / `apply(Event)` (mutates) split, reusing the existing `OrderDomainEvent` sealed
hierarchy. A hand-rolled `OrderEventStore` persists/replays it via three new tables
(`order_events`, `order_aggregate_version`, `order_snapshots`) plus a tiny `order_id_allocations`
ID-minting table. A new `OrderTransitions` facade interface (implementations `JpaOrderTransitions`
/ `EventSourcedOrderTransitions`) replaces direct `OrderRepository` use in the controller, saga
services, and orchestrators, selected via `@ConditionalOnProperty(persistence.mode=...)` exactly
like the existing `saga.mode` trigger pattern. `order_events` payloads store the *wire-format*
`OrderEvent` JSON (not the raw domain event), so the existing Ch.3 Debezium CDC pipeline can tail
it unchanged and no consumer in kitchen-service/accounting-service needs to change.
Orchestration-mode saga commands go through a second new facade (`SagaCommandPublisher`) with a
book-faithful two-step pseudo-event mechanism (`order_saga_command_requests` table + a
`@Scheduled` poller) instead of the JPA path's direct outbox write.

**Tech Stack:** Java 17, Spring Boot 3.5.16, Spring Data JPA, Jackson (records, no extra module
needed — already proven by `SagaReply`/`KitchenCommand`), JUnit 5 + AssertJ + Mockito, H2
(`MODE=MySQL`) for tests, MySQL + Debezium/Kafka Connect for the real event relay.

## Global Constraints

- No new external dependencies (no Eventuate Tram/Client/Local) — hand-rolled, per
  `docs/superpowers/specs/2026-07-22-order-event-sourcing-design.md`.
- `PERSISTENCE_MODE=jpa` is the default; all existing `jpa`-mode tests and behavior must stay
  green and unchanged.
- `order_events.payload` must contain the same `OrderEvent` wire-format JSON the CDC pipeline
  already forwards to `order.events` today — kitchen-service and accounting-service consumers get
  zero changes.
- New JPA entities/repositories must be added to `PersistenceConfig`'s `@EntityScan`/
  `@EnableJpaRepositories` base packages (kept in a separate `@Configuration` class, never on
  `@SpringBootApplication`, so `@WebMvcTest` slices keep working — see
  `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/config/PersistenceConfig.java`).
- All DDL is Hibernate-managed (`spring.jpa.hibernate.ddl-auto: update` in main, `create-drop` in
  test) — no Flyway/Liquibase in this project; new tables just need `@Entity` classes.
- Follow existing code comment convention: only comment non-obvious *why*, never *what*.

---

### Task 1: Extract `OrderEventSerializer`, add `OrderCreatedEvent`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderEventSerializer.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEvent.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderEventSerializerTest.java`

**Interfaces:**
- Produces: `OrderCreatedEvent(Long orderId, Long consumerId, Long restaurantId, List<OrderLineItem> lineItems) implements OrderDomainEvent`; `OrderEventSerializer.toWireEvent(String eventId, OrderDomainEvent event): OrderEvent`, `.fromWireEvent(OrderEvent wireEvent): OrderDomainEvent`, `.toJson(OrderEvent event): String`, `.fromJson(String json): OrderEvent`.
- Consumes: existing `OrderEvent` wire record and `OrderLineItem` (both already in `com.sanjay.ftgo.order.domain`).

This event is new — order creation today bypasses the sealed hierarchy via
`OrderDomainEventPublisher.publishOrderCreated`. Adding `OrderCreatedEvent` to the sealed
interface is required so `OrderAggregate` (Task 3) can create orders through the same
`process()`/`apply()` pattern as every other transition. `publishOrderCreated` itself is left
completely untouched in this task — only the sealed interface and the exhaustive switches grow a
case.

- [ ] **Step 1: Write `OrderCreatedEvent`**

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderCreatedEvent(Long orderId, Long consumerId, Long restaurantId,
                                 List<OrderLineItem> lineItems) implements OrderDomainEvent {
}
```

- [ ] **Step 2: Add it to the sealed interface**

Edit `OrderDomainEvent.java`:

```java
package com.sanjay.ftgo.order.domain;

public sealed interface OrderDomainEvent
        permits OrderCreatedEvent, OrderApprovedEvent, OrderRejectedEvent, OrderCancelledEvent,
                OrderCancelConfirmedEvent, OrderCancelRejectedEvent, OrderRevisionProposedEvent,
                OrderRevisedEvent, OrderRevisionRejectedEvent {

    Long orderId();
}
```

- [ ] **Step 3: Write the failing serializer test**

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderEventSerializerTest {

    private final OrderEventSerializer serializer = new OrderEventSerializer(new com.fasterxml.jackson.databind.ObjectMapper());

    @Test
    void roundTripsOrderCreatedEvent() {
        OrderCreatedEvent event = new OrderCreatedEvent(42L, 1L, 2L, List.of(new OrderLineItem(10L, 3)));

        OrderEvent wire = serializer.toWireEvent("evt-1", event);
        assertThat(wire.eventType()).isEqualTo("OrderCreated");
        assertThat(wire.orderId()).isEqualTo(42L);
        assertThat(wire.consumerId()).isEqualTo(1L);
        assertThat(wire.restaurantId()).isEqualTo(2L);

        OrderDomainEvent roundTripped = serializer.fromWireEvent(wire);
        assertThat(roundTripped).isEqualTo(event);
    }

    @Test
    void roundTripsOrderApprovedEvent() {
        OrderApprovedEvent event = new OrderApprovedEvent(42L);

        OrderEvent wire = serializer.toWireEvent("evt-2", event);
        OrderDomainEvent roundTripped = serializer.fromWireEvent(wire);

        assertThat(roundTripped).isEqualTo(event);
    }

    @Test
    void roundTripsOrderRevisionProposedEvent() {
        OrderRevisionProposedEvent event = new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5)));

        OrderEvent wire = serializer.toWireEvent("evt-3", event);
        OrderDomainEvent roundTripped = serializer.fromWireEvent(wire);

        assertThat(roundTripped).isEqualTo(event);
    }

    @Test
    void jsonRoundTrip() {
        OrderEvent wire = new OrderEvent("evt-4", "OrderApproved", 42L, null, null, null);

        String json = serializer.toJson(wire);
        OrderEvent parsed = serializer.fromJson(json);

        assertThat(parsed).isEqualTo(wire);
    }
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderEventSerializerTest"`
Expected: FAIL — `OrderEventSerializer` does not exist yet.

- [ ] **Step 5: Write `OrderEventSerializer`**

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderEventSerializer {

    private final ObjectMapper objectMapper;

    public OrderEventSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public OrderEvent toWireEvent(String eventId, OrderDomainEvent event) {
        return switch (event) {
            case OrderCreatedEvent e -> new OrderEvent(eventId, "OrderCreated", e.orderId(),
                    e.consumerId(), e.restaurantId(), toWireLineItems(e.lineItems()));
            case OrderApprovedEvent e -> new OrderEvent(eventId, "OrderApproved", e.orderId(), null, null, null);
            case OrderRejectedEvent e -> new OrderEvent(eventId, "OrderRejected", e.orderId(), null, null, null);
            case OrderCancelledEvent e -> new OrderEvent(eventId, "OrderCancelled", e.orderId(), null, null, null);
            case OrderCancelConfirmedEvent e ->
                    new OrderEvent(eventId, "OrderCancelConfirmed", e.orderId(), null, null, null);
            case OrderCancelRejectedEvent e ->
                    new OrderEvent(eventId, "OrderCancelRejected", e.orderId(), null, null, null);
            case OrderRevisionProposedEvent e -> new OrderEvent(eventId, "OrderRevisionProposed", e.orderId(),
                    null, null, toWireLineItems(e.revisedLineItems()));
            case OrderRevisedEvent e -> new OrderEvent(eventId, "OrderRevised", e.orderId(),
                    null, null, toWireLineItems(e.revisedLineItems()));
            case OrderRevisionRejectedEvent e ->
                    new OrderEvent(eventId, "OrderRevisionRejected", e.orderId(), null, null, null);
        };
    }

    public OrderDomainEvent fromWireEvent(OrderEvent wireEvent) {
        return switch (wireEvent.eventType()) {
            case "OrderCreated" -> new OrderCreatedEvent(wireEvent.orderId(), wireEvent.consumerId(),
                    wireEvent.restaurantId(), toDomainLineItems(wireEvent.lineItems()));
            case "OrderApproved" -> new OrderApprovedEvent(wireEvent.orderId());
            case "OrderRejected" -> new OrderRejectedEvent(wireEvent.orderId());
            case "OrderCancelled" -> new OrderCancelledEvent(wireEvent.orderId());
            case "OrderCancelConfirmed" -> new OrderCancelConfirmedEvent(wireEvent.orderId());
            case "OrderCancelRejected" -> new OrderCancelRejectedEvent(wireEvent.orderId());
            case "OrderRevisionProposed" ->
                    new OrderRevisionProposedEvent(wireEvent.orderId(), toDomainLineItems(wireEvent.lineItems()));
            case "OrderRevised" -> new OrderRevisedEvent(wireEvent.orderId(), toDomainLineItems(wireEvent.lineItems()));
            case "OrderRevisionRejected" -> new OrderRevisionRejectedEvent(wireEvent.orderId());
            default -> throw new IllegalArgumentException("Unknown order event type: " + wireEvent.eventType());
        };
    }

    public String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order event " + event.eventType(), e);
        }
    }

    public OrderEvent fromJson(String json) {
        try {
            return objectMapper.readValue(json, OrderEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize order event payload", e);
        }
    }

    private List<OrderEvent.LineItem> toWireLineItems(List<OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    private List<OrderLineItem> toDomainLineItems(List<OrderEvent.LineItem> lineItems) {
        if (lineItems == null) {
            return null;
        }
        return lineItems.stream()
                .map(lineItem -> new OrderLineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }
}
```

- [ ] **Step 6: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderEventSerializerTest"`
Expected: PASS (4 tests)

- [ ] **Step 7: Delegate `OrderDomainEventPublisher` to the new serializer**

Edit `OrderDomainEventPublisher.java` — replace the private `toWireEvent`/`toJson`/`toWireLineItems`
methods with delegation, leaving `publishOrderCreated`/`publishRevisionCompensationRequested`/
`publish`/`publishEvent`/`save` untouched:

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class OrderDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventSerializer serializer;

    public OrderDomainEventPublisher(OutboxEventRepository outboxEventRepository, OrderEventSerializer serializer) {
        this.outboxEventRepository = outboxEventRepository;
        this.serializer = serializer;
    }

    public void publishOrderCreated(Order order, String eventId) {
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderCreated", order.getId(),
                order.getConsumerId(), order.getRestaurantId(), toWireLineItems(order.getLineItems()));
        save(eventId, wireEvent);
    }

    // Order itself doesn't transition here (it stays REVISION_PENDING until kitchen's undo is
    // confirmed), so this bypasses the OrderDomainEvent sealed interface entirely, same as
    // publishOrderCreated does for its own one-off case.
    public void publishRevisionCompensationRequested(Order order, String eventId) {
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderRevisionCompensationRequested", order.getId(),
                null, null, toWireLineItems(order.getLineItems()));
        save(eventId, wireEvent);
    }

    public void publish(List<OrderDomainEvent> events) {
        events.forEach(this::publishEvent);
    }

    private void publishEvent(OrderDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        save(eventId, serializer.toWireEvent(eventId, event));
    }

    private List<OrderEvent.LineItem> toWireLineItems(List<OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    private void save(String eventId, OrderEvent wireEvent) {
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "order.events", serializer.toJson(wireEvent)));
    }
}
```

Note `publishOrderCreated`/`publishRevisionCompensationRequested` keep their own private
`toWireLineItems` copy (kept local, not delegated) since they build `OrderEvent` directly from an
`Order` rather than from an `OrderDomainEvent` — no behavior change, purely mechanical.

- [ ] **Step 8: Run full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — `OrderDomainEventPublisherTest` and all other existing tests stay green (this
step is the regression check for Task 1's refactor).

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderEventSerializer.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderEventSerializerTest.java
git commit -m "refactor: extract OrderEventSerializer, add OrderCreatedEvent to sealed hierarchy"
```

---

### Task 2: `OrderCommand` marker interface and command records

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/CreateOrderCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/ApproveOrderCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/RejectOrderCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/CancelOrderCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/NoteOrderCancelledCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/UndoCancelCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/ReviseOrderCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/ConfirmRevisionCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/RejectRevisionCommand.java`

**Interfaces:**
- Consumes: `com.sanjay.ftgo.order.domain.OrderLineItem`, `com.sanjay.ftgo.order.domain.OrderRevision`.
- Produces: `OrderCommand` marker interface and the 9 records below, consumed by `OrderAggregate` (Task 3).

No test for this task — plain data records with no logic. Verified by Task 3's tests, which
construct and pass them.

- [ ] **Step 1: Write the marker interface**

```java
package com.sanjay.ftgo.order.eventsourcing;

public interface OrderCommand {
}
```

- [ ] **Step 2: Write the 9 command records**

`CreateOrderCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderLineItem;

import java.util.List;

public record CreateOrderCommand(Long orderId, Long consumerId, Long restaurantId,
                                  List<OrderLineItem> lineItems) implements OrderCommand {
}
```

`ApproveOrderCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

public record ApproveOrderCommand() implements OrderCommand {
}
```

`RejectOrderCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

public record RejectOrderCommand() implements OrderCommand {
}
```

`CancelOrderCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

public record CancelOrderCommand() implements OrderCommand {
}
```

`NoteOrderCancelledCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

public record NoteOrderCancelledCommand() implements OrderCommand {
}
```

`UndoCancelCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

public record UndoCancelCommand() implements OrderCommand {
}
```

`ReviseOrderCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderRevision;

public record ReviseOrderCommand(OrderRevision revision) implements OrderCommand {
}
```

`ConfirmRevisionCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

public record ConfirmRevisionCommand() implements OrderCommand {
}
```

`RejectRevisionCommand.java`:
```java
package com.sanjay.ftgo.order.eventsourcing;

public record RejectRevisionCommand() implements OrderCommand {
}
```

- [ ] **Step 3: Compile check**

Run: `./gradlew :ftgo-order-service:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/CreateOrderCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/ApproveOrderCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/RejectOrderCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/CancelOrderCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/NoteOrderCancelledCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/UndoCancelCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/ReviseOrderCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/ConfirmRevisionCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/RejectRevisionCommand.java
git commit -m "feat: add OrderCommand types for event-sourced Order aggregate"
```

---

### Task 3: `OrderAggregate` (process/apply for all 9 transitions)

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSnapshotData.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregate.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregateTest.java`

**Interfaces:**
- Consumes: `OrderCommand` subtypes (Task 2); `com.sanjay.ftgo.order.domain.{OrderDomainEvent,OrderStatus,OrderLineItem,OrderRevision,UnsupportedStateTransitionException,OrderCannotBeCancelledException,OrderCreatedEvent,OrderApprovedEvent,...}` (Task 1 + pre-existing).
- Produces: `OrderAggregate` — no-arg constructor, `getId()/getConsumerId()/getRestaurantId()/getLineItems()/getStatus()/getPendingRevisedLineItems()`, one `process(X): List<OrderDomainEvent>` per command type, one `apply(X)` per event type, a generic `apply(OrderDomainEvent)` dispatcher, `toSnapshotData(): OrderSnapshotData`, static `fromSnapshot(OrderSnapshotData): OrderAggregate`. Consumed by `OrderEventStore` (Task 5).

This is the core of the chapter's material — mirrors `OrderTest.java`'s existing style (plain
JUnit5+AssertJ, `orderIn(status)`-style helper) but split into two assertions per transition:
`process()` returns the right events without mutating state, and `apply()` produces the right
state from a given event.

- [ ] **Step 1: Write `OrderSnapshotData`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderStatus;

import java.util.List;

public record OrderSnapshotData(Long id, Long consumerId, Long restaurantId, List<OrderLineItem> lineItems,
                                 OrderStatus status, List<OrderLineItem> pendingRevisedLineItems) {
}
```

- [ ] **Step 2: Write the failing test file**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCancelConfirmedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderCreatedEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderRevisedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderAggregateTest {

    private OrderAggregate aggregateIn(OrderStatus status) {
        OrderAggregate aggregate = new OrderAggregate();
        aggregate.apply(new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2))));
        if (status != OrderStatus.APPROVAL_PENDING) {
            reachStatus(aggregate, status);
        }
        return aggregate;
    }

    private void reachStatus(OrderAggregate aggregate, OrderStatus status) {
        switch (status) {
            case APPROVED -> aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
            case REJECTED -> aggregate.apply(new com.sanjay.ftgo.order.domain.OrderRejectedEvent(42L));
            case CANCEL_PENDING -> {
                aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
                aggregate.apply(new OrderCancelledEvent(42L));
            }
            case CANCELLED -> {
                aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
                aggregate.apply(new OrderCancelledEvent(42L));
                aggregate.apply(new OrderCancelConfirmedEvent(42L));
            }
            case REVISION_PENDING -> {
                aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
                aggregate.apply(new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5))));
            }
            default -> throw new IllegalArgumentException("Unsupported target status in test: " + status);
        }
    }

    @Test
    void processCreateOrderCommandEmitsOrderCreatedEvent() {
        OrderAggregate aggregate = new OrderAggregate();
        CreateOrderCommand command = new CreateOrderCommand(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)));

        List<OrderDomainEvent> events = aggregate.process(command);

        assertThat(events).containsExactly(new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2))));
        assertThat(aggregate.getStatus()).isNull();
    }

    @Test
    void applyOrderCreatedEventInitializesState() {
        OrderAggregate aggregate = new OrderAggregate();

        aggregate.apply(new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2))));

        assertThat(aggregate.getId()).isEqualTo(42L);
        assertThat(aggregate.getConsumerId()).isEqualTo(1L);
        assertThat(aggregate.getRestaurantId()).isEqualTo(1L);
        assertThat(aggregate.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void processApproveOrderCommandFromApprovalPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new ApproveOrderCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void processApproveOrderCommandFromWrongStatusThrows() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVED);

        assertThatThrownBy(() -> aggregate.process(new ApproveOrderCommand()))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void processRejectOrderCommandFromApprovalPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new RejectOrderCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new com.sanjay.ftgo.order.domain.OrderRejectedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    void processCancelOrderCommandFromApproved() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVED);

        List<OrderDomainEvent> events = aggregate.process(new CancelOrderCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderCancelledEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
    }

    @Test
    void processCancelOrderCommandFromWrongStatusThrowsOrderCannotBeCancelled() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVAL_PENDING);

        assertThatThrownBy(() -> aggregate.process(new CancelOrderCommand()))
                .isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void processNoteOrderCancelledCommandFromCancelPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new NoteOrderCancelledCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderCancelConfirmedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void processUndoCancelCommandFromCancelPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new UndoCancelCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderCancelRejectedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void processReviseOrderCommandFromApproved() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        List<OrderDomainEvent> events = aggregate.process(new ReviseOrderCommand(revision));
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5))));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        assertThat(aggregate.getPendingRevisedLineItems()).containsExactly(new OrderLineItem(10L, 5));
    }

    @Test
    void processConfirmRevisionCommandFromRevisionPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.REVISION_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new ConfirmRevisionCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 5))));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(aggregate.getLineItems()).containsExactly(new OrderLineItem(10L, 5));
    }

    @Test
    void processRejectRevisionCommandFromRevisionPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.REVISION_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new RejectRevisionCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderRevisionRejectedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(aggregate.getPendingRevisedLineItems()).isNull();
    }

    @Test
    void snapshotRoundTripPreservesState() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.REVISION_PENDING);

        OrderAggregate restored = OrderAggregate.fromSnapshot(aggregate.toSnapshotData());

        assertThat(restored.getId()).isEqualTo(aggregate.getId());
        assertThat(restored.getStatus()).isEqualTo(aggregate.getStatus());
        assertThat(restored.getLineItems()).isEqualTo(aggregate.getLineItems());
        assertThat(restored.getPendingRevisedLineItems()).isEqualTo(aggregate.getPendingRevisedLineItems());
    }

    @Test
    void genericApplyDispatchesToCorrectOverload() {
        OrderAggregate aggregate = new OrderAggregate();

        OrderDomainEvent event = new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)));
        aggregate.apply(event);

        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.OrderAggregateTest"`
Expected: FAIL — `OrderAggregate` does not exist yet.

- [ ] **Step 4: Write `OrderAggregate`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCancelConfirmedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderApprovedEvent;
import com.sanjay.ftgo.order.domain.OrderCreatedEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;

import java.util.ArrayList;
import java.util.List;

public class OrderAggregate {

    private Long id;
    private Long consumerId;
    private Long restaurantId;
    private List<OrderLineItem> lineItems;
    private OrderStatus status;
    private List<OrderLineItem> pendingRevisedLineItems;

    public OrderAggregate() {
    }

    private OrderAggregate(OrderSnapshotData data) {
        this.id = data.id();
        this.consumerId = data.consumerId();
        this.restaurantId = data.restaurantId();
        this.lineItems = data.lineItems() == null ? null : new ArrayList<>(data.lineItems());
        this.status = data.status();
        this.pendingRevisedLineItems =
                data.pendingRevisedLineItems() == null ? null : new ArrayList<>(data.pendingRevisedLineItems());
    }

    public static OrderAggregate fromSnapshot(OrderSnapshotData data) {
        return new OrderAggregate(data);
    }

    public OrderSnapshotData toSnapshotData() {
        return new OrderSnapshotData(id, consumerId, restaurantId, lineItems, status, pendingRevisedLineItems);
    }

    public Long getId() {
        return id;
    }

    public Long getConsumerId() {
        return consumerId;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public List<OrderLineItem> getLineItems() {
        return lineItems;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public List<OrderLineItem> getPendingRevisedLineItems() {
        return pendingRevisedLineItems;
    }

    public List<OrderDomainEvent> process(CreateOrderCommand command) {
        return List.of(new OrderCreatedEvent(command.orderId(), command.consumerId(), command.restaurantId(),
                command.lineItems()));
    }

    public void apply(OrderCreatedEvent event) {
        this.id = event.orderId();
        this.consumerId = event.consumerId();
        this.restaurantId = event.restaurantId();
        this.lineItems = new ArrayList<>(event.lineItems());
        this.status = OrderStatus.APPROVAL_PENDING;
    }

    public List<OrderDomainEvent> process(ApproveOrderCommand command) {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderApprovedEvent(id));
    }

    public void apply(OrderApprovedEvent event) {
        this.status = OrderStatus.APPROVED;
    }

    public List<OrderDomainEvent> process(RejectOrderCommand command) {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRejectedEvent(id));
    }

    public void apply(OrderRejectedEvent event) {
        this.status = OrderStatus.REJECTED;
    }

    public List<OrderDomainEvent> process(CancelOrderCommand command) {
        if (status != OrderStatus.APPROVED) {
            throw new OrderCannotBeCancelledException(id);
        }
        return List.of(new OrderCancelledEvent(id));
    }

    public void apply(OrderCancelledEvent event) {
        this.status = OrderStatus.CANCEL_PENDING;
    }

    public List<OrderDomainEvent> process(NoteOrderCancelledCommand command) {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderCancelConfirmedEvent(id));
    }

    public void apply(OrderCancelConfirmedEvent event) {
        this.status = OrderStatus.CANCELLED;
    }

    public List<OrderDomainEvent> process(UndoCancelCommand command) {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderCancelRejectedEvent(id));
    }

    public void apply(OrderCancelRejectedEvent event) {
        this.status = OrderStatus.APPROVED;
    }

    public List<OrderDomainEvent> process(ReviseOrderCommand command) {
        if (status != OrderStatus.APPROVED) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRevisionProposedEvent(id, command.revision().revisedLineItems()));
    }

    public void apply(OrderRevisionProposedEvent event) {
        this.status = OrderStatus.REVISION_PENDING;
        this.pendingRevisedLineItems = new ArrayList<>(event.revisedLineItems());
    }

    public List<OrderDomainEvent> process(ConfirmRevisionCommand command) {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRevisedEvent(id, pendingRevisedLineItems));
    }

    public void apply(OrderRevisedEvent event) {
        this.status = OrderStatus.APPROVED;
        this.lineItems = new ArrayList<>(event.revisedLineItems());
        this.pendingRevisedLineItems = null;
    }

    public List<OrderDomainEvent> process(RejectRevisionCommand command) {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRevisionRejectedEvent(id));
    }

    public void apply(OrderRevisionRejectedEvent event) {
        this.status = OrderStatus.APPROVED;
        this.pendingRevisedLineItems = null;
    }

    // Generic dispatcher used by OrderEventStore during replay, where only the sealed
    // OrderDomainEvent supertype is known at the call site (not its concrete subtype).
    public void apply(OrderDomainEvent event) {
        switch (event) {
            case OrderCreatedEvent e -> apply(e);
            case OrderApprovedEvent e -> apply(e);
            case OrderRejectedEvent e -> apply(e);
            case OrderCancelledEvent e -> apply(e);
            case OrderCancelConfirmedEvent e -> apply(e);
            case OrderCancelRejectedEvent e -> apply(e);
            case OrderRevisionProposedEvent e -> apply(e);
            case OrderRevisedEvent e -> apply(e);
            case OrderRevisionRejectedEvent e -> apply(e);
        }
    }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.OrderAggregateTest"`
Expected: PASS (14 tests)

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSnapshotData.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregate.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregateTest.java
git commit -m "feat: add OrderAggregate with process()/apply() event-sourcing pattern"
```

---

### Task 4: Event-store JPA entities and repositories

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderEventEntity.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderEventEntityRepository.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderIdAllocation.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderIdAllocationRepository.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregateVersion.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregateVersionRepository.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSnapshot.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSnapshotRepository.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/config/PersistenceConfig.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/OrderEventEntityRepositoryTest.java`

**Interfaces:**
- Produces: `OrderEventEntity` (table `order_events`: `id, event_id, event_type, order_id, payload, triggering_event`), `OrderIdAllocation` (table `order_id_allocations`, pure IDENTITY generator), `OrderAggregateVersion` (table `order_aggregate_version`: `order_id, last_event_id, version` with `@Version` optimistic locking), `OrderSnapshot` (table `order_snapshots`: `order_id, last_event_entity_id, snapshot_json`). Consumed by `OrderEventStore` (Task 5).
- Consumes: none new.

`order_events`'s columns are deliberately named to match the Debezium Outbox Event Router field
mapping already configured for `outbox_events` in
`infrastructure/debezium/outbox-connector.json` (`event_id`, `order_id`, `payload`, `event_type`)
— Task 18 adds this table to the same connector's `table.include.list` without changing any field
mappings.

- [ ] **Step 1: Write `OrderEventEntity`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_events")
public class OrderEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "triggering_event")
    private String triggeringEvent;

    protected OrderEventEntity() {
    }

    public OrderEventEntity(String eventId, String eventType, Long orderId, String payload, String triggeringEvent) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
        this.payload = payload;
        this.triggeringEvent = triggeringEvent;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getPayload() {
        return payload;
    }

    public String getTriggeringEvent() {
        return triggeringEvent;
    }
}
```

- [ ] **Step 2: Write `OrderEventEntityRepository`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderEventEntityRepository extends JpaRepository<OrderEventEntity, Long> {

    List<OrderEventEntity> findByOrderIdOrderByIdAsc(Long orderId);

    List<OrderEventEntity> findByOrderIdAndIdGreaterThanOrderByIdAsc(Long orderId, Long id);

    long countByOrderId(Long orderId);

    OrderEventEntity findTopByOrderIdOrderByIdDesc(Long orderId);
}
```

- [ ] **Step 3: Write `OrderIdAllocation` and its repository**

A pure ID-minting table — no fields besides the generated key — used because event-sourced
`Order`s need a `Long` id allocated up front (before the first event exists), the same way
`orders.id` is auto-generated via `GenerationType.IDENTITY` today.

```java
package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_id_allocations")
public class OrderIdAllocation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    public Long getId() {
        return id;
    }
}
```

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderIdAllocationRepository extends JpaRepository<OrderIdAllocation, Long> {
}
```

- [ ] **Step 4: Write `OrderAggregateVersion` and its repository**

`lastEventId` exists specifically so every `update()` performs a real field mutation — a
`@Version` column alone doesn't force Hibernate to issue an UPDATE (and therefore run its
optimistic-lock check) unless some other column actually changes.

```java
package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "order_aggregate_version")
public class OrderAggregateVersion {

    @Id
    private Long orderId;

    @Column(name = "last_event_id", nullable = false)
    private String lastEventId;

    @Version
    private Long version;

    protected OrderAggregateVersion() {
    }

    public OrderAggregateVersion(Long orderId, String lastEventId) {
        this.orderId = orderId;
        this.lastEventId = lastEventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getLastEventId() {
        return lastEventId;
    }

    public Long getVersion() {
        return version;
    }

    public void recordEvent(String eventId) {
        this.lastEventId = eventId;
    }
}
```

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderAggregateVersionRepository extends JpaRepository<OrderAggregateVersion, Long> {
}
```

- [ ] **Step 5: Write `OrderSnapshot` and its repository**

```java
package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_snapshots")
public class OrderSnapshot {

    @Id
    private Long orderId;

    @Column(name = "last_event_entity_id", nullable = false)
    private Long lastEventEntityId;

    @Lob
    @Column(name = "snapshot_json", nullable = false)
    private String snapshotJson;

    protected OrderSnapshot() {
    }

    public OrderSnapshot(Long orderId, Long lastEventEntityId, String snapshotJson) {
        this.orderId = orderId;
        this.lastEventEntityId = lastEventEntityId;
        this.snapshotJson = snapshotJson;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getLastEventEntityId() {
        return lastEventEntityId;
    }

    public String getSnapshotJson() {
        return snapshotJson;
    }

    public OrderSnapshot update(Long lastEventEntityId, String snapshotJson) {
        this.lastEventEntityId = lastEventEntityId;
        this.snapshotJson = snapshotJson;
        return this;
    }
}
```

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderSnapshotRepository extends JpaRepository<OrderSnapshot, Long> {
}
```

- [ ] **Step 6: Register the new package with `PersistenceConfig`**

Edit `PersistenceConfig.java`:

```java
package com.sanjay.ftgo.order.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Explicitly scans this service's own JPA entities/repositories (both the JPA-mode domain
 * package and the event-sourcing package) and the shared outbox ones from ftgo-common, since the
 * latter live outside FtgoOrderServiceApplication's default @SpringBootApplication base package.
 * (ftgo-common's OutboxPublisher/KafkaProducerConfig beans don't need scanning here — they're
 * registered automatically via ftgo-common's own Spring Boot auto-configuration, see
 * OutboxAutoConfiguration in ftgo-common.)
 *
 * Kept in its own @Configuration class (rather than directly on
 * FtgoOrderServiceApplication) because @WebMvcTest slice tests only filter
 * out @Configuration-annotated classes during component scanning -
 * annotations placed directly on the @SpringBootApplication class itself are
 * NOT filtered and would otherwise pull in JPA repository beans (which need
 * an entityManagerFactory the web slice doesn't provide) into controller
 * tests like OrderControllerTest.
 */
@Configuration
@EntityScan(basePackages = {
        "com.sanjay.ftgo.order.domain", "com.sanjay.ftgo.order.eventsourcing", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {
        "com.sanjay.ftgo.order.domain", "com.sanjay.ftgo.order.eventsourcing", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
```

- [ ] **Step 7: Write the failing repository test**

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.dao.OptimisticLockingFailureException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class OrderEventEntityRepositoryTest {

    @Autowired
    private OrderEventEntityRepository eventRepository;

    @Autowired
    private OrderIdAllocationRepository idAllocationRepository;

    @Autowired
    private OrderAggregateVersionRepository versionRepository;

    @Autowired
    private org.springframework.test.context.junit.jupiter.SpringExtension springExtension;

    @Autowired
    private jakarta.persistence.EntityManager entityManager;

    @Test
    void allocatesIncreasingIds() {
        Long first = idAllocationRepository.save(new OrderIdAllocation()).getId();
        Long second = idAllocationRepository.save(new OrderIdAllocation()).getId();

        assertThat(second).isGreaterThan(first);
    }

    @Test
    void findsEventsInInsertOrder() {
        eventRepository.save(new OrderEventEntity("evt-1", "OrderCreated", 42L, "{}", "trigger-1"));
        eventRepository.save(new OrderEventEntity("evt-2", "OrderApproved", 42L, "{}", "trigger-2"));
        eventRepository.save(new OrderEventEntity("evt-3", "OrderApproved", 99L, "{}", "trigger-3"));

        List<OrderEventEntity> events = eventRepository.findByOrderIdOrderByIdAsc(42L);

        assertThat(events).extracting(OrderEventEntity::getEventId).containsExactly("evt-1", "evt-2");
    }

    @Test
    void concurrentVersionUpdateThrowsOptimisticLockingFailure() {
        versionRepository.saveAndFlush(new OrderAggregateVersion(42L, "evt-1"));
        entityManager.clear();

        OrderAggregateVersion first = versionRepository.findById(42L).orElseThrow();
        entityManager.detach(first);
        OrderAggregateVersion second = versionRepository.findById(42L).orElseThrow();

        first.recordEvent("evt-2");
        versionRepository.saveAndFlush(first);
        entityManager.clear();

        second.recordEvent("evt-3");
        assertThatThrownBy(() -> versionRepository.saveAndFlush(second))
                .isInstanceOf(OptimisticLockingFailureException.class);
    }
}
```

- [ ] **Step 8: Run to verify it fails, then passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.OrderEventEntityRepositoryTest"`

First run (before Step 1–6 exist) is expected to FAIL to compile; since Steps 1–6 are already
applied by this point in the task, re-run after writing the test and confirm PASS (3 tests). If
`allocatesIncreasingIds`/`findsEventsInInsertOrder` pass but the optimistic-lock test doesn't
throw, check that `entityManager.detach(first)` actually ran before `second` was loaded — H2's
`MODE=MySQL` (this project's test datasource) supports `@Version` optimistic locking identically
to MySQL.

- [ ] **Step 9: Run full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — confirms `PersistenceConfig`'s widened `@EntityScan` didn't break
`OrderControllerTest`'s `@WebMvcTest` slice.

- [ ] **Step 10: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderEventEntity.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderEventEntityRepository.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderIdAllocation.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderIdAllocationRepository.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregateVersion.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderAggregateVersionRepository.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSnapshot.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSnapshotRepository.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/config/PersistenceConfig.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/OrderEventEntityRepositoryTest.java
git commit -m "feat: add order_events/order_aggregate_version/order_snapshots JPA entities"
```

---

### Task 5: `OrderEventStore`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderEventStore.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/OrderEventStoreTest.java`

**Interfaces:**
- Consumes: `OrderEventEntityRepository`, `OrderIdAllocationRepository`, `OrderAggregateVersionRepository`, `OrderSnapshotRepository` (Task 4), `OrderAggregate`, `OrderSnapshotData` (Task 3), `com.sanjay.ftgo.order.domain.{OrderEventSerializer,OrderEvent,OrderDomainEvent}` (Task 1), `com.sanjay.ftgo.order.domain.OrderNotFoundException` (pre-existing).
- Produces: `OrderEventStore.save(CreateOrderCommand command, String triggeringEventId): OrderAggregate`, `.find(Long orderId): OrderAggregate` (throws `OrderNotFoundException`), `.update(Long orderId, Function<OrderAggregate, List<OrderDomainEvent>> process, String triggeringEventId): OrderAggregate` (throws `OrderNotFoundException`, `UnsupportedStateTransitionException`, `OrderCannotBeCancelledException`, or `org.springframework.dao.OptimisticLockingFailureException`). Consumed by `EventSourcedOrderTransitions` (Task 8).

- [ ] **Step 1: Write the failing test**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCreatedEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderEventSerializer;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({OrderEventSerializer.class, OrderEventStore.class})
class OrderEventStoreTest {

    @Autowired
    private OrderEventStore eventStore;

    @Autowired
    private OrderEventEntityRepository eventRepository;

    @Autowired
    private OrderSnapshotRepository snapshotRepository;

    private OrderAggregate createOrder() {
        CreateOrderCommand command = new CreateOrderCommand(null, 1L, 1L, List.of(new OrderLineItem(10L, 2)));
        return eventStore.save(command, "trigger-create");
    }

    @Test
    void saveAllocatesIdAndPersistsCreatedEvent() {
        OrderAggregate aggregate = createOrder();

        assertThat(aggregate.getId()).isNotNull();
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
        assertThat(eventRepository.findByOrderIdOrderByIdAsc(aggregate.getId())).hasSize(1);
        assertThat(eventRepository.findByOrderIdOrderByIdAsc(aggregate.getId()).get(0).getEventType())
                .isEqualTo("OrderCreated");
    }

    @Test
    void findReplaysPersistedEvents() {
        OrderAggregate created = createOrder();

        OrderAggregate found = eventStore.find(created.getId());

        assertThat(found.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
        assertThat(found.getConsumerId()).isEqualTo(1L);
    }

    @Test
    void findThrowsWhenOrderDoesNotExist() {
        assertThatThrownBy(() -> eventStore.find(999L)).isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void updateAppliesCommandAndPersistsNewEvent() {
        OrderAggregate created = createOrder();

        OrderAggregate updated = eventStore.update(created.getId(),
                aggregate -> aggregate.process(new ApproveOrderCommand()), "trigger-approve");

        assertThat(updated.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(eventRepository.findByOrderIdOrderByIdAsc(created.getId())).hasSize(2);

        OrderAggregate reloaded = eventStore.find(created.getId());
        assertThat(reloaded.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void updateThrowsWhenOrderDoesNotExist() {
        assertThatThrownBy(() -> eventStore.update(999L,
                aggregate -> aggregate.process(new ApproveOrderCommand()), "trigger"))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void writesSnapshotAfterConfiguredEventThreshold() {
        OrderAggregate created = createOrder();
        Long orderId = created.getId();

        // 1 create + 4 approve/reject-style transitions = 5 events, tripping the threshold.
        eventStore.update(orderId, aggregate -> aggregate.process(new ApproveOrderCommand()), "t2");
        eventStore.update(orderId, aggregate -> aggregate.process(new CancelOrderCommand()), "t3");
        eventStore.update(orderId, aggregate -> aggregate.process(new NoteOrderCancelledCommand()), "t4");
        eventStore.update(orderId, aggregate ->
                java.util.Collections.<OrderDomainEvent>emptyList(), "t5-noop");

        assertThat(snapshotRepository.findById(orderId)).isPresent();
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.OrderEventStoreTest"`
Expected: FAIL — `OrderEventStore` does not exist yet.

- [ ] **Step 3: Write `OrderEventStore`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderEvent;
import com.sanjay.ftgo.order.domain.OrderEventSerializer;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

@Component
public class OrderEventStore {

    private static final int SNAPSHOT_EVERY_N_EVENTS = 5;

    private final OrderEventEntityRepository eventRepository;
    private final OrderIdAllocationRepository idAllocationRepository;
    private final OrderAggregateVersionRepository versionRepository;
    private final OrderSnapshotRepository snapshotRepository;
    private final OrderEventSerializer serializer;

    public OrderEventStore(OrderEventEntityRepository eventRepository,
                            OrderIdAllocationRepository idAllocationRepository,
                            OrderAggregateVersionRepository versionRepository,
                            OrderSnapshotRepository snapshotRepository,
                            OrderEventSerializer serializer) {
        this.eventRepository = eventRepository;
        this.idAllocationRepository = idAllocationRepository;
        this.versionRepository = versionRepository;
        this.snapshotRepository = snapshotRepository;
        this.serializer = serializer;
    }

    @Transactional
    public OrderAggregate save(CreateOrderCommand command, String triggeringEventId) {
        Long orderId = idAllocationRepository.save(new OrderIdAllocation()).getId();
        CreateOrderCommand withId =
                new CreateOrderCommand(orderId, command.consumerId(), command.restaurantId(), command.lineItems());

        OrderAggregate aggregate = new OrderAggregate();
        List<OrderDomainEvent> events = aggregate.process(withId);
        events.forEach(aggregate::apply);

        List<String> eventIds = appendEvents(orderId, events, triggeringEventId);
        versionRepository.save(new OrderAggregateVersion(orderId, lastEventId(eventIds, triggeringEventId)));
        maybeSnapshot(orderId, aggregate);
        return aggregate;
    }

    @Transactional(readOnly = true)
    public OrderAggregate find(Long orderId) {
        return replay(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    @Transactional
    public OrderAggregate update(Long orderId, Function<OrderAggregate, List<OrderDomainEvent>> process,
                                  String triggeringEventId) {
        OrderAggregateVersion versionRow =
                versionRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
        OrderAggregate aggregate = replay(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));

        List<OrderDomainEvent> events = process.apply(aggregate);
        events.forEach(aggregate::apply);

        List<String> eventIds = appendEvents(orderId, events, triggeringEventId);
        versionRow.recordEvent(lastEventId(eventIds, triggeringEventId));
        versionRepository.save(versionRow);

        maybeSnapshot(orderId, aggregate);
        return aggregate;
    }

    private Optional<OrderAggregate> replay(Long orderId) {
        Optional<OrderSnapshot> snapshotOpt = snapshotRepository.findById(orderId);
        OrderAggregate aggregate;
        List<OrderEventEntity> tail;
        if (snapshotOpt.isPresent()) {
            OrderSnapshot snapshot = snapshotOpt.get();
            aggregate = OrderAggregate.fromSnapshot(readSnapshotData(snapshot.getSnapshotJson()));
            tail = eventRepository.findByOrderIdAndIdGreaterThanOrderByIdAsc(orderId, snapshot.getLastEventEntityId());
        } else {
            aggregate = new OrderAggregate();
            tail = eventRepository.findByOrderIdOrderByIdAsc(orderId);
        }
        if (snapshotOpt.isEmpty() && tail.isEmpty()) {
            return Optional.empty();
        }
        for (OrderEventEntity row : tail) {
            OrderEvent wireEvent = serializer.fromJson(row.getPayload());
            aggregate.apply(serializer.fromWireEvent(wireEvent));
        }
        return Optional.of(aggregate);
    }

    private List<String> appendEvents(Long orderId, List<OrderDomainEvent> events, String triggeringEventId) {
        List<String> eventIds = new ArrayList<>();
        for (OrderDomainEvent event : events) {
            String eventId = UUID.randomUUID().toString();
            OrderEvent wireEvent = serializer.toWireEvent(eventId, event);
            eventRepository.save(new OrderEventEntity(
                    eventId, wireEvent.eventType(), orderId, serializer.toJson(wireEvent), triggeringEventId));
            eventIds.add(eventId);
        }
        return eventIds;
    }

    private String lastEventId(List<String> eventIds, String fallback) {
        return eventIds.isEmpty() ? fallback : eventIds.get(eventIds.size() - 1);
    }

    private void maybeSnapshot(Long orderId, OrderAggregate aggregate) {
        long totalEvents = eventRepository.countByOrderId(orderId);
        if (totalEvents == 0 || totalEvents % SNAPSHOT_EVERY_N_EVENTS != 0) {
            return;
        }
        OrderEventEntity lastEvent = eventRepository.findTopByOrderIdOrderByIdDesc(orderId);
        String json = writeSnapshotData(aggregate.toSnapshotData());
        OrderSnapshot snapshot = snapshotRepository.findById(orderId)
                .map(existing -> existing.update(lastEvent.getId(), json))
                .orElseGet(() -> new OrderSnapshot(orderId, lastEvent.getId(), json));
        snapshotRepository.save(snapshot);
    }

    private OrderSnapshotData readSnapshotData(String json) {
        return serializer.readSnapshotData(json);
    }

    private String writeSnapshotData(OrderSnapshotData data) {
        return serializer.writeSnapshotData(data);
    }
}
```

- [ ] **Step 4: Add snapshot (de)serialization to `OrderEventSerializer`**

`OrderSnapshotData` is a plain record with no custom types beyond ones Jackson already handles
elsewhere in this codebase (`OrderLineItem`, `OrderStatus`) — reuse the same `ObjectMapper`
already injected into `OrderEventSerializer` rather than introducing a second serialization path.

Edit `OrderEventSerializer.java` (add two methods and one import):

```java
    public String writeSnapshotData(com.sanjay.ftgo.order.eventsourcing.OrderSnapshotData data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order snapshot", e);
        }
    }

    public com.sanjay.ftgo.order.eventsourcing.OrderSnapshotData readSnapshotData(String json) {
        try {
            return objectMapper.readValue(json, com.sanjay.ftgo.order.eventsourcing.OrderSnapshotData.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize order snapshot", e);
        }
    }
```

(Fully-qualified inline rather than an import, since `com.sanjay.ftgo.order.eventsourcing` is a
child of `com.sanjay.ftgo.order.domain`'s sibling package and this keeps the diff to the two new
methods only.)

- [ ] **Step 5: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.OrderEventStoreTest"`
Expected: PASS (6 tests)

- [ ] **Step 6: Run full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderEventStore.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderEventSerializer.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/OrderEventStoreTest.java
git commit -m "feat: add OrderEventStore (save/find/update with optimistic locking and snapshots)"
```

---

### Task 6: `OrderTransitions` facade interface

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderTransitions.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/TransitionResult.java`

**Interfaces:**
- Produces: `OrderTransitions` — `create(Long,Long,List<OrderLineItem>,String): Order`; `cancel(Long,String): TransitionResult` and `revise(Long,OrderRevision,String): TransitionResult` (both throw `OrderNotFoundException`/`OrderCannotBeCancelledException`/`UnsupportedStateTransitionException`, matching today's `OrderController` behavior exactly); `approve/reject/noteCancelled/undoCancel/confirmRevision/rejectRevision/requestRevisionCompensation(Long,String): void` (silently no-op on not-found or wrong-state, matching today's saga-service behavior exactly). `TransitionResult(Order order, List<OrderDomainEvent> events)`.
- Consumed by: `JpaOrderTransitions` (Task 7), `EventSourcedOrderTransitions` (Task 8), and every rewired call site (Tasks 9–13, 20–22).

This single interface is why the plan can collapse ~12 near-identical
find→mutate→save→publish→catch blocks (spread today across `OrderSagaService`,
`OrderCancelSagaService`, `OrderReviseSagaService`, and the three orchestrators) into one
implementation per persistence mode.

- [ ] **Step 1: Write `TransitionResult`**

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record TransitionResult(Order order, List<OrderDomainEvent> events) {
}
```

- [ ] **Step 2: Write `OrderTransitions`**

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public interface OrderTransitions {

    Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId);

    TransitionResult cancel(Long orderId, String eventId);

    TransitionResult revise(Long orderId, OrderRevision revision, String eventId);

    void approve(Long orderId, String eventId);

    void reject(Long orderId, String eventId);

    void noteCancelled(Long orderId, String eventId);

    void undoCancel(Long orderId, String eventId);

    void confirmRevision(Long orderId, String eventId);

    void rejectRevision(Long orderId, String eventId);

    void requestRevisionCompensation(Long orderId, String eventId);
}
```

- [ ] **Step 3: Compile check**

Run: `./gradlew :ftgo-order-service:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderTransitions.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/TransitionResult.java
git commit -m "feat: add OrderTransitions persistence-mode facade interface"
```

---

### Task 7: `JpaOrderTransitions`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/JpaOrderTransitions.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/JpaOrderTransitionsTest.java`

**Interfaces:**
- Consumes: `OrderRepository`, `OrderDomainEventPublisher` (pre-existing), `OrderTransitions`/`TransitionResult` (Task 6).
- Produces: `JpaOrderTransitions implements OrderTransitions`, active when `persistence.mode=jpa` (or unset).

This replicates the exact current behavior of `OrderController.cancel/revise` (throws) and the
saga services' find→mutate→save→publish→catch-and-log (silent no-op) shape — a pure extraction,
not a behavior change.

- [ ] **Step 1: Write the failing test**

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JpaOrderTransitionsTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderDomainEventPublisher domainEventPublisher;

    @InjectMocks
    private JpaOrderTransitions transitions;

    private Order orderIn(OrderStatus status) {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), status);
    }

    @Test
    void createSavesNewOrder() {
        when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Order created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThat(created.getConsumerId()).isEqualTo(1L);
        assertThat(created.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void cancelThrowsWhenOrderNotFound() {
        when(orderRepository.findById(42L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> transitions.cancel(42L, "evt-1")).isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void cancelThrowsWhenWrongState() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVAL_PENDING)));

        assertThatThrownBy(() -> transitions.cancel(42L, "evt-1")).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelSavesAndReturnsEvents() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVED)));

        TransitionResult result = transitions.cancel(42L, "evt-1");

        assertThat(result.order().getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
        assertThat(result.events()).containsExactly(new OrderCancelledEvent(42L));
        verify(orderRepository).save(result.order());
    }

    @Test
    void approveSilentlyNoOpsWhenOrderNotFound() {
        when(orderRepository.findById(42L)).thenReturn(Optional.empty());

        transitions.approve(42L, "evt-1");

        verify(orderRepository, never()).save(any());
        verify(domainEventPublisher, never()).publish(any());
    }

    @Test
    void approveSilentlyNoOpsWhenWrongState() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVED)));

        transitions.approve(42L, "evt-1");

        verify(orderRepository, never()).save(any());
        verify(domainEventPublisher, never()).publish(any());
    }

    @Test
    void approveSavesAndPublishesOnSuccess() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVAL_PENDING)));

        transitions.approve(42L, "evt-1");

        verify(orderRepository).save(any());
        verify(domainEventPublisher).publish(List.of(new OrderApprovedEvent(42L)));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.JpaOrderTransitionsTest"`
Expected: FAIL — `JpaOrderTransitions` does not exist yet.

- [ ] **Step 3: Write `JpaOrderTransitions`**

```java
package com.sanjay.ftgo.order.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.function.Function;

@Service
@ConditionalOnProperty(name = "persistence.mode", havingValue = "jpa", matchIfMissing = true)
public class JpaOrderTransitions implements OrderTransitions {

    private static final Logger log = LoggerFactory.getLogger(JpaOrderTransitions.class);

    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public JpaOrderTransitions(OrderRepository orderRepository, OrderDomainEventPublisher domainEventPublisher) {
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    @Transactional
    public Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId) {
        return orderRepository.save(new Order(consumerId, restaurantId, lineItems, OrderStatus.APPROVAL_PENDING));
    }

    @Override
    @Transactional
    public TransitionResult cancel(Long orderId, String eventId) {
        Order order = findOrThrow(orderId);
        List<OrderDomainEvent> events = order.cancel();
        orderRepository.save(order);
        return new TransitionResult(order, events);
    }

    @Override
    @Transactional
    public TransitionResult revise(Long orderId, OrderRevision revision, String eventId) {
        Order order = findOrThrow(orderId);
        List<OrderDomainEvent> events = order.revise(revision);
        orderRepository.save(order);
        return new TransitionResult(order, events);
    }

    @Override
    @Transactional
    public void approve(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteApproved, "approve");
    }

    @Override
    @Transactional
    public void reject(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteRejected, "reject");
    }

    @Override
    @Transactional
    public void noteCancelled(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteCancelled, "cancel confirmation");
    }

    @Override
    @Transactional
    public void undoCancel(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::undoCancel, "cancel rejection");
    }

    @Override
    @Transactional
    public void confirmRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::confirmRevision, "revision confirmation");
    }

    @Override
    @Transactional
    public void rejectRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::rejectRevision, "revision rejection");
    }

    @Override
    @Transactional
    public void requestRevisionCompensation(Long orderId, String eventId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.REVISION_PENDING) {
            return;
        }
        domainEventPublisher.publishRevisionCompensationRequested(order, eventId);
    }

    private void applyBestEffort(Long orderId, Function<Order, List<OrderDomainEvent>> transition, String description) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = transition.apply(order);
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring {} for order {}: {}", description, orderId, e.getMessage());
        }
    }

    private Order findOrThrow(Long orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.JpaOrderTransitionsTest"`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/JpaOrderTransitions.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/JpaOrderTransitionsTest.java
git commit -m "feat: add JpaOrderTransitions (jpa-mode OrderTransitions implementation)"
```

---

### Task 8: `EventSourcedOrderTransitions`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitions.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitionsTest.java`

**Interfaces:**
- Consumes: `OrderEventStore` (Task 5), the 9 `OrderCommand` types (Task 2), `com.sanjay.ftgo.order.domain.{OrderTransitions,TransitionResult,Order,OrderStatus,OrderLineItem,OrderRevision,OrderDomainEvent,OrderNotFoundException,UnsupportedStateTransitionException}`.
- Produces: `EventSourcedOrderTransitions implements OrderTransitions`, active when `persistence.mode=event-sourcing`.

Callers (controller, saga services, orchestrators) all deal in the existing `Order` value type,
not the new `OrderAggregate` — this class is the translation boundary, converting
`OrderAggregate` → `Order` after every operation so no other file needs to know which persistence
mode is active.

- [ ] **Step 1: Add a 6-arg constructor to `Order` for the translation boundary**

`Order`'s existing constructors don't set `pendingRevisedLineItems` (JPA mode never needs to —
Hibernate populates it via reflection when loading a managed entity). The event-sourced
translation path needs to set it explicitly when reconstructing an `Order` value from an
`OrderAggregate`. Add this constructor to `Order.java`, directly below the existing 5-arg one:

```java
    public Order(Long id, Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status,
                 List<OrderLineItem> pendingRevisedLineItems) {
        this(id, consumerId, restaurantId, lineItems, status);
        this.pendingRevisedLineItems = pendingRevisedLineItems;
    }
```

- [ ] **Step 2: Write the failing test**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.TransitionResult;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({com.sanjay.ftgo.order.domain.OrderEventSerializer.class, OrderEventStore.class, EventSourcedOrderTransitions.class})
class EventSourcedOrderTransitionsTest {

    @Autowired
    private EventSourcedOrderTransitions transitions;

    @Test
    void createReturnsOrderInApprovalPending() {
        var order = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThat(order.getId()).isNotNull();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void approveThenCancelReachesCancelPending() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");

        TransitionResult result = transitions.cancel(created.getId(), "evt-3");

        assertThat(result.order().getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
        assertThat(result.events()).containsExactly(new OrderCancelledEvent(created.getId()));
    }

    @Test
    void cancelThrowsWhenOrderNotFound() {
        assertThatThrownBy(() -> transitions.cancel(999L, "evt-1")).isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void cancelThrowsWhenWrongState() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThatThrownBy(() -> transitions.cancel(created.getId(), "evt-2"))
                .isInstanceOf(com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException.class);
    }

    @Test
    void approveSilentlyNoOpsWhenOrderNotFound() {
        transitions.approve(999L, "evt-1");
        // No exception — matches JpaOrderTransitions' silent no-op contract.
    }

    @Test
    void approveSilentlyNoOpsWhenWrongState() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");

        transitions.approve(created.getId(), "evt-3");
        // Second approve() from APPROVED is a no-op, not a thrown UnsupportedStateTransitionException.
    }

    @Test
    void reviseThenConfirmRevisionSucceedsWithoutError() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");

        TransitionResult revised = transitions.revise(created.getId(),
                new OrderRevision(List.of(new OrderLineItem(10L, 5))), "evt-3");
        assertThat(revised.order().getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);

        transitions.confirmRevision(created.getId(), "evt-4");
        // Correctness of the resulting APPROVED state + updated line items is covered by
        // OrderAggregateTest (Task 3, in-memory) and OrderEventStoreTest (Task 5, replay) —
        // this test's job is only to prove EventSourcedOrderTransitions wires those pieces
        // together without throwing. A read-based assertion on the post-confirm state is added
        // in Task 17 once OrderTransitions.findById exists.
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.EventSourcedOrderTransitionsTest"`
Expected: FAIL — `EventSourcedOrderTransitions` does not exist yet.

- [ ] **Step 4: Write `EventSourcedOrderTransitions`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderTransitions;
import com.sanjay.ftgo.order.domain.TransitionResult;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedOrderTransitions implements OrderTransitions {

    private static final Logger log = LoggerFactory.getLogger(EventSourcedOrderTransitions.class);

    private final OrderEventStore eventStore;

    public EventSourcedOrderTransitions(OrderEventStore eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId) {
        CreateOrderCommand command = new CreateOrderCommand(null, consumerId, restaurantId, lineItems);
        return toOrder(eventStore.save(command, eventId));
    }

    @Override
    public TransitionResult cancel(Long orderId, String eventId) {
        OrderAggregate aggregate =
                eventStore.update(orderId, a -> a.process(new CancelOrderCommand()), eventId);
        return new TransitionResult(toOrder(aggregate), List.of(new com.sanjay.ftgo.order.domain.OrderCancelledEvent(orderId)));
    }

    @Override
    public TransitionResult revise(Long orderId, OrderRevision revision, String eventId) {
        OrderAggregate aggregate =
                eventStore.update(orderId, a -> a.process(new ReviseOrderCommand(revision)), eventId);
        return new TransitionResult(toOrder(aggregate),
                List.of(new com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent(orderId, revision.revisedLineItems())));
    }

    @Override
    public void approve(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new ApproveOrderCommand()), eventId, "approve");
    }

    @Override
    public void reject(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new RejectOrderCommand()), eventId, "reject");
    }

    @Override
    public void noteCancelled(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new NoteOrderCancelledCommand()), eventId, "cancel confirmation");
    }

    @Override
    public void undoCancel(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new UndoCancelCommand()), eventId, "cancel rejection");
    }

    @Override
    public void confirmRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new ConfirmRevisionCommand()), eventId, "revision confirmation");
    }

    @Override
    public void rejectRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new RejectRevisionCommand()), eventId, "revision rejection");
    }

    @Override
    public void requestRevisionCompensation(Long orderId, String eventId) {
        // Order itself doesn't transition here, mirroring JpaOrderTransitions' equivalent — kitchen's
        // undo is what needs to run before Order leaves REVISION_PENDING. Recorded as an
        // OrderAggregate-bypassing wire event, same deliberate exception the JPA path takes via
        // OrderDomainEventPublisher.publishRevisionCompensationRequested.
        eventStore.appendCompensationRequestedEvent(orderId, eventId);
    }

    private void applyBestEffort(Long orderId,
                                  java.util.function.Function<OrderAggregate, List<com.sanjay.ftgo.order.domain.OrderDomainEvent>> process,
                                  String eventId, String description) {
        try {
            eventStore.update(orderId, process, eventId);
        } catch (com.sanjay.ftgo.order.domain.OrderNotFoundException e) {
            log.debug("Ignoring {} for unknown order {}", description, orderId);
        } catch (UnsupportedStateTransitionException | com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException e) {
            log.debug("Ignoring {} for order {}: {}", description, orderId, e.getMessage());
        }
    }

    private Order toOrder(OrderAggregate aggregate) {
        return new Order(aggregate.getId(), aggregate.getConsumerId(), aggregate.getRestaurantId(),
                aggregate.getLineItems(), aggregate.getStatus(), aggregate.getPendingRevisedLineItems());
    }
}
```

- [ ] **Step 5: Add `appendCompensationRequestedEvent` to `OrderEventStore`**

This writes a wire event directly (bypassing `OrderAggregate.process()`/`apply()` entirely, since
Order's own state doesn't change) — the event-sourcing-mode mirror of
`OrderDomainEventPublisher.publishRevisionCompensationRequested`. Add to `OrderEventStore.java`:

```java
    @Transactional
    public void appendCompensationRequestedEvent(Long orderId, String triggeringEventId) {
        OrderAggregate aggregate = replay(orderId).orElse(null);
        if (aggregate == null || aggregate.getStatus() != com.sanjay.ftgo.order.domain.OrderStatus.REVISION_PENDING) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderRevisionCompensationRequested", orderId,
                null, null, toWireLineItems(aggregate.getLineItems()));
        eventRepository.save(new OrderEventEntity(
                eventId, wireEvent.eventType(), orderId, serializer.toJson(wireEvent), triggeringEventId));
    }

    private List<OrderEvent.LineItem> toWireLineItems(List<com.sanjay.ftgo.order.domain.OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }
```

Add `import com.sanjay.ftgo.order.domain.OrderLineItem;`-free — the fully-qualified reference
above avoids a second import block edit. This pseudo-event is *not* replayed by `OrderAggregate`
(there's no `apply()` overload for it, matching `OrderAggregate`'s sealed-interface exhaustiveness
— it never enters the sealed `OrderDomainEvent` hierarchy at all, same as the JPA path), so it's
written straight to `order_events` without going through `appendEvents`.

- [ ] **Step 6: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.EventSourcedOrderTransitionsTest"`
Expected: PASS (7 tests)

- [ ] **Step 7: Run full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — confirms the `Order` constructor addition didn't disturb any JPA-mode test.

- [ ] **Step 8: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitions.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderEventStore.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitionsTest.java
git commit -m "feat: add EventSourcedOrderTransitions (event-sourcing-mode OrderTransitions implementation)"
```

---

### Task 9: Rewire `OrderController`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`

**Interfaces:**
- Consumes: `OrderTransitions`/`TransitionResult` (Task 6). No longer depends on `OrderRepository` directly.

- [ ] **Step 1: Rewrite `OrderController.java`**

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderRevisionSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderTransitions;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.TransitionResult;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderTransitions orderTransitions;
    private final OrderCancellationSagaTrigger cancellationSagaTrigger;
    private final OrderRevisionSagaTrigger revisionSagaTrigger;

    public OrderController(OrderService orderService, OrderTransitions orderTransitions,
                            OrderCancellationSagaTrigger cancellationSagaTrigger,
                            OrderRevisionSagaTrigger revisionSagaTrigger) {
        this.orderService = orderService;
        this.orderTransitions = orderTransitions;
        this.cancellationSagaTrigger = cancellationSagaTrigger;
        this.revisionSagaTrigger = revisionSagaTrigger;
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest request) {
        if (request.consumerId() == null) {
            return ResponseEntity.badRequest().body("consumerId is required");
        }
        if (request.restaurantId() == null) {
            return ResponseEntity.badRequest().body("restaurantId is required");
        }
        if (request.lineItems() == null || request.lineItems().isEmpty()) {
            return ResponseEntity.badRequest().body("lineItems must not be empty");
        }

        List<OrderLineItem> lineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();

        Order order = orderService.createOrder(request.consumerId(), request.restaurantId(), lineItems);
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.from(order));
    }

    @PostMapping("/{id}/cancel")
    @Transactional
    public ResponseEntity<OrderResponse> cancel(@PathVariable Long id) {
        TransitionResult result = orderTransitions.cancel(id, UUID.randomUUID().toString());
        cancellationSagaTrigger.onOrderCancelled(result.order(), result.events());
        return ResponseEntity.ok(OrderResponse.from(result.order()));
    }

    @PostMapping("/{id}/revise")
    @Transactional
    public ResponseEntity<OrderResponse> revise(@PathVariable Long id, @RequestBody ReviseOrderRequest request) {
        List<OrderLineItem> revisedLineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();
        TransitionResult result =
                orderTransitions.revise(id, new OrderRevision(revisedLineItems), UUID.randomUUID().toString());
        revisionSagaTrigger.onOrderRevised(result.order(), result.events());
        return ResponseEntity.ok(OrderResponse.from(result.order()));
    }

    @ExceptionHandler({RestaurantNotFoundException.class, MenuItemNotFoundException.class, OrderNotFoundException.class})
    public ResponseEntity<String> handleNotFound(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(RestaurantServiceUnavailableException.class)
    public ResponseEntity<String> handleUnavailable(RestaurantServiceUnavailableException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
    }

    @ExceptionHandler({OrderCannotBeCancelledException.class, UnsupportedStateTransitionException.class})
    public ResponseEntity<String> handleConflict(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
    }
}
```

- [ ] **Step 2: Rewrite the cancel/revise sections of `OrderControllerTest.java`**

Change the `@MockitoBean private OrderRepository orderRepository;` field to
`@MockitoBean private OrderTransitions orderTransitions;` (remove the now-unused
`OrderRepository` import, add `OrderTransitions`/`TransitionResult` imports), then replace each
of the 6 cancel/revise test bodies:

```java
    @Test
    void cancelsAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
        when(orderTransitions.cancel(eq(5L), any()))
                .thenReturn(new TransitionResult(order, List.of(new com.sanjay.ftgo.order.domain.OrderCancelledEvent(5L))));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCEL_PENDING"));

        verify(cancellationSagaTrigger).onOrderCancelled(eq(order), any());
    }

    @Test
    void returns404WhenCancellingUnknownOrder() throws Exception {
        when(orderTransitions.cancel(eq(99L), any())).thenThrow(new OrderNotFoundException(99L));

        mockMvc.perform(post("/orders/99/cancel"))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenCancellingAnOrderThatCannotBeCancelled() throws Exception {
        when(orderTransitions.cancel(eq(5L), any())).thenThrow(new OrderCannotBeCancelledException(5L));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isConflict());
    }

    @Test
    void revisesAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.REVISION_PENDING);
        when(orderTransitions.revise(eq(5L), any(), any())).thenReturn(new TransitionResult(order,
                List.of(new com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent(5L, List.of(new OrderLineItem(10L, 5))))));

        mockMvc.perform(post("/orders/5/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REVISION_PENDING"));

        verify(revisionSagaTrigger).onOrderRevised(eq(order), any());
    }

    @Test
    void returns404WhenRevisingUnknownOrder() throws Exception {
        when(orderTransitions.revise(eq(99L), any(), any())).thenThrow(new OrderNotFoundException(99L));

        mockMvc.perform(post("/orders/99/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenRevisingAnOrderNotYetApproved() throws Exception {
        when(orderTransitions.revise(eq(5L), any(), any()))
                .thenThrow(new UnsupportedStateTransitionException(OrderStatus.APPROVAL_PENDING));

        mockMvc.perform(post("/orders/5/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isConflict());
    }
```

Also update the imports block at the top: replace
`import com.sanjay.ftgo.order.domain.OrderRepository;` with
`import com.sanjay.ftgo.order.domain.OrderTransitions;` and
`import com.sanjay.ftgo.order.domain.TransitionResult;`, and add
`import static org.mockito.ArgumentMatchers.any;` if not already present (it already is, per the
existing `createsOrderSuccessfully` test).

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest"`
Expected: PASS (all 12 tests)

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java
git commit -m "refactor: rewire OrderController through the OrderTransitions facade"
```

---

### Task 10: Rewire `OrderService.createOrder`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`

**Interfaces:**
- Consumes: `OrderTransitions` (Task 6) instead of `OrderRepository` directly.

- [ ] **Step 1: Rewrite `OrderService.java`**

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final RestaurantServicePort restaurantServicePort;
    private final OrderTransitions orderTransitions;
    private final OrderCreationSagaTrigger orderCreationSagaTrigger;

    public OrderService(RestaurantServicePort restaurantServicePort,
                         OrderTransitions orderTransitions,
                         OrderCreationSagaTrigger orderCreationSagaTrigger) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderTransitions = orderTransitions;
        this.orderCreationSagaTrigger = orderCreationSagaTrigger;
    }

    @Transactional
    public Order createOrder(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems) {
        RestaurantInfo restaurant = restaurantServicePort.findRestaurant(restaurantId);

        Set<Long> validMenuItemIds = restaurant.menuItems().stream()
                .map(RestaurantInfo.MenuItemInfo::id)
                .collect(Collectors.toSet());

        for (OrderLineItem lineItem : lineItems) {
            if (!validMenuItemIds.contains(lineItem.menuItemId())) {
                throw new MenuItemNotFoundException(lineItem.menuItemId(), restaurantId);
            }
        }

        String eventId = UUID.randomUUID().toString();
        Order order = orderTransitions.create(consumerId, restaurantId, lineItems, eventId);

        orderCreationSagaTrigger.onOrderCreated(order, eventId);

        return order;
    }
}
```

(The `eventId` is now generated once and reused for both `create()` and the trigger call, rather
than generated separately after — a minor tightening with no observable behavior change, since
the trigger's `eventId` today isn't tied to any specific persisted row anyway.)

- [ ] **Step 2: Update `OrderServiceTest.java`**

Change the mocked/injected `OrderRepository` field and its `when(orderRepository.save(any()))`
stub to `OrderTransitions` with
`when(orderTransitions.create(any(), any(), any(), any())).thenReturn(...)`, keeping every
existing assertion (restaurant lookup, `MenuItemNotFoundException`, saga trigger invocation)
unchanged — the test's job (verify `createOrder`'s validation and orchestration) doesn't change,
only which collaborator it mocks for persistence.

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java
git commit -m "refactor: rewire OrderService.createOrder through the OrderTransitions facade"
```

---

### Task 11: Rewire `OrderSagaService`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java`

**Interfaces:**
- Consumes: `OrderTransitions` (Task 6) instead of `OrderRepository`/`OrderDomainEventPublisher` directly. `ProcessedEventRepository` idempotency check is unchanged (stays the primary dedup gate, called before `OrderTransitions`).

- [ ] **Step 1: Rewrite `OrderSagaService.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderSagaService {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;

    public OrderSagaService(OrderTransitions orderTransitions, ProcessedEventRepository processedEventRepository) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void approve(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.approve(orderId, eventId);
    }

    @Transactional
    public void reject(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.reject(orderId, eventId);
    }
}
```

- [ ] **Step 2: Update `OrderSagaServiceTest.java`**

Replace mocked `OrderRepository`/`OrderDomainEventPublisher` collaborators with a mocked
`OrderTransitions`; existing assertions that previously checked "order not found → no save/no
publish" and "wrong state → no save/no publish" now assert
`verify(orderTransitions, never()).approve(...)`/`verifyNoInteractions` is no longer meaningful at
this layer (that behavior is now `JpaOrderTransitionsTest`'s job, Task 7) — keep only the tests
that verify `OrderSagaService`'s own responsibility: the idempotency gate (`existsById` short-circuits
before `orderTransitions.approve/reject` is ever called) and that a fresh `eventId` correctly
delegates through. Remove any test asserting internal `Order` state-transition behavior — that's
now `OrderAggregateTest`'s (Task 3) and `JpaOrderTransitionsTest`'s (Task 7) job, not this class's.

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderSagaServiceTest"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java
git commit -m "refactor: rewire OrderSagaService through the OrderTransitions facade"
```

---

### Task 12: Rewire `OrderCancelSagaService`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelSagaService.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderCancelSagaServiceTest.java`

**Interfaces:**
- Consumes: `OrderTransitions` (Task 6), same shape as Task 11.

- [ ] **Step 1: Rewrite `OrderCancelSagaService.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderCancelSagaService {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;

    public OrderCancelSagaService(OrderTransitions orderTransitions, ProcessedEventRepository processedEventRepository) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void confirmCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.noteCancelled(orderId, eventId);
    }

    @Transactional
    public void rejectCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.undoCancel(orderId, eventId);
    }
}
```

- [ ] **Step 2: Update `OrderCancelSagaServiceTest.java`**

Same treatment as Task 11 Step 2, mocking `OrderTransitions` in place of
`OrderRepository`/`OrderDomainEventPublisher`, keeping only this class's own responsibility
(idempotency gating) under test.

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderCancelSagaServiceTest"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelSagaService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderCancelSagaServiceTest.java
git commit -m "refactor: rewire OrderCancelSagaService through the OrderTransitions facade"
```

---

### Task 13: Rewire `OrderReviseSagaService`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderReviseSagaService.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderReviseSagaServiceTest.java`

**Interfaces:**
- Consumes: `OrderTransitions` (Task 6), including its `requestRevisionCompensation` method for
  `compensateRevision`'s special one-off case.

- [ ] **Step 1: Rewrite `OrderReviseSagaService.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderReviseSagaService {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;

    public OrderReviseSagaService(OrderTransitions orderTransitions, ProcessedEventRepository processedEventRepository) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void confirmRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.confirmRevision(orderId, eventId);
    }

    @Transactional
    public void rejectRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.rejectRevision(orderId, eventId);
    }

    // Triggers kitchen's undo without changing Order's own status - Order must stay
    // REVISION_PENDING until finalizeRejectedRevision runs, once the undo is confirmed.
    @Transactional
    public void compensateRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.requestRevisionCompensation(orderId, eventId);
    }

    @Transactional
    public void finalizeRejectedRevision(Long orderId, String eventId) {
        rejectRevision(orderId, eventId);
    }
}
```

- [ ] **Step 2: Update `OrderReviseSagaServiceTest.java`**

Same treatment as Task 11 Step 2, plus a test for `compensateRevision` calling
`orderTransitions.requestRevisionCompensation(orderId, eventId)` after the idempotency check
passes.

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderReviseSagaServiceTest"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderReviseSagaService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderReviseSagaServiceTest.java
git commit -m "refactor: rewire OrderReviseSagaService through the OrderTransitions facade"
```

---

### Task 14: Fix double-publish in event-sourcing mode — no-op choreography trigger siblings

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTrigger.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCancellationSagaTrigger.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderRevisionSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyOrderCreationSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyOrderCancellationSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyOrderRevisionSagaTrigger.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyTriggersTest.java`

**Interfaces:**
- Consumes: `OrderCreationSagaTrigger`/`OrderCancellationSagaTrigger`/`OrderRevisionSagaTrigger` (pre-existing).
- Produces: three no-op trigger implementations active when `saga.mode=choreography` AND `persistence.mode=event-sourcing`.

**Why this task exists:** in `event-sourcing` mode, `OrderTransitions.create/cancel/revise`
already durably wrote the relevant `OrderDomainEvent`(s) to `order_events`, which the Ch.3 CDC
pipeline (Task 18) publishes to `order.events` automatically. The existing
`Choreography*SagaTrigger` classes call `domainEventPublisher.publish(...)`/`publishOrderCreated`
unconditionally, which would publish the *same* event a second time (via the JPA-only
`outbox_events` table) if left unguarded — a real duplicate-message bug. The fix: gate the
existing three choreography triggers to `persistence.mode=jpa` (matchIfMissing=true, so
behavior is 100% unchanged when `persistence.mode` is unset) and add three no-op siblings for
`persistence.mode=event-sourcing`.

- [ ] **Step 1: Add the `persistence.mode` guard to the 3 existing choreography triggers**

Edit `ChoreographyOrderCreationSagaTrigger.java` — add a second `@ConditionalOnProperty`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "jpa", matchIfMissing = true)
public class ChoreographyOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    private final OrderDomainEventPublisher domainEventPublisher;

    public ChoreographyOrderCreationSagaTrigger(OrderDomainEventPublisher domainEventPublisher) {
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    public void onOrderCreated(Order order, String eventId) {
        domainEventPublisher.publishOrderCreated(order, eventId);
    }
}
```

Apply the identical two-annotation change to `ChoreographyOrderCancellationSagaTrigger.java` and
`ChoreographyOrderRevisionSagaTrigger.java` — their bodies (`onOrderCancelled`/`onOrderRevised`
calling `domainEventPublisher.publish(events)`) are otherwise unchanged.

- [ ] **Step 2: Write the failing test for the no-op siblings**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderStatus;
import org.junit.jupiter.api.Test;

import java.util.List;

class EventSourcedChoreographyTriggersTest {

    private final Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

    @Test
    void creationTriggerDoesNothing() {
        new EventSourcedChoreographyOrderCreationSagaTrigger().onOrderCreated(order, "evt-1");
        // No collaborator to verify — the point of this test is that construction and the call
        // succeed without throwing, proving there's no hidden publish side effect.
    }

    @Test
    void cancellationTriggerDoesNothing() {
        List<OrderDomainEvent> events = List.of(new OrderCancelledEvent(1L));
        new EventSourcedChoreographyOrderCancellationSagaTrigger().onOrderCancelled(order, events);
    }

    @Test
    void revisionTriggerDoesNothing() {
        List<OrderDomainEvent> events = List.of();
        new EventSourcedChoreographyOrderRevisionSagaTrigger().onOrderRevised(order, events);
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.EventSourcedChoreographyTriggersTest"`
Expected: FAIL — the three no-op classes don't exist yet.

- [ ] **Step 4: Write the three no-op trigger classes**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCreationSagaTrigger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

// Publishing already happened as a side effect of OrderTransitions.create() writing to
// order_events, tailed by the CDC pipeline (see EventSourcedOrderTransitions, Task 8, and the
// Debezium connector config extended in Task 18) — this class exists purely to prevent
// ChoreographyOrderCreationSagaTrigger's JPA-only outbox publish from running a second time.
@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedChoreographyOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    @Override
    public void onOrderCreated(Order order, String eventId) {
    }
}
```

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

// See EventSourcedChoreographyOrderCreationSagaTrigger for why this is a no-op.
@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedChoreographyOrderCancellationSagaTrigger implements OrderCancellationSagaTrigger {

    @Override
    public void onOrderCancelled(Order order, List<OrderDomainEvent> events) {
    }
}
```

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionSagaTrigger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

// See EventSourcedChoreographyOrderCreationSagaTrigger for why this is a no-op.
@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedChoreographyOrderRevisionSagaTrigger implements OrderRevisionSagaTrigger {

    @Override
    public void onOrderRevised(Order order, List<OrderDomainEvent> events) {
    }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.EventSourcedChoreographyTriggersTest"`
Expected: PASS (3 tests)

- [ ] **Step 6: Run full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — confirms the stacked `@ConditionalOnProperty` on the 3 existing triggers didn't
break Spring context loading in `jpa` mode (the default, `persistence.mode` unset, both
conditions' `matchIfMissing=true` should keep them active exactly as before).

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCancellationSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderRevisionSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyOrderCreationSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyOrderCancellationSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyOrderRevisionSagaTrigger.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedChoreographyTriggersTest.java
git commit -m "fix: prevent double-publish of Order events in event-sourcing + choreography mode"
```

---

### Task 15: `PERSISTENCE_MODE` environment wiring

**Files:**
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Modify: `ftgo-order-service/src/test/resources/application.yml`
- Modify: `compose.yml`

**Interfaces:** none — pure configuration, consumed by every `@ConditionalOnProperty(name =
"persistence.mode", ...)` bean added in Tasks 7, 8, and 14.

- [ ] **Step 1: Add `persistence.mode: jpa` to main `application.yml`**

Edit `ftgo-order-service/src/main/resources/application.yml`, adding a new top-level key next to
the existing `saga:` block:

```yaml
persistence:
  mode: jpa

saga:
  mode: choreography
```

- [ ] **Step 2: Add the same default to test `application.yml`**

Edit `ftgo-order-service/src/test/resources/application.yml`, adding:

```yaml
persistence:
  mode: jpa
```

anywhere at the top level (matches the main config's default, keeping every existing test running
in `jpa` mode unless a specific test overrides the property).

- [ ] **Step 3: Add the env var to `compose.yml`'s `order-service` block**

Edit the `order-service` service's `environment` block in `compose.yml`, following the exact
`${VAR:-default}` pattern already used for `OUTBOX_PUBLISH_MODE`/`SAGA_MODE`:

```yaml
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/
      OUTBOX_PUBLISH_MODE: ${OUTBOX_PUBLISH_MODE:-polling}
      SAGA_MODE: ${SAGA_MODE:-choreography}
      PERSISTENCE_MODE: ${PERSISTENCE_MODE:-jpa}
```

(Spring Boot's relaxed binding maps the env var `PERSISTENCE_MODE` to the `persistence.mode`
property automatically, the same way `SAGA_MODE` already maps to `saga.mode` — no extra
`application.yml` placeholder syntax like `${PERSISTENCE_MODE}` is needed, matching how
`SAGA_MODE`/`OUTBOX_PUBLISH_MODE` are wired today.)

- [ ] **Step 4: Run full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — confirms adding the property doesn't change any existing test's Spring context.

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/resources/application.yml \
        compose.yml
git commit -m "feat: wire PERSISTENCE_MODE env var through order-service and compose.yml"
```

---

### Task 16: Extend the Debezium connector to tail `order_events`

**Files:**
- Modify: `infrastructure/debezium/outbox-connector.json`

**Interfaces:** none — infrastructure config only.

`order_events`'s columns (`event_id`, `event_type`, `order_id`, `payload` — see Task 4) were
deliberately named to match the same
`table.field.event.id`/`table.field.event.key`/`table.field.event.payload` mapping the connector
already uses for `outbox_events`, and `route.topic.replacement` is already a fixed
`"order.events"` regardless of source table — so extending `table.include.list` is the entire
change needed; no other connector property changes.

- [ ] **Step 1: Add `order_events` to `table.include.list`**

Edit `infrastructure/debezium/outbox-connector.json`:

```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "mysql",
  "database.port": "3306",
  "database.user": "debezium",
  "database.password": "debezium",
  "database.server.id": "184054",
  "topic.prefix": "ftgo",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
  "schema.history.internal.kafka.topic": "schema-changes.ftgo_order",
  "database.include.list": "ftgo_order",
  "table.include.list": "ftgo_order.outbox_events,ftgo_order.order_events",
  "snapshot.mode": "no_data",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "event_id",
  "transforms.outbox.table.field.event.key": "order_id",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.route.by.field": "event_type",
  "transforms.outbox.route.topic.regex": ".*",
  "transforms.outbox.route.topic.replacement": "order.events"
}
```

This is only exercised in `OUTBOX_PUBLISH_MODE=cdc` (per the existing `connector-registrar`
service, unaffected by this task) — in `polling` mode this file isn't registered with Kafka
Connect at all, matching today's behavior. When `PERSISTENCE_MODE=jpa`, `order_events` simply
stays empty, so watching it is harmless.

- [ ] **Step 2: Commit**

```bash
git add infrastructure/debezium/outbox-connector.json
git commit -m "feat: extend Debezium connector to tail order_events for event-sourcing mode"
```

(Verified end-to-end in Task 24's Docker pass — this file alone can't be unit-tested.)

---

### Task 17: Add a read-only `findById` to `OrderTransitions`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderTransitions.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/JpaOrderTransitions.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitions.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/JpaOrderTransitionsTest.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitionsTest.java`

**Interfaces:**
- Produces: `OrderTransitions.findById(Long orderId): Optional<Order>` — silent-empty-on-missing,
  matching the "return null if not found" idiom every saga/orchestrator call site already uses,
  unlike `cancel`/`revise`'s throwing contract.

`ReviseOrderSagaOrchestrator` (Task 23) needs to read an order's *current* line items to compute
quantities for `tryAuthorize`/`sendUndoReviseTicket` — a genuine read, not a state transition, so
it doesn't fit any of the 9 existing facade methods.

- [ ] **Step 1: Add the method to the interface**

Edit `OrderTransitions.java`, adding one import and one method:

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;
import java.util.Optional;

public interface OrderTransitions {

    Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId);

    Optional<Order> findById(Long orderId);

    TransitionResult cancel(Long orderId, String eventId);

    TransitionResult revise(Long orderId, OrderRevision revision, String eventId);

    void approve(Long orderId, String eventId);

    void reject(Long orderId, String eventId);

    void noteCancelled(Long orderId, String eventId);

    void undoCancel(Long orderId, String eventId);

    void confirmRevision(Long orderId, String eventId);

    void rejectRevision(Long orderId, String eventId);

    void requestRevisionCompensation(Long orderId, String eventId);
}
```

- [ ] **Step 2: Implement it in `JpaOrderTransitions`**

Add to `JpaOrderTransitions.java`:

```java
    @Override
    public java.util.Optional<Order> findById(Long orderId) {
        return orderRepository.findById(orderId);
    }
```

- [ ] **Step 3: Implement it in `EventSourcedOrderTransitions`**

Add to `EventSourcedOrderTransitions.java`:

```java
    @Override
    public java.util.Optional<Order> findById(Long orderId) {
        try {
            return java.util.Optional.of(toOrder(eventStore.find(orderId)));
        } catch (com.sanjay.ftgo.order.domain.OrderNotFoundException e) {
            return java.util.Optional.empty();
        }
    }
```

- [ ] **Step 4: Add one test per implementation**

Add to `JpaOrderTransitionsTest.java`:

```java
    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        when(orderRepository.findById(42L)).thenReturn(Optional.empty());

        assertThat(transitions.findById(42L)).isEmpty();
    }
```

Add to `EventSourcedOrderTransitionsTest.java`:

```java
    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        assertThat(transitions.findById(999L)).isEmpty();
    }

    @Test
    void findByIdReturnsOrderAfterCreate() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThat(transitions.findById(created.getId())).isPresent();
    }
```

- [ ] **Step 5: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.JpaOrderTransitionsTest" --tests "com.sanjay.ftgo.order.eventsourcing.EventSourcedOrderTransitionsTest"`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderTransitions.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/JpaOrderTransitions.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitions.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/JpaOrderTransitionsTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedOrderTransitionsTest.java
git commit -m "feat: add read-only OrderTransitions.findById for orchestrator quantity lookups"
```

---

### Task 18: `OrderSagaCommandRequest` entity and repository

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSagaCommandRequest.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSagaCommandRequestRepository.java`

**Interfaces:**
- Produces: `OrderSagaCommandRequest` (table `order_saga_command_requests`), consumed by `EventSourcedSagaCommandPublisher` (Task 19) and `SagaCommandRequestPublisher` (Task 20).

This is a *separate* table from `order_events`, not a row type within it — the book's own
`events` table mixes pseudo-events with real domain events because Eventuate Local's generic
dispatch mechanism replays everything from one table regardless of purpose. This hand-rolled
implementation keeps them apart deliberately: `order_events` rows are CDC-tailed to `order.events`
unconditionally (Task 16), and a saga command request going out to `kitchen.commands` or
`accounting.commands` would corrupt that stream if it lived in the same table. Writing to a
separate table within the *same* `@Transactional` method that also appends to `order_events` (see
`CreateOrderSagaOrchestrator.start`, Task 21) still gives the same atomicity guarantee the book
describes, since both tables live in this service's one MySQL schema.

- [ ] **Step 1: Write `OrderSagaCommandRequest`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "order_saga_command_requests")
public class OrderSagaCommandRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "command_type", nullable = false)
    private String commandType;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "target_topic", nullable = false)
    private String targetTopic;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OrderSagaCommandRequest() {
    }

    public OrderSagaCommandRequest(String eventId, String commandType, Long orderId, String targetTopic, String payload) {
        this.eventId = eventId;
        this.commandType = commandType;
        this.orderId = orderId;
        this.targetTopic = targetTopic;
        this.payload = payload;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getTargetTopic() {
        return targetTopic;
    }

    public String getPayload() {
        return payload;
    }

    public boolean isPublished() {
        return publishedAt != null;
    }

    public void markPublished() {
        this.publishedAt = Instant.now();
    }
}
```

- [ ] **Step 2: Write the repository**

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderSagaCommandRequestRepository extends JpaRepository<OrderSagaCommandRequest, Long> {

    List<OrderSagaCommandRequest> findByPublishedAtIsNullOrderByIdAsc();
}
```

- [ ] **Step 3: Compile check**

Run: `./gradlew :ftgo-order-service:compileJava`
Expected: BUILD SUCCESSFUL (already covered by `PersistenceConfig`'s
`com.sanjay.ftgo.order.eventsourcing` base-package scan from Task 4 — no config change needed)

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSagaCommandRequest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/OrderSagaCommandRequestRepository.java
git commit -m "feat: add order_saga_command_requests table for event-sourcing-mode saga commands"
```

---

### Task 19: `SagaCommandPublisher` facade

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SagaCommandPublisher.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxSagaCommandPublisher.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedSagaCommandPublisher.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OutboxSagaCommandPublisherTest.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedSagaCommandPublisherTest.java`

**Interfaces:**
- Produces: `SagaCommandPublisher.publish(String topic, String eventId, String eventType, Long orderId, Object command)`. `OutboxSagaCommandPublisher` (jpa mode, replicates the 3 orchestrators' current `publishCommand`/`publishKitchenCommand`/`publishAccountingCommand` logic exactly). `EventSourcedSagaCommandPublisher` (event-sourcing mode, writes to `OrderSagaCommandRequestRepository`, Task 18). Consumed by the 3 orchestrators (Tasks 21–23).

- [ ] **Step 1: Write the interface**

```java
package com.sanjay.ftgo.order.domain;

public interface SagaCommandPublisher {

    void publish(String topic, String eventId, String eventType, Long orderId, Object command);
}
```

- [ ] **Step 2: Write the failing JPA-mode test**

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OutboxSagaCommandPublisherTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Test
    void publishSavesAnOutboxEventWithTheGivenTopic() {
        OutboxSagaCommandPublisher publisher = new OutboxSagaCommandPublisher(outboxEventRepository, new ObjectMapper());

        publisher.publish("kitchen.commands", "evt-1", "CreateTicket", 42L,
                new KitchenCommand("evt-1", "CreateTicket", 42L, 3, "CreateOrder"));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getEventId()).isEqualTo("evt-1");
        assertThat(captor.getValue().getEventType()).isEqualTo("CreateTicket");
        assertThat(captor.getValue().getAggregateId()).isEqualTo(42L);
        assertThat(captor.getValue().getTopic()).isEqualTo("kitchen.commands");
        assertThat(captor.getValue().getPayload()).contains("\"commandType\":\"CreateTicket\"");
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OutboxSagaCommandPublisherTest"`
Expected: FAIL — `OutboxSagaCommandPublisher` does not exist yet.

- [ ] **Step 4: Write `OutboxSagaCommandPublisher`**

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "persistence.mode", havingValue = "jpa", matchIfMissing = true)
public class OutboxSagaCommandPublisher implements SagaCommandPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxSagaCommandPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(String topic, String eventId, String eventType, Long orderId, Object command) {
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, topic, toJson(command)));
    }

    private String toJson(Object command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga command", e);
        }
    }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OutboxSagaCommandPublisherTest"`
Expected: PASS

- [ ] **Step 6: Write the failing event-sourcing-mode test**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.KitchenCommand;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({EventSourcedSagaCommandPublisher.class, ObjectMapper.class})
class EventSourcedSagaCommandPublisherTest {

    @Autowired
    private EventSourcedSagaCommandPublisher publisher;

    @Autowired
    private OrderSagaCommandRequestRepository repository;

    @Test
    void publishWritesAnUnpublishedRequestRow() {
        publisher.publish("kitchen.commands", "evt-1", "CreateTicket", 42L,
                new KitchenCommand("evt-1", "CreateTicket", 42L, 3, "CreateOrder"));

        var pending = repository.findByPublishedAtIsNullOrderByIdAsc();
        assertThat(pending).hasSize(1);
        assertThat(pending.get(0).getEventId()).isEqualTo("evt-1");
        assertThat(pending.get(0).getOrderId()).isEqualTo(42L);
        assertThat(pending.get(0).getTargetTopic()).isEqualTo("kitchen.commands");
        assertThat(pending.get(0).isPublished()).isFalse();
    }
}
```

- [ ] **Step 7: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.EventSourcedSagaCommandPublisherTest"`
Expected: FAIL — `EventSourcedSagaCommandPublisher` does not exist yet.

- [ ] **Step 8: Write `EventSourcedSagaCommandPublisher`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.SagaCommandPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedSagaCommandPublisher implements SagaCommandPublisher {

    private final OrderSagaCommandRequestRepository repository;
    private final ObjectMapper objectMapper;

    public EventSourcedSagaCommandPublisher(OrderSagaCommandRequestRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(String topic, String eventId, String eventType, Long orderId, Object command) {
        repository.save(new OrderSagaCommandRequest(eventId, eventType, orderId, topic, toJson(command)));
    }

    private String toJson(Object command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga command", e);
        }
    }
}
```

- [ ] **Step 9: Run to verify it passes, then run the full suite**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.EventSourcedSagaCommandPublisherTest"`
Expected: PASS

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SagaCommandPublisher.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxSagaCommandPublisher.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedSagaCommandPublisher.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OutboxSagaCommandPublisherTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/EventSourcedSagaCommandPublisherTest.java
git commit -m "feat: add SagaCommandPublisher facade (Outbox and event-sourced implementations)"
```

---

### Task 20: `SagaCommandRequestPublisher` scheduled poller

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/SagaCommandRequestPublisher.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/SagaCommandRequestPublisherTest.java`

**Interfaces:**
- Consumes: `OrderSagaCommandRequestRepository` (Task 18), `KafkaTemplate<String,String>` (already
  provided by `ftgo-common`'s `KafkaProducerConfig`, per `OutboxPublisher`'s existing usage).
- Produces: a `@Scheduled` bean active only when `persistence.mode=event-sourcing`, publishing
  pending `OrderSagaCommandRequest` rows to Kafka and marking them sent — the second half of the
  book's §6.3.4 two-step pseudo-event mechanism (Task 19's `EventSourcedSagaCommandPublisher` is
  the first half: durably recording the intent to send).

- [ ] **Step 1: Write the failing test**

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SagaCommandRequestPublisherTest {

    @Mock
    private OrderSagaCommandRequestRepository repository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private SagaCommandRequestPublisher publisher;

    @Test
    void publishesPendingRequestsAndMarksThemPublished() {
        publisher = new SagaCommandRequestPublisher(repository, kafkaTemplate);
        OrderSagaCommandRequest request =
                new OrderSagaCommandRequest("evt-1", "CreateTicket", 42L, "kitchen.commands", "{\"foo\":1}");
        when(repository.findByPublishedAtIsNullOrderByIdAsc()).thenReturn(List.of(request));

        publisher.publishPending();

        verify(kafkaTemplate).send("kitchen.commands", "42", "{\"foo\":1}");
        verify(repository).save(request);
        assertThat(request.isPublished()).isTrue();
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.SagaCommandRequestPublisherTest"`
Expected: FAIL — `SagaCommandRequestPublisher` does not exist yet.

- [ ] **Step 3: Write `SagaCommandRequestPublisher`**

```java
package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class SagaCommandRequestPublisher {

    private final OrderSagaCommandRequestRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SagaCommandRequestPublisher(OrderSagaCommandRequestRepository repository,
                                        KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms:2000}")
    public void publishPending() {
        List<OrderSagaCommandRequest> pending = repository.findByPublishedAtIsNullOrderByIdAsc();
        for (OrderSagaCommandRequest request : pending) {
            kafkaTemplate.send(request.getTargetTopic(), request.getOrderId().toString(), request.getPayload());
            request.markPublished();
            repository.save(request);
        }
    }
}
```

`@EnableScheduling` is already present on `FtgoOrderServiceApplication` (added in Ch.4 for the
existing `OutboxPublisher`), so no application-class change is needed here.

- [ ] **Step 4: Run to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.eventsourcing.SagaCommandRequestPublisherTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/eventsourcing/SagaCommandRequestPublisher.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/eventsourcing/SagaCommandRequestPublisherTest.java
git commit -m "feat: add SagaCommandRequestPublisher poller for event-sourcing-mode saga commands"
```

---

### Task 21: Rewire `CreateOrderSagaOrchestrator`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java`

**Interfaces:**
- Consumes: `OrderTransitions` (Tasks 6, 17), `SagaCommandPublisher` (Task 19) instead of `OrderRepository`/`OutboxEventRepository`/`OrderDomainEventPublisher`/`ObjectMapper` directly.

`approveOrder`/`rejectOrder`'s old null-check-then-try/catch blocks are dropped entirely —
`OrderTransitions.approve`/`.reject` already silently no-op on a missing order or wrong state, so
looking the order up first here would be redundant.

- [ ] **Step 1: Rewrite `CreateOrderSagaOrchestrator.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class CreateOrderSagaOrchestrator {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository;
    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public CreateOrderSagaOrchestrator(CreateOrderSagaInstanceRepository sagaInstanceRepository,
                                        OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
    }

    @Transactional
    public void start(Order order) {
        int totalQuantity = totalQuantity(order.getLineItems());
        sagaInstanceRepository.save(new CreateOrderSagaInstance(order.getId(), totalQuantity));

        String verifyEventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("consumer.commands", verifyEventId, "VerifyConsumerCommand", order.getId(),
                new VerifyConsumerCommand(verifyEventId, order.getId(), order.getConsumerId()));

        String createTicketEventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", createTicketEventId, "CreateTicket", order.getId(),
                new KitchenCommand(createTicketEventId, "CreateTicket", order.getId(), totalQuantity, "CreateOrder"));
    }

    @Transactional
    public void handleReply(String eventId, String participant, String eventType, Long orderId, String reason) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        CreateOrderSagaInstance instance = sagaInstanceRepository.findById(orderId).orElse(null);
        if (instance == null) {
            return;
        }

        if (instance.isFailed()) {
            if ("kitchen".equals(participant) && "TicketCreated".equals(eventType)) {
                sendCancelTicket(orderId);
            }
            return;
        }

        switch (participant) {
            case "consumer" -> handleConsumerReply(instance, eventType);
            case "kitchen" -> handleKitchenReply(instance, eventType);
            case "accounting" -> handleAccountingReply(instance, eventType);
            default -> { }
        }
    }

    private void handleConsumerReply(CreateOrderSagaInstance instance, String eventType) {
        if ("ConsumerVerificationFailed".equals(eventType)) {
            fail(instance);
            return;
        }
        instance.markConsumerVerified();
        sagaInstanceRepository.save(instance);
        tryAuthorize(instance);
    }

    private void handleKitchenReply(CreateOrderSagaInstance instance, String eventType) {
        if ("TicketCreationFailed".equals(eventType)) {
            fail(instance);
            return;
        }
        instance.markTicketCreated();
        sagaInstanceRepository.save(instance);
        tryAuthorize(instance);
    }

    private void handleAccountingReply(CreateOrderSagaInstance instance, String eventType) {
        Long orderId = instance.getOrderId();
        if ("CardAuthorized".equals(eventType)) {
            orderTransitions.approve(orderId, UUID.randomUUID().toString());
            String eventId = UUID.randomUUID().toString();
            sagaCommandPublisher.publish("kitchen.commands", eventId, "ConfirmTicket", orderId,
                    new KitchenCommand(eventId, "ConfirmTicket", orderId, null, "CreateOrder"));
        } else {
            orderTransitions.reject(orderId, UUID.randomUUID().toString());
            sendCancelTicket(orderId);
        }
    }

    private void tryAuthorize(CreateOrderSagaInstance instance) {
        if (!instance.isConsumerVerified() || !instance.isTicketCreated()) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("accounting.commands", eventId, "AuthorizeCard", instance.getOrderId(),
                new AccountingCommand(eventId, "AuthorizeCard", instance.getOrderId(), instance.getTotalQuantity(), "CreateOrder"));
    }

    private void fail(CreateOrderSagaInstance instance) {
        instance.markFailed();
        sagaInstanceRepository.save(instance);

        orderTransitions.reject(instance.getOrderId(), UUID.randomUUID().toString());

        if (instance.isTicketCreated()) {
            sendCancelTicket(instance.getOrderId());
        }
    }

    private void sendCancelTicket(Long orderId) {
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", eventId, "CancelTicket", orderId,
                new KitchenCommand(eventId, "CancelTicket", orderId, null, "CreateOrder"));
    }

    private int totalQuantity(List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }
}
```

- [ ] **Step 2: Update `CreateOrderSagaOrchestratorTest.java`**

Replace mocked `OrderRepository`/`OutboxEventRepository`/`OrderDomainEventPublisher`/`ObjectMapper`
collaborators with mocked `OrderTransitions`/`SagaCommandPublisher`. Existing assertions that
verified specific `OutboxEvent` rows being saved now verify
`sagaCommandPublisher.publish(topic, any(), eventType, orderId, any())` was called with the
expected topic/eventType/orderId; assertions that verified `Order.noteApproved()`/`noteRejected()`
being invoked (directly or via `orderRepository.save`) now verify
`orderTransitions.approve(orderId, any())`/`.reject(orderId, any())` was called.

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestratorTest"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java
git commit -m "refactor: rewire CreateOrderSagaOrchestrator through OrderTransitions/SagaCommandPublisher"
```

---

### Task 22: Rewire `CancelOrderSagaOrchestrator`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestrator.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java`

**Interfaces:**
- Consumes: `OrderTransitions` (Tasks 6, 17), `SagaCommandPublisher` (Task 19), same shape as Task 21.

- [ ] **Step 1: Rewrite `CancelOrderSagaOrchestrator.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// Deliberately stateless, unlike CreateOrderSagaOrchestrator: Cancel Order is a strict
// linear pipeline (kitchen cancel -> accounting reversal -> order cancelled) with no
// parallel replies to join, so there's no need for a persisted saga instance table.
@Service
public class CancelOrderSagaOrchestrator {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public CancelOrderSagaOrchestrator(OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
    }

    @Transactional
    public void start(Order order) {
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", eventId, "CancelTicket", order.getId(),
                new KitchenCommand(eventId, "CancelTicket", order.getId(), null, "CancelOrder"));
    }

    @Transactional
    public void handleReply(String eventId, String participant, String eventType, Long orderId, String reason) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        switch (participant) {
            case "kitchen" -> handleKitchenReply(eventType, orderId);
            case "accounting" -> handleAccountingReply(eventType, orderId);
            default -> { }
        }
    }

    private void handleKitchenReply(String eventType, Long orderId) {
        if ("TicketCancellationRejected".equals(eventType)) {
            orderTransitions.undoCancel(orderId, UUID.randomUUID().toString());
            return;
        }
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("accounting.commands", eventId, "ReverseAuthorization", orderId,
                new AccountingCommand(eventId, "ReverseAuthorization", orderId, null, "CancelOrder"));
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        if ("AuthorizationReversed".equals(eventType)) {
            orderTransitions.noteCancelled(orderId, UUID.randomUUID().toString());
        }
    }
}
```

- [ ] **Step 2: Update `CancelOrderSagaOrchestratorTest.java`**

Same treatment as Task 21 Step 2 — mock `OrderTransitions`/`SagaCommandPublisher`, verify
`orderTransitions.undoCancel`/`.noteCancelled` and `sagaCommandPublisher.publish` calls in place of
the old `orderRepository`/`outboxEventRepository` assertions.

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestratorTest"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java
git commit -m "refactor: rewire CancelOrderSagaOrchestrator through OrderTransitions/SagaCommandPublisher"
```

---

### Task 23: Rewire `ReviseOrderSagaOrchestrator`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestrator.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestratorTest.java`

**Interfaces:**
- Consumes: `OrderTransitions` (Tasks 6, 17 — including the new `findById` for quantity lookups), `SagaCommandPublisher` (Task 19).

- [ ] **Step 1: Rewrite `ReviseOrderSagaOrchestrator.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// Deliberately stateless, like CancelOrderSagaOrchestrator: Revise Order is a strict linear
// pipeline (kitchen re-check -> accounting re-authorize -> confirm/reject), and both the pending
// revised quantity and the original quantity are recomputed from Order's own line items rather
// than threaded through the reply chain, so no persisted saga instance table is needed either.
@Service
public class ReviseOrderSagaOrchestrator {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public ReviseOrderSagaOrchestrator(OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
    }

    @Transactional
    public void start(Order order) {
        int newTotalQuantity = totalQuantity(order.getPendingRevisedLineItems());
        String eventId = UUID.randomUUID().toString();
        publishKitchenCommand(eventId, "ReviseTicket", order.getId(), newTotalQuantity);
    }

    @Transactional
    public void handleReply(String eventId, String participant, String eventType, Long orderId, String reason) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        switch (participant) {
            case "kitchen" -> handleKitchenReply(eventType, orderId);
            case "accounting" -> handleAccountingReply(eventType, orderId);
            default -> { }
        }
    }

    private void handleKitchenReply(String eventType, Long orderId) {
        switch (eventType) {
            case "TicketRevisionRejected", "TicketRevisionUndone" ->
                    orderTransitions.rejectRevision(orderId, UUID.randomUUID().toString());
            case "TicketQuantityRevised" -> tryAuthorize(orderId);
            default -> { }
        }
    }

    private void tryAuthorize(Long orderId) {
        orderTransitions.findById(orderId).ifPresent(order -> {
            int newTotalQuantity = totalQuantity(order.getPendingRevisedLineItems());
            String eventId = UUID.randomUUID().toString();
            publishAccountingCommand(eventId, "ReviseAuthorization", orderId, newTotalQuantity);
        });
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        switch (eventType) {
            case "AuthorizationRevised" -> orderTransitions.confirmRevision(orderId, UUID.randomUUID().toString());
            case "AuthorizationRevisionRejected" -> sendUndoReviseTicket(orderId);
            default -> { }
        }
    }

    private void sendUndoReviseTicket(Long orderId) {
        orderTransitions.findById(orderId).ifPresent(order -> {
            int originalTotalQuantity = totalQuantity(order.getLineItems());
            String eventId = UUID.randomUUID().toString();
            publishKitchenCommand(eventId, "UndoReviseTicket", orderId, originalTotalQuantity);
        });
    }

    private int totalQuantity(java.util.List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }

    private void publishKitchenCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        sagaCommandPublisher.publish("kitchen.commands", eventId, commandType, orderId,
                new KitchenCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"));
    }

    private void publishAccountingCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        sagaCommandPublisher.publish("accounting.commands", eventId, commandType, orderId,
                new AccountingCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"));
    }
}
```

- [ ] **Step 2: Update `ReviseOrderSagaOrchestratorTest.java`**

Same treatment as Task 21 Step 2, plus updating any test that previously stubbed
`orderRepository.findById(...)` for the quantity-lookup paths (`tryAuthorize`,
`sendUndoReviseTicket`) to stub `orderTransitions.findById(...)` returning an `Optional<Order>`
instead.

- [ ] **Step 3: Run to verify**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.ReviseOrderSagaOrchestratorTest"`
Expected: PASS

- [ ] **Step 4: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — all 3 orchestrators, both trigger sets, and every earlier task's tests green
together for the first time.

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestratorTest.java
git commit -m "refactor: rewire ReviseOrderSagaOrchestrator through OrderTransitions/SagaCommandPublisher"
```

---

### Task 24: Full regression build

**Files:** none — verification only.

**Interfaces:** none.

- [ ] **Step 1: Full clean build**

Run: `./gradlew clean build`
Expected: BUILD SUCCESSFUL — every module compiles and every test (including
`ftgo-common`, `ftgo-kitchen-service`, `ftgo-accounting-service`, `ftgo-restaurant-service`,
`ftgo-service-registry`, none of which this plan touches) passes. This is the checkpoint that
confirms Tasks 1–23's `jpa`-mode behavior is unchanged and no other service was accidentally
affected.

- [ ] **Step 2: Confirm no lingering references to removed dependencies**

Run: `grep -rn "OrderRepository" ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/ ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/*SagaService.java ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/*Orchestrator.java`

Expected: no matches — confirms Tasks 9–13 and 21–23 fully removed the direct `OrderRepository`
dependency from every rewired call site (it's still legitimately used inside
`JpaOrderTransitions`, which is correct and expected).

- [ ] **Step 3: Commit if the greps above required any follow-up fixes**

Only commit if Step 2 surfaced something to fix; otherwise this task produces no diff and is
purely a verification gate — record it as done without a commit.

---

### Task 25: Docker e2e verification and Ch.6 documentation sweep

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `ftgo-order-service/README.md` (if one exists; create a minimal one matching the other
  services' style if not — check `ftgo-kitchen-service/README.md`/`ftgo-accounting-service/README.md`
  for the established format first)

**Interfaces:** none — verification and documentation only.

Per this repo's `CLAUDE.md`, flipping Ch.6's row to Done in `CONTEXT.md`'s progress table
triggers a full documentation sweep, not just the per-change updates already made along the way
(Tasks 1–23 didn't touch any docs). This task is that sweep.

- [ ] **Step 1: Manual Docker verification, `PERSISTENCE_MODE=event-sourcing`, `SAGA_MODE=choreography`**

```bash
docker compose up -d --build
```

Then, with `PERSISTENCE_MODE=event-sourcing SAGA_MODE=choreography` set for `order-service` (via
`.env` or `docker compose ... environment` override — match however `SAGA_MODE`/`OUTBOX_PUBLISH_MODE`
were overridden in prior chapters' sessions):

- Create Order happy path: `POST /orders` → confirm `order_events` has an `OrderCreated` row,
  `order.events` receives the message (via CDC, `OUTBOX_PUBLISH_MODE=cdc`), kitchen-service
  creates a `Ticket`, accounting-service authorizes, order reaches `APPROVED`.
- Cancel Order: `POST /orders/{id}/cancel` → `CANCEL_PENDING` → kitchen confirms →
  `CANCELLED`, `Authorization` reaches `REVERSED`.
- Revise Order: `POST /orders/{id}/revise` → `REVISION_PENDING` → kitchen provisionally revises
  → accounting re-authorizes → `APPROVED` with updated line items; separately, one
  kitchen-rejects and one accounting-declines-after-kitchen-confirms run, confirming the
  compensation path (kitchen's undo) still resolves correctly.
- Redelivery/idempotency: force a Kafka redelivery (same approach as prior chapters — reset
  `sent_at`/replay from the connector) and confirm `order_events` row count and downstream
  `Ticket`/`Authorization` state are unchanged.
- Optimistic-lock sanity check: fire two concurrent requests against the same order (e.g. two
  rapid `/cancel` calls) and confirm only one succeeds observably (the other's saga step becomes
  a documented no-op via the facade's silent-no-op contract, not a 500).

- [ ] **Step 2: Manual Docker verification, `PERSISTENCE_MODE=event-sourcing`, `SAGA_MODE=orchestration`**

Repeat all of Step 1's scenarios with `SAGA_MODE=orchestration`. Additionally confirm:

- `order_saga_command_requests` rows are created during `start()`/reply-handling and flip to
  published (non-null `published_at`) within one `outbox.poll-fixed-delay-ms` interval.
- The three choreography-mode listeners (`KitchenEventListener`/`AccountingEventListener`/
  `ConsumerEventListener`) do *not* fire in orchestration mode (unchanged from before this
  chapter — sanity-check only, not new behavior).

- [ ] **Step 3: Regression check, `PERSISTENCE_MODE=jpa` (or unset), both saga modes**

Repeat the same happy-path + one compensation case for both saga modes with `PERSISTENCE_MODE`
unset (or explicitly `jpa`), confirming identical end states to what Ch.5's sessions already
verified — this is the proof that the default path is truly undisturbed.

- [ ] **Step 4: Update `README.md`**

Update the "Book progress" table's Ch.6 row to `Done`, and the tech-stack/architecture summary if
it mentions persistence patterns per-service, to note `ftgo-order-service` now supports both JPA
and event-sourced persistence for `Order`, switchable via `PERSISTENCE_MODE`.

- [ ] **Step 5: Update `CONTEXT.md`**

- Progress table: flip Ch.6's row to `Done`, confidence `High`, with a one-line note (aggregate,
  mode, both saga modes covered).
- "Current position" section: update to reflect Ch.6 complete, Ch.7 (CQRS/API composition) not
  started.
- "Services to build" table: update `ftgo-order-service`'s row to mention the event-sourced
  `Order` persistence path.
- "Patterns reference" checklist: check off `Event sourcing (Ch. 6)`.
- "Concept understanding" section: move "Event sourcing" out of any "Needs more depth"/"Open
  questions" entries (Ch.6 had none pending going in, per the 2026-07-22 session note — confirm
  and update accordingly) into "Understood well", summarizing what was actually built (the
  `process()`/`apply()` split, hand-rolled event store, snapshot mechanism, CDC reuse, and the
  book-faithful pseudo-event mechanism for orchestration-mode sagas).
- Session log: add a one-liner for this session dated with today's actual date, following the
  existing entries' style and level of detail.

- [ ] **Step 6: Update `docs/ARCHITECTURE.md`**

Add a new section for event sourcing (matching the depth given to the existing saga sections —
sequence diagrams for both saga styles, happy path, and the Revise Order compensation case),
covering: the `process()`/`apply()` split, the `order_events`/`order_aggregate_version`/
`order_snapshots`/`order_id_allocations` schema, how CDC publishing was extended, and the
orchestration-mode pseudo-event mechanism (`order_saga_command_requests` +
`SagaCommandRequestPublisher`) with its own sequence diagram showing the two-step
durable-write-then-poll flow.

- [ ] **Step 7: Update or create `ftgo-order-service/README.md`**

Full API/events/domain-model parity with the current code, per this repo's chapter-completion
rule — document both persistence modes, the `PERSISTENCE_MODE` env var, and the event-sourcing
package's public surface (`OrderAggregate`, `OrderEventStore`, `OrderTransitions` and its two
implementations).

- [ ] **Step 8: Commit the documentation sweep**

```bash
git add README.md CONTEXT.md docs/ARCHITECTURE.md ftgo-order-service/README.md
git commit -m "docs: Ch.6 event sourcing documentation sweep (Order aggregate, both persistence modes)"
```

---

**End of plan.** 25 tasks across 8 parts: shared building blocks (1–3), event store persistence
(4–5), the `OrderTransitions` facade and call-site rewiring (6–14), environment/CDC wiring
(15–16), the orchestration-mode pseudo-event mechanism (17–20), orchestrator rewiring (21–23),
and verification/documentation (24–25).
