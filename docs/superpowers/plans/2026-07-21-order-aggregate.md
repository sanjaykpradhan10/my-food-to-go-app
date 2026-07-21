# Order Aggregate Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor order-service's `Order` from an anemic entity + two duplicated ad hoc guard-and-mutate call sites into a real DDD aggregate (Ch. 5 of *Microservices Patterns*) with the full Fig. 5.14 state machine (create, cancel, revise) and returned domain events, adding `POST /orders/{id}/cancel` and `POST /orders/{id}/revise` REST endpoints ahead of the sagas (future sessions) that will resolve their pending states.

**Architecture:** `Order` gains 3 new `OrderStatus` values and 8 guarded state-changing methods (`noteApproved`, `noteRejected`, `cancel`, `noteCancelled`, `undoCancel`, `revise`, `confirmRevision`, `rejectRevision`) that enforce legal transitions and return `List<OrderDomainEvent>` — one class per event. A new `OrderDomainEventPublisher` translates those typed events (plus order creation, handled separately) into a generic flat `OrderEvent` wire record on `order.events`, replacing the single-purpose `OrderCreatedEvent` wire record. `OrderSagaService` (choreography) and `CreateOrderSagaOrchestrator` (orchestration) both call the new aggregate methods instead of the old unguarded `markApproved()`/`markRejected()`. `OrderController` gains the two new REST endpoints. `ftgo-kitchen-service`'s `OrderEventListener` gets a required correctness fix so it doesn't misinterpret the new event types as `OrderCreated`.

**Tech Stack:** Java 17+, Spring Boot 3.5.16, Spring Data JPA, Spring Web, Spring Kafka, JUnit 5, Mockito, AssertJ, MockMvc + `@WebMvcTest`.

## Global Constraints

- **`OrderCreated`'s wire shape is unchanged, byte-for-byte.** Renaming the wire record class from `OrderCreatedEvent` to a generic `OrderEvent` must not change any JSON field name, order, or presence for the `OrderCreated` case — `ftgo-kitchen-service`'s independent copy of `OrderCreatedEvent` deserializes it today and must keep working unmodified except for the one required fix below.
- **`ftgo-kitchen-service`'s `OrderEventListener` must check `eventType` before acting.** Today it deserializes every `order.events` payload as `OrderCreatedEvent` and calls `handleOrderCreated` unconditionally. Once `order.events` carries `OrderApproved`/`OrderCancelled`/etc., this would create a bogus `Ticket` from null fields unless fixed. This is in scope and required by this plan (Task 7), not optional cleanup.
- **No DB schema changes** beyond `OrderStatus` gaining 3 new enum values (`@Enumerated(EnumType.STRING)` on the existing `status` column, same pattern as `TicketState`).
- **No new capability beyond what's in the spec.** No REST endpoints for `noteCancelled`/`undoCancel`/`confirmRevision`/`rejectRevision` — those are saga-internal, for a future session (Cancel/Revise Order sagas) to call.
- **A known, temporary gap is expected and correct, not a bug to work around**: calling `/cancel` or `/revise` will leave the order in `CANCEL_PENDING`/`REVISION_PENDING` with nothing to resolve it yet. Do not add compensating logic, timeouts, or auto-resolution for this — it's explicitly deferred to future sessions (Cancel Order saga, Revise Order saga).
- Java 17+ sealed interfaces/records/switch pattern matching are preferred over `instanceof` chains, matching this codebase's existing style (see `TicketDomainEvent`/`TicketDomainEventPublisher`).
- Follow this codebase's existing test conventions exactly: Mockito-mocked collaborators + AssertJ assertions for service/domain tests, `@WebMvcTest` + `MockMvc` + `@MockitoBean` for controller tests (see `OrderControllerTest.java`, `TicketTest.java`, `TicketDomainEventPublisherTest.java` for the established shape).

---

## Task 1: Domain event types, extended state enum, exceptions, `OrderRevision`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderStatus.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderApprovedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRejectedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelledEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelConfirmedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelRejectedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisionProposedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisionRejectedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevision.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderNotFoundException.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCannotBeCancelledException.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/UnsupportedStateTransitionException.java`

**Interfaces:**
- Produces: `OrderStatus` enum with 6 values (`APPROVAL_PENDING, APPROVED, REJECTED, CANCEL_PENDING, CANCELLED, REVISION_PENDING`); sealed `OrderDomainEvent` interface with `Long orderId()`, permitting the 8 event records below; `OrderRevision(List<OrderLineItem> revisedLineItems)` — used by Task 2. `OrderNotFoundException(Long orderId)`, `OrderCannotBeCancelledException(Long orderId)`, `UnsupportedStateTransitionException(OrderStatus status)` — used by Tasks 2, 4, 5, 6.

These are pure data/exception types with no behavior of their own (exercised indirectly by Task 2's aggregate tests), so this task has no independent TDD cycle — it's scaffolding the next tasks depend on. Verified by compiling.

- [ ] **Step 1: Rewrite `OrderStatus.java`**

```java
package com.sanjay.ftgo.order.domain;

public enum OrderStatus {
    APPROVAL_PENDING,
    APPROVED,
    REJECTED,
    CANCEL_PENDING,
    CANCELLED,
    REVISION_PENDING
}
```

- [ ] **Step 2: Create `OrderDomainEvent.java`**

```java
package com.sanjay.ftgo.order.domain;

public sealed interface OrderDomainEvent
        permits OrderApprovedEvent, OrderRejectedEvent, OrderCancelledEvent, OrderCancelConfirmedEvent,
                OrderCancelRejectedEvent, OrderRevisionProposedEvent, OrderRevisedEvent, OrderRevisionRejectedEvent {

    Long orderId();
}
```

- [ ] **Step 3: Create the 8 event records**

`OrderApprovedEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

public record OrderApprovedEvent(Long orderId) implements OrderDomainEvent {
}
```

`OrderRejectedEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

public record OrderRejectedEvent(Long orderId) implements OrderDomainEvent {
}
```

`OrderCancelledEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

public record OrderCancelledEvent(Long orderId) implements OrderDomainEvent {
}
```

`OrderCancelConfirmedEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

public record OrderCancelConfirmedEvent(Long orderId) implements OrderDomainEvent {
}
```

`OrderCancelRejectedEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

public record OrderCancelRejectedEvent(Long orderId) implements OrderDomainEvent {
}
```

`OrderRevisionProposedEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderRevisionProposedEvent(Long orderId, List<OrderLineItem> revisedLineItems) implements OrderDomainEvent {
}
```

`OrderRevisedEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderRevisedEvent(Long orderId, List<OrderLineItem> revisedLineItems) implements OrderDomainEvent {
}
```

`OrderRevisionRejectedEvent.java`:
```java
package com.sanjay.ftgo.order.domain;

public record OrderRevisionRejectedEvent(Long orderId) implements OrderDomainEvent {
}
```

- [ ] **Step 4: Create `OrderRevision.java`**

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderRevision(List<OrderLineItem> revisedLineItems) {
}
```

- [ ] **Step 5: Create the 3 exception types**

`OrderNotFoundException.java`:
```java
package com.sanjay.ftgo.order.domain;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(Long orderId) {
        super("Order not found: " + orderId);
    }
}
```

`OrderCannotBeCancelledException.java`:
```java
package com.sanjay.ftgo.order.domain;

public class OrderCannotBeCancelledException extends RuntimeException {

    public OrderCannotBeCancelledException(Long orderId) {
        super("Order " + orderId + " cannot be cancelled in its current state");
    }
}
```

`UnsupportedStateTransitionException.java`:
```java
package com.sanjay.ftgo.order.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(OrderStatus status) {
        super("Unsupported transition from status " + status);
    }
}
```

- [ ] **Step 6: Verify the module compiles**

Run: `./gradlew :ftgo-order-service:compileJava`
Expected: `BUILD SUCCESSFUL` (these types aren't referenced anywhere yet, so nothing else can break).

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderStatus.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderApprovedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRejectedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelledEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelConfirmedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelRejectedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisionProposedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisionRejectedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevision.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderNotFoundException.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCannotBeCancelledException.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/UnsupportedStateTransitionException.java
git commit -m "feat(order-service): add Order domain event types and extended status enum"
```

---

## Task 2: `Order` aggregate — state machine and domain events

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderTest.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java` (only the `order.markRejected()` call in the test helper — full rewrite happens in Task 4; here it's changed to `order.noteRejected()` since `markRejected()` is being removed in this step and the file must keep compiling)
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java` (no code change needed — it never calls `markApproved`/`markRejected` directly, only asserts `getStatus()`; listed here only because Step 2's compile-check step touches the whole module)

**Interfaces:**
- Consumes: `OrderStatus`, `OrderDomainEvent`, `OrderApprovedEvent`, `OrderRejectedEvent`, `OrderCancelledEvent`, `OrderCancelConfirmedEvent`, `OrderCancelRejectedEvent`, `OrderRevisionProposedEvent`, `OrderRevisedEvent`, `OrderRevisionRejectedEvent`, `OrderRevision`, `OrderCannotBeCancelledException`, `UnsupportedStateTransitionException` (all from Task 1).
- Produces: `Order` instance methods `noteApproved()`, `noteRejected()`, `cancel()`, `noteCancelled()`, `undoCancel()`, `revise(OrderRevision)`, `confirmRevision(OrderRevision)`, `rejectRevision()` — each returns `List<OrderDomainEvent>` and mutates `status` (and `lineItems`, for `confirmRevision`). `markApproved()`/`markRejected()` are removed. Consumed by Task 4 (`OrderSagaService`), Task 5 (`CreateOrderSagaOrchestrator`), and Task 6 (`OrderController`).

- [ ] **Step 1: Write the failing test suite**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private Order orderIn(OrderStatus status) {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), status);
    }

    @Test
    void noteApprovedMovesFromApprovalPendingToApproved() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = order.noteApproved();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(events).containsExactly(new OrderApprovedEvent(42L));
    }

    @Test
    void noteApprovedFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::noteApproved).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void noteRejectedMovesFromApprovalPendingToRejected() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = order.noteRejected();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(events).containsExactly(new OrderRejectedEvent(42L));
    }

    @Test
    void noteRejectedFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.REJECTED);

        assertThatThrownBy(order::noteRejected).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromApprovedMovesToCancelPending() {
        Order order = orderIn(OrderStatus.APPROVED);

        List<OrderDomainEvent> events = order.cancel();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
        assertThat(events).containsExactly(new OrderCancelledEvent(42L));
    }

    @Test
    void cancelFromApprovalPendingThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromRejectedThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.REJECTED);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromCancelPendingThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.CANCEL_PENDING);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromCancelledThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.CANCELLED);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromRevisionPendingThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.REVISION_PENDING);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void noteCancelledMovesFromCancelPendingToCancelled() {
        Order order = orderIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = order.noteCancelled();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(events).containsExactly(new OrderCancelConfirmedEvent(42L));
    }

    @Test
    void noteCancelledFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::noteCancelled).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void undoCancelMovesFromCancelPendingBackToApproved() {
        Order order = orderIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = order.undoCancel();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(events).containsExactly(new OrderCancelRejectedEvent(42L));
    }

    @Test
    void undoCancelFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::undoCancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseFromApprovedMovesToRevisionPending() {
        Order order = orderIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        List<OrderDomainEvent> events = order.revise(revision);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        assertThat(events).containsExactly(new OrderRevisionProposedEvent(42L, revision.revisedLineItems()));
    }

    @Test
    void reviseFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        assertThatThrownBy(() -> order.revise(revision)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void confirmRevisionMovesFromRevisionPendingToApprovedAndAppliesLineItems() {
        Order order = orderIn(OrderStatus.REVISION_PENDING);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        List<OrderDomainEvent> events = order.confirmRevision(revision);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).isEqualTo(revision.revisedLineItems());
        assertThat(events).containsExactly(new OrderRevisedEvent(42L, revision.revisedLineItems()));
    }

    @Test
    void confirmRevisionFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        assertThatThrownBy(() -> order.confirmRevision(revision))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void rejectRevisionMovesFromRevisionPendingToApprovedWithoutChangingLineItems() {
        Order order = orderIn(OrderStatus.REVISION_PENDING);
        List<OrderLineItem> originalLineItems = order.getLineItems();

        List<OrderDomainEvent> events = order.rejectRevision();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).isEqualTo(originalLineItems);
        assertThat(events).containsExactly(new OrderRevisionRejectedEvent(42L));
    }

    @Test
    void rejectRevisionFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::rejectRevision).isInstanceOf(UnsupportedStateTransitionException.class);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderTest"`
Expected: Compilation failure — `Order` has no `noteApproved`, `noteRejected`, `cancel`, `noteCancelled`, `undoCancel`, `revise`, `confirmRevision`, or `rejectRevision` yet (current `Order` only has unguarded `markApproved()`/`markRejected()`).

- [ ] **Step 3: Rewrite `Order.java`**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`:

```java
package com.sanjay.ftgo.order.domain;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;

import java.util.List;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long consumerId;

    private Long restaurantId;

    @ElementCollection
    @CollectionTable(name = "order_line_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderLineItem> lineItems;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    protected Order() {
    }

    public Order(Long id, Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this.id = id;
        this.consumerId = consumerId;
        this.restaurantId = restaurantId;
        this.lineItems = lineItems;
        this.status = status;
    }

    public Order(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this(null, consumerId, restaurantId, lineItems, status);
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

    public List<OrderDomainEvent> noteApproved() {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        return List.of(new OrderApprovedEvent(id));
    }

    public List<OrderDomainEvent> noteRejected() {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.REJECTED;
        return List.of(new OrderRejectedEvent(id));
    }

    public List<OrderDomainEvent> cancel() {
        if (status != OrderStatus.APPROVED) {
            throw new OrderCannotBeCancelledException(id);
        }
        this.status = OrderStatus.CANCEL_PENDING;
        return List.of(new OrderCancelledEvent(id));
    }

    public List<OrderDomainEvent> noteCancelled() {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.CANCELLED;
        return List.of(new OrderCancelConfirmedEvent(id));
    }

    public List<OrderDomainEvent> undoCancel() {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        return List.of(new OrderCancelRejectedEvent(id));
    }

    public List<OrderDomainEvent> revise(OrderRevision revision) {
        if (status != OrderStatus.APPROVED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.REVISION_PENDING;
        return List.of(new OrderRevisionProposedEvent(id, revision.revisedLineItems()));
    }

    public List<OrderDomainEvent> confirmRevision(OrderRevision revision) {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        this.lineItems = revision.revisedLineItems();
        return List.of(new OrderRevisedEvent(id, revision.revisedLineItems()));
    }

    public List<OrderDomainEvent> rejectRevision() {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        return List.of(new OrderRevisionRejectedEvent(id));
    }
}
```

- [ ] **Step 4: Fix the now-broken `markRejected()` call in `OrderSagaServiceTest`**

In `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java`, change line 53 (inside `doesNotReapproveAnAlreadyRejectedOrder`) from:

```java
        order.markRejected();
```

to:

```java
        order.noteRejected();
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderTest"`
Expected: `BUILD SUCCESSFUL`, all 20 tests pass.

- [ ] **Step 6: Run the full order-service test suite to confirm nothing else broke**

Run: `./gradlew :ftgo-order-service:test`
Expected: Compiles and runs (some tests in `OrderSagaServiceTest`/`CreateOrderSagaOrchestratorTest` will still pass since they only call `getStatus()`/`approve()`/`reject()`, which still delegate to the still-present-but-not-yet-rewired service layer — those services are rewritten in Tasks 4–5).

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java
git commit -m "feat(order-service): make Order a real DDD aggregate with the full Fig 5.14 state machine"
```

---

## Task 3: `OrderDomainEventPublisher` and the generic `OrderEvent` wire record

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderEvent.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisherTest.java`

**Interfaces:**
- Consumes: `Order` (Task 2), `OrderDomainEvent` and its 8 implementations (Task 1), `com.sanjay.ftgo.common.outbox.OutboxEvent`/`OutboxEventRepository` (existing, `ftgo-common`).
- Produces: `OrderEvent(String eventId, String eventType, Long orderId, Long consumerId, Long restaurantId, List<OrderEvent.LineItem> lineItems)` — the new generic wire record, `OrderEvent.LineItem(Long menuItemId, int quantity)`. `OrderDomainEventPublisher.publishOrderCreated(Order order, String eventId)` and `OrderDomainEventPublisher.publish(List<OrderDomainEvent> events)` — consumed by Task 4 (`OrderSagaService`), Task 5 (`CreateOrderSagaOrchestrator`, `ChoreographyOrderCreationSagaTrigger`), and Task 6 (`OrderController`).

- [ ] **Step 1: Write the failing test**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisherTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final OrderDomainEventPublisher publisher =
            new OrderDomainEventPublisher(outboxEventRepository, new ObjectMapper());

    @Test
    void publishOrderCreatedKeepsTheSameWireShapeAsBeforeTheRefactor() {
        Order order = new Order(42L, 7L, 3L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

        publisher.publishOrderCreated(order, "event-1");

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCreated".equals(row.getEventType())
                        && "order.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"eventId\":\"event-1\"")
                        && row.getPayload().contains("\"eventType\":\"OrderCreated\"")
                        && row.getPayload().contains("\"orderId\":42")
                        && row.getPayload().contains("\"consumerId\":7")
                        && row.getPayload().contains("\"restaurantId\":3")
                        && row.getPayload().contains("\"menuItemId\":10")
                        && row.getPayload().contains("\"quantity\":2")));
    }

    @Test
    void publishesOrderApprovedWithoutConsumerOrLineItemFields() {
        publisher.publish(List.of(new OrderApprovedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderApproved".equals(row.getEventType())
                        && "order.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"orderId\":42")
                        && row.getPayload().contains("\"consumerId\":null")
                        && row.getPayload().contains("\"lineItems\":null")));
    }

    @Test
    void publishesOrderRejected() {
        publisher.publish(List.of(new OrderRejectedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRejected".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderCancelledProposal() {
        publisher.publish(List.of(new OrderCancelledEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCancelled".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderCancelConfirmed() {
        publisher.publish(List.of(new OrderCancelConfirmedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCancelConfirmed".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderCancelRejected() {
        publisher.publish(List.of(new OrderCancelRejectedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCancelRejected".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderRevisionProposedWithRevisedLineItems() {
        publisher.publish(List.of(new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5)))));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevisionProposed".equals(row.getEventType())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"menuItemId\":10")
                        && row.getPayload().contains("\"quantity\":5")));
    }

    @Test
    void publishesOrderRevisedWithRevisedLineItems() {
        publisher.publish(List.of(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 5)))));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevised".equals(row.getEventType())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"quantity\":5")));
    }

    @Test
    void publishesOrderRevisionRejected() {
        publisher.publish(List.of(new OrderRevisionRejectedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevisionRejected".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderDomainEventPublisherTest"`
Expected: Compilation failure — `OrderDomainEventPublisher` doesn't exist yet.

- [ ] **Step 3: Delete `OrderCreatedEvent.java` and create `OrderEvent.java`**

Delete `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java`.

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderEvent.java`:

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        Long restaurantId,
        List<LineItem> lineItems) {

    public record LineItem(Long menuItemId, int quantity) {
    }
}
```

- [ ] **Step 4: Create `OrderDomainEventPublisher.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class OrderDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publishOrderCreated(Order order, String eventId) {
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderCreated", order.getId(),
                order.getConsumerId(), order.getRestaurantId(), toWireLineItems(order.getLineItems()));
        save(eventId, wireEvent);
    }

    public void publish(List<OrderDomainEvent> events) {
        events.forEach(this::publishEvent);
    }

    private void publishEvent(OrderDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        save(eventId, toWireEvent(eventId, event));
    }

    private OrderEvent toWireEvent(String eventId, OrderDomainEvent event) {
        return switch (event) {
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

    private List<OrderEvent.LineItem> toWireLineItems(List<OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    private void save(String eventId, OrderEvent wireEvent) {
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "order.events", toJson(wireEvent)));
    }

    private String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order event " + event.eventType(), e);
        }
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderDomainEventPublisherTest"`
Expected: `BUILD SUCCESSFUL`, all 9 tests pass.

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisherTest.java
git rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java
git commit -m "feat(order-service): add OrderDomainEventPublisher and generalize the order.events wire record"
```

---

## Task 4: Rewrite `OrderSagaService` (choreography) to use the aggregate

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java`

**Interfaces:**
- Consumes: `Order.noteApproved()`/`noteRejected()` (Task 2), `OrderDomainEventPublisher.publish(List<OrderDomainEvent>)` (Task 3), `UnsupportedStateTransitionException` (Task 1).
- Produces: `OrderSagaService` constructor signature changes to add an `OrderDomainEventPublisher` parameter — consumed by Spring's autowiring (no explicit change needed elsewhere) and by `OrderSagaServiceTest`.

- [ ] **Step 1: Write the updated test file (failing first)**

Replace the full contents of `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class OrderSagaServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final OrderSagaService orderSagaService =
            new OrderSagaService(orderRepository, processedEventRepository, domainEventPublisher);

    private Order pendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void approvesOrderInApprovalPending() {
        Order order = pendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.approve(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(orderRepository).save(order);
        verify(domainEventPublisher).publish(List.of(new OrderApprovedEvent(42L)));
    }

    @Test
    void rejectsOrderInApprovalPending() {
        Order order = pendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.reject(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(orderRepository).save(order);
        verify(domainEventPublisher).publish(List.of(new OrderRejectedEvent(42L)));
    }

    @Test
    void doesNotReapproveAnAlreadyRejectedOrder() {
        Order order = pendingOrder();
        order.noteRejected();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.approve(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(orderRepository, never()).save(any());
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderSagaService.approve(42L, "e1");

        verify(orderRepository, never()).findById(any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderSagaServiceTest"`
Expected: Compilation failure — `OrderSagaService`'s constructor doesn't accept an `OrderDomainEventPublisher` yet.

- [ ] **Step 3: Rewrite `OrderSagaService.java`**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java`:

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class OrderSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderSagaService(OrderRepository orderRepository,
                             ProcessedEventRepository processedEventRepository,
                             OrderDomainEventPublisher domainEventPublisher) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Transactional
    public void approve(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.noteApproved();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring approve for order {}: {}", orderId, e.getMessage());
        }
    }

    @Transactional
    public void reject(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.noteRejected();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring reject for order {}: {}", orderId, e.getMessage());
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderSagaServiceTest"`
Expected: `BUILD SUCCESSFUL`, all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java
git commit -m "refactor(order-service): OrderSagaService orchestrates the Order aggregate instead of building events inline"
```

---

## Task 5: Rewrite `CreateOrderSagaOrchestrator` (orchestration) and `ChoreographyOrderCreationSagaTrigger`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTrigger.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTriggerTest.java`

**Interfaces:**
- Consumes: `Order.noteApproved()`/`noteRejected()` (Task 2), `OrderDomainEventPublisher.publish(List<OrderDomainEvent>)`/`publishOrderCreated(Order, String)` (Task 3), `UnsupportedStateTransitionException` (Task 1).
- Produces: `CreateOrderSagaOrchestrator` constructor gains an `OrderDomainEventPublisher` parameter. `ChoreographyOrderCreationSagaTrigger` constructor changes from `(OutboxEventRepository, ObjectMapper)` to `(OrderDomainEventPublisher)` — both consumed by Spring's autowiring and by their respective tests.

- [ ] **Step 1: Write the updated `CreateOrderSagaOrchestratorTest` (failing first)**

The only required change is adding the `OrderDomainEventPublisher` mock to the constructor call and two new assertions verifying it's used. Modify `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java`:

Replace lines 1–28 (imports and field declarations) with:

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CreateOrderSagaOrchestratorTest {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository = mock(CreateOrderSagaInstanceRepository.class);
    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final CreateOrderSagaOrchestrator orchestrator = new CreateOrderSagaOrchestrator(
            sagaInstanceRepository, orderRepository, processedEventRepository, outboxEventRepository,
            domainEventPublisher, objectMapper);

    private Order pendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
    }
```

Then add these two assertions to the existing `approvesOrderDirectlyOnCardAuthorizedWithoutWaitingForConfirmation` and `rejectsOrderAndCancelsTicketOnCardAuthorizationFailed` tests (insert immediately after each test's existing `assertThat(order.getStatus())...` line):

```java
        verify(domainEventPublisher).publish(List.of(new OrderApprovedEvent(42L)));
```

and, respectively:

```java
        verify(domainEventPublisher).publish(List.of(new OrderRejectedEvent(42L)));
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestratorTest"`
Expected: Compilation failure — `CreateOrderSagaOrchestrator`'s constructor doesn't accept an `OrderDomainEventPublisher` yet.

- [ ] **Step 3: Rewrite `CreateOrderSagaOrchestrator.java`**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java`:

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class CreateOrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(CreateOrderSagaOrchestrator.class);

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository;
    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public CreateOrderSagaOrchestrator(CreateOrderSagaInstanceRepository sagaInstanceRepository,
                                        OrderRepository orderRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        OutboxEventRepository outboxEventRepository,
                                        OrderDomainEventPublisher domainEventPublisher,
                                        ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void start(Order order) {
        int totalQuantity = totalQuantity(order.getLineItems());
        sagaInstanceRepository.save(new CreateOrderSagaInstance(order.getId(), totalQuantity));

        String verifyEventId = UUID.randomUUID().toString();
        publishCommand("consumer.commands", verifyEventId, "VerifyConsumerCommand", order.getId(),
                new VerifyConsumerCommand(verifyEventId, order.getId(), order.getConsumerId()));

        String createTicketEventId = UUID.randomUUID().toString();
        publishCommand("kitchen.commands", createTicketEventId, "CreateTicket", order.getId(),
                new KitchenCommand(createTicketEventId, "CreateTicket", order.getId(), totalQuantity));
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
        Order order = orderRepository.findById(instance.getOrderId()).orElse(null);
        if (order == null) {
            return;
        }
        if ("CardAuthorized".equals(eventType)) {
            approveOrder(order);
            String eventId = UUID.randomUUID().toString();
            publishCommand("kitchen.commands", eventId, "ConfirmTicket", instance.getOrderId(),
                    new KitchenCommand(eventId, "ConfirmTicket", instance.getOrderId(), null));
        } else {
            rejectOrder(order);
            sendCancelTicket(instance.getOrderId());
        }
    }

    private void tryAuthorize(CreateOrderSagaInstance instance) {
        if (!instance.isConsumerVerified() || !instance.isTicketCreated()) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        publishCommand("accounting.commands", eventId, "AuthorizeCard", instance.getOrderId(),
                new AuthorizeCardCommand(eventId, instance.getOrderId(), instance.getTotalQuantity()));
    }

    private void fail(CreateOrderSagaInstance instance) {
        instance.markFailed();
        sagaInstanceRepository.save(instance);

        Order order = orderRepository.findById(instance.getOrderId()).orElse(null);
        if (order != null) {
            rejectOrder(order);
        }

        if (instance.isTicketCreated()) {
            sendCancelTicket(instance.getOrderId());
        }
    }

    private void sendCancelTicket(Long orderId) {
        String eventId = UUID.randomUUID().toString();
        publishCommand("kitchen.commands", eventId, "CancelTicket", orderId,
                new KitchenCommand(eventId, "CancelTicket", orderId, null));
    }

    private void approveOrder(Order order) {
        try {
            List<OrderDomainEvent> events = order.noteApproved();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring approve for order {}: {}", order.getId(), e.getMessage());
        }
    }

    private void rejectOrder(Order order) {
        try {
            List<OrderDomainEvent> events = order.noteRejected();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring reject for order {}: {}", order.getId(), e.getMessage());
        }
    }

    private int totalQuantity(List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }

    private void publishCommand(String topic, String eventId, String eventType, Long orderId, Object command) {
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

- [ ] **Step 4: Run the orchestrator test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestratorTest"`
Expected: `BUILD SUCCESSFUL`, all 9 tests pass.

- [ ] **Step 5: Write the updated `ChoreographyOrderCreationSagaTriggerTest` (failing first)**

Replace the full contents of `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTriggerTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ChoreographyOrderCreationSagaTriggerTest {

    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);

    private final ChoreographyOrderCreationSagaTrigger trigger =
            new ChoreographyOrderCreationSagaTrigger(domainEventPublisher);

    @Test
    void delegatesToOrderDomainEventPublisher() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

        trigger.onOrderCreated(order, "event-1");

        verify(domainEventPublisher).publishOrderCreated(order, "event-1");
    }
}
```

- [ ] **Step 6: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.ChoreographyOrderCreationSagaTriggerTest"`
Expected: Compilation failure — `ChoreographyOrderCreationSagaTrigger`'s constructor doesn't accept an `OrderDomainEventPublisher` yet.

- [ ] **Step 7: Rewrite `ChoreographyOrderCreationSagaTrigger.java`**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTrigger.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
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

- [ ] **Step 8: Run both tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.ChoreographyOrderCreationSagaTriggerTest" --tests "com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestratorTest"`
Expected: `BUILD SUCCESSFUL`, all tests pass.

- [ ] **Step 9: Run the full order-service test suite to check for regressions**

Run: `./gradlew :ftgo-order-service:test`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 10: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTrigger.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTriggerTest.java
git commit -m "refactor(order-service): saga orchestrator and creation trigger publish through OrderDomainEventPublisher"
```

---

## Task 6: `OrderController` — cancel/revise REST endpoints

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/ReviseOrderRequest.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`
- Modify: `README.md`

**Interfaces:**
- Consumes: `Order.cancel()`/`revise(OrderRevision)` (Task 2), `OrderDomainEventPublisher.publish(List<OrderDomainEvent>)` (Task 3), `OrderNotFoundException`, `OrderCannotBeCancelledException`, `UnsupportedStateTransitionException` (Task 1), `OrderRevision` (Task 1), `OrderRepository` (existing).
- Produces: `POST /orders/{id}/cancel` → 200 with `OrderResponse` body, `POST /orders/{id}/revise` → 200 with `OrderResponse` body. 404 on unknown order, 409 on illegal transition.

- [ ] **Step 1: Write the updated test file (failing first)**

Replace the full contents of `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`:

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderDomainEventPublisher;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderService orderService;

    @MockitoBean
    private OrderRepository orderRepository;

    @MockitoBean
    private OrderDomainEventPublisher domainEventPublisher;

    @Test
    void createsOrderSuccessfully() throws Exception {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderService.createOrder(eq(1L), eq(1L), any())).thenReturn(order);

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":2}]}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.consumerId").value(1))
                .andExpect(jsonPath("$.restaurantId").value(1))
                .andExpect(jsonPath("$.status").value("APPROVAL_PENDING"));
    }

    @Test
    void returns404WhenRestaurantNotFound() throws Exception {
        when(orderService.createOrder(eq(1L), eq(99L), any())).thenThrow(new RestaurantNotFoundException(99L));

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":99,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns404WhenMenuItemNotFound() throws Exception {
        when(orderService.createOrder(eq(1L), eq(1L), any())).thenThrow(new MenuItemNotFoundException(999L, 1L));

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":999,"quantity":1}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns503WhenRestaurantServiceUnavailable() throws Exception {
        when(orderService.createOrder(eq(1L), eq(1L), any()))
                .thenThrow(new RestaurantServiceUnavailableException(1L, new RuntimeException("timeout")));

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void returns400WhenConsumerIdMissing() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returns400WhenRestaurantIdMissing() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returns400WhenLineItemsEmpty() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void cancelsAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCEL_PENDING"));
    }

    @Test
    void returns404WhenCancellingUnknownOrder() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/orders/99/cancel"))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenCancellingAnOrderThatCannotBeCancelled() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isConflict());
    }

    @Test
    void revisesAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REVISION_PENDING"));
    }

    @Test
    void returns404WhenRevisingUnknownOrder() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/orders/99/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenRevisingAnOrderNotYetApproved() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isConflict());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest"`
Expected: Compilation failure — `OrderController`'s constructor doesn't accept `OrderRepository`/`OrderDomainEventPublisher` yet, and `/orders/{id}/cancel`/`/revise` don't exist.

- [ ] **Step 3: Create `ReviseOrderRequest.java`**

```java
package com.sanjay.ftgo.order.api;

import java.util.List;

public record ReviseOrderRequest(List<LineItemRequest> lineItems) {

    public record LineItemRequest(Long menuItemId, int quantity) {
    }
}
```

- [ ] **Step 4: Rewrite `OrderController.java`**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`:

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEventPublisher;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderController(OrderService orderService, OrderRepository orderRepository,
                            OrderDomainEventPublisher domainEventPublisher) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
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
    public ResponseEntity<OrderResponse> cancel(@PathVariable Long id) {
        Order order = findOrder(id);
        apply(order, order.cancel());
        return ResponseEntity.ok(OrderResponse.from(order));
    }

    @PostMapping("/{id}/revise")
    public ResponseEntity<OrderResponse> revise(@PathVariable Long id, @RequestBody ReviseOrderRequest request) {
        Order order = findOrder(id);
        List<OrderLineItem> revisedLineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();
        apply(order, order.revise(new OrderRevision(revisedLineItems)));
        return ResponseEntity.ok(OrderResponse.from(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    private void apply(Order order, List<OrderDomainEvent> events) {
        orderRepository.save(order);
        domainEventPublisher.publish(events);
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

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest"`
Expected: `BUILD SUCCESSFUL`, all 13 tests pass.

- [ ] **Step 6: Update `README.md`**

In `README.md`, find line 16 (the `ftgo-order-service` row of the service table) and replace:

```
| ftgo-order-service | 8082 | Order lifecycle (saga participant/coordinator) | `POST /orders`; choreography: reacts to 3 event topics; orchestration: `CreateOrderSagaOrchestrator` sends commands and reacts to replies |
```

with:

```
| ftgo-order-service | 8082 | Order lifecycle (saga participant/coordinator); `Order` is a DDD aggregate (Ch.5) with the full create/cancel/revise state machine | `POST /orders`, `POST /orders/{id}/cancel`, `POST /orders/{id}/revise`; choreography: reacts to 3 event topics; orchestration: `CreateOrderSagaOrchestrator` sends commands and reacts to replies |
```

Find line 116 (the Ch.5 row of the "Book progress" table) and replace:

```
| 5 | Designing business logic | `Ticket` (kitchen-service) refactored into a DDD aggregate with enforced state transitions and domain events; `Order` (order-service) not yet done |
```

with:

```
| 5 | Designing business logic | `Ticket` (kitchen-service) and `Order` (order-service) both refactored into DDD aggregates with enforced state transitions and domain events; `Order`'s cancel/revise use cases await their sagas (future sessions) |
```

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/ReviseOrderRequest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java \
        README.md
git commit -m "feat(order-service): add cancel/revise REST endpoints for the Order aggregate"
```

---

## Task 7: Fix `ftgo-kitchen-service`'s `OrderEventListener` to check `eventType`

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java`
- Create: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListenerTest.java`

**Interfaces:**
- Consumes: `com.sanjay.ftgo.kitchen.domain.OrderCreatedEvent` (existing, unchanged), `com.sanjay.ftgo.kitchen.domain.TicketService.handleOrderCreated` (existing, unchanged).
- Produces: no new public interface — this is a correctness fix to an existing `@KafkaListener` method's behavior, verified by the new test.

- [ ] **Step 1: Write the failing test**

Create `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListenerTest.java`:

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.OrderCreatedEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OrderEventListenerTest {

    private final TicketService ticketService = mock(TicketService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final OrderEventListener listener = new OrderEventListener(ticketService, objectMapper);

    @Test
    void handlesOrderCreatedEvent() {
        String payload = """
                {"eventId":"e1","eventType":"OrderCreated","orderId":42,"restaurantId":1,
                 "lineItems":[{"menuItemId":10,"quantity":2}]}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleOrderCreated(any(OrderCreatedEvent.class));
    }

    @Test
    void ignoresNonOrderCreatedEventTypesInsteadOfMisfiringOnNullFields() {
        String payload = """
                {"eventId":"e2","eventType":"OrderApproved","orderId":42}
                """;

        listener.onMessage(payload);

        verify(ticketService, never()).handleOrderCreated(any());
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verify(ticketService, never()).handleOrderCreated(any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.infrastructure.OrderEventListenerTest"`
Expected: `ignoresNonOrderCreatedEventTypesInsteadOfMisfiringOnNullFields` fails — the current listener calls `handleOrderCreated` unconditionally regardless of `eventType`.

- [ ] **Step 3: Rewrite `OrderEventListener.java`**

Replace the full contents of `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java`:

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.OrderCreatedEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.events", groupId = "kitchen-service")
    public void onMessage(String payload) {
        OrderCreatedEvent event;
        try {
            event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed order event: {}", payload, e);
            return;
        }
        // order-service's order.events topic now carries every Order lifecycle event
        // (OrderApproved, OrderCancelled, etc.), not just OrderCreated — without this
        // check, deserializing e.g. an OrderApproved payload into OrderCreatedEvent
        // would succeed with null consumerId/restaurantId/lineItems and create a bogus Ticket.
        if (!"OrderCreated".equals(event.eventType())) {
            return;
        }
        ticketService.handleOrderCreated(event);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.infrastructure.OrderEventListenerTest"`
Expected: `BUILD SUCCESSFUL`, all 3 tests pass.

- [ ] **Step 5: Run the full kitchen-service test suite to check for regressions**

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 6: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListenerTest.java
git commit -m "fix(kitchen-service): OrderEventListener ignores non-OrderCreated events instead of misfiring"
```

---

## Task 8: Full-stack build and manual Docker e2e verification

**Files:** none (verification only).

- [ ] **Step 1: Run the full multi-module build**

Run: `./gradlew clean build`
Expected: `BUILD SUCCESSFUL` across all modules (`ftgo-common`, `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-accounting-service`, `ftgo-restaurant-service`, `ftgo-service-registry`).

- [ ] **Step 2: Start the full stack in choreography mode (default)**

Run: `docker compose up --build -d`
Expected: all containers healthy; `docker compose ps` shows `mysql`, `zookeeper`, `kafka`, `service-registry`, `restaurant-service`, `order-service`, `kitchen-service`, `consumer-service`, `accounting-service` all `Up`.

- [ ] **Step 3: Verify the choreography happy path still works**

Run:
```bash
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":1,"quantity":2}]}'
```
Expected: `201`, `"status":"APPROVAL_PENDING"`. Note the returned order `id`.

Wait a few seconds for the saga to complete, then:
```bash
docker exec -it $(docker compose ps -q mysql) mysql -uroot -proot -e \
  "SELECT id, status FROM ftgo_order.orders WHERE id = <order-id>;"
```
Expected: `status = APPROVED`.

- [ ] **Step 4: Verify the new `/cancel` endpoint transitions to `CANCEL_PENDING` and doesn't create a spurious kitchen ticket**

Run:
```bash
curl -s -X POST http://localhost:8082/orders/<order-id>/cancel
```
Expected: `200`, `"status":"CANCEL_PENDING"`.

Check `order.events` carried the proposal and kitchen-service did not react to it (no new/duplicate ticket):
```bash
docker exec -it $(docker compose ps -q mysql) mysql -uroot -proot -e \
  "SELECT COUNT(*) FROM ftgo_kitchen.tickets WHERE order_id = <order-id>;"
```
Expected: exactly `1` (the original ticket from creation — no second/bogus ticket created in reaction to `OrderCancelled`).

- [ ] **Step 5: Verify the new `/revise` endpoint on a second order**

Place a second order, wait for `APPROVED`, then:
```bash
curl -s -X POST http://localhost:8082/orders/<second-order-id>/revise \
  -H "Content-Type: application/json" \
  -d '{"lineItems":[{"menuItemId":1,"quantity":5}]}'
```
Expected: `200`, `"status":"REVISION_PENDING"`.

- [ ] **Step 6: Verify orchestration mode still reaches the same end states**

Run: `docker compose down` then `SAGA_MODE=orchestration docker compose up --build -d`

Repeat Step 3's create-order flow. Expected: same `201` → `APPROVED` outcome via `CreateOrderSagaOrchestrator`.

- [ ] **Step 7: Verify one compensation case (inactive consumer)**

Run:
```bash
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"consumerId":2,"restaurantId":1,"lineItems":[{"menuItemId":1,"quantity":1}]}'
```
(`consumerId: 2` is seeded as inactive.) Expected: order ends in `REJECTED`, associated ticket ends in `CANCELLED`.

- [ ] **Step 8: Redelivery/idempotency spot-check**

Restart kitchen-service to force any in-flight redelivery: `docker compose restart kitchen-service`. Expected: `processed_events` row counts and per-order ticket counts in `ftgo_kitchen` are unchanged after restart (no duplicate processing).

- [ ] **Step 9: Tear down**

Run: `docker compose down`

- [ ] **Step 10: Update `CONTEXT.md` and add a new session doc**

Update `CONTEXT.md`:
- "Current position" section: mark `Order` aggregate done, matching how `Ticket`'s completion was recorded on 2026-07-20.
- "Patterns reference" → Business logic section: note `Order` (order-service) is now done, not just `Ticket`.
- "Services to build" table, `ftgo-order-service` row: mention the DDD aggregate and cancel/revise endpoints.
- Session log: add a one-line entry for this session dated with today's actual date, summarizing what was built (mirror the style of the 2026-07-20 entry).

Create `docs/session-<today's-date>.md` following the exact structure of `docs/session-2026-07-20.md` (What we did / What's implemented now / Project state at end of session / Next actions / Resuming in a new session), covering: the Order aggregate refactor, the required `OrderEventListener` fix, and clearly flagging Cancel Order saga and Revise Order saga as the two remaining sub-projects for future sessions.

- [ ] **Step 11: Commit the docs update**

```bash
git add CONTEXT.md docs/session-*.md
git commit -m "docs: record Order aggregate refactor session"
```

---

## Deferred (not in this plan)

- **Cancel Order saga** (sub-project 2 of 3) — a separate future session, resolving `CANCEL_PENDING` via real kitchen/accounting saga participants in both saga modes.
- **Revise Order saga** (sub-project 3 of 3) — a separate future session, resolving `REVISION_PENDING` via real kitchen capacity re-check and accounting re-authorization in both saga modes.
