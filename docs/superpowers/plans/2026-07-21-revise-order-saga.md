# Revise Order Saga Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve `Order.REVISION_PENDING` by wiring a real cross-service Revise Order saga (kitchen capacity re-check gating accounting re-authorization, with a genuine compensation path when accounting declines after kitchen has already committed) in both saga modes, completing the third and final `Order` sub-project (`Order` aggregate → Cancel Order saga → Revise Order saga).

**Architecture:** Sequential, kitchen-gates-accounting saga, same shape as Cancel Order. Kitchen re-checks capacity for the revised quantity and applies it provisionally before accounting is ever contacted; only if kitchen confirms does accounting re-check its authorization threshold. Because kitchen commits before accounting decides, a genuinely new compensation case exists (absent from Cancel Order, where `Authorization.reverse()` is unconditional): if accounting declines, a compensating command/event carrying the original quantity is sent back to kitchen, which reverts via a new `Ticket.undoRevision()`, and only then does `Order.rejectRevision()` fire. `Order.confirmRevision()` changes from taking an `OrderRevision` parameter to reading a newly persisted `pendingRevisedLineItems` field, since the confirming reply arrives in a different transaction than the original `/revise` request.

**Tech Stack:** Java 17+, Spring Boot 3.5.16, Spring Data JPA, Spring Kafka, JUnit 5, Mockito, AssertJ.

## Global Constraints

- **Sequential saga, kitchen-gates-accounting.** Accounting is never contacted unless kitchen confirms the revised quantity fits capacity. If kitchen rejects, `Order.rejectRevision()` fires immediately with no accounting call.
- **Kitchen applies the revised quantity before accounting is asked.** If accounting later declines (over the authorization threshold), a compensating command/event carrying `originalTotalQuantity` reverts kitchen's `Ticket.totalQuantity` via the new `undoRevision()` method — `Order` stays `REVISION_PENDING` until that undo is confirmed, then `rejectRevision()` fires.
- **Both `Ticket` and `Authorization` gain a persisted `totalQuantity` field.** Neither stores it today.
- **`Order.confirmRevision()` changes signature** from `confirmRevision(OrderRevision revision)` to no-arg, reading the new `pendingRevisedLineItems` field (set by `revise()`, cleared by `confirmRevision()`/`rejectRevision()`). `OrderController.revise()` is unaffected.
- **The choreography compensation trigger is `"OrderRevisionCompensationRequested"`, distinct from `"OrderRevisionRejected"`.** The latter fires only on the real terminal state transition; conflating them would make kitchen try to undo a revision that was rejected outright (nothing to undo). Published directly via a new `OrderDomainEventPublisher.publishRevisionCompensationRequested(order, eventId)` method (bypasses the `OrderDomainEvent` sealed interface, mirroring `publishOrderCreated`, since `Order` itself doesn't transition here) — reuses the existing generic `OrderEvent`/`lineItems` wire shape, carrying `Order`'s still-untouched pre-revision `lineItems`.
- **No new join-instance table for orchestration mode.** `ReviseOrderSagaOrchestrator` is stateless, like `CancelOrderSagaOrchestrator` — this is a strict linear pipeline. It recomputes `originalTotalQuantity`/pending revised quantity by loading `Order` fresh rather than threading them through the reply chain.
- **No two-tier exception split for revision.** Unlike `cancel()`'s `TicketCannotBeCancelledException` vs. generic split, `reviseQuantity()`/`undoRevision()` uniformly throw `UnsupportedStateTransitionException` for illegal states.
- **No new fields on `KitchenCommand`/`AccountingCommand`/`SagaReply`.** `sagaType="ReviseOrder"` is simply a new value alongside `"CreateOrder"`/`"CancelOrder"`.
- Follow this codebase's existing test conventions exactly: Mockito-mocked collaborators + AssertJ assertions, `verifyNoInteractions`/`never()` for negative-path assertions, `@WebMvcTest` + `@MockitoBean` for controller tests.
- Spec: `docs/superpowers/specs/2026-07-21-revise-order-saga-design.md`.

---

## Task 1: `Order` aggregate — `pendingRevisedLineItems` field, `confirmRevision()` signature change

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderTest.java`

**Interfaces:**
- Produces: `Order.getPendingRevisedLineItems()` (new getter, consumed by Task 12's orchestrator); `Order.confirmRevision()` (signature change, no-arg, consumed by Tasks 11 and 12).

- [ ] **Step 1: Write the failing tests**

In `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderTest.java`, replace the two `confirmRevision` tests:

```java
    @Test
    void confirmRevisionMovesFromRevisionPendingToApprovedAndAppliesLineItems() {
        Order order = orderIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));
        order.revise(revision);

        List<OrderDomainEvent> events = order.confirmRevision();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).isEqualTo(revision.revisedLineItems());
        assertThat(events).containsExactly(new OrderRevisedEvent(42L, revision.revisedLineItems()));
    }

    @Test
    void confirmRevisionFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::confirmRevision)
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseThenConfirmExposesPendingRevisedLineItemsUntilConfirmed() {
        Order order = orderIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        order.revise(revision);

        assertThat(order.getPendingRevisedLineItems()).isEqualTo(revision.revisedLineItems());

        order.confirmRevision();

        assertThat(order.getPendingRevisedLineItems()).isNull();
    }

    @Test
    void rejectRevisionClearsPendingRevisedLineItems() {
        Order order = orderIn(OrderStatus.APPROVED);
        order.revise(new OrderRevision(List.of(new OrderLineItem(10L, 5))));

        order.rejectRevision();

        assertThat(order.getPendingRevisedLineItems()).isNull();
    }
```

(These replace the old `confirmRevisionMovesFromRevisionPendingToApprovedAndAppliesLineItems`/`confirmRevisionFromWrongStatusThrows` tests that called `order.confirmRevision(revision)`.)

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderTest"`
Expected: Compilation failure — `confirmRevision()` (no-arg) and `getPendingRevisedLineItems()` don't exist yet.

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

    // Only populated between revise() and confirmRevision()/rejectRevision() - the confirming
    // reply arrives in a different transaction than the original /revise request, so the
    // OrderRevision object itself no longer exists to pass back in.
    @ElementCollection
    @CollectionTable(name = "order_pending_revised_line_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderLineItem> pendingRevisedLineItems;

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

    public List<OrderLineItem> getPendingRevisedLineItems() {
        return pendingRevisedLineItems;
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
        this.pendingRevisedLineItems = revision.revisedLineItems();
        return List.of(new OrderRevisionProposedEvent(id, revision.revisedLineItems()));
    }

    public List<OrderDomainEvent> confirmRevision() {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        this.lineItems = pendingRevisedLineItems;
        List<OrderDomainEvent> events = List.of(new OrderRevisedEvent(id, pendingRevisedLineItems));
        this.pendingRevisedLineItems = null;
        return events;
    }

    public List<OrderDomainEvent> rejectRevision() {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        this.pendingRevisedLineItems = null;
        return List.of(new OrderRevisionRejectedEvent(id));
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderTest.java
git commit -m "feat(order-service): persist pendingRevisedLineItems, change confirmRevision to no-arg"
```

---

## Task 2: `OrderDomainEventPublisher` — compensation-trigger publish method

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisherTest.java`

**Interfaces:**
- Produces: `OrderDomainEventPublisher.publishRevisionCompensationRequested(Order order, String eventId)` — consumed by Task 11.

- [ ] **Step 1: Write the failing test**

Append to `OrderDomainEventPublisherTest.java`:

```java
    @Test
    void publishesRevisionCompensationRequestedWithOriginalLineItems() {
        Order order = new Order(42L, 7L, 3L, List.of(new OrderLineItem(10L, 2)), OrderStatus.REVISION_PENDING);

        publisher.publishRevisionCompensationRequested(order, "event-9");

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevisionCompensationRequested".equals(row.getEventType())
                        && "order.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"eventId\":\"event-9\"")
                        && row.getPayload().contains("\"menuItemId\":10")
                        && row.getPayload().contains("\"quantity\":2")));
    }
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderDomainEventPublisherTest"`
Expected: Compilation failure — `publishRevisionCompensationRequested` doesn't exist yet.

- [ ] **Step 3: Add the method**

In `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java`, add this method right after `publishOrderCreated`:

```java
    // Order itself doesn't transition here (it stays REVISION_PENDING until kitchen's undo is
    // confirmed), so this bypasses the OrderDomainEvent sealed interface entirely, same as
    // publishOrderCreated does for its own one-off case.
    public void publishRevisionCompensationRequested(Order order, String eventId) {
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderRevisionCompensationRequested", order.getId(),
                null, null, toWireLineItems(order.getLineItems()));
        save(eventId, wireEvent);
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderDomainEventPublisherTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisherTest.java
git commit -m "feat(order-service): add publishRevisionCompensationRequested"
```

---

## Task 3: `Ticket` aggregate — persisted `totalQuantity`, `reviseQuantity()`/`undoRevision()`

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketQuantityRevisedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRevisionRejectedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRevisionUndoneEvent.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketTest.java`

**Interfaces:**
- Produces: `Ticket.getTotalQuantity()`, `Ticket.reviseQuantity(int newTotalQuantity)` → `List<TicketDomainEvent>` (throws `UnsupportedStateTransitionException` from `READY_FOR_PICKUP`/`PICKED_UP`/`CANCELLED`), `Ticket.undoRevision(int originalTotalQuantity)` → `List<TicketDomainEvent>` (throws `UnsupportedStateTransitionException` only from `CANCELLED`). Consumed by Task 5.

- [ ] **Step 1: Write the failing tests**

Create `TicketQuantityRevisedEvent.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketQuantityRevisedEvent(Long orderId, int totalQuantity) implements TicketDomainEvent {
}
```

Create `TicketRevisionRejectedEvent.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketRevisionRejectedEvent(Long orderId, String reason) implements TicketDomainEvent {
}
```

Create `TicketRevisionUndoneEvent.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketRevisionUndoneEvent(Long orderId, int totalQuantity) implements TicketDomainEvent {
}
```

Update `TicketDomainEvent.java`'s permits clause:

```java
package com.sanjay.ftgo.kitchen.domain;

public sealed interface TicketDomainEvent
        permits TicketCreatedEvent, TicketCreationFailedEvent, TicketConfirmedEvent, TicketCancelledEvent,
                TicketCancellationRejectedEvent, TicketAcceptedEvent, TicketPreparingStartedEvent,
                TicketReadyForPickupEvent, TicketPickedUpEvent, TicketQuantityRevisedEvent,
                TicketRevisionRejectedEvent, TicketRevisionUndoneEvent {

    Long orderId();
}
```

Append to `TicketTest.java` (before the closing `acceptedTicket()` helper's preceding brace — i.e. as the last tests in the class):

```java
    @Test
    void createTicketStoresTotalQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 7).ticket();

        assertThat(ticket.getTotalQuantity()).isEqualTo(7);
    }

    @Test
    void reviseQuantityFromCreatePendingSucceeds() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        List<TicketDomainEvent> events = ticket.reviseQuantity(8);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        assertThat(events).containsExactly(new TicketQuantityRevisedEvent(42L, 8));
    }

    @Test
    void reviseQuantityFromPreparingSucceeds() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();

        List<TicketDomainEvent> events = ticket.reviseQuantity(8);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        assertThat(events).containsExactly(new TicketQuantityRevisedEvent(42L, 8));
    }

    @Test
    void reviseQuantityFromReadyForPickupThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();

        assertThatThrownBy(() -> ticket.reviseQuantity(8)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseQuantityFromPickedUpThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();
        ticket.pickedUp();

        assertThatThrownBy(() -> ticket.reviseQuantity(8)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseQuantityFromCancelledThrowsUnsupportedStateTransition() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(() -> ticket.reviseQuantity(8)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void undoRevisionRestoresOriginalQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.reviseQuantity(8);

        List<TicketDomainEvent> events = ticket.undoRevision(3);

        assertThat(ticket.getTotalQuantity()).isEqualTo(3);
        assertThat(events).containsExactly(new TicketRevisionUndoneEvent(42L, 3));
    }

    @Test
    void undoRevisionFromCancelledThrowsUnsupportedStateTransition() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(() -> ticket.undoRevision(3)).isInstanceOf(UnsupportedStateTransitionException.class);
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketTest"`
Expected: Compilation failure — `getTotalQuantity`, `reviseQuantity`, `undoRevision` don't exist yet.

- [ ] **Step 3: Rewrite `Ticket.java`**

Replace the full contents of `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.ZonedDateTime;
import java.util.List;

@Entity
@Table(name = "tickets")
public class Ticket {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    @Enumerated(EnumType.STRING)
    private TicketState state;

    private ZonedDateTime readyBy;

    private int totalQuantity;

    protected Ticket() {
    }

    private Ticket(Long orderId, TicketState state) {
        this.orderId = orderId;
        this.state = state;
    }

    public static TicketCreationResult createTicket(Long orderId, int totalQuantity) {
        Ticket ticket = new Ticket(orderId, TicketState.CREATE_PENDING);
        ticket.totalQuantity = totalQuantity;
        return new TicketCreationResult(ticket, List.of(new TicketCreatedEvent(orderId, totalQuantity)));
    }

    public static Ticket createCancelled(Long orderId) {
        return new Ticket(orderId, TicketState.CANCELLED);
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public TicketState getState() {
        return state;
    }

    public int getTotalQuantity() {
        return totalQuantity;
    }

    public List<TicketDomainEvent> confirm() {
        if (state != TicketState.CREATE_PENDING) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.AWAITING_ACCEPTANCE;
        return List.of(new TicketConfirmedEvent(orderId));
    }

    public List<TicketDomainEvent> accept(ZonedDateTime readyBy) {
        if (state != TicketState.AWAITING_ACCEPTANCE) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.ACCEPTED;
        this.readyBy = readyBy;
        return List.of(new TicketAcceptedEvent(orderId, readyBy));
    }

    public List<TicketDomainEvent> preparing() {
        if (state != TicketState.ACCEPTED) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.PREPARING;
        return List.of(new TicketPreparingStartedEvent(orderId));
    }

    public List<TicketDomainEvent> readyForPickup() {
        if (state != TicketState.PREPARING) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.READY_FOR_PICKUP;
        return List.of(new TicketReadyForPickupEvent(orderId));
    }

    public List<TicketDomainEvent> pickedUp() {
        if (state != TicketState.READY_FOR_PICKUP) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.PICKED_UP;
        return List.of(new TicketPickedUpEvent(orderId));
    }

    public List<TicketDomainEvent> cancel() {
        return switch (state) {
            case CREATE_PENDING, AWAITING_ACCEPTANCE, ACCEPTED -> {
                this.state = TicketState.CANCELLED;
                yield List.of(new TicketCancelledEvent(orderId));
            }
            case READY_FOR_PICKUP -> throw new TicketCannotBeCancelledException(orderId);
            case PREPARING, PICKED_UP, CANCELLED -> throw new UnsupportedStateTransitionException(state);
        };
    }

    // No two-tier exception split here, unlike cancel() - that distinction is specific to the
    // book's cancel example, not a pattern every guarded transition needs.
    public List<TicketDomainEvent> reviseQuantity(int newTotalQuantity) {
        return switch (state) {
            case CREATE_PENDING, AWAITING_ACCEPTANCE, ACCEPTED, PREPARING -> {
                this.totalQuantity = newTotalQuantity;
                yield List.of(new TicketQuantityRevisedEvent(orderId, newTotalQuantity));
            }
            case READY_FOR_PICKUP, PICKED_UP, CANCELLED -> throw new UnsupportedStateTransitionException(state);
        };
    }

    // No state restriction beyond CANCELLED - this undoes a change this same saga just made,
    // so reverting to a previously-valid quantity is always legal from any other state.
    public List<TicketDomainEvent> undoRevision(int originalTotalQuantity) {
        if (state == TicketState.CANCELLED) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.totalQuantity = originalTotalQuantity;
        return List.of(new TicketRevisionUndoneEvent(orderId, originalTotalQuantity));
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketQuantityRevisedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRevisionRejectedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRevisionUndoneEvent.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketTest.java
git commit -m "feat(kitchen-service): persist Ticket.totalQuantity, add reviseQuantity/undoRevision"
```

---

## Task 4: `TicketDomainEventPublisher` — wire mapping for the 3 new events

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisherTest.java`

**Interfaces:**
- Consumes: `TicketQuantityRevisedEvent`, `TicketRevisionRejectedEvent`, `TicketRevisionUndoneEvent` (Task 3).
- Produces: wire `KitchenEvent` rows with `eventType` `"TicketQuantityRevised"`/`"TicketRevisionRejected"`/`"TicketRevisionUndone"` on `kitchen.events` — consumed downstream by accounting-service (Task 10) and order-service (Task 13).

- [ ] **Step 1: Write the failing tests**

Append to `TicketDomainEventPublisherTest.java`:

```java
    @Test
    void publishesTicketQuantityRevisedWithNewTotalQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        publisher.publish(ticket, List.of(new TicketQuantityRevisedEvent(42L, 8)));

        verify(outboxEventRepository).save(argThat(row ->
                "TicketQuantityRevised".equals(row.getEventType())
                        && "kitchen.events".equals(row.getTopic())
                        && row.getPayload().contains("\"totalQuantity\":8")));
    }

    @Test
    void publishesTicketRevisionRejectedWithReason() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        publisher.publish(ticket, List.of(new TicketRevisionRejectedEvent(42L, "order exceeds kitchen capacity")));

        verify(outboxEventRepository).save(argThat(row ->
                "TicketRevisionRejected".equals(row.getEventType())
                        && row.getPayload().contains("order exceeds kitchen capacity")));
    }

    @Test
    void publishesTicketRevisionUndoneWithOriginalTotalQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        publisher.publish(ticket, List.of(new TicketRevisionUndoneEvent(42L, 3)));

        verify(outboxEventRepository).save(argThat(row ->
                "TicketRevisionUndone".equals(row.getEventType())
                        && row.getPayload().contains("\"totalQuantity\":3")));
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisherTest"`
Expected: Compilation failure — the `switch` in `toWireEvent` is non-exhaustive (sealed interface now has 3 more permitted types).

- [ ] **Step 3: Extend the switch**

In `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java`, add these 3 cases to `toWireEvent`'s switch, right before the closing `};`:

```java
            case TicketQuantityRevisedEvent e ->
                    new KitchenEvent(eventId, "TicketQuantityRevised", e.orderId(), ticketId, e.totalQuantity(), null);
            case TicketRevisionRejectedEvent e ->
                    new KitchenEvent(eventId, "TicketRevisionRejected", e.orderId(), ticketId, null, e.reason());
            case TicketRevisionUndoneEvent e ->
                    new KitchenEvent(eventId, "TicketRevisionUndone", e.orderId(), ticketId, e.totalQuantity(), null);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisherTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisherTest.java
git commit -m "feat(kitchen-service): wire TicketQuantityRevised/TicketRevisionRejected/TicketRevisionUndone to kitchen.events"
```

---

## Task 5: `TicketService` — revision handlers (choreography + orchestration + compensation)

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`

**Interfaces:**
- Consumes: `Ticket.reviseQuantity`/`undoRevision` (Task 3), `OrderCreatedEvent` (existing wire type, reused — carries `eventId`/`orderId`/`lineItems`, sufficient for both the proposal and the compensation trigger).
- Produces: `TicketService.handleOrderRevisionProposed(OrderCreatedEvent event)` (choreography forward step), `TicketService.handleOrderRevisionRejected(OrderCreatedEvent event)` (choreography compensation), `TicketService.handleReviseTicketCommand(String eventId, Long orderId, Integer newTotalQuantity)` (orchestration forward step), `TicketService.handleUndoReviseTicketCommand(String eventId, Long orderId, Integer originalTotalQuantity)` (orchestration compensation) — consumed by Task 6.

- [ ] **Step 1: Write the failing tests**

Append to `TicketServiceTest.java` (add `import java.util.Optional;` and `import static org.mockito.ArgumentMatchers.any;` if not already present — both are already imported per the existing file):

```java
    @Test
    void handlesOrderRevisionProposedWithinCapacityAndPublishesQuantityRevised() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        OrderCreatedEvent event = new OrderCreatedEvent("evt-10", "OrderRevisionProposed", 42L, null,
                List.of(new OrderCreatedEvent.LineItem(10L, 8)));
        when(processedEventRepository.existsById("evt-10")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderRevisionProposed(event);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketQuantityRevisedEvent));
    }

    @Test
    void handlesOrderRevisionProposedOverCapacityAndPublishesRevisionRejectedWithoutMutating() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        OrderCreatedEvent event = new OrderCreatedEvent("evt-11", "OrderRevisionProposed", 42L, null,
                List.of(new OrderCreatedEvent.LineItem(10L, 25)));
        when(processedEventRepository.existsById("evt-11")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleOrderRevisionProposed(event);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(ticketRepository, never()).save(any());
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketRevisionRejectedEvent));
    }

    @Test
    void handlesOrderRevisionRejectedCompensationAndUndoesQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.reviseQuantity(8);
        OrderCreatedEvent event = new OrderCreatedEvent("evt-12", "OrderRevisionCompensationRequested", 42L, null,
                List.of(new OrderCreatedEvent.LineItem(10L, 2)));
        when(processedEventRepository.existsById("evt-12")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderRevisionRejected(event);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketRevisionUndoneEvent));
    }

    @Test
    void reviseTicketCommandWithinCapacityRepliesQuantityRevised() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-10")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleReviseTicketCommand("cmd-10", 42L, 8);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketQuantityRevised".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }

    @Test
    void reviseTicketCommandOverCapacityRepliesRevisionRejected() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-11")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleReviseTicketCommand("cmd-11", 42L, 25);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketRevisionRejected".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void undoReviseTicketCommandRestoresOriginalQuantityAndReplies() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.reviseQuantity(8);
        when(processedEventRepository.existsById("cmd-12")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleUndoReviseTicketCommand("cmd-12", 42L, 2);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketRevisionUndone".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: Compilation failure — the 4 new handler methods don't exist yet.

- [ ] **Step 3: Add the handlers to `TicketService.java`**

First, replace `handleOrderCreated`'s inline quantity computation to use a new shared helper (so the new handlers reuse it), changing:

```java
        int totalQuantity = event.lineItems().stream()
                .mapToInt(OrderCreatedEvent.LineItem::quantity)
                .sum();
```

to:

```java
        int totalQuantity = totalQuantity(event.lineItems());
```

Then add these 4 methods and the private helper, after `handleOrderCancelled` and before `isWithinCapacity`:

```java
    @Transactional
    public void handleOrderRevisionProposed(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        Ticket ticket = ticketRepository.findByOrderId(event.orderId()).orElse(null);
        if (ticket == null) {
            return;
        }

        int newTotalQuantity = totalQuantity(event.lineItems());
        if (!isWithinCapacity(newTotalQuantity)) {
            domainEventPublisher.publish(ticket, List.of(
                    new TicketRevisionRejectedEvent(event.orderId(), "order exceeds kitchen capacity")));
            return;
        }

        List<TicketDomainEvent> events = ticket.reviseQuantity(newTotalQuantity);
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @Transactional
    public void handleOrderRevisionRejected(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        Ticket ticket = ticketRepository.findByOrderId(event.orderId()).orElse(null);
        if (ticket == null) {
            return;
        }

        int originalTotalQuantity = totalQuantity(event.lineItems());
        List<TicketDomainEvent> events = ticket.undoRevision(originalTotalQuantity);
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @Transactional
    public void handleReviseTicketCommand(String eventId, Long orderId, Integer newTotalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }

        if (newTotalQuantity == null) {
            publishReply("TicketRevisionRejected", orderId, "totalQuantity is required", "ReviseOrder");
            return;
        }

        if (!isWithinCapacity(newTotalQuantity)) {
            publishReply("TicketRevisionRejected", orderId, "order exceeds kitchen capacity", "ReviseOrder");
            return;
        }

        ticket.reviseQuantity(newTotalQuantity);
        ticketRepository.save(ticket);
        publishReply("TicketQuantityRevised", orderId, null, "ReviseOrder");
    }

    @Transactional
    public void handleUndoReviseTicketCommand(String eventId, Long orderId, Integer originalTotalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (originalTotalQuantity == null) {
            return;
        }

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }

        ticket.undoRevision(originalTotalQuantity);
        ticketRepository.save(ticket);
        publishReply("TicketRevisionUndone", orderId, null, "ReviseOrder");
    }

    private int totalQuantity(List<OrderCreatedEvent.LineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderCreatedEvent.LineItem::quantity).sum();
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java
git commit -m "feat(kitchen-service): add revision handlers for choreography, orchestration, and compensation"
```

---

## Task 6: Kitchen-service infrastructure — `OrderEventListener` + `KitchenCommandListener`

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListener.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListenerTest.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListenerTest.java`

**Interfaces:**
- Consumes: `TicketService.handleOrderRevisionProposed`/`handleOrderRevisionRejected`/`handleReviseTicketCommand`/`handleUndoReviseTicketCommand` (Task 5).

- [ ] **Step 1: Write the failing tests**

Append to `OrderEventListenerTest.java`:

```java
    @Test
    void handlesOrderRevisionProposedEvent() {
        String payload = """
                {"eventId":"e10","eventType":"OrderRevisionProposed","orderId":42,"lineItems":[{"menuItemId":10,"quantity":8}]}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleOrderRevisionProposed(argThat(event ->
                "e10".equals(event.eventId()) && event.orderId().equals(42L)));
    }

    @Test
    void handlesOrderRevisionCompensationRequestedEvent() {
        String payload = """
                {"eventId":"e11","eventType":"OrderRevisionCompensationRequested","orderId":42,"lineItems":[{"menuItemId":10,"quantity":2}]}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleOrderRevisionRejected(argThat(event ->
                "e11".equals(event.eventId()) && event.orderId().equals(42L)));
    }
```

(Add `import static org.mockito.ArgumentMatchers.argThat;` if not already present in this file.)

Append to `KitchenCommandListenerTest.java`:

```java
    @Test
    void dispatchesReviseTicketCommand() {
        String payload = """
                {"eventId":"c10","commandType":"ReviseTicket","orderId":42,"totalQuantity":8,"sagaType":"ReviseOrder"}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleReviseTicketCommand("c10", 42L, 8);
    }

    @Test
    void dispatchesUndoReviseTicketCommand() {
        String payload = """
                {"eventId":"c11","commandType":"UndoReviseTicket","orderId":42,"totalQuantity":2,"sagaType":"ReviseOrder"}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleUndoReviseTicketCommand("c11", 42L, 2);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.infrastructure.OrderEventListenerTest" --tests "com.sanjay.ftgo.kitchen.infrastructure.KitchenCommandListenerTest"`
Expected: Failures — the new `eventType`/`commandType` cases aren't dispatched yet (verify calls never happen).

- [ ] **Step 3: Extend both listeners' switches**

In `OrderEventListener.java`, change:

```java
        switch (event.eventType()) {
            case "OrderCreated" -> ticketService.handleOrderCreated(event);
            case "OrderCancelled" -> ticketService.handleOrderCancelled(event.eventId(), event.orderId());
            default -> { }
        }
```

to:

```java
        switch (event.eventType()) {
            case "OrderCreated" -> ticketService.handleOrderCreated(event);
            case "OrderCancelled" -> ticketService.handleOrderCancelled(event.eventId(), event.orderId());
            case "OrderRevisionProposed" -> ticketService.handleOrderRevisionProposed(event);
            case "OrderRevisionCompensationRequested" -> ticketService.handleOrderRevisionRejected(event);
            default -> { }
        }
```

In `KitchenCommandListener.java`, change:

```java
        switch (command.commandType()) {
            case "CreateTicket" ->
                    ticketService.handleCreateTicketCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ConfirmTicket" -> ticketService.handleConfirmTicketCommand(command.eventId(), command.orderId());
            case "CancelTicket" -> ticketService.handleCancelTicketCommand(command.eventId(), command.orderId(), command.sagaType());
            default -> log.warn("Unknown kitchen command type: {}", command.commandType());
        }
```

to:

```java
        switch (command.commandType()) {
            case "CreateTicket" ->
                    ticketService.handleCreateTicketCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ConfirmTicket" -> ticketService.handleConfirmTicketCommand(command.eventId(), command.orderId());
            case "CancelTicket" -> ticketService.handleCancelTicketCommand(command.eventId(), command.orderId(), command.sagaType());
            case "ReviseTicket" ->
                    ticketService.handleReviseTicketCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "UndoReviseTicket" ->
                    ticketService.handleUndoReviseTicketCommand(command.eventId(), command.orderId(), command.totalQuantity());
            default -> log.warn("Unknown kitchen command type: {}", command.commandType());
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.infrastructure.OrderEventListenerTest" --tests "com.sanjay.ftgo.kitchen.infrastructure.KitchenCommandListenerTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListener.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListenerTest.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListenerTest.java
git commit -m "feat(kitchen-service): dispatch revision events/commands to TicketService"
```

---

## Task 7: `Authorization` aggregate — persisted `totalQuantity`, `reviseAuthorization()`

**Files:**
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/Authorization.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationRevisedEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationRevisionRejectedEvent.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationTest.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelServiceTest.java`

**Interfaces:**
- Produces: `Authorization.authorize(Long orderId, int totalQuantity)` / `Authorization.decline(Long orderId, String reason, int totalQuantity)` (signature change — both gain a `totalQuantity` parameter); `Authorization.getTotalQuantity()`; `Authorization.reviseAuthorization(int newTotalQuantity)` → `List<AuthorizationDomainEvent>` (legal only from `AUTHORIZED`). Consumed by Task 9 and by the existing `SagaJoinService`.

- [ ] **Step 1: Write the failing tests**

In `AuthorizationTest.java`, update every `Authorization.authorize(42L)` call to `Authorization.authorize(42L, 3)` and every `Authorization.decline(42L, "...")` call to `Authorization.decline(42L, "...", 3)` (3 occurrences of `authorize`, 2 of `decline` in the existing file). Then append:

```java
    @Test
    void reviseAuthorizationUpdatesStoredTotalQuantity() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();

        List<AuthorizationDomainEvent> events = authorization.reviseAuthorization(8);

        assertThat(authorization.getTotalQuantity()).isEqualTo(8);
        assertThat(authorization.getStatus()).isEqualTo(AuthorizationStatus.AUTHORIZED);
        assertThat(events).containsExactly(new AuthorizationRevisedEvent(42L, 8));
    }

    @Test
    void reviseAuthorizationFromDeclinedThrows() {
        Authorization authorization = Authorization.decline(42L, "reason", 3).authorization();

        assertThatThrownBy(() -> authorization.reviseAuthorization(8))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseAuthorizationFromReversedThrows() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        authorization.reverse();

        assertThatThrownBy(() -> authorization.reviseAuthorization(8))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }
```

In `AuthorizationCancelServiceTest.java`, update both `Authorization.authorize(42L)` calls to `Authorization.authorize(42L, 3)`.

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationTest" --tests "com.sanjay.ftgo.accounting.domain.AuthorizationCancelServiceTest"`
Expected: Compilation failure — `authorize`/`decline` don't accept a 2nd/3rd argument yet, `reviseAuthorization` doesn't exist.

- [ ] **Step 3: Create the 2 new event records and update the sealed interface**

Create `AuthorizationRevisedEvent.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public record AuthorizationRevisedEvent(Long orderId, int totalQuantity) implements AuthorizationDomainEvent {
}
```

Create `AuthorizationRevisionRejectedEvent.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public record AuthorizationRevisionRejectedEvent(Long orderId, String reason) implements AuthorizationDomainEvent {
}
```

Update `AuthorizationDomainEvent.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public sealed interface AuthorizationDomainEvent
        permits CardAuthorizedEvent, CardAuthorizationDeclinedEvent, AuthorizationReversedEvent,
                AuthorizationRevisedEvent, AuthorizationRevisionRejectedEvent {

    Long orderId();
}
```

- [ ] **Step 4: Rewrite `Authorization.java`**

Replace the full contents of `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/Authorization.java`:

```java
package com.sanjay.ftgo.accounting.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.List;

@Entity
@Table(name = "authorizations")
public class Authorization {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    @Enumerated(EnumType.STRING)
    private AuthorizationStatus status;

    private int totalQuantity;

    protected Authorization() {
    }

    private Authorization(Long orderId, AuthorizationStatus status, int totalQuantity) {
        this.orderId = orderId;
        this.status = status;
        this.totalQuantity = totalQuantity;
    }

    public static AuthorizationResult authorize(Long orderId, int totalQuantity) {
        Authorization authorization = new Authorization(orderId, AuthorizationStatus.AUTHORIZED, totalQuantity);
        return new AuthorizationResult(authorization, List.of(new CardAuthorizedEvent(orderId)));
    }

    public static AuthorizationResult decline(Long orderId, String reason, int totalQuantity) {
        Authorization authorization = new Authorization(orderId, AuthorizationStatus.DECLINED, totalQuantity);
        return new AuthorizationResult(authorization, List.of(new CardAuthorizationDeclinedEvent(orderId, reason)));
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public AuthorizationStatus getStatus() {
        return status;
    }

    public int getTotalQuantity() {
        return totalQuantity;
    }

    public List<AuthorizationDomainEvent> reverse() {
        if (status != AuthorizationStatus.AUTHORIZED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = AuthorizationStatus.REVERSED;
        return List.of(new AuthorizationReversedEvent(orderId));
    }

    public List<AuthorizationDomainEvent> reviseAuthorization(int newTotalQuantity) {
        if (status != AuthorizationStatus.AUTHORIZED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.totalQuantity = newTotalQuantity;
        return List.of(new AuthorizationRevisedEvent(orderId, newTotalQuantity));
    }
}
```

- [ ] **Step 5: Fix `SagaJoinService.java`'s 4 call sites**

In `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`, change `handleAuthorizeCardCommand`:

```java
        boolean authorized = isAuthorized(totalQuantity);
        AuthorizationResult result = authorized
                ? Authorization.authorize(orderId)
                : Authorization.decline(orderId, "order quantity exceeds authorization limit");
```

to:

```java
        boolean authorized = isAuthorized(totalQuantity);
        AuthorizationResult result = authorized
                ? Authorization.authorize(orderId, totalQuantity)
                : Authorization.decline(orderId, "order quantity exceeds authorization limit", totalQuantity);
```

And change `tryResolve`:

```java
        boolean authorized = isAuthorized(state.getTotalQuantity());
        AuthorizationResult result = authorized
                ? Authorization.authorize(state.getOrderId())
                : Authorization.decline(state.getOrderId(), "order quantity exceeds authorization limit");
```

to:

```java
        boolean authorized = isAuthorized(state.getTotalQuantity());
        AuthorizationResult result = authorized
                ? Authorization.authorize(state.getOrderId(), state.getTotalQuantity())
                : Authorization.decline(state.getOrderId(), "order quantity exceeds authorization limit", state.getTotalQuantity());
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationTest" --tests "com.sanjay.ftgo.accounting.domain.AuthorizationCancelServiceTest" --tests "com.sanjay.ftgo.accounting.domain.SagaJoinServiceTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 7: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/Authorization.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationRevisedEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationRevisionRejectedEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationTest.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelServiceTest.java
git commit -m "feat(accounting-service): persist Authorization.totalQuantity, add reviseAuthorization"
```

---

## Task 8: `AuthorizationDomainEventPublisher` — wire mapping for the 2 new events

**Files:**
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisher.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisherTest.java`

**Interfaces:**
- Consumes: `AuthorizationRevisedEvent`, `AuthorizationRevisionRejectedEvent` (Task 7).
- Produces: wire `AccountingEvent` rows with `eventType` `"AuthorizationRevised"`/`"AuthorizationRevisionRejected"` on `accounting.events` — consumed by order-service (Task 13).

- [ ] **Step 1: Write the failing tests**

Append to `AuthorizationDomainEventPublisherTest.java`:

```java
    @Test
    void publishesAuthorizationRevised() {
        publisher.publish(List.of(new AuthorizationRevisedEvent(42L, 8)));

        verify(outboxEventRepository).save(argThat(row ->
                "AuthorizationRevised".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesAuthorizationRevisionRejectedWithReason() {
        publisher.publish(List.of(new AuthorizationRevisionRejectedEvent(42L, "order quantity exceeds authorization limit")));

        verify(outboxEventRepository).save(argThat(row ->
                "AuthorizationRevisionRejected".equals(row.getEventType())
                        && row.getPayload().contains("order quantity exceeds authorization limit")));
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationDomainEventPublisherTest"`
Expected: Compilation failure — the `switch` in `toWireEvent` is non-exhaustive.

- [ ] **Step 3: Extend the switch**

In `AuthorizationDomainEventPublisher.java`, add these 2 cases to `toWireEvent`'s switch, right before the closing `};`:

```java
            case AuthorizationRevisedEvent e -> new AccountingEvent(eventId, "AuthorizationRevised", e.orderId(), null);
            case AuthorizationRevisionRejectedEvent e ->
                    new AccountingEvent(eventId, "AuthorizationRevisionRejected", e.orderId(), e.reason());
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationDomainEventPublisherTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisher.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisherTest.java
git commit -m "feat(accounting-service): wire AuthorizationRevised/AuthorizationRevisionRejected to accounting.events"
```

---

## Task 9: `AuthorizationReviseService` — choreography + orchestration revision handling

**Files:**
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationReviseService.java`
- Create: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationReviseServiceTest.java`

**Interfaces:**
- Consumes: `Authorization.reviseAuthorization` (Task 7).
- Produces: `AuthorizationReviseService.reviseForChoreography(String eventId, Long orderId, Integer newTotalQuantity)`, `AuthorizationReviseService.reviseForCommand(String eventId, Long orderId, Integer newTotalQuantity, String sagaType)` — consumed by Task 10.

- [ ] **Step 1: Write the failing tests**

Create `AuthorizationReviseServiceTest.java`:

```java
package com.sanjay.ftgo.accounting.domain;

import com.sanjay.ftgo.common.outbox.OutboxEvent;
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

class AuthorizationReviseServiceTest {

    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final AuthorizationDomainEventPublisher domainEventPublisher = mock(AuthorizationDomainEventPublisher.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AuthorizationReviseService service = new AuthorizationReviseService(
            authorizationRepository, processedEventRepository, domainEventPublisher, outboxEventRepository, objectMapper);

    @Test
    void reviseForChoreographyWithinLimitReviseAuthorizationAndPublishesDomainEvent() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reviseForChoreography("e1", 42L, 8);

        assertThat(authorization.getTotalQuantity()).isEqualTo(8);
        verify(domainEventPublisher).publish(List.of(new AuthorizationRevisedEvent(42L, 8)));
    }

    @Test
    void reviseForChoreographyOverLimitPublishesRevisionRejectedWithoutMutating() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e2")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reviseForChoreography("e2", 42L, 15);

        assertThat(authorization.getTotalQuantity()).isEqualTo(3);
        verify(authorizationRepository, never()).save(any());
        verify(domainEventPublisher).publish(List.of(
                new AuthorizationRevisionRejectedEvent(42L, "order quantity exceeds authorization limit")));
    }

    @Test
    void reviseForChoreographySkipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.reviseForChoreography("e1", 42L, 8);

        verify(authorizationRepository, never()).findByOrderId(any());
    }

    @Test
    void reviseForCommandWithinLimitReplyAuthorizationRevised() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e3")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reviseForCommand("e3", 42L, 8, "ReviseOrder");

        assertThat(authorization.getTotalQuantity()).isEqualTo(8);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "AuthorizationRevised".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }

    @Test
    void reviseForCommandOverLimitRepliesAuthorizationRevisionRejectedWithoutMutating() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e4")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reviseForCommand("e4", 42L, 15, "ReviseOrder");

        assertThat(authorization.getTotalQuantity()).isEqualTo(3);
        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "AuthorizationRevisionRejected".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void reviseForCommandSkipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e3")).thenReturn(true);

        service.reviseForCommand("e3", 42L, 8, "ReviseOrder");

        verify(authorizationRepository, never()).findByOrderId(any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationReviseServiceTest"`
Expected: Compilation failure — `AuthorizationReviseService` doesn't exist yet.

- [ ] **Step 3: Create `AuthorizationReviseService.java`**

```java
package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class AuthorizationReviseService {

    private static final int AUTHORIZATION_QUANTITY_LIMIT = 10;

    private final AuthorizationRepository authorizationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final AuthorizationDomainEventPublisher domainEventPublisher;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public AuthorizationReviseService(AuthorizationRepository authorizationRepository,
                                       ProcessedEventRepository processedEventRepository,
                                       AuthorizationDomainEventPublisher domainEventPublisher,
                                       OutboxEventRepository outboxEventRepository,
                                       ObjectMapper objectMapper) {
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    // Choreography: kitchen's TicketQuantityRevised domain event triggers this directly, same
    // broadcast-on-accounting.events shape as every other choreography-mode transition here.
    @Transactional
    public void reviseForChoreography(String eventId, Long orderId, Integer newTotalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null || newTotalQuantity == null) {
            return;
        }

        if (!isAuthorized(newTotalQuantity)) {
            domainEventPublisher.publish(List.of(
                    new AuthorizationRevisionRejectedEvent(orderId, "order quantity exceeds authorization limit")));
            return;
        }

        List<AuthorizationDomainEvent> events = authorization.reviseAuthorization(newTotalQuantity);
        authorizationRepository.save(authorization);
        domainEventPublisher.publish(events);
    }

    // Orchestration: ReviseOrderSagaOrchestrator sent a ReviseAuthorization command and is
    // waiting on saga.replies, same split as AuthorizationCancelService's choreography/command
    // methods (see docs/session-2026-07-21b.md for why this split matters).
    @Transactional
    public void reviseForCommand(String eventId, Long orderId, Integer newTotalQuantity, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null) {
            return;
        }

        if (newTotalQuantity == null || !isAuthorized(newTotalQuantity)) {
            publishReply("AuthorizationRevisionRejected", orderId, "order quantity exceeds authorization limit", sagaType);
            return;
        }

        authorization.reviseAuthorization(newTotalQuantity);
        authorizationRepository.save(authorization);
        publishReply("AuthorizationRevised", orderId, null, sagaType);
    }

    private boolean isAuthorized(int totalQuantity) {
        return totalQuantity <= AUTHORIZATION_QUANTITY_LIMIT;
    }

    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "accounting", eventType, orderId, reason, sagaType);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga event", e);
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationReviseServiceTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationReviseService.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationReviseServiceTest.java
git commit -m "feat(accounting-service): add AuthorizationReviseService for choreography and orchestration"
```

---

## Task 10: Accounting-service infrastructure — `KitchenEventListener` + `AccountingCommandListener`

**Files:**
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListener.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListenerTest.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListenerTest.java`

**Interfaces:**
- Consumes: `AuthorizationReviseService.reviseForChoreography`/`reviseForCommand` (Task 9).

- [ ] **Step 1: Write the failing tests**

Append to `KitchenEventListenerTest.java`:

```java
    @Test
    void dispatchesTicketQuantityRevisedToAuthorizationReviseService() {
        String payload = """
                {"eventId":"e10","eventType":"TicketQuantityRevised","orderId":42,"ticketId":1,"totalQuantity":8}
                """;

        listener.onMessage(payload);

        verify(authorizationReviseService).reviseForChoreography("e10", 42L, 8);
    }
```

Append to `AccountingCommandListenerTest.java`:

```java
    @Test
    void dispatchesReviseAuthorizationCommand() {
        String payload = """
                {"eventId":"c10","commandType":"ReviseAuthorization","orderId":42,"totalQuantity":8,"sagaType":"ReviseOrder"}
                """;

        listener.onMessage(payload);

        verify(authorizationReviseService).reviseForCommand("c10", 42L, 8, "ReviseOrder");
    }
```

(If either test file's mock field is named differently than `authorizationReviseService`, use the naming convention already established for `authorizationCancelService` in the same file — a mock field of type `AuthorizationReviseService`, injected into the listener's constructor alongside the existing collaborators.)

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.infrastructure.KitchenEventListenerTest" --tests "com.sanjay.ftgo.accounting.infrastructure.AccountingCommandListenerTest"`
Expected: Compilation failure — the listeners don't have an `AuthorizationReviseService` dependency yet.

- [ ] **Step 3: Wire `AuthorizationReviseService` into both listeners**

In `KitchenEventListener.java`, add a constructor parameter/field `authorizationReviseService` (same pattern as the existing `sagaJoinService`/`authorizationCancelService` fields), and change the switch:

```java
        switch (event.eventType()) {
            case "TicketCreated", "TicketCreationFailed" ->
                    sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
            case "TicketCancelled" -> authorizationCancelService.reverseForChoreography(event.eventId(), event.orderId());
            default -> { }
        }
```

to:

```java
        switch (event.eventType()) {
            case "TicketCreated", "TicketCreationFailed" ->
                    sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
            case "TicketCancelled" -> authorizationCancelService.reverseForChoreography(event.eventId(), event.orderId());
            case "TicketQuantityRevised" ->
                    authorizationReviseService.reviseForChoreography(event.eventId(), event.orderId(), event.totalQuantity());
            default -> { }
        }
```

In `AccountingCommandListener.java`, add the same `authorizationReviseService` field/constructor parameter, and change the switch:

```java
        switch (command.commandType()) {
            case "AuthorizeCard" ->
                    sagaJoinService.handleAuthorizeCardCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ReverseAuthorization" ->
                    authorizationCancelService.reverseForCommand(command.eventId(), command.orderId(), command.sagaType());
            default -> log.warn("Unknown accounting command type: {}", command.commandType());
        }
```

to:

```java
        switch (command.commandType()) {
            case "AuthorizeCard" ->
                    sagaJoinService.handleAuthorizeCardCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ReverseAuthorization" ->
                    authorizationCancelService.reverseForCommand(command.eventId(), command.orderId(), command.sagaType());
            case "ReviseAuthorization" ->
                    authorizationReviseService.reviseForCommand(command.eventId(), command.orderId(), command.totalQuantity(), command.sagaType());
            default -> log.warn("Unknown accounting command type: {}", command.commandType());
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.infrastructure.KitchenEventListenerTest" --tests "com.sanjay.ftgo.accounting.infrastructure.AccountingCommandListenerTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListener.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListenerTest.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListenerTest.java
git commit -m "feat(accounting-service): dispatch revision events/commands to AuthorizationReviseService"
```

---

## Task 11: `OrderReviseSagaService` (choreography)

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderReviseSagaService.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderReviseSagaServiceTest.java`

**Interfaces:**
- Consumes: `Order.confirmRevision()`/`rejectRevision()` (Task 1), `OrderDomainEventPublisher.publish`/`publishRevisionCompensationRequested` (Task 2).
- Produces: `OrderReviseSagaService.confirmRevision(Long orderId, String eventId)`, `rejectRevision(Long orderId, String eventId)`, `compensateRevision(Long orderId, String eventId)`, `finalizeRejectedRevision(Long orderId, String eventId)` — consumed by Task 13.

- [ ] **Step 1: Write the failing tests**

Create `OrderReviseSagaServiceTest.java`:

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
import static org.mockito.Mockito.when;

class OrderReviseSagaServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);

    private final OrderReviseSagaService service =
            new OrderReviseSagaService(orderRepository, processedEventRepository, domainEventPublisher);

    private Order revisionPendingOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        order.revise(new OrderRevision(List.of(new OrderLineItem(10L, 8))));
        return order;
    }

    @Test
    void confirmRevisionMovesOrderToApprovedWithRevisedLineItems() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.confirmRevision(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 8));
        verify(domainEventPublisher).publish(List.of(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 8)))));
    }

    @Test
    void rejectRevisionMovesOrderBackToApprovedWithOriginalLineItems() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e2")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.rejectRevision(42L, "e2");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
    }

    @Test
    void compensateRevisionPublishesCompensationRequestWithoutChangingOrderStatus() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e3")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.compensateRevision(42L, "e3");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        verify(domainEventPublisher).publishRevisionCompensationRequested(any(Order.class), any());
        verify(orderRepository, never()).save(any());
    }

    @Test
    void finalizeRejectedRevisionMovesOrderBackToApproved() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e4")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.finalizeRejectedRevision(42L, "e4");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
    }

    @Test
    void skipsDuplicateConfirmRevisionDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.confirmRevision(42L, "e1");

        verify(orderRepository, never()).findById(any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderReviseSagaServiceTest"`
Expected: Compilation failure — `OrderReviseSagaService` doesn't exist yet.

- [ ] **Step 3: Create `OrderReviseSagaService.java`**

```java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class OrderReviseSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderReviseSagaService.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderReviseSagaService(OrderRepository orderRepository,
                                   ProcessedEventRepository processedEventRepository,
                                   OrderDomainEventPublisher domainEventPublisher) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Transactional
    public void confirmRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.confirmRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    @Transactional
    public void rejectRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.rejectRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision rejection for order {}: {}", orderId, e.getMessage());
        }
    }

    // Triggers kitchen's undo without changing Order's own status - Order must stay
    // REVISION_PENDING until finalizeRejectedRevision runs, once the undo is confirmed.
    @Transactional
    public void compensateRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.REVISION_PENDING) {
            return;
        }
        String compensationEventId = UUID.randomUUID().toString();
        domainEventPublisher.publishRevisionCompensationRequested(order, compensationEventId);
    }

    @Transactional
    public void finalizeRejectedRevision(Long orderId, String eventId) {
        rejectRevision(orderId, eventId);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderReviseSagaServiceTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderReviseSagaService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderReviseSagaServiceTest.java
git commit -m "feat(order-service): add OrderReviseSagaService for choreography"
```

---

## Task 12: `ReviseOrderSagaOrchestrator` (orchestration) + `OrchestratorReplyListener` routing

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestrator.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestratorTest.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListener.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListenerTest.java`

**Interfaces:**
- Consumes: `Order.getPendingRevisedLineItems()`, `confirmRevision()`, `rejectRevision()` (Task 1).
- Produces: `ReviseOrderSagaOrchestrator.start(Order order)`, `handleReply(String eventId, String participant, String eventType, Long orderId, String reason)` — consumed by Task 13 and `OrchestratorReplyListener`.

- [ ] **Step 1: Write the failing tests**

Create `ReviseOrderSagaOrchestratorTest.java`:

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

class ReviseOrderSagaOrchestratorTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ReviseOrderSagaOrchestrator orchestrator = new ReviseOrderSagaOrchestrator(
            orderRepository, processedEventRepository, outboxEventRepository, domainEventPublisher, objectMapper);

    private Order revisionPendingOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        order.revise(new OrderRevision(List.of(new OrderLineItem(10L, 8))));
        return order;
    }

    @Test
    void startSendsReviseTicketCommandWithPendingRevisedQuantity() {
        orchestrator.start(revisionPendingOrder());

        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "ReviseTicket".equals(e.getEventType())
                && e.getPayload().contains("\"totalQuantity\":8")
                && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }

    @Test
    void ticketQuantityRevisedTriggersReviseAuthorizationWithPendingRevisedQuantity() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketQuantityRevised", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "accounting.commands".equals(e.getTopic())
                && "ReviseAuthorization".equals(e.getEventType())
                && e.getPayload().contains("\"totalQuantity\":8")));
    }

    @Test
    void ticketRevisionRejectedRejectsRevisionWithoutContactingAccounting() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketRevisionRejected", 42L, "order exceeds kitchen capacity");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
        verify(outboxEventRepository, never()).save(argThat(e -> "accounting.commands".equals(e.getTopic())));
    }

    @Test
    void authorizationRevisedConfirmsRevision() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e2", "accounting", "AuthorizationRevised", 42L, null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 8));
        verify(domainEventPublisher).publish(List.of(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 8)))));
    }

    @Test
    void authorizationRevisionRejectedSendsUndoReviseTicketWithOriginalQuantity() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e2", "accounting", "AuthorizationRevisionRejected", 42L, "order quantity exceeds authorization limit");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "UndoReviseTicket".equals(e.getEventType())
                && e.getPayload().contains("\"totalQuantity\":2")));
        verify(orderRepository, never()).save(any());
    }

    @Test
    void ticketRevisionUndoneFinalizesRejection() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e3", "kitchen", "TicketRevisionUndone", 42L, null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "kitchen", "TicketQuantityRevised", 42L, null);

        verify(orderRepository, never()).findById(any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.ReviseOrderSagaOrchestratorTest"`
Expected: Compilation failure — `ReviseOrderSagaOrchestrator` doesn't exist yet.

- [ ] **Step 3: Create `ReviseOrderSagaOrchestrator.java`**

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

// Deliberately stateless, like CancelOrderSagaOrchestrator: Revise Order is a strict linear
// pipeline (kitchen re-check -> accounting re-authorize -> confirm/reject), and both the pending
// revised quantity and the original quantity are recomputed from Order's own line items rather
// than threaded through the reply chain, so no persisted saga instance table is needed either.
@Service
public class ReviseOrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ReviseOrderSagaOrchestrator.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public ReviseOrderSagaOrchestrator(OrderRepository orderRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        OutboxEventRepository outboxEventRepository,
                                        OrderDomainEventPublisher domainEventPublisher,
                                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
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
            case "TicketRevisionRejected", "TicketRevisionUndone" -> rejectRevision(orderId);
            case "TicketQuantityRevised" -> tryAuthorize(orderId);
            default -> { }
        }
    }

    private void tryAuthorize(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        int newTotalQuantity = totalQuantity(order.getPendingRevisedLineItems());
        String eventId = UUID.randomUUID().toString();
        publishAccountingCommand(eventId, "ReviseAuthorization", orderId, newTotalQuantity);
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        switch (eventType) {
            case "AuthorizationRevised" -> confirmRevision(orderId);
            case "AuthorizationRevisionRejected" -> sendUndoReviseTicket(orderId);
            default -> { }
        }
    }

    private void sendUndoReviseTicket(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        int originalTotalQuantity = totalQuantity(order.getLineItems());
        String eventId = UUID.randomUUID().toString();
        publishKitchenCommand(eventId, "UndoReviseTicket", orderId, originalTotalQuantity);
    }

    private void confirmRevision(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.confirmRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    private void rejectRevision(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.rejectRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision rejection for order {}: {}", orderId, e.getMessage());
        }
    }

    private int totalQuantity(List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }

    private void publishKitchenCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        outboxEventRepository.save(new OutboxEvent(eventId, commandType, orderId, "kitchen.commands",
                toJson(new KitchenCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"))));
    }

    private void publishAccountingCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        outboxEventRepository.save(new OutboxEvent(eventId, commandType, orderId, "accounting.commands",
                toJson(new AccountingCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"))));
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

- [ ] **Step 4: Wire `ReviseOrderSagaOrchestrator` into `OrchestratorReplyListener.java`**

Add a failing test first — append to `OrchestratorReplyListenerTest.java`:

```java
    @Test
    void routesReviseOrderReplyToReviseOrderSagaOrchestrator() {
        String payload = """
                {"eventId":"e5","participant":"kitchen","eventType":"TicketQuantityRevised","orderId":42,"sagaType":"ReviseOrder"}
                """;

        listener.onMessage(payload);

        verify(reviseOrderSagaOrchestrator).handleReply("e5", "kitchen", "TicketQuantityRevised", 42L, null);
    }
```

(Use whatever mock-field naming convention the existing file already uses for `cancelOrderSagaOrchestrator` — add a matching `reviseOrderSagaOrchestrator` mock field of type `ReviseOrderSagaOrchestrator`.)

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.OrchestratorReplyListenerTest"`
Expected: Compilation failure.

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListener.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.ReviseOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.SagaReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Single consumer group on saga.replies shared by all three orchestrators: Kafka replies carry
// a sagaType field so this listener can route to the right orchestrator before any of their
// handleReply methods are ever called, keeping each orchestrator's own logic untouched.
@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestratorReplyListener {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorReplyListener.class);

    private final CreateOrderSagaOrchestrator createOrderSagaOrchestrator;
    private final CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator;
    private final ReviseOrderSagaOrchestrator reviseOrderSagaOrchestrator;
    private final ObjectMapper objectMapper;

    public OrchestratorReplyListener(CreateOrderSagaOrchestrator createOrderSagaOrchestrator,
                                      CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator,
                                      ReviseOrderSagaOrchestrator reviseOrderSagaOrchestrator,
                                      ObjectMapper objectMapper) {
        this.createOrderSagaOrchestrator = createOrderSagaOrchestrator;
        this.cancelOrderSagaOrchestrator = cancelOrderSagaOrchestrator;
        this.reviseOrderSagaOrchestrator = reviseOrderSagaOrchestrator;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "saga.replies", groupId = "order-service")
    public void onMessage(String payload) {
        SagaReply reply;
        try {
            reply = objectMapper.readValue(payload, SagaReply.class);
        } catch (Exception e) {
            log.warn("Skipping malformed saga reply: {}", payload, e);
            return;
        }
        switch (reply.sagaType()) {
            case "CreateOrder" -> createOrderSagaOrchestrator.handleReply(
                    reply.eventId(), reply.participant(), reply.eventType(), reply.orderId(), reply.reason());
            case "CancelOrder" -> cancelOrderSagaOrchestrator.handleReply(
                    reply.eventId(), reply.participant(), reply.eventType(), reply.orderId(), reply.reason());
            case "ReviseOrder" -> reviseOrderSagaOrchestrator.handleReply(
                    reply.eventId(), reply.participant(), reply.eventType(), reply.orderId(), reply.reason());
            default -> log.warn("Unknown saga type on reply: {}", reply.sagaType());
        }
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.ReviseOrderSagaOrchestratorTest" --tests "com.sanjay.ftgo.order.infrastructure.OrchestratorReplyListenerTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ReviseOrderSagaOrchestratorTest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListener.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListenerTest.java
git commit -m "feat(order-service): add stateless ReviseOrderSagaOrchestrator and route saga.replies by sagaType"
```

---

## Task 13: `OrderRevisionSagaTrigger` + `OrderController` wiring + order-service listeners

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisionSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderRevisionSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrchestrationOrderRevisionSagaTrigger.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListenerTest.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListenerTest.java`

**Interfaces:**
- Consumes: `OrderReviseSagaService` (Task 11), `ReviseOrderSagaOrchestrator` (Task 12).

- [ ] **Step 1: Write the failing tests**

In `OrderControllerTest.java`, add a mock field alongside the existing `cancellationSagaTrigger`:

```java
    @MockitoBean
    private OrderRevisionSagaTrigger revisionSagaTrigger;
```

(Add `import com.sanjay.ftgo.order.domain.OrderRevisionSagaTrigger;` to the import list.)

Update the `revisesAnApprovedOrder` test to verify the trigger fires:

```java
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

        verify(revisionSagaTrigger).onOrderRevised(eq(order), any());
    }
```

Append to `KitchenEventListenerTest.java` (order-service):

```java
    @Test
    void handlesTicketRevisionRejectedEvent() {
        String payload = """
                {"eventId":"e10","eventType":"TicketRevisionRejected","orderId":42,"reason":"order exceeds kitchen capacity"}
                """;

        listener.onMessage(payload);

        verify(orderReviseSagaService).rejectRevision(42L, "e10");
    }

    @Test
    void handlesTicketRevisionUndoneEvent() {
        String payload = """
                {"eventId":"e11","eventType":"TicketRevisionUndone","orderId":42,"totalQuantity":2}
                """;

        listener.onMessage(payload);

        verify(orderReviseSagaService).finalizeRejectedRevision(42L, "e11");
    }
```

Append to `AccountingEventListenerTest.java` (order-service):

```java
    @Test
    void handlesAuthorizationRevisedEvent() {
        String payload = """
                {"eventId":"e10","eventType":"AuthorizationRevised","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderReviseSagaService).confirmRevision(42L, "e10");
    }

    @Test
    void handlesAuthorizationRevisionRejectedEvent() {
        String payload = """
                {"eventId":"e11","eventType":"AuthorizationRevisionRejected","orderId":42,"reason":"order quantity exceeds authorization limit"}
                """;

        listener.onMessage(payload);

        verify(orderReviseSagaService).compensateRevision(42L, "e11");
    }
```

(Both listener test files already inject `OrderCancelSagaService` under a `orderCancelSagaService` mock field per the existing convention — add an `orderReviseSagaService` mock field of type `OrderReviseSagaService` alongside it.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest" --tests "com.sanjay.ftgo.order.infrastructure.KitchenEventListenerTest" --tests "com.sanjay.ftgo.order.infrastructure.AccountingEventListenerTest"`
Expected: Compilation failure — `OrderRevisionSagaTrigger` doesn't exist, listeners don't have an `OrderReviseSagaService` dependency yet.

- [ ] **Step 3: Create the trigger interface and both implementations**

Create `OrderRevisionSagaTrigger.java`:

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public interface OrderRevisionSagaTrigger {

    void onOrderRevised(Order order, List<OrderDomainEvent> events);
}
```

Create `ChoreographyOrderRevisionSagaTrigger.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ChoreographyOrderRevisionSagaTrigger implements OrderRevisionSagaTrigger {

    private final OrderDomainEventPublisher domainEventPublisher;

    public ChoreographyOrderRevisionSagaTrigger(OrderDomainEventPublisher domainEventPublisher) {
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    public void onOrderRevised(Order order, List<OrderDomainEvent> events) {
        domainEventPublisher.publish(events);
    }
}
```

Create `OrchestrationOrderRevisionSagaTrigger.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestrationOrderRevisionSagaTrigger implements OrderRevisionSagaTrigger {

    private final ReviseOrderSagaOrchestrator orchestrator;

    public OrchestrationOrderRevisionSagaTrigger(ReviseOrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void onOrderRevised(Order order, List<OrderDomainEvent> events) {
        orchestrator.start(order);
    }
}
```

- [ ] **Step 4: Wire the trigger into `OrderController.java`**

Add the field, constructor parameter, and update `revise()`, removing the now-unused `apply` helper (its only caller was `revise()`):

```java
    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final OrderCancellationSagaTrigger cancellationSagaTrigger;
    private final OrderRevisionSagaTrigger revisionSagaTrigger;

    public OrderController(OrderService orderService, OrderRepository orderRepository,
                            OrderDomainEventPublisher domainEventPublisher,
                            OrderCancellationSagaTrigger cancellationSagaTrigger,
                            OrderRevisionSagaTrigger revisionSagaTrigger) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.cancellationSagaTrigger = cancellationSagaTrigger;
        this.revisionSagaTrigger = revisionSagaTrigger;
    }
```

Replace the `revise` endpoint and remove `apply`:

```java
    @PostMapping("/{id}/revise")
    @Transactional
    public ResponseEntity<OrderResponse> revise(@PathVariable Long id, @RequestBody ReviseOrderRequest request) {
        Order order = findOrder(id);
        List<OrderLineItem> revisedLineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();
        List<OrderDomainEvent> events = order.revise(new OrderRevision(revisedLineItems));
        orderRepository.save(order);
        revisionSagaTrigger.onOrderRevised(order, events);
        return ResponseEntity.ok(OrderResponse.from(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
```

(This removes the `apply(Order order, List<OrderDomainEvent> events)` private method entirely — it had no other callers.)

- [ ] **Step 5: Extend `KitchenEventListener.java` and `AccountingEventListener.java` (order-service)**

In `KitchenEventListener.java`, add an `orderReviseSagaService` field/constructor parameter (same pattern as `orderCancelSagaService`), and change the switch:

```java
        switch (event.eventType()) {
            case "TicketConfirmed" -> orderSagaService.approve(event.orderId(), event.eventId());
            case "TicketCreationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "TicketCancellationRejected" -> orderCancelSagaService.rejectCancel(event.orderId(), event.eventId());
            default -> { }
        }
```

to:

```java
        switch (event.eventType()) {
            case "TicketConfirmed" -> orderSagaService.approve(event.orderId(), event.eventId());
            case "TicketCreationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "TicketCancellationRejected" -> orderCancelSagaService.rejectCancel(event.orderId(), event.eventId());
            case "TicketRevisionRejected" -> orderReviseSagaService.rejectRevision(event.orderId(), event.eventId());
            case "TicketRevisionUndone" -> orderReviseSagaService.finalizeRejectedRevision(event.orderId(), event.eventId());
            default -> { }
        }
```

In `AccountingEventListener.java`, add the same `orderReviseSagaService` field/constructor parameter, and change the switch:

```java
        switch (event.eventType()) {
            case "CardAuthorizationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "AuthorizationReversed" -> orderCancelSagaService.confirmCancel(event.orderId(), event.eventId());
            default -> { }
        }
```

to:

```java
        switch (event.eventType()) {
            case "CardAuthorizationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "AuthorizationReversed" -> orderCancelSagaService.confirmCancel(event.orderId(), event.eventId());
            case "AuthorizationRevised" -> orderReviseSagaService.confirmRevision(event.orderId(), event.eventId());
            case "AuthorizationRevisionRejected" -> orderReviseSagaService.compensateRevision(event.orderId(), event.eventId());
            default -> { }
        }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest" --tests "com.sanjay.ftgo.order.infrastructure.KitchenEventListenerTest" --tests "com.sanjay.ftgo.order.infrastructure.AccountingEventListenerTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 7: Run the full multi-module build**

Run: `./gradlew clean build`
Expected: `BUILD SUCCESSFUL` across all modules.

- [ ] **Step 8: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRevisionSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderRevisionSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrchestrationOrderRevisionSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListenerTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListenerTest.java
git commit -m "feat(order-service): wire OrderController.revise() through a saga-mode-aware OrderRevisionSagaTrigger"
```

---

## Manual Docker e2e verification (after all 13 tasks, both saga modes)

Not a subagent task — run by hand after the plan is fully implemented, mirroring the verification already done for the Order aggregate (PR #12) and Cancel Order saga (PR #13):

1. **Happy path**: `POST /orders` → approve via the normal Create Order flow → `POST /orders/{id}/revise` with a still-within-limits line item quantity → confirm `Order` reaches `APPROVED` with the new line items, kitchen's `tickets.total_quantity` and accounting's `authorizations.total_quantity` both updated to match.
2. **Kitchen-rejects path**: revise to a quantity exceeding `KITCHEN_CAPACITY_LIMIT` (20) → confirm `Order` returns to `APPROVED` with the *original* line items, `tickets.total_quantity`/`authorizations.total_quantity` both unchanged, and accounting-service logs show no `ReviseAuthorization`/`TicketQuantityRevised` activity for that order.
3. **Kitchen-confirms-then-accounting-declines path**: revise to a quantity within kitchen capacity (≤20) but exceeding `AUTHORIZATION_QUANTITY_LIMIT` (10) → confirm `tickets.total_quantity` ends back at the original value (visibly changed then reverted, or captured via a brief poll), `authorizations.total_quantity` never changed, `Order` back to `APPROVED` with original line items.
4. Repeat all three scenarios with `SAGA_MODE=orchestration` and confirm identical end states to choreography.
5. Redelivery/idempotency: force a Kafka redelivery of one revision-flow message (same pattern as prior sessions) and confirm no duplicate quantity changes or duplicate `processed_events` rows.

After verification, update `CONTEXT.md`, `README.md`, and add `docs/session-2026-07-21c.md` per this project's documentation-sync convention, then open a PR.
