# Ticket Aggregate Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor kitchen-service's `Ticket` from an anemic entity + transaction-script `TicketService` into a real DDD aggregate (Ch. 5 of *Microservices Patterns*) with enforced state transitions and returned domain events, and add the restaurant-worker REST lifecycle (`accept`/`preparing`/`readyForPickup`/`pickedUp`) the book models but this app never had.

**Architecture:** `Ticket` gains a `TicketState` enum and state-changing methods (`confirm`, `accept`, `preparing`, `readyForPickup`, `pickedUp`, `cancel`) that enforce legal transitions and return `List<TicketDomainEvent>` — one class per event. A new `TicketDomainEventPublisher` translates those typed events into the existing flat `KitchenEvent` wire record and writes them to the outbox, preserving exact wire-format compatibility with `order-service`'s existing Kafka consumer. `TicketService` becomes a thinner orchestrator: look up the aggregate, call one method, save, publish. A new `TicketController` (kitchen-service's first REST controller) exposes the worker-driven transitions.

**Tech Stack:** Java 17+, Spring Boot 3.5.16, Spring Data JPA, Spring Web (new for kitchen-service), Spring Kafka, JUnit 5, Mockito, AssertJ, MockMvc + `@WebMvcTest`.

## Global Constraints

- **Wire-format compatibility is non-negotiable.** `order-service`'s `KitchenEventListener` deserializes `kitchen.events` payloads into its own independent `KitchenEvent(eventId, eventType, orderId, ticketId, totalQuantity, reason)` record and only handles `"TicketConfirmed"`/`"TicketCreationFailed"`; unknown fields would fail deserialization under Jackson's default strict mode. Every event published to `kitchen.events` in this plan reuses kitchen-service's existing flat `KitchenEvent` record as the JSON payload shape — **no new fields**, regardless of what the typed domain event object itself carries in memory (e.g. `TicketAcceptedEvent.readyBy()` is never serialized onto the wire).
- **No DB schema changes.** The `tickets.status` column keeps its name and existing string values (`CREATE_PENDING`, `AWAITING_ACCEPTANCE`, `CANCELLED`); `TicketState` just adds compile-time-checked new values (`ACCEPTED`, `PREPARING`, `READY_FOR_PICKUP`, `PICKED_UP`) on top.
- **No new capability beyond what's in the spec.** No staff-initiated `reject()`, no persisted `readyBy`/per-transition timestamps.
- Java 17+ sealed interfaces/records/switch pattern matching are available and preferred over `instanceof` chains, matching this codebase's existing use of Java 17+ features (records already used throughout, e.g. `OrderCreatedEvent`, `SagaReply`).
- Follow this codebase's existing test conventions exactly: Mockito-mocked collaborators + AssertJ assertions for service/domain tests (see `TicketServiceTest.java`), `@WebMvcTest` + `MockMvc` + `@MockitoBean` for controller tests (see `OrderControllerTest.java` in `ftgo-order-service`).

---

## Task 1: Domain event types, state enum, and exceptions

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketState.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCreatedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCreationFailedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketConfirmedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCancelledEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketAcceptedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketPreparingStartedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketReadyForPickupEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketPickedUpEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCreationResult.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketNotFoundException.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCannotBeCancelledException.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/UnsupportedStateTransitionException.java`

**Interfaces:**
- Produces: `TicketState` enum (`CREATE_PENDING`, `AWAITING_ACCEPTANCE`, `ACCEPTED`, `PREPARING`, `READY_FOR_PICKUP`, `PICKED_UP`, `CANCELLED`); sealed `TicketDomainEvent` interface with `Long orderId()`, permitting the 8 event records below; `TicketCreationResult(Ticket ticket, List<TicketDomainEvent> events)` — used by Task 2. `TicketNotFoundException(Long ticketId)`, `TicketCannotBeCancelledException(Long orderId)`, `UnsupportedStateTransitionException(TicketState state)` — used by Tasks 2 and 5.

These are pure data/exception types with no behavior of their own (exercised indirectly by Task 2's aggregate tests), so this task has no independent TDD cycle — it's scaffolding the next task depends on. Verified by compiling.

- [ ] **Step 1: Create `TicketState.java`**

```java
package com.sanjay.ftgo.kitchen.domain;

public enum TicketState {
    CREATE_PENDING,
    AWAITING_ACCEPTANCE,
    ACCEPTED,
    PREPARING,
    READY_FOR_PICKUP,
    PICKED_UP,
    CANCELLED
}
```

- [ ] **Step 2: Create `TicketDomainEvent.java`**

```java
package com.sanjay.ftgo.kitchen.domain;

public sealed interface TicketDomainEvent
        permits TicketCreatedEvent, TicketCreationFailedEvent, TicketConfirmedEvent, TicketCancelledEvent,
                TicketAcceptedEvent, TicketPreparingStartedEvent, TicketReadyForPickupEvent, TicketPickedUpEvent {

    Long orderId();
}
```

- [ ] **Step 3: Create the 8 event records**

`TicketCreatedEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketCreatedEvent(Long orderId, int totalQuantity) implements TicketDomainEvent {
}
```

`TicketCreationFailedEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketCreationFailedEvent(Long orderId, String reason) implements TicketDomainEvent {
}
```

`TicketConfirmedEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketConfirmedEvent(Long orderId) implements TicketDomainEvent {
}
```

`TicketCancelledEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketCancelledEvent(Long orderId) implements TicketDomainEvent {
}
```

`TicketAcceptedEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

import java.time.ZonedDateTime;

public record TicketAcceptedEvent(Long orderId, ZonedDateTime readyBy) implements TicketDomainEvent {
}
```

`TicketPreparingStartedEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketPreparingStartedEvent(Long orderId) implements TicketDomainEvent {
}
```

`TicketReadyForPickupEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketReadyForPickupEvent(Long orderId) implements TicketDomainEvent {
}
```

`TicketPickedUpEvent.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketPickedUpEvent(Long orderId) implements TicketDomainEvent {
}
```

- [ ] **Step 4: Create `TicketCreationResult.java`**

```java
package com.sanjay.ftgo.kitchen.domain;

import java.util.List;

public record TicketCreationResult(Ticket ticket, List<TicketDomainEvent> events) {
}
```

- [ ] **Step 5: Create the 3 exception types**

`TicketNotFoundException.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public class TicketNotFoundException extends RuntimeException {

    public TicketNotFoundException(Long ticketId) {
        super("Ticket not found: " + ticketId);
    }
}
```

`TicketCannotBeCancelledException.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public class TicketCannotBeCancelledException extends RuntimeException {

    public TicketCannotBeCancelledException(Long orderId) {
        super("Ticket for order " + orderId + " cannot be cancelled once ready for pickup");
    }
}
```

`UnsupportedStateTransitionException.java`:
```java
package com.sanjay.ftgo.kitchen.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(TicketState state) {
        super("Unsupported transition from state " + state);
    }
}
```

- [ ] **Step 6: Verify the module compiles**

Run: `./gradlew :ftgo-kitchen-service:compileJava`
Expected: `BUILD SUCCESSFUL` (these types aren't referenced anywhere yet, so nothing else can break).

- [ ] **Step 7: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketState.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCreatedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCreationFailedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketConfirmedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCancelledEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketAcceptedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketPreparingStartedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketReadyForPickupEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketPickedUpEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCreationResult.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketNotFoundException.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCannotBeCancelledException.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/UnsupportedStateTransitionException.java
git commit -m "feat(kitchen-service): add Ticket domain event types and state enum"
```

---

## Task 2: `Ticket` aggregate — state machine and domain events

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java`
- Create: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketTest.java`

**Interfaces:**
- Consumes: `TicketState`, `TicketDomainEvent`, `TicketCreatedEvent`, `TicketConfirmedEvent`, `TicketCancelledEvent`, `TicketAcceptedEvent`, `TicketPreparingStartedEvent`, `TicketReadyForPickupEvent`, `TicketPickedUpEvent`, `TicketCreationResult`, `TicketCannotBeCancelledException`, `UnsupportedStateTransitionException` (all from Task 1).
- Produces: `Ticket.createTicket(Long orderId, int totalQuantity)` → `TicketCreationResult`; `Ticket.createCancelled(Long orderId)` → `Ticket`; instance methods `getId()`, `getOrderId()`, `getState()`, `confirm()`, `accept(ZonedDateTime readyBy)`, `preparing()`, `readyForPickup()`, `pickedUp()`, `cancel()` — each of the latter 6 returns `List<TicketDomainEvent>` and mutates `state`. These are consumed by Task 4 (`TicketService`) and Task 5 (`TicketController`).

- [ ] **Step 1: Write the failing test suite**

Create `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketTest.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TicketTest {

    @Test
    void createTicketStartsInCreatePendingAndEmitsTicketCreated() {
        TicketCreationResult result = Ticket.createTicket(42L, 3);

        assertThat(result.ticket().getState()).isEqualTo(TicketState.CREATE_PENDING);
        assertThat(result.ticket().getOrderId()).isEqualTo(42L);
        assertThat(result.events()).containsExactly(new TicketCreatedEvent(42L, 3));
    }

    @Test
    void createCancelledStartsDirectlyInCancelled() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(ticket.getOrderId()).isEqualTo(42L);
    }

    @Test
    void confirmMovesFromCreatePendingToAwaitingAcceptance() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        List<TicketDomainEvent> events = ticket.confirm();

        assertThat(ticket.getState()).isEqualTo(TicketState.AWAITING_ACCEPTANCE);
        assertThat(events).containsExactly(new TicketConfirmedEvent(42L));
    }

    @Test
    void confirmFromWrongStateThrows() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(ticket::confirm).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void acceptMovesFromAwaitingAcceptanceToAccepted() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        ZonedDateTime readyBy = ZonedDateTime.now().plusMinutes(30);

        List<TicketDomainEvent> events = ticket.accept(readyBy);

        assertThat(ticket.getState()).isEqualTo(TicketState.ACCEPTED);
        assertThat(events).containsExactly(new TicketAcceptedEvent(42L, readyBy));
    }

    @Test
    void acceptFromWrongStateThrows() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        assertThatThrownBy(() -> ticket.accept(ZonedDateTime.now()))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void preparingMovesFromAcceptedToPreparing() {
        Ticket ticket = acceptedTicket();

        List<TicketDomainEvent> events = ticket.preparing();

        assertThat(ticket.getState()).isEqualTo(TicketState.PREPARING);
        assertThat(events).containsExactly(new TicketPreparingStartedEvent(42L));
    }

    @Test
    void preparingFromWrongStateThrows() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        assertThatThrownBy(ticket::preparing).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void readyForPickupMovesFromPreparingToReadyForPickup() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();

        List<TicketDomainEvent> events = ticket.readyForPickup();

        assertThat(ticket.getState()).isEqualTo(TicketState.READY_FOR_PICKUP);
        assertThat(events).containsExactly(new TicketReadyForPickupEvent(42L));
    }

    @Test
    void readyForPickupFromWrongStateThrows() {
        Ticket ticket = acceptedTicket();

        assertThatThrownBy(ticket::readyForPickup).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void pickedUpMovesFromReadyForPickupToPickedUp() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();

        List<TicketDomainEvent> events = ticket.pickedUp();

        assertThat(ticket.getState()).isEqualTo(TicketState.PICKED_UP);
        assertThat(events).containsExactly(new TicketPickedUpEvent(42L));
    }

    @Test
    void pickedUpFromWrongStateThrows() {
        Ticket ticket = acceptedTicket();

        assertThatThrownBy(ticket::pickedUp).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromCreatePendingSucceeds() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        List<TicketDomainEvent> events = ticket.cancel();

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(events).containsExactly(new TicketCancelledEvent(42L));
    }

    @Test
    void cancelFromAwaitingAcceptanceSucceeds() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();

        List<TicketDomainEvent> events = ticket.cancel();

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(events).containsExactly(new TicketCancelledEvent(42L));
    }

    @Test
    void cancelFromAcceptedSucceeds() {
        Ticket ticket = acceptedTicket();

        List<TicketDomainEvent> events = ticket.cancel();

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(events).containsExactly(new TicketCancelledEvent(42L));
    }

    @Test
    void cancelFromReadyForPickupThrowsTicketCannotBeCancelled() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();

        assertThatThrownBy(ticket::cancel).isInstanceOf(TicketCannotBeCancelledException.class);
    }

    @Test
    void cancelFromPreparingThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();

        assertThatThrownBy(ticket::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromPickedUpThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();
        ticket.pickedUp();

        assertThatThrownBy(ticket::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromCancelledThrowsUnsupportedStateTransition() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(ticket::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    private Ticket acceptedTicket() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        return ticket;
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketTest"`
Expected: Compilation failure — `Ticket` has no `createTicket`, `createCancelled`, `confirm`, `accept`, `preparing`, `readyForPickup`, `pickedUp`, or a `cancel()` that returns `List<TicketDomainEvent>` yet (current `Ticket` only has `markAwaitingAcceptance()`/`markCancelled()` and a `String status`).

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

    protected Ticket() {
    }

    private Ticket(Long orderId, TicketState state) {
        this.orderId = orderId;
        this.state = state;
    }

    public static TicketCreationResult createTicket(Long orderId, int totalQuantity) {
        Ticket ticket = new Ticket(orderId, TicketState.CREATE_PENDING);
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
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketTest"`
Expected: `BUILD SUCCESSFUL`, all 17 tests pass.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketTest.java
git commit -m "feat(kitchen-service): make Ticket a real DDD aggregate with enforced state transitions"
```

---

## Task 3: `TicketDomainEventPublisher`

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java`
- Create: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisherTest.java`

**Interfaces:**
- Consumes: `Ticket` (Task 2), `TicketDomainEvent` and its 8 implementations (Task 1), `com.sanjay.ftgo.common.outbox.OutboxEvent`/`OutboxEventRepository` (existing, `ftgo-common`), `com.sanjay.ftgo.kitchen.domain.KitchenEvent` (existing flat wire record).
- Produces: `TicketDomainEventPublisher.publish(Ticket ticket, List<TicketDomainEvent> events)` and `publishCreationFailed(TicketCreationFailedEvent event)` (a separate, distinctly-named method rather than a nullable-`Ticket` overload of `publish` — Java can't disambiguate two overloads of the same method name by a nullable argument alone when both are exercised through Mockito matchers in tests, and a distinct name is also more self-documenting for the one case with no aggregate instance). Consumed by Task 4 (`TicketService`) and Task 5 (`TicketController`).

- [ ] **Step 1: Write the failing test**

Create `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisherTest.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TicketDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final TicketDomainEventPublisher publisher =
            new TicketDomainEventPublisher(outboxEventRepository, new ObjectMapper());

    @Test
    void publishesTicketCreatedWithTotalQuantityAndTicketId() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        publisher.publish(ticket, List.of(new TicketCreatedEvent(42L, 3)));

        verify(outboxEventRepository).save(argThat(row ->
                "TicketCreated".equals(row.getEventType())
                        && "kitchen.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"totalQuantity\":3")
                        && !row.getPayload().contains("readyBy")));
    }

    @Test
    void publishesTicketAcceptedWithoutLeakingReadyByOntoTheWire() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ZonedDateTime readyBy = ZonedDateTime.parse("2026-07-20T18:00:00Z");

        publisher.publish(ticket, List.of(new TicketAcceptedEvent(42L, readyBy)));

        verify(outboxEventRepository).save(argThat(row ->
                "TicketAccepted".equals(row.getEventType())
                        && "kitchen.events".equals(row.getTopic())
                        && !row.getPayload().contains("readyBy")));
    }

    @Test
    void publishesTicketCreationFailedWithoutATicketInstance() {
        publisher.publishCreationFailed(new TicketCreationFailedEvent(43L, "order exceeds kitchen capacity"));

        verify(outboxEventRepository).save(argThat((OutboxEvent row) ->
                "TicketCreationFailed".equals(row.getEventType())
                        && row.getAggregateId().equals(43L)
                        && row.getPayload().contains("order exceeds kitchen capacity")));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisherTest"`
Expected: Compilation failure — `TicketDomainEventPublisher` doesn't exist yet.

- [ ] **Step 3: Create `TicketDomainEventPublisher.java`**

```java
package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class TicketDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public TicketDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publish(Ticket ticket, List<TicketDomainEvent> events) {
        events.forEach(event -> publishEvent(ticket.getId(), event));
    }

    public void publishCreationFailed(TicketCreationFailedEvent event) {
        publishEvent(null, event);
    }

    private void publishEvent(Long ticketId, TicketDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        KitchenEvent wireEvent = toWireEvent(eventId, ticketId, event);
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "kitchen.events", toJson(wireEvent)));
    }

    private KitchenEvent toWireEvent(String eventId, Long ticketId, TicketDomainEvent event) {
        return switch (event) {
            case TicketCreatedEvent e ->
                    new KitchenEvent(eventId, "TicketCreated", e.orderId(), ticketId, e.totalQuantity(), null);
            case TicketCreationFailedEvent e ->
                    new KitchenEvent(eventId, "TicketCreationFailed", e.orderId(), ticketId, null, e.reason());
            case TicketConfirmedEvent e ->
                    new KitchenEvent(eventId, "TicketConfirmed", e.orderId(), ticketId, null, null);
            case TicketCancelledEvent e ->
                    new KitchenEvent(eventId, "TicketCancelled", e.orderId(), ticketId, null, null);
            case TicketAcceptedEvent e ->
                    new KitchenEvent(eventId, "TicketAccepted", e.orderId(), ticketId, null, null);
            case TicketPreparingStartedEvent e ->
                    new KitchenEvent(eventId, "TicketPreparingStarted", e.orderId(), ticketId, null, null);
            case TicketReadyForPickupEvent e ->
                    new KitchenEvent(eventId, "TicketReadyForPickup", e.orderId(), ticketId, null, null);
            case TicketPickedUpEvent e ->
                    new KitchenEvent(eventId, "TicketPickedUp", e.orderId(), ticketId, null, null);
        };
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize kitchen event", e);
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisherTest"`
Expected: `BUILD SUCCESSFUL`, all 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisherTest.java
git commit -m "feat(kitchen-service): add TicketDomainEventPublisher translating domain events to the kitchen.events wire format"
```

---

## Task 4: Rewrite `TicketService` to use the `Ticket` aggregate

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`

**Interfaces:**
- Consumes: `Ticket.createTicket`/`createCancelled`/`confirm`/`cancel` (Task 2), `TicketDomainEventPublisher.publish` (Task 3), `TicketCreationFailedEvent` (Task 1).
- Produces: `TicketService` constructor signature changes to add a `TicketDomainEventPublisher` parameter — consumed by Task 5's Spring wiring (autowired, no explicit change needed elsewhere since Spring resolves it) and by `TicketServiceTest`.

- [ ] **Step 1: Write the updated test file (failing first)**

Replace the full contents of `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TicketServiceTest {

    private final TicketRepository ticketRepository = mock(TicketRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final FailedOrderRepository failedOrderRepository = mock(FailedOrderRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final TicketDomainEventPublisher domainEventPublisher = mock(TicketDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final TicketService ticketService = new TicketService(
            ticketRepository, processedEventRepository, failedOrderRepository,
            outboxEventRepository, domainEventPublisher, objectMapper);

    private final OrderCreatedEvent event = new OrderCreatedEvent(
            "event-1", "OrderCreated", 42L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 2)));

    @Test
    void createsTicketInCreatePendingOnFirstDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository).save(any());
        verify(ticketRepository).save(argThat(t -> t.getState() == TicketState.CREATE_PENDING));
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCreatedEvent));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(true);

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository, never()).save(any());
        verify(ticketRepository, never()).save(any());
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void createsTicketDirectlyAsCancelledWhenOrderAlreadyFailed() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(true);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(ticketRepository).save(argThat(t -> t.getState() == TicketState.CANCELLED));
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void rejectsTicketCreationWhenQuantityExceedsKitchenCapacity() {
        OrderCreatedEvent bigEvent = new OrderCreatedEvent(
                "event-2", "OrderCreated", 43L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 25)));
        when(processedEventRepository.existsById("event-2")).thenReturn(false);
        when(failedOrderRepository.existsById(43L)).thenReturn(false);

        ticketService.handleOrderCreated(bigEvent);

        verify(ticketRepository, never()).save(any());
        verify(domainEventPublisher).publishCreationFailed(argThat(e -> e.orderId().equals(43L)));
    }

    @Test
    void confirmsTicketWhenCardAuthorized() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("acct-event-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-1", 42L, "CardAuthorized");

        assertThat(ticket.getState()).isEqualTo(TicketState.AWAITING_ACCEPTANCE);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketConfirmedEvent));
    }

    @Test
    void cancelsTicketWhenCardAuthorizationFailed() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("acct-event-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-2", 42L, "CardAuthorizationFailed");

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancelledEvent));
    }

    @Test
    void cancelsExistingTicketWhenConsumerVerificationFails() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cons-event-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleConsumerVerificationFailed("cons-event-1", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancelledEvent));
        verify(failedOrderRepository, never()).save(any());
    }

    @Test
    void recordsFailedOrderWhenNoTicketExistsYet() {
        when(processedEventRepository.existsById("cons-event-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(43L)).thenReturn(Optional.empty());

        ticketService.handleConsumerVerificationFailed("cons-event-2", 43L);

        verify(failedOrderRepository).save(any());
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void createsTicketViaCommandWhenWithinCapacity() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(false);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleCreateTicketCommand("cmd-1", 42L, 5);

        verify(ticketRepository).save(argThat(t -> t.getState() == TicketState.CREATE_PENDING));
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCreated".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void repliesTicketCreationFailedViaCommandWhenOverCapacity() {
        when(processedEventRepository.existsById("cmd-2")).thenReturn(false);

        ticketService.handleCreateTicketCommand("cmd-2", 43L, 25);

        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCreationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void repliesTicketCreationFailedViaCommandWhenTotalQuantityIsNull() {
        when(processedEventRepository.existsById("cmd-5")).thenReturn(false);

        ticketService.handleCreateTicketCommand("cmd-5", 44L, null);

        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCreationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void confirmsTicketViaCommand() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-3")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleConfirmTicketCommand("cmd-3", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.AWAITING_ACCEPTANCE);
    }

    @Test
    void cancelsTicketViaCommand() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-4")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleCancelTicketCommand("cmd-4", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: Compilation failure — `TicketService`'s constructor doesn't accept a `TicketDomainEventPublisher` yet, and `new Ticket(orderId, "CREATE_PENDING")`-style construction is now impossible (private constructor).

- [ ] **Step 3: Rewrite `TicketService.java`**

Replace the full contents of `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

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
public class TicketService {

    private static final int KITCHEN_CAPACITY_LIMIT = 20;

    private final TicketRepository ticketRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final FailedOrderRepository failedOrderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final TicketDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public TicketService(TicketRepository ticketRepository,
                          ProcessedEventRepository processedEventRepository,
                          FailedOrderRepository failedOrderRepository,
                          OutboxEventRepository outboxEventRepository,
                          TicketDomainEventPublisher domainEventPublisher,
                          ObjectMapper objectMapper) {
        this.ticketRepository = ticketRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedOrderRepository = failedOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        int totalQuantity = event.lineItems().stream()
                .mapToInt(OrderCreatedEvent.LineItem::quantity)
                .sum();

        if (failedOrderRepository.existsById(event.orderId())) {
            ticketRepository.save(Ticket.createCancelled(event.orderId()));
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            domainEventPublisher.publishCreationFailed(
                    new TicketCreationFailedEvent(event.orderId(), "order exceeds kitchen capacity"));
            return;
        }

        TicketCreationResult result = Ticket.createTicket(event.orderId(), totalQuantity);
        Ticket ticket = ticketRepository.save(result.ticket());
        domainEventPublisher.publish(ticket, result.events());
    }

    @Transactional
    public void handleAccountingEvent(String eventId, Long orderId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }

        List<TicketDomainEvent> events = "CardAuthorized".equals(eventType) ? ticket.confirm() : ticket.cancel();
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @Transactional
    public void handleConsumerVerificationFailed(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            List<TicketDomainEvent> events = ticket.cancel();
            ticketRepository.save(ticket);
            domainEventPublisher.publish(ticket, events);
        } else {
            failedOrderRepository.save(new FailedOrder(orderId));
        }
    }

    @Transactional
    public void handleCreateTicketCommand(String eventId, Long orderId, Integer totalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (totalQuantity == null) {
            publishReply("TicketCreationFailed", orderId, "totalQuantity is required");
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            publishReply("TicketCreationFailed", orderId, "order exceeds kitchen capacity");
            return;
        }

        TicketCreationResult result = Ticket.createTicket(orderId, totalQuantity);
        ticketRepository.save(result.ticket());
        publishReply("TicketCreated", orderId, null);
    }

    @Transactional
    public void handleConfirmTicketCommand(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            ticket.confirm();
            ticketRepository.save(ticket);
        }
    }

    @Transactional
    public void handleCancelTicketCommand(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            ticket.cancel();
            ticketRepository.save(ticket);
        }
    }

    private boolean isWithinCapacity(int totalQuantity) {
        return totalQuantity <= KITCHEN_CAPACITY_LIMIT;
    }

    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "kitchen", eventType, orderId, reason);
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

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: `BUILD SUCCESSFUL`, all 14 tests pass.

- [ ] **Step 5: Run the full kitchen-service test suite to check for regressions**

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: `BUILD SUCCESSFUL` — `SchedulingEnabledTest` and `FtgoKitchenServiceApplicationTests` unaffected (neither touches `Ticket`/`TicketService` internals).

- [ ] **Step 6: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java
git commit -m "refactor(kitchen-service): TicketService orchestrates the Ticket aggregate instead of building events inline"
```

---

## Task 5: `TicketController` — restaurant-worker REST lifecycle

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/AcceptTicketRequest.java`
- Create: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerTest.java`
- Modify: `ftgo-kitchen-service/build.gradle` (add `spring-boot-starter-web` if not already present — see Step 0)
- Modify: `README.md` (kitchen-service description gains a REST API)
- Modify: `CONTEXT.md` (Ch.5 patterns checklist, "Services to build" table)

**Interfaces:**
- Consumes: `Ticket`, `TicketRepository` (existing `findById` from `JpaRepository`), `TicketDomainEventPublisher.publish(Ticket, List<TicketDomainEvent>)` (Task 3), `TicketNotFoundException`, `UnsupportedStateTransitionException` (Task 1).
- Produces: kitchen-service's first REST endpoints — `POST /tickets/{ticketId}/accept`, `/preparing`, `/ready-for-pickup`, `/picked-up`. No other task depends on this one; it's the outermost layer.

- [ ] **Step 0: Confirm `spring-boot-starter-web` is already a kitchen-service dependency**

Run: `grep -n "spring-boot-starter-web" ftgo-kitchen-service/build.gradle`
Expected: A match. Kitchen-service already depends on Spring Kafka and JPA; if `spring-boot-starter-web` isn't already present as a transitive or direct dependency, add `implementation 'org.springframework.boot:spring-boot-starter-web'` to `ftgo-kitchen-service/build.gradle`'s `dependencies` block before continuing (Spring MVC's `@RestController` machinery requires it). If the grep already matches, skip straight to Step 1.

- [ ] **Step 1: Write the failing test**

Create `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerTest.java`:

```java
package com.sanjay.ftgo.kitchen.api;

import com.sanjay.ftgo.kitchen.domain.Ticket;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisher;
import com.sanjay.ftgo.kitchen.domain.TicketRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.ZonedDateTime;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(TicketController.class)
class TicketControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TicketRepository ticketRepository;

    @MockitoBean
    private TicketDomainEventPublisher domainEventPublisher;

    @Test
    void acceptsAwaitingAcceptanceTicket() throws Exception {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/accept")
                        .contentType("application/json")
                        .content("""
                                {"readyBy":"2026-07-20T18:00:00Z"}
                                """))
                .andExpect(status().isOk());
    }

    @Test
    void returns404WhenTicketNotFoundOnAccept() throws Exception {
        when(ticketRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/tickets/99/accept")
                        .contentType("application/json")
                        .content("""
                                {"readyBy":"2026-07-20T18:00:00Z"}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenAcceptingTicketInWrongState() throws Exception {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/accept")
                        .contentType("application/json")
                        .content("""
                                {"readyBy":"2026-07-20T18:00:00Z"}
                                """))
                .andExpect(status().isConflict());
    }

    @Test
    void movesAcceptedTicketToPreparing() throws Exception {
        Ticket ticket = acceptedTicket();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/preparing"))
                .andExpect(status().isOk());
    }

    @Test
    void returns409WhenMovingCreatePendingTicketToPreparing() throws Exception {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/preparing"))
                .andExpect(status().isConflict());
    }

    @Test
    void movesPreparingTicketToReadyForPickup() throws Exception {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/ready-for-pickup"))
                .andExpect(status().isOk());
    }

    @Test
    void movesReadyForPickupTicketToPickedUp() throws Exception {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/picked-up"))
                .andExpect(status().isOk());
    }

    @Test
    void returns404WhenTicketNotFoundOnPickedUp() throws Exception {
        when(ticketRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/tickets/99/picked-up"))
                .andExpect(status().isNotFound());
    }

    private Ticket acceptedTicket() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        return ticket;
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.api.TicketControllerTest"`
Expected: Compilation failure — `TicketController` and `AcceptTicketRequest` don't exist yet.

- [ ] **Step 3: Create `AcceptTicketRequest.java`**

```java
package com.sanjay.ftgo.kitchen.api;

import java.time.ZonedDateTime;

public record AcceptTicketRequest(ZonedDateTime readyBy) {
}
```

- [ ] **Step 4: Create `TicketController.java`**

```java
package com.sanjay.ftgo.kitchen.api;

import com.sanjay.ftgo.kitchen.domain.Ticket;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEvent;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisher;
import com.sanjay.ftgo.kitchen.domain.TicketNotFoundException;
import com.sanjay.ftgo.kitchen.domain.TicketRepository;
import com.sanjay.ftgo.kitchen.domain.UnsupportedStateTransitionException;
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

@RestController
@RequestMapping("/tickets")
@Transactional
public class TicketController {

    private final TicketRepository ticketRepository;
    private final TicketDomainEventPublisher domainEventPublisher;

    public TicketController(TicketRepository ticketRepository, TicketDomainEventPublisher domainEventPublisher) {
        this.ticketRepository = ticketRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @PostMapping("/{ticketId}/accept")
    public ResponseEntity<Void> accept(@PathVariable Long ticketId, @RequestBody AcceptTicketRequest request) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.accept(request.readyBy()));
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{ticketId}/preparing")
    public ResponseEntity<Void> preparing(@PathVariable Long ticketId) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.preparing());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{ticketId}/ready-for-pickup")
    public ResponseEntity<Void> readyForPickup(@PathVariable Long ticketId) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.readyForPickup());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{ticketId}/picked-up")
    public ResponseEntity<Void> pickedUp(@PathVariable Long ticketId) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.pickedUp());
        return ResponseEntity.ok().build();
    }

    private Ticket findTicket(Long ticketId) {
        return ticketRepository.findById(ticketId)
                .orElseThrow(() -> new TicketNotFoundException(ticketId));
    }

    private void apply(Ticket ticket, List<TicketDomainEvent> events) {
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @ExceptionHandler(TicketNotFoundException.class)
    public ResponseEntity<String> handleNotFound(TicketNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(UnsupportedStateTransitionException.class)
    public ResponseEntity<String> handleConflict(UnsupportedStateTransitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.api.TicketControllerTest"`
Expected: `BUILD SUCCESSFUL`, all 8 tests pass.

- [ ] **Step 6: Run the full kitchen-service build**

Run: `./gradlew :ftgo-kitchen-service:build`
Expected: `BUILD SUCCESSFUL` — all tests across `TicketTest`, `TicketDomainEventPublisherTest`, `TicketServiceTest`, `TicketControllerTest`, `SchedulingEnabledTest`, `FtgoKitchenServiceApplicationTests` pass.

- [ ] **Step 7: Update `README.md`**

Find kitchen-service's entry in the service list/status table and update its description to mention the new REST API (worker lifecycle) alongside its existing Kafka-driven responsibilities. Exact wording depends on the current table layout — read the file first, then edit the kitchen-service row to note: "REST API for restaurant staff (accept/preparing/ready-for-pickup/picked-up) added; Ticket is now a DDD aggregate with enforced state transitions."

- [ ] **Step 8: Update `CONTEXT.md`**

- In the "Patterns reference" section, check off `Domain model / DDD aggregates (Ch. 5)` and `Domain events (Ch. 5)`.
- In "Services to build" table, update the `ftgo-kitchen-service` row to mention the new REST API and aggregate refactor.
- Add a "Current position" update noting `Ticket` aggregate is done, `Order` aggregate remains for a future session.
- Add a session log one-liner for today's date summarizing: read Ch.5 from the book PDF directly, brainstormed + planned + implemented the `Ticket` aggregate refactor (state machine, class-per-event domain events, `TicketDomainEventPublisher`, new REST API for the restaurant-worker lifecycle), verified via Docker.

- [ ] **Step 9: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/AcceptTicketRequest.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerTest.java \
        ftgo-kitchen-service/build.gradle \
        README.md CONTEXT.md
git commit -m "feat(kitchen-service): add restaurant-worker REST API (accept/preparing/ready-for-pickup/picked-up)"
```

---

## Task 6: Manual Docker e2e verification

**Files:** None (verification only — no code changes). If any bug is found, fix it in the relevant file from Tasks 1–5 and note the fix in the commit message, following this project's established pattern (e.g. the Ch.4 `@EnableScheduling` bug, the Ch.5-extraction `@ComponentScan` bug).

- [ ] **Step 1: Build and start the full stack**

Run: `./gradlew build -x test && docker compose up --build -d`
Expected: All containers (mysql, zookeeper, kafka, service-registry, restaurant-service, order-service, kitchen-service, consumer-service, accounting-service) start and reach healthy/running state.

- [ ] **Step 2: Choreography happy path through to the new worker lifecycle**

Place an order via `order-service`'s `POST /orders` (small quantity, consumerId seeded active, e.g. `1`). Confirm via logs/DB that `Order` reaches `APPROVED` and `Ticket` reaches `AWAITING_ACCEPTANCE`, exactly as before this change. Then drive the new REST lifecycle manually against kitchen-service (`POST /tickets/{id}/accept` with a `readyBy`, then `/preparing`, `/ready-for-pickup`, `/picked-up`) and confirm each call returns `200` and the `tickets.status` column advances correctly in MySQL.

- [ ] **Step 3: Orchestration happy path**

Restart with `SAGA_MODE=orchestration` set for order-service/kitchen-service/consumer-service/accounting-service (matching this project's existing env-var convention). Repeat step 2's order-placement check — confirm `Order`/`Ticket`/`Authorization` reach the same end states as choreography mode, proving `TicketService`'s saga-reply translation still works with the new `Ticket` aggregate underneath it.

- [ ] **Step 4: One compensation case**

Place an order with the seeded inactive consumerId (`2`) to trigger consumer-verification failure. Confirm `Ticket` (if already created) or the `failed_orders` table (if not) reaches the same compensation outcome as before this change.

- [ ] **Step 5: New illegal-transition check via REST**

Attempt `POST /tickets/{id}/accept` on a ticket already in `PICKED_UP` (from step 2) and confirm the response is `409 Conflict` with a message referencing `UnsupportedStateTransitionException`.

- [ ] **Step 6: Redelivery/idempotency spot-check**

Confirm `processed_events` still prevents double-processing: re-trigger a Kafka message kitchen-service already consumed (e.g. by resetting a consumer group offset or replaying via the existing project convention from prior sessions) and confirm no duplicate `Ticket` mutation occurs.

- [ ] **Step 7: Tear down**

Run: `docker compose down`
