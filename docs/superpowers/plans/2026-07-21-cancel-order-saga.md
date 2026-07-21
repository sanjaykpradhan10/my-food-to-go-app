# Cancel Order Saga Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve `Order.CANCEL_PENDING` by wiring a real cross-service Cancel Order saga (kitchen ticket cancellation, gated accounting authorization reversal) in both saga modes, matching this project's dual-mode-switchable pattern for Create Order saga.

**Architecture:** Sequential, kitchen-gates-accounting saga. `Order.cancel()` (already merged) triggers kitchen first; only if kitchen confirms the ticket cancellable does the saga proceed to reverse the accounting authorization. `Authorization` (accounting-service) becomes a DDD aggregate (`AUTHORIZED → REVERSED`). `SagaReply` and the shared `KitchenCommand`/new `AccountingCommand` wire records gain a `sagaType` field so `order-service`'s shared `saga.replies` listener can route between `CreateOrderSagaOrchestrator` and a new, stateless `CancelOrderSagaOrchestrator`.

**Tech Stack:** Java 17+, Spring Boot 3.5.16, Spring Data JPA, Spring Kafka, JUnit 5, Mockito, AssertJ.

## Global Constraints

- **Sequential saga, not parallel.** Accounting is never contacted unless kitchen confirms the ticket cancellable. If kitchen rejects (`TicketCannotBeCancelledException`/`UnsupportedStateTransitionException` from `Ticket.cancel()`), `Order.undoCancel()` fires immediately and no accounting call is ever made.
- **`TicketService.handleCancelTicketCommand`'s `"CancelTicket"` command is shared by both sagas** — Create Order saga's existing compensation path (`CreateOrderSagaOrchestrator.sendCancelTicket`) and this saga's primary flow. `KitchenCommand` gains a `sagaType` field so the handler can echo it back correctly in its reply; it cannot be inferred from context alone.
- **`accounting.commands` currently has no `commandType` discriminator at all** (`AuthorizeCardCommand` has no such field) — safe only because it has only ever carried one command type. Adding `ReverseAuthorization` requires renaming/generalizing it to `AccountingCommand(eventId, commandType, orderId, totalQuantity, sagaType)`, mirroring `KitchenCommand`'s existing shape.
- **No new join-instance table.** `CancelOrderSagaOrchestrator` is stateless — no `CancelOrderSagaInstance` — since this saga is a strict linear pipeline (kitchen replies, then conditionally accounting replies), unlike Create Order saga's parallel join.
- **`CreateOrderSagaOrchestrator.handleReply`'s signature and internal logic do not change.** Routing between the two orchestrators happens entirely in `order-service`'s `OrchestratorReplyListener`, based on `SagaReply.sagaType()`, before either orchestrator's `handleReply` is ever called.
- Follow this codebase's existing test conventions exactly: Mockito-mocked collaborators + AssertJ assertions, `verifyNoInteractions`/`never()` for negative-path assertions.

---

## Task 1: Wire format scaffolding — `sagaType` on `SagaReply`/`KitchenCommand`, generalize `AccountingCommand`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SagaReply.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/SagaReply.java`
- Modify: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/SagaReply.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaReply.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenCommand.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/KitchenCommand.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AuthorizeCardCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingCommand.java`
- Delete: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizeCardCommand.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AccountingCommand.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AuthorizeCardCommandListener.java`
- Modify: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java`

**Interfaces:**
- Produces: `SagaReply(eventId, participant, eventType, orderId, reason, sagaType)` (all 4 services); `KitchenCommand(eventId, commandType, orderId, totalQuantity, sagaType)` (order + kitchen); `AccountingCommand(eventId, commandType, orderId, totalQuantity, sagaType)` (order + accounting, replacing `AuthorizeCardCommand`). Consumed by every later task.

This task is pure wire-format scaffolding with no behavior change — every existing call site is updated to pass the literal `"CreateOrder"`. Verified by a full multi-module build, not new tests.

- [ ] **Step 1: Update all 4 `SagaReply` records**

Each service's copy becomes (only the package line differs):

```java
package com.sanjay.ftgo.order.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason, String sagaType) {
}
```

Apply the same change (package `com.sanjay.ftgo.kitchen.domain`, `com.sanjay.ftgo.consumer.domain`, `com.sanjay.ftgo.accounting.domain`) to the other 3 files.

- [ ] **Step 2: Update both `KitchenCommand` records**

`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenCommand.java` and `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/KitchenCommand.java` both become:

```java
package com.sanjay.ftgo.order.domain; // or com.sanjay.ftgo.kitchen.domain

public record KitchenCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType) {
}
```

- [ ] **Step 3: Delete `AuthorizeCardCommand.java` and create `AccountingCommand.java` in both order-service and accounting-service**

Delete `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AuthorizeCardCommand.java` and `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizeCardCommand.java`.

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingCommand.java`:

```java
package com.sanjay.ftgo.order.domain;

public record AccountingCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType) {
}
```

Create `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AccountingCommand.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public record AccountingCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType) {
}
```

- [ ] **Step 4: Update `CreateOrderSagaOrchestrator.java`'s 4 command-construction call sites**

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
        Order order = orderRepository.findById(instance.getOrderId()).orElse(null);
        if (order == null) {
            return;
        }
        if ("CardAuthorized".equals(eventType)) {
            approveOrder(order);
            String eventId = UUID.randomUUID().toString();
            publishCommand("kitchen.commands", eventId, "ConfirmTicket", instance.getOrderId(),
                    new KitchenCommand(eventId, "ConfirmTicket", instance.getOrderId(), null, "CreateOrder"));
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
                new AccountingCommand(eventId, "AuthorizeCard", instance.getOrderId(), instance.getTotalQuantity(), "CreateOrder"));
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
                new KitchenCommand(eventId, "CancelTicket", orderId, null, "CreateOrder"));
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

- [ ] **Step 5: Update `TicketService.java`'s `publishReply` and its 3 call sites**

In `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`, change:

```java
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
```

to:

```java
    @Transactional
    public void handleCreateTicketCommand(String eventId, Long orderId, Integer totalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (totalQuantity == null) {
            publishReply("TicketCreationFailed", orderId, "totalQuantity is required", "CreateOrder");
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            publishReply("TicketCreationFailed", orderId, "order exceeds kitchen capacity", "CreateOrder");
            return;
        }

        TicketCreationResult result = Ticket.createTicket(orderId, totalQuantity);
        ticketRepository.save(result.ticket());
        publishReply("TicketCreated", orderId, null, "CreateOrder");
    }
```

And change:

```java
    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "kitchen", eventType, orderId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }
```

to:

```java
    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "kitchen", eventType, orderId, reason, sagaType);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }
```

Leave `handleCancelTicketCommand` and `handleConfirmTicketCommand` untouched — Task 2 rewrites `handleCancelTicketCommand`.

- [ ] **Step 6: Update `SagaJoinService.java`'s `publishReply`**

In `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`, change:

```java
        if (totalQuantity == null) {
            publishReply("CardAuthorizationFailed", orderId, "order quantity is missing");
            return;
        }
```

to:

```java
        if (totalQuantity == null) {
            publishReply("CardAuthorizationFailed", orderId, "order quantity is missing", "CreateOrder");
            return;
        }
```

And change:

```java
    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "accounting", eventType, orderId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }
```

to:

```java
    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "accounting", eventType, orderId, reason, sagaType);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }
```

- [ ] **Step 7: Update `AuthorizeCardCommandListener.java` to deserialize `AccountingCommand`**

Replace the full contents of `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AuthorizeCardCommandListener.java`:

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AccountingCommand;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class AuthorizeCardCommandListener {

    private static final Logger log = LoggerFactory.getLogger(AuthorizeCardCommandListener.class);

    private final SagaJoinService sagaJoinService;
    private final ObjectMapper objectMapper;

    public AuthorizeCardCommandListener(SagaJoinService sagaJoinService, ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.commands", groupId = "accounting-service")
    public void onMessage(String payload) {
        AccountingCommand command;
        try {
            command = objectMapper.readValue(payload, AccountingCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting command: {}", payload, e);
            return;
        }
        sagaJoinService.handleAuthorizeCardCommand(command.eventId(), command.orderId(), command.totalQuantity());
    }
}
```

(This is deliberately unchanged in dispatch shape — no `commandType` switch yet, since only `"AuthorizeCard"` exists until Task 4. Renaming the class file itself to `AccountingCommandListener` happens in Task 4, alongside adding the switch, to keep this step's diff minimal.)

- [ ] **Step 8: Update `ConsumerVerificationService.java`'s `publishReply`**

In `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java`, change:

```java
        publishReply(eventType, orderId, result.reason());
```

to:

```java
        publishReply(eventType, orderId, result.reason(), "CreateOrder");
```

And change:

```java
    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "consumer", eventType, orderId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }
```

to:

```java
    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "consumer", eventType, orderId, reason, sagaType);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }
```

- [ ] **Step 9: Run the full multi-module build**

Run: `./gradlew clean build`
Expected: `BUILD SUCCESSFUL` across all modules — no test file constructs `SagaReply`/`KitchenCommand`/`AuthorizeCardCommand` directly, so no test changes are needed for this purely mechanical step.

- [ ] **Step 10: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SagaReply.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/SagaReply.java \
        ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/SagaReply.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaReply.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenCommand.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/KitchenCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingCommand.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AccountingCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AuthorizeCardCommandListener.java \
        ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java
git rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AuthorizeCardCommand.java \
       ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizeCardCommand.java
git commit -m "feat: add sagaType to SagaReply/KitchenCommand and generalize accounting.commands wire format"
```

---

## Task 2: Kitchen-service — fix `handleCancelTicketCommand`, add `handleOrderCancelled`

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCancellationRejectedEvent.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEvent.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListener.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisherTest.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListenerTest.java`

**Interfaces:**
- Consumes: `KitchenCommand.sagaType()` (Task 1), `Ticket.cancel()` (existing, returns `List<TicketDomainEvent>` or throws `TicketCannotBeCancelledException`/`UnsupportedStateTransitionException`).
- Produces: `TicketService.handleCancelTicketCommand(String eventId, Long orderId, String sagaType)` (signature change — now publishes a reply) and `TicketService.handleOrderCancelled(String eventId, Long orderId)` (new) — consumed by `KitchenCommandListener` and `OrderEventListener` respectively.

- [ ] **Step 1: Write the failing tests**

Create `TicketCancellationRejectedEvent.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

public record TicketCancellationRejectedEvent(Long orderId, String reason) implements TicketDomainEvent {
}
```

Update `TicketDomainEvent.java`'s permits clause:

```java
package com.sanjay.ftgo.kitchen.domain;

public sealed interface TicketDomainEvent
        permits TicketCreatedEvent, TicketCreationFailedEvent, TicketConfirmedEvent, TicketCancelledEvent,
                TicketCancellationRejectedEvent, TicketAcceptedEvent, TicketPreparingStartedEvent,
                TicketReadyForPickupEvent, TicketPickedUpEvent {

    Long orderId();
}
```

Add to `TicketDomainEventPublisherTest.java` (append inside the class):

```java
    @Test
    void publishesTicketCancellationRejectedWithReason() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        publisher.publish(ticket, List.of(new TicketCancellationRejectedEvent(42L, "cannot cancel once ready for pickup")));

        verify(outboxEventRepository).save(argThat(row ->
                "TicketCancellationRejected".equals(row.getEventType())
                        && "kitchen.events".equals(row.getTopic())
                        && row.getPayload().contains("cannot cancel once ready for pickup")));
    }
```

Replace `TicketServiceTest.java`'s existing `cancelsTicketViaCommand` test and append new tests (full replacement of that one test plus 3 new ones — insert after the last existing test, replacing the old `cancelsTicketViaCommand`):

```java
    @Test
    void cancelsTicketViaCommandAndRepliesTicketCancelled() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-4")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleCancelTicketCommand("cmd-4", 42L, "CreateOrder");

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCancelled".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"CreateOrder\"")));
    }

    @Test
    void repliesTicketCancellationRejectedWhenTicketCannotBeCancelled() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        ticket.preparing();
        ticket.readyForPickup();
        when(processedEventRepository.existsById("cmd-5")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleCancelTicketCommand("cmd-5", 42L, "CancelOrder");

        assertThat(ticket.getState()).isEqualTo(TicketState.READY_FOR_PICKUP);
        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCancellationRejected".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"CancelOrder\"")));
    }

    @Test
    void handlesOrderCancelledChoreographyAndPublishesTicketCancelled() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCancelled("evt-1", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancelledEvent));
    }

    @Test
    void handlesOrderCancelledChoreographyRejectionWithoutMutatingTicket() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        ticket.preparing();
        ticket.readyForPickup();
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleOrderCancelled("evt-2", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.READY_FOR_PICKUP);
        verify(ticketRepository, never()).save(any());
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancellationRejectedEvent));
    }
```

Add `import java.time.ZonedDateTime;` to `TicketServiceTest.java`'s import list (not already present).

Add to `OrderEventListenerTest.java` (kitchen-service, append inside the class):

```java
    @Test
    void handlesOrderCancelledEvent() {
        String payload = """
                {"eventId":"e3","eventType":"OrderCancelled","orderId":42}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleOrderCancelled("e3", 42L);
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest" --tests "com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisherTest" --tests "com.sanjay.ftgo.kitchen.infrastructure.OrderEventListenerTest"`
Expected: Compilation failure — `TicketCancellationRejectedEvent` doesn't exist yet, `handleCancelTicketCommand` doesn't accept a 3rd parameter, `handleOrderCancelled` doesn't exist.

- [ ] **Step 3: Update `TicketDomainEventPublisher.java`'s switch**

Add this case to the `toWireEvent` switch in `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java` (alongside the existing 8 cases, before the closing `};`):

```java
            case TicketCancellationRejectedEvent e ->
                    new KitchenEvent(eventId, "TicketCancellationRejected", e.orderId(), ticketId, null, e.reason());
```

- [ ] **Step 4: Rewrite `TicketService.java`'s `handleCancelTicketCommand` and add `handleOrderCancelled`**

Replace:

```java
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
```

with:

```java
    @Transactional
    public void handleCancelTicketCommand(String eventId, Long orderId, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }
        try {
            ticket.cancel();
            ticketRepository.save(ticket);
            publishReply("TicketCancelled", orderId, null, sagaType);
        } catch (TicketCannotBeCancelledException | UnsupportedStateTransitionException e) {
            publishReply("TicketCancellationRejected", orderId, e.getMessage(), sagaType);
        }
    }

    @Transactional
    public void handleOrderCancelled(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }
        try {
            List<TicketDomainEvent> events = ticket.cancel();
            ticketRepository.save(ticket);
            domainEventPublisher.publish(ticket, events);
        } catch (TicketCannotBeCancelledException | UnsupportedStateTransitionException e) {
            domainEventPublisher.publish(ticket, List.of(new TicketCancellationRejectedEvent(orderId, e.getMessage())));
        }
    }
```

- [ ] **Step 5: Update `KitchenCommandListener.java`'s `"CancelTicket"` dispatch**

In `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListener.java`, change:

```java
            case "CancelTicket" -> ticketService.handleCancelTicketCommand(command.eventId(), command.orderId());
```

to:

```java
            case "CancelTicket" -> ticketService.handleCancelTicketCommand(command.eventId(), command.orderId(), command.sagaType());
```

- [ ] **Step 6: Update `OrderEventListener.java`'s dispatch**

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
        // order-service's order.events topic carries every Order lifecycle event; this
        // listener only reacts to the two that concern the kitchen ticket lifecycle.
        // OrderCreatedEvent's fields are reused for OrderCancelled too — we only read
        // eventId/orderId from it in that case, which deserialize fine regardless.
        switch (event.eventType()) {
            case "OrderCreated" -> ticketService.handleOrderCreated(event);
            case "OrderCancelled" -> ticketService.handleOrderCancelled(event.eventId(), event.orderId());
            default -> { }
        }
    }
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: `BUILD SUCCESSFUL`, all tests pass (existing + new).

- [ ] **Step 8: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketCancellationRejectedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisher.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListener.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketDomainEventPublisherTest.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListenerTest.java
git commit -m "fix(kitchen-service): handleCancelTicketCommand replies instead of silently propagating; add handleOrderCancelled"
```

---

## Task 3: Accounting-service — `Authorization` DDD aggregate + publisher

**Files:**
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/Authorization.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationStatus.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/CardAuthorizedEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/CardAuthorizationDeclinedEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationReversedEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationResult.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/UnsupportedStateTransitionException.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisher.java`
- Create: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationTest.java`
- Create: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisherTest.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java` (minimal compile-fix only — full rewiring is Task 4)
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java` (minimal compile-fix only — the 5 `"AUTHORIZED"/"DECLINED"`-string assertions must be updated since `getStatus()` now returns an enum, not a String)

**Interfaces:**
- Produces: `Authorization.authorize(orderId)` / `Authorization.decline(orderId, reason)` → `AuthorizationResult(Authorization authorization, List<AuthorizationDomainEvent> events)`; `Authorization.reverse()` → `List<AuthorizationDomainEvent>`; `AuthorizationDomainEventPublisher.publish(List<AuthorizationDomainEvent>)`. Consumed by Task 4.

- [ ] **Step 1: Write the failing test suites**

Create `AuthorizationStatus.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public enum AuthorizationStatus {
    AUTHORIZED,
    DECLINED,
    REVERSED
}
```

Create `AuthorizationDomainEvent.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public sealed interface AuthorizationDomainEvent
        permits CardAuthorizedEvent, CardAuthorizationDeclinedEvent, AuthorizationReversedEvent {

    Long orderId();
}
```

Create `CardAuthorizedEvent.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public record CardAuthorizedEvent(Long orderId) implements AuthorizationDomainEvent {
}
```

Create `CardAuthorizationDeclinedEvent.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public record CardAuthorizationDeclinedEvent(Long orderId, String reason) implements AuthorizationDomainEvent {
}
```

Create `AuthorizationReversedEvent.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public record AuthorizationReversedEvent(Long orderId) implements AuthorizationDomainEvent {
}
```

Create `AuthorizationResult.java`:

```java
package com.sanjay.ftgo.accounting.domain;

import java.util.List;

public record AuthorizationResult(Authorization authorization, List<AuthorizationDomainEvent> events) {
}
```

Create `UnsupportedStateTransitionException.java`:

```java
package com.sanjay.ftgo.accounting.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(AuthorizationStatus status) {
        super("Unsupported transition from status " + status);
    }
}
```

Create `AuthorizationTest.java`:

```java
package com.sanjay.ftgo.accounting.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthorizationTest {

    @Test
    void authorizeStartsInAuthorizedAndEmitsCardAuthorized() {
        AuthorizationResult result = Authorization.authorize(42L);

        assertThat(result.authorization().getStatus()).isEqualTo(AuthorizationStatus.AUTHORIZED);
        assertThat(result.authorization().getOrderId()).isEqualTo(42L);
        assertThat(result.events()).containsExactly(new CardAuthorizedEvent(42L));
    }

    @Test
    void declineStartsInDeclinedAndEmitsCardAuthorizationDeclined() {
        AuthorizationResult result = Authorization.decline(42L, "order quantity exceeds authorization limit");

        assertThat(result.authorization().getStatus()).isEqualTo(AuthorizationStatus.DECLINED);
        assertThat(result.events()).containsExactly(
                new CardAuthorizationDeclinedEvent(42L, "order quantity exceeds authorization limit"));
    }

    @Test
    void reverseMovesFromAuthorizedToReversed() {
        Authorization authorization = Authorization.authorize(42L).authorization();

        List<AuthorizationDomainEvent> events = authorization.reverse();

        assertThat(authorization.getStatus()).isEqualTo(AuthorizationStatus.REVERSED);
        assertThat(events).containsExactly(new AuthorizationReversedEvent(42L));
    }

    @Test
    void reverseFromDeclinedThrows() {
        Authorization authorization = Authorization.decline(42L, "reason").authorization();

        assertThatThrownBy(authorization::reverse).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reverseFromAlreadyReversedThrows() {
        Authorization authorization = Authorization.authorize(42L).authorization();
        authorization.reverse();

        assertThatThrownBy(authorization::reverse).isInstanceOf(UnsupportedStateTransitionException.class);
    }
}
```

Create `AuthorizationDomainEventPublisherTest.java`:

```java
package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AuthorizationDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final AuthorizationDomainEventPublisher publisher =
            new AuthorizationDomainEventPublisher(outboxEventRepository, new ObjectMapper());

    @Test
    void publishesCardAuthorized() {
        publisher.publish(List.of(new CardAuthorizedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "CardAuthorized".equals(row.getEventType())
                        && "accounting.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesCardAuthorizationDeclinedWithReason() {
        publisher.publish(List.of(new CardAuthorizationDeclinedEvent(42L, "order quantity exceeds authorization limit")));

        verify(outboxEventRepository).save(argThat(row ->
                "CardAuthorizationFailed".equals(row.getEventType())
                        && row.getPayload().contains("order quantity exceeds authorization limit")));
    }

    @Test
    void publishesAuthorizationReversed() {
        publisher.publish(List.of(new AuthorizationReversedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "AuthorizationReversed".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationTest" --tests "com.sanjay.ftgo.accounting.domain.AuthorizationDomainEventPublisherTest"`
Expected: Compilation failure — `Authorization.authorize`/`decline`/`reverse` and `AuthorizationDomainEventPublisher` don't exist yet.

- [ ] **Step 3: Rewrite `Authorization.java`**

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

    protected Authorization() {
    }

    private Authorization(Long orderId, AuthorizationStatus status) {
        this.orderId = orderId;
        this.status = status;
    }

    public static AuthorizationResult authorize(Long orderId) {
        Authorization authorization = new Authorization(orderId, AuthorizationStatus.AUTHORIZED);
        return new AuthorizationResult(authorization, List.of(new CardAuthorizedEvent(orderId)));
    }

    public static AuthorizationResult decline(Long orderId, String reason) {
        Authorization authorization = new Authorization(orderId, AuthorizationStatus.DECLINED);
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

    public List<AuthorizationDomainEvent> reverse() {
        if (status != AuthorizationStatus.AUTHORIZED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = AuthorizationStatus.REVERSED;
        return List.of(new AuthorizationReversedEvent(orderId));
    }
}
```

- [ ] **Step 4: Create `AuthorizationDomainEventPublisher.java`**

```java
package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class AuthorizationDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public AuthorizationDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publish(List<AuthorizationDomainEvent> events) {
        events.forEach(this::publishEvent);
    }

    private void publishEvent(AuthorizationDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        AccountingEvent wireEvent = toWireEvent(eventId, event);
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "accounting.events", toJson(wireEvent)));
    }

    private AccountingEvent toWireEvent(String eventId, AuthorizationDomainEvent event) {
        return switch (event) {
            case CardAuthorizedEvent e -> new AccountingEvent(eventId, "CardAuthorized", e.orderId(), null);
            case CardAuthorizationDeclinedEvent e ->
                    new AccountingEvent(eventId, "CardAuthorizationFailed", e.orderId(), e.reason());
            case AuthorizationReversedEvent e -> new AccountingEvent(eventId, "AuthorizationReversed", e.orderId(), null);
        };
    }

    private String toJson(AccountingEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize accounting event " + event.eventType(), e);
        }
    }
}
```

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationTest" --tests "com.sanjay.ftgo.accounting.domain.AuthorizationDomainEventPublisherTest"`
Expected: `BUILD SUCCESSFUL`, all 8 tests pass.

- [ ] **Step 6: Apply the minimal compile-fix to `SagaJoinService.java`**

`Authorization`'s constructor is now private, so `SagaJoinService`'s two direct `new Authorization(orderId, "AUTHORIZED"/"DECLINED")` constructions no longer compile. Change:

```java
        boolean authorized = isAuthorized(totalQuantity);
        authorizationRepository.save(new Authorization(orderId, authorized ? "AUTHORIZED" : "DECLINED"));

        if (authorized) {
            publishReply("CardAuthorized", orderId, null, "CreateOrder");
        } else {
            publishReply("CardAuthorizationFailed", orderId, "order quantity exceeds authorization limit", "CreateOrder");
        }
```

to:

```java
        boolean authorized = isAuthorized(totalQuantity);
        Authorization authorization = authorized
                ? Authorization.authorize(orderId).authorization()
                : Authorization.decline(orderId, "order quantity exceeds authorization limit").authorization();
        authorizationRepository.save(authorization);

        if (authorized) {
            publishReply("CardAuthorized", orderId, null, "CreateOrder");
        } else {
            publishReply("CardAuthorizationFailed", orderId, "order quantity exceeds authorization limit", "CreateOrder");
        }
```

And change:

```java
        boolean authorized = isAuthorized(state.getTotalQuantity());
        authorizationRepository.save(new Authorization(state.getOrderId(), authorized ? "AUTHORIZED" : "DECLINED"));
```

to:

```java
        boolean authorized = isAuthorized(state.getTotalQuantity());
        Authorization authorization = authorized
                ? Authorization.authorize(state.getOrderId()).authorization()
                : Authorization.decline(state.getOrderId(), "order quantity exceeds authorization limit").authorization();
        authorizationRepository.save(authorization);
```

(This is a mechanical, behavior-preserving swap — `SagaJoinService`'s publishing logic is fully rewired to the new `AuthorizationDomainEventPublisher` in Task 4, not here.)

- [ ] **Step 7: Fix `SagaJoinServiceTest.java`'s 5 status-string assertions**

`getStatus()` now returns `AuthorizationStatus`, not `String`. Change all 5 occurrences of the pattern `"AUTHORIZED".equals(a.getStatus())` / `"DECLINED".equals(a.getStatus())` to enum comparisons:

- Line with `verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));` (appears twice, in `authorizesWhenConsumerVerifiedArrivesFirstThenTicketCreatedUnderLimit` and `authorizesWhenTicketCreatedArrivesFirstThenConsumerVerified`) → `verify(authorizationRepository).save(argThat(a -> a.getStatus() == AuthorizationStatus.AUTHORIZED));`
- Line with `verify(authorizationRepository).save(argThat(a -> "DECLINED".equals(a.getStatus())));` (in `declinesWhenTotalQuantityExceedsLimit`) → `verify(authorizationRepository).save(argThat(a -> a.getStatus() == AuthorizationStatus.DECLINED));`
- Line with `verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));` (in `authorizesViaCommandWhenWithinLimit`) → same enum-comparison fix
- Line with `verify(authorizationRepository).save(argThat(a -> "DECLINED".equals(a.getStatus())));` (in `declinesViaCommandWhenOverLimit`) → same enum-comparison fix

- [ ] **Step 8: Run the full accounting-service test suite**

Run: `./gradlew :ftgo-accounting-service:test`
Expected: `BUILD SUCCESSFUL` — all existing `SagaJoinServiceTest` cases pass with the corrected assertions, plus the new `AuthorizationTest`/`AuthorizationDomainEventPublisherTest`.

- [ ] **Step 9: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/Authorization.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationStatus.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/CardAuthorizedEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/CardAuthorizationDeclinedEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationReversedEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationResult.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/UnsupportedStateTransitionException.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisher.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationTest.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationDomainEventPublisherTest.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java
git commit -m "feat(accounting-service): make Authorization a real DDD aggregate with enforced state transitions"
```

---

## Task 4: Accounting-service — wire `Authorization` into `SagaJoinService`, add `AuthorizationCancelService`, extend listeners

**Files:**
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelService.java`
- Create: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelServiceTest.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListenerTest.java` (new file — none exists yet)
- Rename: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AuthorizeCardCommandListener.java` → `AccountingCommandListener.java`
- Create: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListenerTest.java` (new file)

**Interfaces:**
- Consumes: `Authorization.authorize`/`decline`/`reverse` (Task 3), `AuthorizationDomainEventPublisher.publish` (Task 3), `AuthorizationRepository` (existing).
- Produces: `AuthorizationCancelService.reverse(String eventId, Long orderId, String sagaType)` — consumed by both the extended `KitchenEventListener` (choreography) and the new command path (orchestration).

- [ ] **Step 1: Write the failing tests**

Create `AuthorizationCancelServiceTest.java`:

```java
package com.sanjay.ftgo.accounting.domain;

import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuthorizationCancelServiceTest {

    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final AuthorizationDomainEventPublisher domainEventPublisher = mock(AuthorizationDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AuthorizationCancelService service = new AuthorizationCancelService(
            authorizationRepository, processedEventRepository, domainEventPublisher);

    @Test
    void reversesAnAuthorizedAuthorization() {
        Authorization authorization = Authorization.authorize(42L).authorization();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reverse("e1", 42L, "CancelOrder");

        assertThat(authorization.getStatus()).isEqualTo(AuthorizationStatus.REVERSED);
        verify(domainEventPublisher).publish(java.util.List.of(new AuthorizationReversedEvent(42L)));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.reverse("e1", 42L, "CancelOrder");

        verify(authorizationRepository, never()).findByOrderId(any());
    }

    @Test
    void doesNothingWhenNoAuthorizationExistsForOrder() {
        when(processedEventRepository.existsById("e2")).thenReturn(false);
        when(authorizationRepository.findByOrderId(43L)).thenReturn(Optional.empty());

        service.reverse("e2", 43L, "CancelOrder");

        verify(authorizationRepository, never()).save(any());
    }
}
```

Note: `AuthorizationRepository` needs a `findByOrderId` method — this is a required addition (the existing repository only has `JpaRepository`'s generic `findById`, keyed by the auto-generated primary key, not `orderId`).

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationCancelServiceTest"`
Expected: Compilation failure — `AuthorizationCancelService` and `AuthorizationRepository.findByOrderId` don't exist yet.

- [ ] **Step 3: Add `findByOrderId` to `AuthorizationRepository.java`**

Replace the full contents of `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationRepository.java`:

```java
package com.sanjay.ftgo.accounting.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AuthorizationRepository extends JpaRepository<Authorization, Long> {

    Optional<Authorization> findByOrderId(Long orderId);
}
```

- [ ] **Step 4: Create `AuthorizationCancelService.java`**

```java
package com.sanjay.ftgo.accounting.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class AuthorizationCancelService {

    private final AuthorizationRepository authorizationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final AuthorizationDomainEventPublisher domainEventPublisher;

    public AuthorizationCancelService(AuthorizationRepository authorizationRepository,
                                       ProcessedEventRepository processedEventRepository,
                                       AuthorizationDomainEventPublisher domainEventPublisher) {
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Transactional
    public void reverse(String eventId, Long orderId, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null) {
            return;
        }
        List<AuthorizationDomainEvent> events = authorization.reverse();
        authorizationRepository.save(authorization);
        domainEventPublisher.publish(events);
    }
}
```

(`sagaType` is accepted but not used to branch logic — accounting only ever reverses on request, regardless of which mode requested it; the parameter exists so both call sites in Task 4's remaining steps have a uniform method signature. `reverse()` is only ever called after kitchen has already confirmed cancellable, so `UnsupportedStateTransitionException` from an already-`REVERSED`/`DECLINED` authorization is a genuine bug, not a race to silently swallow — it's allowed to propagate, matching how this codebase treats truly-unexpected states elsewhere only when no legitimate race can cause them.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.AuthorizationCancelServiceTest"`
Expected: `BUILD SUCCESSFUL`, all 3 tests pass.

- [ ] **Step 6: Fully rewire `SagaJoinService.java` to use the publisher**

Replace the full contents of `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`:

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

import java.util.UUID;

@Service
public class SagaJoinService {

    private static final int AUTHORIZATION_QUANTITY_LIMIT = 10;

    private final SagaJoinStateRepository sagaJoinStateRepository;
    private final AuthorizationRepository authorizationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final AuthorizationDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public SagaJoinService(SagaJoinStateRepository sagaJoinStateRepository,
                            AuthorizationRepository authorizationRepository,
                            ProcessedEventRepository processedEventRepository,
                            OutboxEventRepository outboxEventRepository,
                            AuthorizationDomainEventPublisher domainEventPublisher,
                            ObjectMapper objectMapper) {
        this.sagaJoinStateRepository = sagaJoinStateRepository;
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleConsumerEvent(String eventId, Long orderId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        SagaJoinState state = sagaJoinStateRepository.findById(orderId).orElseGet(() -> new SagaJoinState(orderId));
        if (state.isResolved() || state.isFailed()) {
            return;
        }

        if ("ConsumerVerificationFailed".equals(eventType)) {
            state.markFailed();
            sagaJoinStateRepository.save(state);
            return;
        }

        state.markConsumerVerified();
        sagaJoinStateRepository.save(state);
        tryResolve(state);
    }

    @Transactional
    public void handleKitchenEvent(String eventId, Long orderId, String eventType, Integer totalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        SagaJoinState state = sagaJoinStateRepository.findById(orderId).orElseGet(() -> new SagaJoinState(orderId));
        if (state.isResolved() || state.isFailed()) {
            return;
        }

        if ("TicketCreationFailed".equals(eventType)) {
            state.markFailed();
            sagaJoinStateRepository.save(state);
            return;
        }

        state.markTicketCreated(totalQuantity);
        sagaJoinStateRepository.save(state);
        tryResolve(state);
    }

    @Transactional
    public void handleAuthorizeCardCommand(String eventId, Long orderId, Integer totalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (totalQuantity == null) {
            publishReply("CardAuthorizationFailed", orderId, "order quantity is missing", "CreateOrder");
            return;
        }

        boolean authorized = isAuthorized(totalQuantity);
        AuthorizationResult result = authorized
                ? Authorization.authorize(orderId)
                : Authorization.decline(orderId, "order quantity exceeds authorization limit");
        authorizationRepository.save(result.authorization());

        if (authorized) {
            publishReply("CardAuthorized", orderId, null, "CreateOrder");
        } else {
            publishReply("CardAuthorizationFailed", orderId, "order quantity exceeds authorization limit", "CreateOrder");
        }
    }

    private void tryResolve(SagaJoinState state) {
        if (!state.isConsumerVerified() || !state.isTicketCreated()) {
            return;
        }
        state.markResolved();
        sagaJoinStateRepository.save(state);

        boolean authorized = isAuthorized(state.getTotalQuantity());
        AuthorizationResult result = authorized
                ? Authorization.authorize(state.getOrderId())
                : Authorization.decline(state.getOrderId(), "order quantity exceeds authorization limit");
        authorizationRepository.save(result.authorization());
        domainEventPublisher.publish(result.events());
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

Note: `handleAuthorizeCardCommand` (the orchestration command path) still publishes its reply on `saga.replies` directly, unchanged in shape — only `tryResolve` (the choreography join path, which publishes to `accounting.events`) is rewired to `AuthorizationDomainEventPublisher`. This matches the existing split: `saga.replies` replies are saga-command replies, `accounting.events` is the choreography broadcast topic.

- [ ] **Step 7: Update `SagaJoinServiceTest.java`'s constructor call and event-based assertions**

Change the field/constructor block:

```java
    private final SagaJoinStateRepository sagaJoinStateRepository = mock(SagaJoinStateRepository.class);
    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final SagaJoinService service = new SagaJoinService(
            sagaJoinStateRepository, authorizationRepository, processedEventRepository, outboxEventRepository, objectMapper);
```

to:

```java
    private final SagaJoinStateRepository sagaJoinStateRepository = mock(SagaJoinStateRepository.class);
    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final AuthorizationDomainEventPublisher domainEventPublisher = mock(AuthorizationDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final SagaJoinService service = new SagaJoinService(
            sagaJoinStateRepository, authorizationRepository, processedEventRepository, outboxEventRepository,
            domainEventPublisher, objectMapper);
```

In the two tests that verify the choreography join path (`authorizesWhenConsumerVerifiedArrivesFirstThenTicketCreatedUnderLimit`, `authorizesWhenTicketCreatedArrivesFirstThenConsumerVerified`, `declinesWhenTotalQuantityExceedsLimit`), the assertion `verify(outboxEventRepository).save(argThat(e -> "CardAuthorized".equals(e.getEventType())));` (and the `"CardAuthorizationFailed"` equivalent) no longer holds — `tryResolve` now publishes via `domainEventPublisher`, not `outboxEventRepository` directly. Replace those 3 lines:

`verify(outboxEventRepository).save(argThat(e -> "CardAuthorized".equals(e.getEventType())));` → `verify(domainEventPublisher).publish(java.util.List.of(new CardAuthorizedEvent(42L)));`

`verify(outboxEventRepository).save(argThat(e -> "CardAuthorizationFailed".equals(e.getEventType())));` (in `declinesWhenTotalQuantityExceedsLimit`) → `verify(domainEventPublisher).publish(java.util.List.of(new CardAuthorizationDeclinedEvent(42L, "order quantity exceeds authorization limit")));`

The two `abandonsJoin*` tests' `verify(outboxEventRepository, never()).save(any());` assertions should additionally verify the publisher had no interactions — add `verifyNoInteractions(domainEventPublisher);` (requires `import static org.mockito.Mockito.verifyNoInteractions;`) to both `abandonsJoinWhenConsumerVerificationFails` and `abandonsJoinWhenTicketCreationFails`.

`ignoresLateDuplicateEventAfterJoinAlreadyResolved`'s `verify(outboxEventRepository, times(1)).save(any());` becomes `verify(domainEventPublisher, times(1)).publish(any());` (needs `import static org.mockito.ArgumentMatchers.any;` already present).

The 4 `handleAuthorizeCardCommand`-based tests (`authorizesViaCommandWhenWithinLimit`, `declinesViaCommandWhenOverLimit`, `skipsDuplicateCommandDelivery`, `declineViaCommandWhenQuantityIsNull`) are unaffected — that path still uses `outboxEventRepository`/`publishReply` directly, unchanged.

- [ ] **Step 8: Extend `KitchenEventListener.java`**

Replace the full contents of `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java`:

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.KitchenEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final SagaJoinService sagaJoinService;
    private final AuthorizationCancelService authorizationCancelService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(SagaJoinService sagaJoinService,
                                 AuthorizationCancelService authorizationCancelService,
                                 ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.authorizationCancelService = authorizationCancelService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "accounting-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "TicketCreated", "TicketCreationFailed" ->
                    sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
            case "TicketCancelled" -> authorizationCancelService.reverse(event.eventId(), event.orderId(), "CancelOrder");
            default -> { }
        }
    }
}
```

(Replaces the old `Set<String> RELEVANT_EVENT_TYPES` early-return guard with an explicit switch, since there are now 3 relevant types across 2 different handlers rather than 2 types feeding one handler.)

- [ ] **Step 9: Write `KitchenEventListenerTest.java` (accounting-service, new file)**

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class KitchenEventListenerTest {

    private final SagaJoinService sagaJoinService = mock(SagaJoinService.class);
    private final AuthorizationCancelService authorizationCancelService = mock(AuthorizationCancelService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KitchenEventListener listener =
            new KitchenEventListener(sagaJoinService, authorizationCancelService, objectMapper);

    @Test
    void routesTicketCreatedToSagaJoinService() {
        String payload = """
                {"eventId":"e1","eventType":"TicketCreated","orderId":42,"ticketId":1,"totalQuantity":5,"reason":null}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleKitchenEvent("e1", 42L, "TicketCreated", 5);
        verify(authorizationCancelService, never()).reverse(any(), any(), any());
    }

    @Test
    void routesTicketCancelledToAuthorizationCancelService() {
        String payload = """
                {"eventId":"e2","eventType":"TicketCancelled","orderId":42,"ticketId":1,"totalQuantity":null,"reason":null}
                """;

        listener.onMessage(payload);

        verify(authorizationCancelService).reverse("e2", 42L, "CancelOrder");
        verify(sagaJoinService, never()).handleKitchenEvent(any(), any(), any(), any());
    }

    @Test
    void ignoresIrrelevantEventTypes() {
        String payload = """
                {"eventId":"e3","eventType":"TicketAccepted","orderId":42,"ticketId":1,"totalQuantity":null,"reason":null}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService, never()).handleKitchenEvent(any(), any(), any(), any());
        verify(authorizationCancelService, never()).reverse(any(), any(), any());
    }
}
```

- [ ] **Step 10: Rename `AuthorizeCardCommandListener.java` to `AccountingCommandListener.java` with a `commandType` switch**

Delete `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AuthorizeCardCommandListener.java`, create `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListener.java`:

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AccountingCommand;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class AccountingCommandListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingCommandListener.class);

    private final SagaJoinService sagaJoinService;
    private final AuthorizationCancelService authorizationCancelService;
    private final ObjectMapper objectMapper;

    public AccountingCommandListener(SagaJoinService sagaJoinService,
                                      AuthorizationCancelService authorizationCancelService,
                                      ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.authorizationCancelService = authorizationCancelService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.commands", groupId = "accounting-service")
    public void onMessage(String payload) {
        AccountingCommand command;
        try {
            command = objectMapper.readValue(payload, AccountingCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting command: {}", payload, e);
            return;
        }
        switch (command.commandType()) {
            case "AuthorizeCard" ->
                    sagaJoinService.handleAuthorizeCardCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ReverseAuthorization" ->
                    authorizationCancelService.reverse(command.eventId(), command.orderId(), command.sagaType());
            default -> log.warn("Unknown accounting command type: {}", command.commandType());
        }
    }
}
```

- [ ] **Step 11: Write `AccountingCommandListenerTest.java` (new file)**

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AccountingCommandListenerTest {

    private final SagaJoinService sagaJoinService = mock(SagaJoinService.class);
    private final AuthorizationCancelService authorizationCancelService = mock(AuthorizationCancelService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AccountingCommandListener listener =
            new AccountingCommandListener(sagaJoinService, authorizationCancelService, objectMapper);

    @Test
    void routesAuthorizeCardToSagaJoinService() {
        String payload = """
                {"eventId":"e1","commandType":"AuthorizeCard","orderId":42,"totalQuantity":5,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleAuthorizeCardCommand("e1", 42L, 5);
        verify(authorizationCancelService, never()).reverse(any(), any(), any());
    }

    @Test
    void routesReverseAuthorizationToAuthorizationCancelService() {
        String payload = """
                {"eventId":"e2","commandType":"ReverseAuthorization","orderId":42,"totalQuantity":null,"sagaType":"CancelOrder"}
                """;

        listener.onMessage(payload);

        verify(authorizationCancelService).reverse("e2", 42L, "CancelOrder");
        verify(sagaJoinService, never()).handleAuthorizeCardCommand(any(), any(), any());
    }
}
```

- [ ] **Step 12: Run the full accounting-service test suite**

Run: `./gradlew :ftgo-accounting-service:test`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 13: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelService.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelServiceTest.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationRepository.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListenerTest.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListener.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/AccountingCommandListenerTest.java
git rm ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AuthorizeCardCommandListener.java
git commit -m "feat(accounting-service): wire Authorization aggregate into SagaJoinService, add reversal path for Cancel Order saga"
```

---

## Task 5: Order-service — `OrderCancelSagaService` (choreography) and extended listeners

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelSagaService.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderCancelSagaServiceTest.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListenerTest.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListenerTest.java`

**Interfaces:**
- Consumes: `Order.noteCancelled()`/`undoCancel()` (existing, from PR #12), `OrderDomainEventPublisher.publish` (existing).
- Produces: `OrderCancelSagaService.confirmCancel(Long orderId, String eventId)` / `rejectCancel(Long orderId, String eventId)` — consumed by the two extended listeners.

- [ ] **Step 1: Write the failing test**

Create `OrderCancelSagaServiceTest.java`:

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

class OrderCancelSagaServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final OrderCancelSagaService service =
            new OrderCancelSagaService(orderRepository, processedEventRepository, domainEventPublisher);

    private Order cancelPendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
    }

    @Test
    void confirmCancelMovesOrderToCancelled() {
        Order order = cancelPendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.confirmCancel(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(orderRepository).save(order);
        verify(domainEventPublisher).publish(List.of(new OrderCancelConfirmedEvent(42L)));
    }

    @Test
    void rejectCancelMovesOrderBackToApproved() {
        Order order = cancelPendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.rejectCancel(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(orderRepository).save(order);
        verify(domainEventPublisher).publish(List.of(new OrderCancelRejectedEvent(42L)));
    }

    @Test
    void ignoresConfirmCancelForAnAlreadyResolvedOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCELLED);
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.confirmCancel(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(orderRepository, never()).save(any());
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.confirmCancel(42L, "e1");

        verify(orderRepository, never()).findById(any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderCancelSagaServiceTest"`
Expected: Compilation failure — `OrderCancelSagaService` doesn't exist yet.

- [ ] **Step 3: Create `OrderCancelSagaService.java`**

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
public class OrderCancelSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelSagaService.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderCancelSagaService(OrderRepository orderRepository,
                                   ProcessedEventRepository processedEventRepository,
                                   OrderDomainEventPublisher domainEventPublisher) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Transactional
    public void confirmCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.noteCancelled();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    @Transactional
    public void rejectCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.undoCancel();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel rejection for order {}: {}", orderId, e.getMessage());
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderCancelSagaServiceTest"`
Expected: `BUILD SUCCESSFUL`, all 4 tests pass.

- [ ] **Step 5: Extend `KitchenEventListener.java` (order-service) and write its first test**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.KitchenEvent;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final OrderSagaService orderSagaService;
    private final OrderCancelSagaService orderCancelSagaService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(OrderSagaService orderSagaService,
                                 OrderCancelSagaService orderCancelSagaService,
                                 ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.orderCancelSagaService = orderCancelSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "order-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "TicketConfirmed" -> orderSagaService.approve(event.orderId(), event.eventId());
            case "TicketCreationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "TicketCancellationRejected" -> orderCancelSagaService.rejectCancel(event.orderId(), event.eventId());
            default -> { }
        }
    }
}
```

Create `KitchenEventListenerTest.java` (order-service, new file):

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class KitchenEventListenerTest {

    private final OrderSagaService orderSagaService = mock(OrderSagaService.class);
    private final OrderCancelSagaService orderCancelSagaService = mock(OrderCancelSagaService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KitchenEventListener listener =
            new KitchenEventListener(orderSagaService, orderCancelSagaService, objectMapper);

    @Test
    void routesTicketConfirmedToApprove() {
        String payload = """
                {"eventId":"e1","eventType":"TicketConfirmed","orderId":42,"ticketId":1,"totalQuantity":null,"reason":null}
                """;

        listener.onMessage(payload);

        verify(orderSagaService).approve(42L, "e1");
    }

    @Test
    void routesTicketCancellationRejectedToOrderCancelSagaService() {
        String payload = """
                {"eventId":"e2","eventType":"TicketCancellationRejected","orderId":42,"ticketId":1,"totalQuantity":null,"reason":"cannot cancel once ready for pickup"}
                """;

        listener.onMessage(payload);

        verify(orderCancelSagaService).rejectCancel(42L, "e2");
        verify(orderSagaService, never()).approve(any(), any());
        verify(orderSagaService, never()).reject(any(), any());
    }
}
```

- [ ] **Step 6: Extend `AccountingEventListener.java` (order-service) and write its first test**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.AccountingEvent;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final OrderSagaService orderSagaService;
    private final OrderCancelSagaService orderCancelSagaService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(OrderSagaService orderSagaService,
                                    OrderCancelSagaService orderCancelSagaService,
                                    ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.orderCancelSagaService = orderCancelSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.events", groupId = "order-service")
    public void onMessage(String payload) {
        AccountingEvent event;
        try {
            event = objectMapper.readValue(payload, AccountingEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "CardAuthorizationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "AuthorizationReversed" -> orderCancelSagaService.confirmCancel(event.orderId(), event.eventId());
            default -> { }
        }
    }
}
```

Create `AccountingEventListenerTest.java` (order-service, new file):

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AccountingEventListenerTest {

    private final OrderSagaService orderSagaService = mock(OrderSagaService.class);
    private final OrderCancelSagaService orderCancelSagaService = mock(OrderCancelSagaService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AccountingEventListener listener =
            new AccountingEventListener(orderSagaService, orderCancelSagaService, objectMapper);

    @Test
    void routesCardAuthorizationFailedToReject() {
        String payload = """
                {"eventId":"e1","eventType":"CardAuthorizationFailed","orderId":42,"reason":"declined"}
                """;

        listener.onMessage(payload);

        verify(orderSagaService).reject(42L, "e1");
    }

    @Test
    void routesAuthorizationReversedToOrderCancelSagaService() {
        String payload = """
                {"eventId":"e2","eventType":"AuthorizationReversed","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verify(orderCancelSagaService).confirmCancel(42L, "e2");
        verify(orderSagaService, never()).reject(42L, "e2");
    }
}
```

- [ ] **Step 7: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 8: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancelSagaService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderCancelSagaServiceTest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListenerTest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListenerTest.java
git commit -m "feat(order-service): add OrderCancelSagaService and route choreography cancel replies to it"
```

---

## Task 6: Order-service — `CancelOrderSagaOrchestrator` (orchestration) and dual-routing `OrchestratorReplyListener`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestrator.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListener.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListenerTest.java`

**Interfaces:**
- Consumes: `Order.noteCancelled()`/`undoCancel()` (existing), `OrderDomainEventPublisher.publish` (existing), `KitchenCommand`/`AccountingCommand` with `sagaType` (Task 1).
- Produces: `CancelOrderSagaOrchestrator.start(Order order)` / `handleReply(String eventId, String participant, String eventType, Long orderId, String reason)` — consumed by `OrchestrationOrderCancellationSagaTrigger` (Task 7) and `OrchestratorReplyListener`.

**`CreateOrderSagaOrchestrator`'s signature and logic are unchanged in this task** — routing between the two orchestrators happens entirely in `OrchestratorReplyListener`, based on `SagaReply.sagaType()`.

- [ ] **Step 1: Write the failing test**

Create `CancelOrderSagaOrchestratorTest.java`:

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

class CancelOrderSagaOrchestratorTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final CancelOrderSagaOrchestrator orchestrator = new CancelOrderSagaOrchestrator(
            orderRepository, processedEventRepository, outboxEventRepository, domainEventPublisher, objectMapper);

    private Order cancelPendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
    }

    @Test
    void startSendsCancelTicketCommand() {
        orchestrator.start(cancelPendingOrder());

        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "CancelTicket".equals(e.getEventType())));
    }

    @Test
    void ticketCancelledTriggersReverseAuthorization() {
        when(processedEventRepository.existsById(any())).thenReturn(false);

        orchestrator.handleReply("e1", "kitchen", "TicketCancelled", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "accounting.commands".equals(e.getTopic())
                && "ReverseAuthorization".equals(e.getEventType())));
    }

    @Test
    void ticketCancellationRejectedUndoesCancelWithoutContactingAccounting() {
        Order order = cancelPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketCancellationRejected", 42L, "cannot cancel once ready for pickup");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(domainEventPublisher).publish(List.of(new OrderCancelRejectedEvent(42L)));
        verify(outboxEventRepository, never()).save(argThat(e -> "accounting.commands".equals(e.getTopic())));
    }

    @Test
    void authorizationReversedConfirmsCancel() {
        Order order = cancelPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e2", "accounting", "AuthorizationReversed", 42L, null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(domainEventPublisher).publish(List.of(new OrderCancelConfirmedEvent(42L)));
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "kitchen", "TicketCancelled", 42L, null);

        verify(orderRepository, never()).findById(any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestratorTest"`
Expected: Compilation failure — `CancelOrderSagaOrchestrator` doesn't exist yet.

- [ ] **Step 3: Create `CancelOrderSagaOrchestrator.java`**

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
public class CancelOrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(CancelOrderSagaOrchestrator.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public CancelOrderSagaOrchestrator(OrderRepository orderRepository,
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
        String eventId = UUID.randomUUID().toString();
        publishCommand("kitchen.commands", eventId, "CancelTicket", order.getId(),
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
            undoCancel(orderId);
            return;
        }
        String eventId = UUID.randomUUID().toString();
        publishCommand("accounting.commands", eventId, "ReverseAuthorization", orderId,
                new AccountingCommand(eventId, "ReverseAuthorization", orderId, null, "CancelOrder"));
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        if ("AuthorizationReversed".equals(eventType)) {
            confirmCancel(orderId);
        }
    }

    private void confirmCancel(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.noteCancelled();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    private void undoCancel(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.undoCancel();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel rejection for order {}: {}", orderId, e.getMessage());
        }
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

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestratorTest"`
Expected: `BUILD SUCCESSFUL`, all 5 tests pass.

- [ ] **Step 5: Write `OrchestratorReplyListenerTest.java` (new file, failing first)**

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestrator;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrchestratorReplyListenerTest {

    private final CreateOrderSagaOrchestrator createOrderSagaOrchestrator = mock(CreateOrderSagaOrchestrator.class);
    private final CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator = mock(CancelOrderSagaOrchestrator.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final OrchestratorReplyListener listener = new OrchestratorReplyListener(
            createOrderSagaOrchestrator, cancelOrderSagaOrchestrator, objectMapper);

    @Test
    void routesCreateOrderReplyToCreateOrderSagaOrchestrator() {
        String payload = """
                {"eventId":"e1","participant":"kitchen","eventType":"TicketCreated","orderId":42,"reason":null,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(createOrderSagaOrchestrator).handleReply("e1", "kitchen", "TicketCreated", 42L, null);
        verifyNoInteractions(cancelOrderSagaOrchestrator);
    }

    @Test
    void routesCancelOrderReplyToCancelOrderSagaOrchestrator() {
        String payload = """
                {"eventId":"e2","participant":"kitchen","eventType":"TicketCancelled","orderId":42,"reason":null,"sagaType":"CancelOrder"}
                """;

        listener.onMessage(payload);

        verify(cancelOrderSagaOrchestrator).handleReply("e2", "kitchen", "TicketCancelled", 42L, null);
        verifyNoInteractions(createOrderSagaOrchestrator);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(createOrderSagaOrchestrator);
        verifyNoInteractions(cancelOrderSagaOrchestrator);
    }
}
```

- [ ] **Step 6: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.OrchestratorReplyListenerTest"`
Expected: Compilation failure — `OrchestratorReplyListener`'s constructor doesn't accept a `CancelOrderSagaOrchestrator` yet.

- [ ] **Step 7: Rewrite `OrchestratorReplyListener.java`**

Replace the full contents of `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListener.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.SagaReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestratorReplyListener {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorReplyListener.class);

    private final CreateOrderSagaOrchestrator createOrderSagaOrchestrator;
    private final CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator;
    private final ObjectMapper objectMapper;

    public OrchestratorReplyListener(CreateOrderSagaOrchestrator createOrderSagaOrchestrator,
                                      CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator,
                                      ObjectMapper objectMapper) {
        this.createOrderSagaOrchestrator = createOrderSagaOrchestrator;
        this.cancelOrderSagaOrchestrator = cancelOrderSagaOrchestrator;
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
            default -> log.warn("Unknown saga type on reply: {}", reply.sagaType());
        }
    }
}
```

- [ ] **Step 8: Run the test to verify it passes, then the full order-service suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: `BUILD SUCCESSFUL` — including `CreateOrderSagaOrchestratorTest`, unchanged and still passing, since `CreateOrderSagaOrchestrator` itself was not touched in this task.

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListener.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListenerTest.java
git commit -m "feat(order-service): add stateless CancelOrderSagaOrchestrator and route saga.replies by sagaType"
```

---

## Task 7: Order-service — `OrderCancellationSagaTrigger` and `OrderController` wiring

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancellationSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCancellationSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrchestrationOrderCancellationSagaTrigger.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCancellationSagaTriggerTest.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrchestrationOrderCancellationSagaTriggerTest.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`

**Interfaces:**
- Consumes: `CancelOrderSagaOrchestrator.start(Order)` (Task 6), `OrderDomainEventPublisher.publish` (existing).
- Produces: `OrderCancellationSagaTrigger.onOrderCancelled(Order order, List<OrderDomainEvent> events)` — consumed by `OrderController.cancel()`.

- [ ] **Step 1: Write the failing tests**

Create `OrderCancellationSagaTrigger.java`:

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public interface OrderCancellationSagaTrigger {

    void onOrderCancelled(Order order, List<OrderDomainEvent> events);
}
```

Create `ChoreographyOrderCancellationSagaTriggerTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ChoreographyOrderCancellationSagaTriggerTest {

    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);

    private final ChoreographyOrderCancellationSagaTrigger trigger =
            new ChoreographyOrderCancellationSagaTrigger(domainEventPublisher);

    @Test
    void publishesTheEventsDirectly() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
        List<OrderDomainEvent> events = List.of(new OrderCancelledEvent(1L));

        trigger.onOrderCancelled(order, events);

        verify(domainEventPublisher).publish(events);
    }
}
```

Create `OrchestrationOrderCancellationSagaTriggerTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrchestrationOrderCancellationSagaTriggerTest {

    private final CancelOrderSagaOrchestrator orchestrator = mock(CancelOrderSagaOrchestrator.class);

    private final OrchestrationOrderCancellationSagaTrigger trigger =
            new OrchestrationOrderCancellationSagaTrigger(orchestrator);

    @Test
    void startsTheOrchestratorWithoutPublishingEventsDirectly() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);

        trigger.onOrderCancelled(order, List.of(new OrderCancelledEvent(1L)));

        verify(orchestrator).start(order);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.ChoreographyOrderCancellationSagaTriggerTest" --tests "com.sanjay.ftgo.order.domain.OrchestrationOrderCancellationSagaTriggerTest"`
Expected: Compilation failure — the trigger implementations don't exist yet.

- [ ] **Step 3: Create the two trigger implementations**

Create `ChoreographyOrderCancellationSagaTrigger.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ChoreographyOrderCancellationSagaTrigger implements OrderCancellationSagaTrigger {

    private final OrderDomainEventPublisher domainEventPublisher;

    public ChoreographyOrderCancellationSagaTrigger(OrderDomainEventPublisher domainEventPublisher) {
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    public void onOrderCancelled(Order order, List<OrderDomainEvent> events) {
        domainEventPublisher.publish(events);
    }
}
```

Create `OrchestrationOrderCancellationSagaTrigger.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestrationOrderCancellationSagaTrigger implements OrderCancellationSagaTrigger {

    private final CancelOrderSagaOrchestrator orchestrator;

    public OrchestrationOrderCancellationSagaTrigger(CancelOrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void onOrderCancelled(Order order, List<OrderDomainEvent> events) {
        orchestrator.start(order);
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.ChoreographyOrderCancellationSagaTriggerTest" --tests "com.sanjay.ftgo.order.domain.OrchestrationOrderCancellationSagaTriggerTest"`
Expected: `BUILD SUCCESSFUL`, both tests pass.

- [ ] **Step 5: Write the updated `OrderControllerTest.java` (failing first)**

Add `@MockitoBean private OrderCancellationSagaTrigger cancellationSagaTrigger;` to the field declarations (alongside the existing `orderService`/`orderRepository`/`domainEventPublisher` mocks), and add `import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;` to the imports.

Replace the existing `cancelsAnApprovedOrder` test:

```java
    @Test
    void cancelsAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCEL_PENDING"));
    }
```

with:

```java
    @Test
    void cancelsAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCEL_PENDING"));

        verify(cancellationSagaTrigger).onOrderCancelled(eq(order), any());
    }
```

(`verify`/`eq`/`any` are already imported in this file.)

- [ ] **Step 6: Run the test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest"`
Expected: Compilation failure — `OrderController`'s constructor doesn't accept an `OrderCancellationSagaTrigger` yet.

- [ ] **Step 7: Update `OrderController.java`**

In `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`, add the import `com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger`, add a field and constructor parameter, and rewrite `cancel()`:

Change:

```java
    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderController(OrderService orderService, OrderRepository orderRepository,
                            OrderDomainEventPublisher domainEventPublisher) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
    }
```

to:

```java
    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final OrderCancellationSagaTrigger cancellationSagaTrigger;

    public OrderController(OrderService orderService, OrderRepository orderRepository,
                            OrderDomainEventPublisher domainEventPublisher,
                            OrderCancellationSagaTrigger cancellationSagaTrigger) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.cancellationSagaTrigger = cancellationSagaTrigger;
    }
```

Change:

```java
    @PostMapping("/{id}/cancel")
    @Transactional
    public ResponseEntity<OrderResponse> cancel(@PathVariable Long id) {
        Order order = findOrder(id);
        apply(order, order.cancel());
        return ResponseEntity.ok(OrderResponse.from(order));
    }
```

to:

```java
    @PostMapping("/{id}/cancel")
    @Transactional
    public ResponseEntity<OrderResponse> cancel(@PathVariable Long id) {
        Order order = findOrder(id);
        List<OrderDomainEvent> events = order.cancel();
        orderRepository.save(order);
        cancellationSagaTrigger.onOrderCancelled(order, events);
        return ResponseEntity.ok(OrderResponse.from(order));
    }
```

`revise()` and the `apply()` helper are unchanged — `apply()` remains used only by `revise()`.

- [ ] **Step 8: Run the test to verify it passes, then the full order-service suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: `BUILD SUCCESSFUL`, all 14 `OrderControllerTest` cases pass (13 existing + the new assertion in `cancelsAnApprovedOrder`).

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCancellationSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCancellationSagaTrigger.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrchestrationOrderCancellationSagaTrigger.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCancellationSagaTriggerTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrchestrationOrderCancellationSagaTriggerTest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java
git commit -m "feat(order-service): wire OrderController.cancel() through a saga-mode-aware OrderCancellationSagaTrigger"
```

---

## Task 8: Full-stack build, Docker e2e verification, and docs

**Files:** `CONTEXT.md`, `README.md`, `docs/session-<date>.md` (new session doc — verification only for code).

- [ ] **Step 1: Full multi-module build**

Run: `./gradlew clean build`
Expected: `BUILD SUCCESSFUL` across all modules.

- [ ] **Step 2: Choreography mode — full cancel success**

Start the stack (`docker compose up --build -d`), wait for services healthy, create an order, wait for it to reach `APPROVED`, then:

```bash
curl -s -X POST http://localhost:8082/orders/<order-id>/cancel
```

Expected: `200`, `"status":"CANCEL_PENDING"` immediately, settling to `"CANCELLED"` within a few seconds. Confirm via:

```bash
docker exec $(docker compose ps -q mysql) mysql -uroot -proot -e \
  "SELECT status FROM ftgo_order.orders WHERE id=<order-id>;"
docker exec $(docker compose ps -q mysql) mysql -uroot -proot -e \
  "SELECT state FROM ftgo_kitchen.tickets WHERE order_id=<order-id>;"
docker exec $(docker compose ps -q mysql) mysql -uroot -proot -e \
  "SELECT status FROM ftgo_accounting.authorizations WHERE order_id=<order-id>;"
```

Expected: `Order.status = CANCELLED`, `Ticket.state = CANCELLED`, `Authorization.status = REVERSED`.

- [ ] **Step 3: Choreography mode — rejection path**

Create a second order, wait for `APPROVED`, drive its ticket to `READY_FOR_PICKUP` via the existing restaurant-worker REST API (`POST /tickets/{ticketId}/accept`, `/preparing`, `/ready-for-pickup`), then:

```bash
curl -s -X POST http://localhost:8082/orders/<second-order-id>/cancel
```

Expected: `200`, `"status":"CANCEL_PENDING"` immediately, settling back to `"APPROVED"`. Confirm the `Authorization` row is untouched (`status` still `AUTHORIZED`, not `REVERSED`) — this is the key proof that accounting was never contacted.

- [ ] **Step 4: Orchestration mode — repeat both scenarios**

`docker compose down`, `SAGA_MODE=orchestration docker compose up --build -d`, repeat Steps 2 and 3. Expected: identical end states (`CANCELLED`/`REVERSED` for the success case, `APPROVED`/`AUTHORIZED` untouched for the rejection case).

- [ ] **Step 5: Redelivery/idempotency spot-check**

Restart `order-service` mid-flow-free-state (after a completed cancellation): `docker compose restart order-service`. Expected: `processed_events` counts and `Order`/`Ticket`/`Authorization` states unchanged after restart.

- [ ] **Step 6: Tear down**

Run: `docker compose down`

- [ ] **Step 7: Update `CONTEXT.md`**

- "Current position": mark the Cancel Order saga (sub-project 2 of 3) done, Revise Order saga (sub-project 3) as the sole remaining item.
- "Patterns reference" → Data consistency section: note the Cancel Order saga is now implemented alongside Create Order saga.
- "Services to build" table: update `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-accounting-service` rows to mention the Cancel Order saga participation and (for accounting) the `Authorization` aggregate.
- Session log: add a one-line entry dated with today's actual date.

- [ ] **Step 8: Update `README.md`**

Update the Ch.5 "Book progress" row and any per-service endpoint/description rows touched by this saga (accounting-service's status column, if one exists, similarly to how order-service's row was updated for PR #12).

- [ ] **Step 9: Create a new session doc**

Create `docs/session-<today's-date>.md` following the structure of `docs/session-2026-07-21.md` (the `Order` aggregate session), covering: the Cancel Order saga's design decisions, the two real gaps found and fixed during planning (`KitchenCommand`'s shared-command `sagaType` problem, `AccountingCommand`'s missing discriminator), the sequential kitchen-gates-accounting flow, and the Revise Order saga as the sole remaining sub-project.

- [ ] **Step 10: Commit the docs update**

```bash
git add CONTEXT.md README.md docs/session-*.md
git commit -m "docs: record Cancel Order saga session"
```

---

## Deferred (not in this plan)

- **Revise Order saga** (sub-project 3 of 3) — the book's trickiest saga (quantity-based revision, kitchen capacity re-check, accounting re-authorization delta), a separate future session.
