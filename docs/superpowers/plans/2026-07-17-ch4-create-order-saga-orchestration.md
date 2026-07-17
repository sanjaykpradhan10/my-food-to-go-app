# Create Order Saga (Orchestration) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Ch.4 Create Order saga a second time using orchestration, switchable against the already-merged choreography implementation via one `SAGA_MODE` env var per service (`choreography` default, `orchestration` alternate). A central `CreateOrderSagaOrchestrator` in order-service drives the same saga via explicit commands/replies on 4 new Kafka topics, reusing choreography's domain decision logic.

**Architecture:** Every existing choreography listener across all four services gets gated behind `@ConditionalOnProperty(saga.mode=choreography, matchIfMissing=true)`. A parallel orchestration path activates under `saga.mode=orchestration`: order-service's orchestrator sends commands on `consumer.commands`/`kitchen.commands`/`accounting.commands` and consumes replies on `saga.replies`; each participant's existing decision logic (extracted into a small shared method) now also drives a command handler that replies instead of publishing a domain event.

**Tech Stack:** Spring Boot 3.5 / Java 21, Spring Data JPA, Spring Kafka, MySQL 8.4 (H2 for tests), Jackson — same as the choreography pass, no new dependencies.

## Global Constraints

- Spec source: `docs/superpowers/specs/2026-07-17-ch4-create-order-saga-orchestration-design.md`. This plan implements it exactly; do not deviate from the channel topology, compensation matrix, or reuse strategy described there.
- No Eventuate Tram, no new framework — consistent with every prior pass in this project.
- **`OutboxEvent` gains a `topic` column in all four services** (Task 1) — every constructor call site across the codebase must pass the topic explicitly from that point on. `OutboxPublisher` reads `event.getTopic()` per row instead of a hardcoded constant.
- **`CreateOrderSagaInstance` needs `@Version` optimistic locking**, for the identical reason accounting-service's `SagaJoinState` needed it in the choreography pass: `ConsumerVerified`/`TicketCreated` replies for the same order can arrive on two different Kafka consumer threads and both mutate the same saga-instance row. Same accepted resolution as before — no custom retry loop; a lost-update race throws `ObjectOptimisticLockingFailureException`, rolls back the transaction (including the dedup-ledger insert), and Spring Kafka's default redelivery-on-exception behavior retries with fresh state.
- All new unit tests use plain Mockito mocks in the existing style — no `@DataJpaTest`/`@SpringBootTest` for business logic. Thin `@KafkaListener` classes that just parse-and-delegate are not unit tested individually, matching the established convention — covered by Task 7's manual e2e pass.
- The exact same manual e2e verification scenarios as the choreography plan's Task 9 (happy path + cases A/B/C + redelivery/idempotency) get re-run with `SAGA_MODE=orchestration` in Task 7, so results are directly comparable.

---

## Task 1: Foundation — per-row outbox topic, SAGA_MODE gating (all 4 services)

**Files:**
- Modify (all 4 services): `OutboxEvent.java`, `OutboxPublisher.java`, `OutboxPublisherTest.java`, `application.yml`
- Modify: `ftgo-order-service/.../domain/OrderService.java` (one call site)
- Modify: `ftgo-kitchen-service/.../domain/TicketService.java` (one call site, inside its existing `publishEvent` helper)
- Modify: `ftgo-consumer-service/.../domain/ConsumerVerificationService.java` (one call site, inside its existing `publishEvent` helper)
- Modify: `ftgo-accounting-service/.../domain/SagaJoinService.java` (one call site, inside its existing `publishEvent` helper)
- Modify (gate behind `saga.mode=choreography`): `ftgo-order-service/.../infrastructure/{ConsumerEventListener,KitchenEventListener,AccountingEventListener}.java`; `ftgo-kitchen-service/.../infrastructure/{OrderEventListener,AccountingEventListener,ConsumerEventListener}.java`; `ftgo-consumer-service/.../infrastructure/OrderEventListener.java`; `ftgo-accounting-service/.../infrastructure/{ConsumerEventListener,KitchenEventListener}.java`

**Interfaces:**
- Produces: `OutboxEvent(String eventId, String eventType, Long orderId, String topic, String payload)` — the new 5-arg constructor every later task's `publishEvent`/`publishReply`/`publishCommand` helper calls. `OutboxPublisher` no longer has a `TOPIC` constant.

- [ ] **Step 1: Update `OutboxEvent` in all 4 services — add `topic` field**

For each of `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-accounting-service`, replace that service's `OutboxEvent.java` (package name is the only thing that differs):

```java
package com.sanjay.ftgo.<order|kitchen|consumer|accounting>.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "sent_at")
    private Instant sentAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String eventType, Long orderId, String topic, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
        this.topic = topic;
        this.payload = payload;
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

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public boolean isSent() {
        return sentAt != null;
    }

    public void markSent() {
        this.sentAt = Instant.now();
    }
}
```

- [ ] **Step 2: Update `OutboxPublisher` in all 4 services — read topic per-row**

For each service, in `OutboxPublisher.java`, remove the `private static final String TOPIC = "...";` line, and change the send call inside `publishPendingEvents()`:

```java
                kafkaTemplate.send(event.getTopic(), String.valueOf(event.getOrderId()), event.getPayload()).get();
```

(Everything else in the class — constructor, `@Scheduled` annotation, batch/retry logic — is unchanged.)

- [ ] **Step 3: Update `OutboxPublisherTest` in all 4 services — pass topic explicitly**

For each service's `OutboxPublisherTest.java`, update both `new OutboxEvent(...)` constructions to include the topic as the 4th argument (keeping each service's existing topic literal), e.g. for order-service:

```java
    @Test
    void marksEventSentAfterSuccessfulPublish() {
        OutboxEvent event = new OutboxEvent("event-1", "OrderCreated", 100L, "order.events", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("order.events"), eq("100"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isTrue();
        verify(outboxEventRepository).save(event);
    }

    @Test
    void leavesEventUnsentWhenPublishFails() {
        OutboxEvent event = new OutboxEvent("event-2", "OrderCreated", 200L, "order.events", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(eq("order.events"), eq("200"), eq("{}"))).thenReturn(failed);

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isFalse();
        verify(outboxEventRepository, never()).save(any());
    }
```

Use `"kitchen.events"` for kitchen-service's test, `"consumer.events"` for consumer-service's, `"accounting.events"` for accounting-service's — matching each service's existing topic literal (unchanged from the choreography pass).

Run per service: `./gradlew :ftgo-<service>:test --tests "*.infrastructure.OutboxPublisherTest"`
Expected: PASS for all 4.

- [ ] **Step 4: Update the 4 existing `OutboxEvent` construction call sites to pass topic**

`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java` — in `createOrder`, change:

```java
        outboxEventRepository.save(new OutboxEvent(eventId, "OrderCreated", order.getId(), toJson(event)));
```

to:

```java
        outboxEventRepository.save(new OutboxEvent(eventId, "OrderCreated", order.getId(), "order.events", toJson(event)));
```

`ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java` — in `publishEvent`, change:

```java
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, toJson(event)));
```

to:

```java
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "kitchen.events", toJson(event)));
```

`ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java` — in `publishEvent`, same change with `"consumer.events"`.

`ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java` — in `publishEvent`, same change with `"accounting.events"`.

Run: `./gradlew test --console=plain` (full multi-module suite)
Expected: BUILD SUCCESSFUL — this confirms the choreography path is unaffected by the topic-column addition.

- [ ] **Step 5: Gate the 9 existing choreography listeners behind `saga.mode=choreography`**

Add the import `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty` and the annotation `@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)` directly above `@Component` on each of these 9 files (no other change):

- `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/ConsumerEventListener.java`
- `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java`
- `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java`
- `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java`
- `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/AccountingEventListener.java`
- `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/ConsumerEventListener.java`
- `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/OrderEventListener.java`
- `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/ConsumerEventListener.java`
- `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java`

Example (order-service's `ConsumerEventListener.java`, same pattern for all 9):

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ConsumerEventListener {
    // ... rest of the class is unchanged ...
}
```

- [ ] **Step 6: Add `saga.mode` property to all 4 services' `application.yml`**

Append to each of `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-accounting-service`'s `src/main/resources/application.yml`:

```yaml
saga:
  mode: choreography
```

- [ ] **Step 7: Run the full suite and commit**

Run: `./gradlew test --console=plain`
Expected: BUILD SUCCESSFUL (all pre-existing tests still pass — this task changes no observable choreography behavior).

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherTest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/ConsumerEventListener.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisher.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisherTest.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/AccountingEventListener.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/ConsumerEventListener.java \
        ftgo-kitchen-service/src/main/resources/application.yml \
        ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OutboxEvent.java \
        ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisher.java \
        ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisherTest.java \
        ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java \
        ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/OrderEventListener.java \
        ftgo-consumer-service/src/main/resources/application.yml \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/OutboxEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisher.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisherTest.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/ConsumerEventListener.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java \
        ftgo-accounting-service/src/main/resources/application.yml
git commit -m "feat: generalize outbox to per-row topic, gate choreography behind SAGA_MODE"
```

---

## Task 2: order-service — CreateOrderSagaOrchestrator

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstance.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstanceRepository.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/VerifyConsumerCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AuthorizeCardCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SagaReply.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreationSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrchestrationOrderCreationSagaTrigger.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OrchestratorReplyListener.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTriggerTest.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java`

**Interfaces:**
- Consumes: `SagaReply(eventId, participant, eventType, orderId, reason)` on topic `saga.replies` — `participant` one of `"consumer"`/`"kitchen"`/`"accounting"` (produced by Tasks 3–5).
- Produces: `VerifyConsumerCommand(eventId, orderId, consumerId)` on `consumer.commands` (consumed by Task 3), `KitchenCommand(eventId, commandType, orderId, totalQuantity)` on `kitchen.commands` with `commandType` one of `"CreateTicket"`/`"ConfirmTicket"`/`"CancelTicket"` (consumed by Task 4), `AuthorizeCardCommand(eventId, orderId, totalQuantity)` on `accounting.commands` (consumed by Task 5).

- [ ] **Step 1: Write the failing `CreateOrderSagaOrchestratorTest`**

```java
package com.sanjay.ftgo.order.domain;

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
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final CreateOrderSagaOrchestrator orchestrator = new CreateOrderSagaOrchestrator(
            sagaInstanceRepository, orderRepository, processedEventRepository, outboxEventRepository, objectMapper);

    private Order pendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void startSendsVerifyConsumerAndCreateTicketCommands() {
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.start(pendingOrder());

        verify(sagaInstanceRepository).save(any());
        verify(outboxEventRepository).save(argThat(e -> "consumer.commands".equals(e.getTopic())));
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "CreateTicket".equals(e.getEventType())));
    }

    @Test
    void authorizesOnceBothConsumerVerifiedAndTicketCreatedRegardlessOfOrder() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerified", 42L, null);
        orchestrator.handleReply("e2", "kitchen", "TicketCreated", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "accounting.commands".equals(e.getTopic())));
    }

    @Test
    void authorizesRegardlessOfReplyOrder() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "kitchen", "TicketCreated", 42L, null);
        orchestrator.handleReply("e2", "consumer", "ConsumerVerified", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "accounting.commands".equals(e.getTopic())));
    }

    @Test
    void approvesOrderDirectlyOnCardAuthorizedWithoutWaitingForConfirmation() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "accounting", "CardAuthorized", 42L, null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "ConfirmTicket".equals(e.getEventType())));
    }

    @Test
    void rejectsOrderAndCancelsTicketOnCardAuthorizationFailed() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "accounting", "CardAuthorizationFailed", 42L, "declined");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "CancelTicket".equals(e.getEventType())));
    }

    @Test
    void rejectsOrderWithoutCompensatingWhenConsumerVerificationFailsBeforeTicketCreated() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerificationFailed", 42L, "not found");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(outboxEventRepository, never()).save(argThat(e -> "kitchen.commands".equals(e.getTopic())));
    }

    @Test
    void compensatesLateTicketCreatedReplyAfterAlreadyFailed() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerificationFailed", 42L, "not found");
        orchestrator.handleReply("e2", "kitchen", "TicketCreated", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "CancelTicket".equals(e.getEventType())));
    }

    @Test
    void rejectsOrderOnTicketCreationFailedWithNoCompensationNeeded() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketCreationFailed", 42L, "capacity");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(outboxEventRepository, never()).save(argThat(e -> "kitchen.commands".equals(e.getTopic())));
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "consumer", "ConsumerVerified", 42L, null);

        verify(sagaInstanceRepository, never()).findById(any());
    }
}
```

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestratorTest"`
Expected: FAIL (compile error — none of the referenced classes exist yet).

- [ ] **Step 2: Add `CreateOrderSagaInstance` entity and repository (with `@Version`)**

```java
package com.sanjay.ftgo.order.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "create_order_saga_instances")
public class CreateOrderSagaInstance {

    @Id
    private Long orderId;

    private boolean consumerVerified;
    private boolean ticketCreated;
    private boolean failed;
    private Integer totalQuantity;

    @Version
    private Long version;

    protected CreateOrderSagaInstance() {
    }

    public CreateOrderSagaInstance(Long orderId, Integer totalQuantity) {
        this.orderId = orderId;
        this.totalQuantity = totalQuantity;
        this.consumerVerified = false;
        this.ticketCreated = false;
        this.failed = false;
    }

    public Long getOrderId() {
        return orderId;
    }

    public boolean isConsumerVerified() {
        return consumerVerified;
    }

    public boolean isTicketCreated() {
        return ticketCreated;
    }

    public boolean isFailed() {
        return failed;
    }

    public Integer getTotalQuantity() {
        return totalQuantity;
    }

    public Long getVersion() {
        return version;
    }

    public void markConsumerVerified() {
        this.consumerVerified = true;
    }

    public void markTicketCreated() {
        this.ticketCreated = true;
    }

    public void markFailed() {
        this.failed = true;
    }
}
```

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface CreateOrderSagaInstanceRepository extends JpaRepository<CreateOrderSagaInstance, Long> {
}
```

- [ ] **Step 3: Add the command and reply records**

```java
package com.sanjay.ftgo.order.domain;

public record VerifyConsumerCommand(String eventId, Long orderId, Long consumerId) {
}
```

```java
package com.sanjay.ftgo.order.domain;

public record KitchenCommand(String eventId, String commandType, Long orderId, Integer totalQuantity) {
}
```

```java
package com.sanjay.ftgo.order.domain;

public record AuthorizeCardCommand(String eventId, Long orderId, Integer totalQuantity) {
}
```

```java
package com.sanjay.ftgo.order.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason) {
}
```

- [ ] **Step 4: Add `CreateOrderSagaOrchestrator`**

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class CreateOrderSagaOrchestrator {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository;
    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public CreateOrderSagaOrchestrator(CreateOrderSagaInstanceRepository sagaInstanceRepository,
                                        OrderRepository orderRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        OutboxEventRepository outboxEventRepository,
                                        ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
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
            if (order.getStatus() == OrderStatus.APPROVAL_PENDING) {
                order.markApproved();
                orderRepository.save(order);
            }
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

    private void rejectOrder(Order order) {
        if (order.getStatus() == OrderStatus.APPROVAL_PENDING) {
            order.markRejected();
            orderRepository.save(order);
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

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestratorTest"`
Expected: PASS.

- [ ] **Step 5: Split `OrderService`'s outbox write into a mode-selected `OrderCreationSagaTrigger`**

```java
package com.sanjay.ftgo.order.domain;

public interface OrderCreationSagaTrigger {

    void onOrderCreated(Order order, String eventId);
}
```

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ChoreographyOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public ChoreographyOrderCreationSagaTrigger(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onOrderCreated(Order order, String eventId) {
        OrderCreatedEvent event = OrderCreatedEvent.from(order, eventId);
        outboxEventRepository.save(new OutboxEvent(eventId, "OrderCreated", order.getId(), "order.events", toJson(event)));
    }

    private String toJson(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderCreatedEvent for order " + event.orderId(), e);
        }
    }
}
```

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestrationOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    private final CreateOrderSagaOrchestrator orchestrator;

    public OrchestrationOrderCreationSagaTrigger(CreateOrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void onOrderCreated(Order order, String eventId) {
        orchestrator.start(order);
    }
}
```

- [ ] **Step 6: Update `OrderService` to delegate via `OrderCreationSagaTrigger`**

Replace `OrderService.java` in full:

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
    private final OrderRepository orderRepository;
    private final OrderCreationSagaTrigger orderCreationSagaTrigger;

    public OrderService(RestaurantServicePort restaurantServicePort,
                         OrderRepository orderRepository,
                         OrderCreationSagaTrigger orderCreationSagaTrigger) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderRepository = orderRepository;
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

        Order order = orderRepository.save(new Order(consumerId, restaurantId, lineItems, OrderStatus.APPROVAL_PENDING));

        String eventId = UUID.randomUUID().toString();
        orderCreationSagaTrigger.onOrderCreated(order, eventId);

        return order;
    }
}
```

- [ ] **Step 7: Update `OrderServiceTest` — mock `OrderCreationSagaTrigger` instead of the outbox directly**

Replace `OrderServiceTest.java` in full:

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderServiceTest {

    private final RestaurantInfo restaurant = new RestaurantInfo(1L, "Ajanta Indian Cuisine", List.of(
            new RestaurantInfo.MenuItemInfo(10L, "Chicken Tikka Masala", new BigDecimal("14.99")),
            new RestaurantInfo.MenuItemInfo(11L, "Garlic Naan", new BigDecimal("3.50"))
    ));

    private final RestaurantServicePort fakePort = restaurantId ->
            restaurantId.equals(1L) ? restaurant : null;

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final OrderCreationSagaTrigger orderCreationSagaTrigger = mock(OrderCreationSagaTrigger.class);

    private final OrderService orderService =
            new OrderService(fakePort, orderRepository, orderCreationSagaTrigger);

    @Test
    void createsOrderInApprovalPendingWhenRestaurantAndMenuItemsAreValid() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Order order = orderService.createOrder(1L, 1L, List.of(new OrderLineItem(10L, 2)));

        assertThat(order.getConsumerId()).isEqualTo(1L);
        assertThat(order.getRestaurantId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
    }

    @Test
    void triggersSagaWhenOrderIsCreated() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orderService.createOrder(1L, 1L, List.of(new OrderLineItem(10L, 2)));

        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        ArgumentCaptor<String> eventIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(orderCreationSagaTrigger).onOrderCreated(orderCaptor.capture(), eventIdCaptor.capture());
        assertThat(orderCaptor.getValue().getRestaurantId()).isEqualTo(1L);
        assertThat(eventIdCaptor.getValue()).isNotBlank();
    }

    @Test
    void rejectsOrderWhenMenuItemDoesNotBelongToRestaurant() {
        assertThatThrownBy(() -> orderService.createOrder(1L, 1L, List.of(new OrderLineItem(999L, 1))))
                .isInstanceOf(MenuItemNotFoundException.class);
    }
}
```

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: PASS.

- [ ] **Step 8: Write `ChoreographyOrderCreationSagaTriggerTest`**

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ChoreographyOrderCreationSagaTriggerTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ChoreographyOrderCreationSagaTrigger trigger =
            new ChoreographyOrderCreationSagaTrigger(outboxEventRepository, objectMapper);

    @Test
    void writesOrderCreatedToOrderEventsTopic() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

        trigger.onOrderCreated(order, "event-1");

        verify(outboxEventRepository).save(argThat(e ->
                "OrderCreated".equals(e.getEventType())
                        && "order.events".equals(e.getTopic())
                        && e.getPayload().contains("\"restaurantId\":1")));
    }
}
```

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (all order-service tests).

- [ ] **Step 9: Add `OrchestratorReplyListener`**

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final CreateOrderSagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper;

    public OrchestratorReplyListener(CreateOrderSagaOrchestrator orchestrator, ObjectMapper objectMapper) {
        this.orchestrator = orchestrator;
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
        orchestrator.handleReply(reply.eventId(), reply.participant(), reply.eventType(), reply.orderId(), reply.reason());
    }
}
```

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (all order-service tests).

- [ ] **Step 10: Commit**

```bash
git add ftgo-order-service/
git commit -m "feat(order-service): add CreateOrderSagaOrchestrator for SAGA_MODE=orchestration"
```

---

## Task 3: consumer-service — `VerifyConsumerCommandListener`

**Files:**
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/VerifyConsumerCommand.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/SagaReply.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/VerifyConsumerCommandListener.java`
- Modify: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java`
- Test: `ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationServiceTest.java`

**Interfaces:**
- Consumes: `VerifyConsumerCommand(eventId, orderId, consumerId)` on `consumer.commands` (produced by Task 2).
- Produces: `SagaReply(eventId, "consumer", eventType, orderId, reason)` on `saga.replies` — consumed by Task 2.

- [ ] **Step 1: Add failing test cases to `ConsumerVerificationServiceTest`**

Add these `@Test` methods to the existing class:

```java
    @Test
    void publishesConsumerVerifiedReplyWhenConsumerIsActiveViaCommand() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Sanjay", true)));

        service.handleVerifyConsumerCommand("cmd-1", 42L, 1L);

        verify(outboxEventRepository).save(argThat(e ->
                "ConsumerVerified".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void publishesConsumerVerificationFailedReplyWhenConsumerIsInactiveViaCommand() {
        when(processedEventRepository.existsById("cmd-2")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Blocked Consumer", false)));

        service.handleVerifyConsumerCommand("cmd-2", 42L, 1L);

        verify(outboxEventRepository).save(argThat(e ->
                "ConsumerVerificationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void skipsDuplicateCommandDelivery() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(true);

        service.handleVerifyConsumerCommand("cmd-1", 42L, 1L);

        verify(outboxEventRepository, never()).save(any());
        verify(consumerRepository, never()).findById(any());
    }
```

Add the required import at the top of the file: `import static org.mockito.ArgumentMatchers.argThat;`

Run: `./gradlew :ftgo-consumer-service:test --tests "com.sanjay.ftgo.consumer.domain.ConsumerVerificationServiceTest"`
Expected: FAIL (compile error — `handleVerifyConsumerCommand` doesn't exist; existing choreography tests will also need their `OutboxEvent`-based assertions updated per Step 1 changes below — see note in Step 3).

- [ ] **Step 2: Add `VerifyConsumerCommand` and `SagaReply` records**

```java
package com.sanjay.ftgo.consumer.domain;

public record VerifyConsumerCommand(String eventId, Long orderId, Long consumerId) {
}
```

```java
package com.sanjay.ftgo.consumer.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason) {
}
```

- [ ] **Step 3: Refactor `ConsumerVerificationService` — extract the verification decision, add the command handler**

Replace `ConsumerVerificationService.java` in full:

```java
package com.sanjay.ftgo.consumer.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ConsumerVerificationService {

    private final ConsumerRepository consumerRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public ConsumerVerificationService(ConsumerRepository consumerRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        OutboxEventRepository outboxEventRepository,
                                        ObjectMapper objectMapper) {
        this.consumerRepository = consumerRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        VerificationResult result = verify(event.consumerId());
        String eventType = result.verified() ? "ConsumerVerified" : "ConsumerVerificationFailed";
        publishEvent(eventType, event.orderId(), event.consumerId(), result.reason());
    }

    @Transactional
    public void handleVerifyConsumerCommand(String eventId, Long orderId, Long consumerId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        VerificationResult result = verify(consumerId);
        String eventType = result.verified() ? "ConsumerVerified" : "ConsumerVerificationFailed";
        publishReply(eventType, orderId, result.reason());
    }

    private VerificationResult verify(Long consumerId) {
        Consumer consumer = consumerRepository.findById(consumerId).orElse(null);
        if (consumer == null) {
            return new VerificationResult(false, "consumer not found");
        }
        if (!consumer.isActive()) {
            return new VerificationResult(false, "consumer is not active");
        }
        return new VerificationResult(true, null);
    }

    private record VerificationResult(boolean verified, String reason) {
    }

    private void publishEvent(String eventType, Long orderId, Long consumerId, String reason) {
        String eventId = UUID.randomUUID().toString();
        ConsumerVerificationEvent event = new ConsumerVerificationEvent(eventId, eventType, orderId, consumerId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "consumer.events", toJson(event)));
    }

    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "consumer", eventType, orderId, reason);
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

Run: `./gradlew :ftgo-consumer-service:test --tests "com.sanjay.ftgo.consumer.domain.ConsumerVerificationServiceTest"`
Expected: PASS (the existing choreography tests keep passing unchanged — the `verify` extraction and `toJson` generalization don't alter `handleOrderCreated`'s observable behavior).

- [ ] **Step 4: Add `VerifyConsumerCommandListener`**

```java
package com.sanjay.ftgo.consumer.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.consumer.domain.ConsumerVerificationService;
import com.sanjay.ftgo.consumer.domain.VerifyConsumerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class VerifyConsumerCommandListener {

    private static final Logger log = LoggerFactory.getLogger(VerifyConsumerCommandListener.class);

    private final ConsumerVerificationService consumerVerificationService;
    private final ObjectMapper objectMapper;

    public VerifyConsumerCommandListener(ConsumerVerificationService consumerVerificationService, ObjectMapper objectMapper) {
        this.consumerVerificationService = consumerVerificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.commands", groupId = "consumer-service")
    public void onMessage(String payload) {
        VerifyConsumerCommand command;
        try {
            command = objectMapper.readValue(payload, VerifyConsumerCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed verify-consumer command: {}", payload, e);
            return;
        }
        consumerVerificationService.handleVerifyConsumerCommand(command.eventId(), command.orderId(), command.consumerId());
    }
}
```

Run: `./gradlew :ftgo-consumer-service:test`
Expected: PASS (all consumer-service tests).

- [ ] **Step 5: Commit**

```bash
git add ftgo-consumer-service/
git commit -m "feat(consumer-service): handle VerifyConsumerCommand for SAGA_MODE=orchestration"
```

---

## Task 4: kitchen-service — `KitchenCommandListener`

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/KitchenCommand.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/SagaReply.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListener.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`

**Interfaces:**
- Consumes: `KitchenCommand(eventId, commandType, orderId, totalQuantity)` on `kitchen.commands`, `commandType` one of `"CreateTicket"`/`"ConfirmTicket"`/`"CancelTicket"` (produced by Task 2).
- Produces: `SagaReply(eventId, "kitchen", eventType, orderId, reason)` on `saga.replies` — consumed by Task 2. (`ConfirmTicket`/`CancelTicket` are fire-and-forget — no reply.)

- [ ] **Step 1: Add failing test cases to `TicketServiceTest`**

Add these `@Test` methods to the existing class:

```java
    @Test
    void createsTicketViaCommandWhenWithinCapacity() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(false);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleCreateTicketCommand("cmd-1", 42L, 5);

        verify(ticketRepository).save(argThatStatusIs("CREATE_PENDING"));
        verify(outboxEventRepository).save(argThat(e ->
                "TicketCreated".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void repliesTicketCreationFailedViaCommandWhenOverCapacity() {
        when(processedEventRepository.existsById("cmd-2")).thenReturn(false);

        ticketService.handleCreateTicketCommand("cmd-2", 43L, 25);

        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat(e ->
                "TicketCreationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void confirmsTicketViaCommand() {
        Ticket ticket = new Ticket(42L, "CREATE_PENDING");
        when(processedEventRepository.existsById("cmd-3")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleConfirmTicketCommand("cmd-3", 42L);

        assertThat(ticket.getStatus()).isEqualTo("AWAITING_ACCEPTANCE");
    }

    @Test
    void cancelsTicketViaCommand() {
        Ticket ticket = new Ticket(42L, "CREATE_PENDING");
        when(processedEventRepository.existsById("cmd-4")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleCancelTicketCommand("cmd-4", 42L);

        assertThat(ticket.getStatus()).isEqualTo("CANCELLED");
    }
```

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: FAIL (compile error — `handleCreateTicketCommand`/`handleConfirmTicketCommand`/`handleCancelTicketCommand` don't exist).

- [ ] **Step 2: Add `KitchenCommand` and `SagaReply` records**

```java
package com.sanjay.ftgo.kitchen.domain;

public record KitchenCommand(String eventId, String commandType, Long orderId, Integer totalQuantity) {
}
```

```java
package com.sanjay.ftgo.kitchen.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason) {
}
```

- [ ] **Step 3: Refactor `TicketService` — extract the capacity check, add the three command handlers**

Replace `TicketService.java` in full:

```java
package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class TicketService {

    private static final int KITCHEN_CAPACITY_LIMIT = 20;

    private final TicketRepository ticketRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final FailedOrderRepository failedOrderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public TicketService(TicketRepository ticketRepository,
                          ProcessedEventRepository processedEventRepository,
                          FailedOrderRepository failedOrderRepository,
                          OutboxEventRepository outboxEventRepository,
                          ObjectMapper objectMapper) {
        this.ticketRepository = ticketRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedOrderRepository = failedOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
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
            ticketRepository.save(new Ticket(event.orderId(), "CANCELLED"));
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            publishEvent("TicketCreationFailed", event.orderId(), null, totalQuantity,
                    "order exceeds kitchen capacity");
            return;
        }

        Ticket ticket = ticketRepository.save(new Ticket(event.orderId(), "CREATE_PENDING"));
        publishEvent("TicketCreated", event.orderId(), ticket.getId(), totalQuantity, null);
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

        if ("CardAuthorized".equals(eventType)) {
            ticket.markAwaitingAcceptance();
            ticketRepository.save(ticket);
            publishEvent("TicketConfirmed", orderId, ticket.getId(), null, null);
        } else {
            ticket.markCancelled();
            ticketRepository.save(ticket);
            publishEvent("TicketCancelled", orderId, ticket.getId(), null, null);
        }
    }

    @Transactional
    public void handleConsumerVerificationFailed(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            ticket.markCancelled();
            ticketRepository.save(ticket);
            publishEvent("TicketCancelled", orderId, ticket.getId(), null, null);
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

        if (!isWithinCapacity(totalQuantity)) {
            publishReply("TicketCreationFailed", orderId, "order exceeds kitchen capacity");
            return;
        }

        ticketRepository.save(new Ticket(orderId, "CREATE_PENDING"));
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
            ticket.markAwaitingAcceptance();
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
            ticket.markCancelled();
            ticketRepository.save(ticket);
        }
    }

    private boolean isWithinCapacity(int totalQuantity) {
        return totalQuantity <= KITCHEN_CAPACITY_LIMIT;
    }

    private void publishEvent(String eventType, Long orderId, Long ticketId, Integer totalQuantity, String reason) {
        String eventId = UUID.randomUUID().toString();
        KitchenEvent event = new KitchenEvent(eventId, eventType, orderId, ticketId, totalQuantity, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "kitchen.events", toJson(event)));
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

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: PASS (choreography's existing tests keep passing — `isWithinCapacity` is a pure extraction, `toJson` a compatible generalization).

- [ ] **Step 4: Add `KitchenCommandListener`**

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.KitchenCommand;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class KitchenCommandListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenCommandListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public KitchenCommandListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.commands", groupId = "kitchen-service")
    public void onMessage(String payload) {
        KitchenCommand command;
        try {
            command = objectMapper.readValue(payload, KitchenCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen command: {}", payload, e);
            return;
        }
        switch (command.commandType()) {
            case "CreateTicket" ->
                    ticketService.handleCreateTicketCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ConfirmTicket" -> ticketService.handleConfirmTicketCommand(command.eventId(), command.orderId());
            case "CancelTicket" -> ticketService.handleCancelTicketCommand(command.eventId(), command.orderId());
            default -> log.warn("Unknown kitchen command type: {}", command.commandType());
        }
    }
}
```

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: PASS (all kitchen-service tests).

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/
git commit -m "feat(kitchen-service): handle CreateTicket/ConfirmTicket/CancelTicket commands for SAGA_MODE=orchestration"
```

---

## Task 5: accounting-service — `AuthorizeCardCommandListener`

**Files:**
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizeCardCommand.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaReply.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/AuthorizeCardCommandListener.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Test: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java`

**Interfaces:**
- Consumes: `AuthorizeCardCommand(eventId, orderId, totalQuantity)` on `accounting.commands` (produced by Task 2).
- Produces: `SagaReply(eventId, "accounting", eventType, orderId, reason)` on `saga.replies` — consumed by Task 2. No join required — the orchestrator only sends this command once both prerequisites already succeeded.

- [ ] **Step 1: Add failing test cases to `SagaJoinServiceTest`**

Add these `@Test` methods to the existing class:

```java
    @Test
    void authorizesViaCommandWhenWithinLimit() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(false);

        service.handleAuthorizeCardCommand("cmd-1", 42L, 5);

        verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e ->
                "CardAuthorized".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void declinesViaCommandWhenOverLimit() {
        when(processedEventRepository.existsById("cmd-2")).thenReturn(false);

        service.handleAuthorizeCardCommand("cmd-2", 42L, 15);

        verify(authorizationRepository).save(argThat(a -> "DECLINED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e ->
                "CardAuthorizationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void skipsDuplicateCommandDelivery() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(true);

        service.handleAuthorizeCardCommand("cmd-1", 42L, 5);

        verify(authorizationRepository, never()).save(any());
    }
```

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.SagaJoinServiceTest"`
Expected: FAIL (compile error — `handleAuthorizeCardCommand` doesn't exist).

- [ ] **Step 2: Add `AuthorizeCardCommand` and `SagaReply` records**

```java
package com.sanjay.ftgo.accounting.domain;

public record AuthorizeCardCommand(String eventId, Long orderId, Integer totalQuantity) {
}
```

```java
package com.sanjay.ftgo.accounting.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason) {
}
```

- [ ] **Step 3: Refactor `SagaJoinService` — extract the authorization decision, add the command handler**

Replace `SagaJoinService.java` in full:

```java
package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final ObjectMapper objectMapper;

    public SagaJoinService(SagaJoinStateRepository sagaJoinStateRepository,
                            AuthorizationRepository authorizationRepository,
                            ProcessedEventRepository processedEventRepository,
                            OutboxEventRepository outboxEventRepository,
                            ObjectMapper objectMapper) {
        this.sagaJoinStateRepository = sagaJoinStateRepository;
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
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

        boolean authorized = isAuthorized(totalQuantity);
        authorizationRepository.save(new Authorization(orderId, authorized ? "AUTHORIZED" : "DECLINED"));

        if (authorized) {
            publishReply("CardAuthorized", orderId, null);
        } else {
            publishReply("CardAuthorizationFailed", orderId, "order quantity exceeds authorization limit");
        }
    }

    private void tryResolve(SagaJoinState state) {
        if (!state.isConsumerVerified() || !state.isTicketCreated()) {
            return;
        }
        state.markResolved();
        sagaJoinStateRepository.save(state);

        boolean authorized = isAuthorized(state.getTotalQuantity());
        authorizationRepository.save(new Authorization(state.getOrderId(), authorized ? "AUTHORIZED" : "DECLINED"));

        if (authorized) {
            publishEvent("CardAuthorized", state.getOrderId(), null);
        } else {
            publishEvent("CardAuthorizationFailed", state.getOrderId(), "order quantity exceeds authorization limit");
        }
    }

    private boolean isAuthorized(int totalQuantity) {
        return totalQuantity <= AUTHORIZATION_QUANTITY_LIMIT;
    }

    private void publishEvent(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        AccountingEvent event = new AccountingEvent(eventId, eventType, orderId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "accounting.events", toJson(event)));
    }

    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "accounting", eventType, orderId, reason);
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

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.SagaJoinServiceTest"`
Expected: PASS (choreography's existing tests, including the resolve-once-guard and optimistic-locking-covering tests from the choreography pass, keep passing unchanged — `isAuthorized` is a pure extraction).

- [ ] **Step 4: Add `AuthorizeCardCommandListener`**

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizeCardCommand;
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
        AuthorizeCardCommand command;
        try {
            command = objectMapper.readValue(payload, AuthorizeCardCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed authorize-card command: {}", payload, e);
            return;
        }
        sagaJoinService.handleAuthorizeCardCommand(command.eventId(), command.orderId(), command.totalQuantity());
    }
}
```

Run: `./gradlew :ftgo-accounting-service:test`
Expected: PASS (all accounting-service tests).

- [ ] **Step 5: Commit**

```bash
git add ftgo-accounting-service/
git commit -m "feat(accounting-service): handle AuthorizeCardCommand for SAGA_MODE=orchestration"
```

---

## Task 6: Docker wiring — `SAGA_MODE` passthrough

**Files:**
- Modify: `compose.yml`

**Interfaces:**
- Consumes: nothing new — wires the `SAGA_MODE` env var (default `choreography`, matching `application.yml`'s default) through to all four services, mirroring the existing `OUTBOX_PUBLISH_MODE` passthrough pattern.

- [ ] **Step 1: Add `SAGA_MODE` to the `environment:` block of `order-service`, `kitchen-service`, `consumer-service`, `accounting-service` in `compose.yml`**

For each of the four services' existing `environment:` block, add one line: `SAGA_MODE: ${SAGA_MODE:-choreography}`. For example, `order-service`'s block becomes:

```yaml
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/
      OUTBOX_PUBLISH_MODE: ${OUTBOX_PUBLISH_MODE:-polling}
      SAGA_MODE: ${SAGA_MODE:-choreography}
```

`kitchen-service`, `consumer-service`, `accounting-service` each get the same `SAGA_MODE: ${SAGA_MODE:-choreography}` line added to their existing `environment:` block (they don't have `OUTBOX_PUBLISH_MODE`, so it's simply the one new line alongside their existing `SPRING_DATASOURCE_URL`/`SPRING_KAFKA_BOOTSTRAP_SERVERS` entries).

- [ ] **Step 2: Validate and build**

Run: `docker compose config --quiet`
Expected: no output, exit code 0.

Run: `docker compose build order-service kitchen-service consumer-service accounting-service`
Expected: all four images build successfully.

- [ ] **Step 3: Commit**

```bash
git add compose.yml
git commit -m "feat: wire SAGA_MODE env var through docker-compose for all 4 saga participants"
```

---

## Task 7: Manual end-to-end verification via Docker (`SAGA_MODE=orchestration`)

**Files:** none — this task runs the full stack with orchestration enabled and inspects behavior. No code changes.

**Interfaces:**
- Consumes: the complete orchestration path built in Tasks 1–6.
- Produces: confirmation the orchestration saga reaches the exact same end states as the choreography saga (already verified in the prior plan) — this is the acceptance gate for the whole plan, and the basis for a direct before/after comparison between the two styles.

- [ ] **Step 1: Bring up the full stack with orchestration enabled**

```bash
SAGA_MODE=orchestration docker compose up -d --build
```

Wait for all containers healthy/running: `docker compose ps`.

- [ ] **Step 2: Happy path — order reaches `APPROVED`, ticket reaches `AWAITING_ACCEPTANCE`, directly on `CardAuthorized` (no confirmation-echo wait)**

```bash
curl -s localhost:8085/restaurants/1
```

Note the actual menu item id from the response (ids are auto-increment, not fixed — confirm against live data, not assumed literals).

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":<ID>,"quantity":2}]}'
```

Wait ~10s, then:

```bash
docker compose exec mysql mysql -uftgo -pftgo -e "SELECT id, status FROM orders WHERE id=<ORDER_ID>;" ftgo_order
docker compose exec mysql mysql -uftgo -pftgo -e "SELECT order_id, status FROM tickets WHERE order_id=<ORDER_ID>;" ftgo_kitchen
docker compose exec mysql mysql -uftgo -pftgo -e "SELECT order_id, status FROM authorizations WHERE order_id=<ORDER_ID>;" ftgo_accounting
docker compose exec mysql mysql -uftgo -pftgo -e "SELECT order_id, consumer_verified, ticket_created, failed FROM create_order_saga_instances WHERE order_id=<ORDER_ID>;" ftgo_order
```

Expected: `orders.status = APPROVED`, `tickets.status = AWAITING_ACCEPTANCE`, `authorizations.status = AUTHORIZED`, saga instance shows `consumer_verified=1, ticket_created=1, failed=0` — identical end states to the choreography run.

- [ ] **Step 3: Case A — consumer verification fails (consumerId 2, seeded inactive)**

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":2,"restaurantId":1,"lineItems":[{"menuItemId":<ID>,"quantity":1}]}'
```

Wait ~10s. Expected: `orders.status = REJECTED`; `tickets.status = CANCELLED` (or no row, depending on timing — same either-outcome-is-correct note as the choreography plan, since `CreateTicketCommand` and `VerifyConsumerCommand` are dispatched in parallel); no `authorizations` row.

- [ ] **Step 4: Case B — kitchen capacity exceeded (quantity 25)**

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":<ID>,"quantity":25}]}'
```

Wait ~10s. Expected: `orders.status = REJECTED`; no `tickets` row; no `authorizations` row.

- [ ] **Step 5: Case C — card authorization declined (quantity 15)**

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":<ID>,"quantity":15}]}'
```

Wait ~10s. Expected: `orders.status = REJECTED`; `tickets.status = CANCELLED` (compensated via `CancelTicketCommand`); `authorizations.status = DECLINED`.

- [ ] **Step 6: Redelivery/idempotency check**

Using the happy-path order's `<ORDER_ID>` from Step 2, force a Kafka redelivery by resetting one already-sent outbox row, e.g. the `TicketCreated` reply row in kitchen-service:

```bash
docker compose exec mysql mysql -uftgo -pftgo -e \
  "UPDATE outbox_events SET sent_at = NULL WHERE order_id=<ORDER_ID> AND event_type='TicketCreated';" ftgo_kitchen
```

Wait ~10s, then re-check `authorizations` row count for that order — must still be exactly 1 (`SELECT COUNT(*) FROM authorizations WHERE order_id=<ORDER_ID>;`), confirming `processed_events` dedup absorbed the redelivery in the orchestrator's `handleReply`.

- [ ] **Step 7: Tear down**

```bash
docker compose down
```

- [ ] **Step 8: Update `CONTEXT.md`, `README.md`, and `docs/session-*.md`, then commit**

Mark orchestration as implemented in `CONTEXT.md`'s "Patterns reference" checklist and "Current position", following the existing convention (per this repo's `CLAUDE.md`: documentation updates land in the same change as the code they describe).

```bash
git add CONTEXT.md README.md docs/
git commit -m "docs: update CONTEXT.md — Ch.4 Create Order saga (orchestration) implemented"
```
