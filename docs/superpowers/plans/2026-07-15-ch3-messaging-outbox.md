# Ch.3 Async Messaging + Transactional Outbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** order-service persists `Order` to MySQL for the first time, reliably publishes an `OrderCreated` event via a hand-rolled transactional outbox, and kitchen-service (currently an empty stub) consumes that event idempotently to create a `Ticket`.

**Architecture:** order-service writes `Order` and `OutboxEvent` rows in one local `@Transactional` method (the outbox pattern's core guarantee — no dual-write race). A separate `@Scheduled` poller reads unsent outbox rows and publishes them to Kafka topic `order.events`, marking each sent on success. kitchen-service's `@KafkaListener` deserializes the event and, in one transaction, checks a `processed_events` dedup ledger before creating a `Ticket` — handling Kafka's at-least-once delivery correctly.

**Tech Stack:** Spring Boot 3.5.3, Spring Data JPA, Spring Kafka, MySQL 8.4 (prod), H2 MODE=MySQL (test), Jackson, JUnit 5 + Mockito + AssertJ.

## Global Constraints

- Java 21, Spring Boot 3.5.3 (from root `build.gradle` — do not change)
- `ddl-auto: update` for all new tables — no migration tooling, matches existing restaurant-service/order-service convention
- No shared common module — each service defines its own copy of `OrderCreatedEvent` (per `CONTEXT.md` architecture decision: "No shared common module yet — extract when Ch. 4 sagas require shared event types")
- JPA repositories are colocated with their entity in the `domain` package (existing convention in this codebase: `Order`, `OutboxEvent`, `Ticket`, `ProcessedEvent` are JPA-annotated directly in `domain`, unlike restaurant-service's `infrastructure`-package repositories, because `OrderService`/`TicketService` — both domain-layer classes — need direct persistence access for the transactional outbox/dedup guarantee)
- Kafka topic name: `order.events`
- Outbox payload stored as `@Lob` (TEXT), not a native MySQL `JSON` column — portable across MySQL and the H2 test database without dialect-specific column types; still holds JSON text as designed in the spec

Full context: `docs/superpowers/specs/2026-07-15-ch3-messaging-outbox-design.md`

---

### Task 1: order-service — persist `Order` via JPA

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderLineItem.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRepository.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`

**Interfaces:**
- Produces: `OrderRepository extends JpaRepository<Order, Long>` — used directly by `OrderService` and by Task 2
- Produces: `Order` now a JPA `@Entity`; public constructor `Order(Long id, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status)` unchanged (used by `OrderControllerTest`), plus a new `Order(Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status)` for pre-save construction
- Produces: `OrderLineItem` now `@Embeddable` — record fields/accessors unchanged (`menuItemId()`, `quantity()`)

- [ ] **Step 1: Write the failing test**

Replace `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderServiceTest {

    private final RestaurantInfo restaurant = new RestaurantInfo(1L, "Ajanta Indian Cuisine", List.of(
            new RestaurantInfo.MenuItemInfo(10L, "Chicken Tikka Masala", new BigDecimal("14.99")),
            new RestaurantInfo.MenuItemInfo(11L, "Garlic Naan", new BigDecimal("3.50"))
    ));

    private final RestaurantServicePort fakePort = restaurantId ->
            restaurantId.equals(1L) ? restaurant : null;

    private final OrderRepository orderRepository = mock(OrderRepository.class);

    private final OrderService orderService = new OrderService(fakePort, orderRepository);

    @Test
    void createsOrderWhenRestaurantAndMenuItemsAreValid() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Order order = orderService.createOrder(1L, List.of(new OrderLineItem(10L, 2)));

        assertThat(order.getRestaurantId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
    }

    @Test
    void rejectsOrderWhenMenuItemDoesNotBelongToRestaurant() {
        assertThatThrownBy(() -> orderService.createOrder(1L, List.of(new OrderLineItem(999L, 1))))
                .isInstanceOf(MenuItemNotFoundException.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: FAIL — compile error, `OrderRepository` does not exist and `OrderService` constructor doesn't accept it yet.

- [ ] **Step 3: Make `OrderLineItem` embeddable**

Replace `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderLineItem.java`:

```java
package com.sanjay.ftgo.order.domain;

import jakarta.persistence.Embeddable;

@Embeddable
public record OrderLineItem(Long menuItemId, int quantity) {
}
```

- [ ] **Step 4: Make `Order` a JPA entity**

Replace `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`:

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

    private Long restaurantId;

    @ElementCollection
    @CollectionTable(name = "order_line_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderLineItem> lineItems;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    protected Order() {
    }

    public Order(Long id, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this.id = id;
        this.restaurantId = restaurantId;
        this.lineItems = lineItems;
        this.status = status;
    }

    public Order(Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this(null, restaurantId, lineItems, status);
    }

    public Long getId() {
        return id;
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
}
```

- [ ] **Step 5: Create `OrderRepository`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRepository.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
```

- [ ] **Step 6: Wire `OrderRepository` into `OrderService`**

Replace `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final RestaurantServicePort restaurantServicePort;
    private final OrderRepository orderRepository;

    public OrderService(RestaurantServicePort restaurantServicePort, OrderRepository orderRepository) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Order createOrder(Long restaurantId, List<OrderLineItem> lineItems) {
        RestaurantInfo restaurant = restaurantServicePort.findRestaurant(restaurantId);

        Set<Long> validMenuItemIds = restaurant.menuItems().stream()
                .map(RestaurantInfo.MenuItemInfo::id)
                .collect(Collectors.toSet());

        for (OrderLineItem lineItem : lineItems) {
            if (!validMenuItemIds.contains(lineItem.menuItemId())) {
                throw new MenuItemNotFoundException(lineItem.menuItemId(), restaurantId);
            }
        }

        return orderRepository.save(new Order(restaurantId, lineItems, OrderStatus.APPROVED));
    }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: PASS

- [ ] **Step 8: Run the full order-service test suite (checks `OrderControllerTest` still passes unmodified)**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — `OrderControllerTest` mocks `OrderService` directly so it's unaffected by the persistence change; `FtgoOrderServiceApplicationTests` boots with the H2 test datasource and `ddl-auto: create-drop`, which creates `orders`/`order_line_items` automatically.

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderLineItem.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderRepository.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java
git commit -m "feat(order-service): persist Order via JPA"
```

---

### Task 2: order-service — transactional outbox write

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEventRepository.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`

**Interfaces:**
- Consumes: `OrderRepository` (Task 1), `Order` (Task 1)
- Produces: `OutboxEventRepository extends JpaRepository<OutboxEvent, Long>` with `findBySentAtIsNullOrderByIdAsc(): List<OutboxEvent>` — used by Task 3's `OutboxPublisher`
- Produces: `OutboxEvent` with `getEventId(): String`, `getPayload(): String`, `isSent(): boolean`, `markSent(): void` — used by Task 3
- Produces: `OrderCreatedEvent` record with static `from(Order order, String eventId): OrderCreatedEvent` — mirrored (not shared) by kitchen-service in Task 4

- [ ] **Step 1: Write the failing test**

Add to `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java` (replace the whole file):

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final OrderService orderService =
            new OrderService(fakePort, orderRepository, outboxEventRepository, objectMapper);

    @Test
    void createsOrderWhenRestaurantAndMenuItemsAreValid() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Order order = orderService.createOrder(1L, List.of(new OrderLineItem(10L, 2)));

        assertThat(order.getRestaurantId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
    }

    @Test
    void writesOutboxEventWhenOrderIsCreated() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orderService.createOrder(1L, List.of(new OrderLineItem(10L, 2)));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent savedEvent = captor.getValue();
        assertThat(savedEvent.getEventType()).isEqualTo("OrderCreated");
        assertThat(savedEvent.getPayload()).contains("\"restaurantId\":1");
        assertThat(savedEvent.isSent()).isFalse();
    }

    @Test
    void rejectsOrderWhenMenuItemDoesNotBelongToRestaurant() {
        assertThatThrownBy(() -> orderService.createOrder(1L, List.of(new OrderLineItem(999L, 1))))
                .isInstanceOf(MenuItemNotFoundException.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: FAIL — compile error, `OutboxEvent`/`OutboxEventRepository`/`OrderCreatedEvent` don't exist yet and `OrderService` constructor doesn't accept the new params.

- [ ] **Step 3: Create `OutboxEvent`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEvent.java`:

```java
package com.sanjay.ftgo.order.domain;

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

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "sent_at")
    private Instant sentAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String eventType, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
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

- [ ] **Step 4: Create `OutboxEventRepository`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEventRepository.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findBySentAtIsNullOrderByIdAsc();
}
```

- [ ] **Step 5: Create `OrderCreatedEvent`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java`:

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long restaurantId,
        List<LineItem> lineItems) {

    public record LineItem(Long menuItemId, int quantity) {
    }

    public static OrderCreatedEvent from(Order order, String eventId) {
        List<LineItem> items = order.getLineItems().stream()
                .map(lineItem -> new LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
        return new OrderCreatedEvent(eventId, "OrderCreated", order.getId(), order.getRestaurantId(), items);
    }
}
```

- [ ] **Step 6: Wire the outbox write into `OrderService.createOrder`**

Replace `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`:

```java
package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(RestaurantServicePort restaurantServicePort,
                         OrderRepository orderRepository,
                         OutboxEventRepository outboxEventRepository,
                         ObjectMapper objectMapper) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(Long restaurantId, List<OrderLineItem> lineItems) {
        RestaurantInfo restaurant = restaurantServicePort.findRestaurant(restaurantId);

        Set<Long> validMenuItemIds = restaurant.menuItems().stream()
                .map(RestaurantInfo.MenuItemInfo::id)
                .collect(Collectors.toSet());

        for (OrderLineItem lineItem : lineItems) {
            if (!validMenuItemIds.contains(lineItem.menuItemId())) {
                throw new MenuItemNotFoundException(lineItem.menuItemId(), restaurantId);
            }
        }

        Order order = orderRepository.save(new Order(restaurantId, lineItems, OrderStatus.APPROVED));

        String eventId = UUID.randomUUID().toString();
        OrderCreatedEvent event = OrderCreatedEvent.from(order, eventId);
        outboxEventRepository.save(new OutboxEvent(eventId, "OrderCreated", toJson(event)));

        return order;
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

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: PASS

- [ ] **Step 8: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — `ddl-auto: create-drop` creates `outbox_events` in the H2 test DB automatically.

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEventRepository.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java
git commit -m "feat(order-service): write OrderCreated event to outbox in same transaction as Order"
```

---

### Task 3: order-service — outbox polling publisher (Kafka producer)

**Files:**
- Modify: `ftgo-order-service/build.gradle`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Modify: `ftgo-order-service/src/test/resources/application.yml`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/FtgoOrderServiceApplication.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KafkaProducerConfig.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherTest.java`

**Interfaces:**
- Consumes: `OutboxEventRepository`, `OutboxEvent` (Task 2)
- Produces: Kafka topic `order.events`, keyed by `orderId`, string JSON value — consumed by Task 4's `OrderEventListener`

- [ ] **Step 1: Add `spring-kafka` dependency**

Modify `ftgo-order-service/build.gradle` — add to the existing `dependencies` block:

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-aop'
    implementation 'io.github.resilience4j:resilience4j-spring-boot3:2.2.0'
    implementation 'org.springframework.kafka:spring-kafka'

    testImplementation 'org.wiremock:wiremock-standalone:3.9.2'
}
```

- [ ] **Step 2: Write the failing test**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherTest.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.OutboxEvent;
import com.sanjay.ftgo.order.domain.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class OutboxPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final OutboxPublisher outboxPublisher =
            new OutboxPublisher(outboxEventRepository, kafkaTemplate, 20);

    @Test
    void marksEventSentAfterSuccessfulPublish() {
        OutboxEvent event = new OutboxEvent("event-1", "OrderCreated", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("order.events"), eq("event-1"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isTrue();
        verify(outboxEventRepository).save(event);
    }

    @Test
    void leavesEventUnsentWhenPublishFails() {
        OutboxEvent event = new OutboxEvent("event-2", "OrderCreated", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(eq("order.events"), eq("event-2"), eq("{}"))).thenReturn(failed);

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isFalse();
        verify(outboxEventRepository, never()).save(any());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.OutboxPublisherTest"`
Expected: FAIL — compile error, `OutboxPublisher` doesn't exist yet.

- [ ] **Step 4: Create `OutboxPublisher`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.OutboxEvent;
import com.sanjay.ftgo.order.domain.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final String TOPIC = "order.events";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${outbox.batch-size:20}") int batchSize) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms:2000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findBySentAtIsNullOrderByIdAsc()
                .stream()
                .limit(batchSize)
                .toList();

        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(TOPIC, event.getEventId(), event.getPayload()).get();
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }
}
```

- [ ] **Step 5: Create the Kafka producer configuration**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KafkaProducerConfig.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> orderEventProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> orderEventKafkaTemplate(ProducerFactory<String, String> orderEventProducerFactory) {
        return new KafkaTemplate<>(orderEventProducerFactory);
    }
}
```

(An explicit `@Configuration` is used instead of relying on Spring Boot's autoconfigured `KafkaTemplate<Object, Object>` bean, matching the existing `RestClientConfig` pattern in this service and avoiding generic-type ambiguity at the `OutboxPublisher` injection point.)

- [ ] **Step 6: Enable scheduling**

Replace `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/FtgoOrderServiceApplication.java`:

```java
package com.sanjay.ftgo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FtgoOrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoOrderServiceApplication.class, args);
    }
}
```

- [ ] **Step 7: Add Kafka + outbox config to `application.yml`**

Modify `ftgo-order-service/src/main/resources/application.yml` — add `kafka` under the existing `spring:` key, and a new top-level `outbox:` block:

```yaml
spring:
  application:
    name: ftgo-order-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_order
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092

server:
  port: 8082

restaurant-service:
  base-url: http://localhost:8085

outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20

resilience4j:
  circuitbreaker:
    instances:
      restaurantService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.sanjay.ftgo.order.domain.RestaurantNotFoundException
```

- [ ] **Step 8: Add the same Kafka bootstrap config to the test `application.yml`**

Modify `ftgo-order-service/src/test/resources/application.yml` — add `kafka` under the existing `spring:` key:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MySQL
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: create-drop
    database-platform: org.hibernate.dialect.H2Dialect
  kafka:
    bootstrap-servers: localhost:9092

restaurant-service:
  base-url: http://localhost:8089

resilience4j:
  circuitbreaker:
    instances:
      restaurantService:
        sliding-window-size: 4
        failure-rate-threshold: 50
        wait-duration-in-open-state: 2s
        permitted-number-of-calls-in-half-open-state: 2
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.sanjay.ftgo.order.domain.RestaurantNotFoundException
```

- [ ] **Step 9: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.OutboxPublisherTest"`
Expected: PASS

- [ ] **Step 10: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS. `FtgoOrderServiceApplicationTests` (`@SpringBootTest`) now boots a `KafkaProducerConfig` and a `@Scheduled` publisher; no live broker is required at context-load time — the producer factory only connects lazily when a send is attempted, and no send happens during a plain context-load test.

- [ ] **Step 11: Commit**

```bash
git add ftgo-order-service/build.gradle \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/resources/application.yml \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/FtgoOrderServiceApplication.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KafkaProducerConfig.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherTest.java
git commit -m "feat(order-service): add outbox polling publisher for order.events"
```

---

### Task 4: kitchen-service — consume `OrderCreated`, create `Ticket` idempotently

**Files:**
- Modify: `ftgo-kitchen-service/build.gradle`
- Modify: `ftgo-kitchen-service/src/main/resources/application.yml`
- Modify: `ftgo-kitchen-service/src/test/resources/application.yml`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRepository.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEventRepository.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OrderCreatedEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`

**Interfaces:**
- Consumes: Kafka topic `order.events` (Task 3), JSON shape matching order-service's `OrderCreatedEvent` (Task 2)
- Produces: `TicketService.handleOrderCreated(OrderCreatedEvent event): void` — called by `OrderEventListener`, directly unit-tested

- [ ] **Step 1: Add `spring-kafka` dependency**

Replace `ftgo-kitchen-service/build.gradle`:

```groovy
dependencies {
    implementation 'org.springframework.kafka:spring-kafka'
}
```

- [ ] **Step 2: Write the failing test**

Create `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TicketServiceTest {

    private final TicketRepository ticketRepository = mock(TicketRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final TicketService ticketService = new TicketService(ticketRepository, processedEventRepository);

    private final OrderCreatedEvent event = new OrderCreatedEvent(
            "event-1", "OrderCreated", 42L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 2)));

    @Test
    void createsTicketOnFirstDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository).save(any());
        verify(ticketRepository).save(any());
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(true);

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository, never()).save(any());
        verify(ticketRepository, never()).save(any());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: FAIL — compile error, none of the referenced classes exist yet.

- [ ] **Step 4: Create `Ticket`**

Create `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "tickets")
public class Ticket {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    private String status;

    protected Ticket() {
    }

    public Ticket(Long orderId, String status) {
        this.orderId = orderId;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getStatus() {
        return status;
    }
}
```

- [ ] **Step 5: Create `TicketRepository`**

Create `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRepository.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TicketRepository extends JpaRepository<Ticket, Long> {
}
```

- [ ] **Step 6: Create `ProcessedEvent`**

Create `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEvent.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId) {
        this.eventId = eventId;
        this.processedAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
```

- [ ] **Step 7: Create `ProcessedEventRepository`**

Create `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEventRepository.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
```

- [ ] **Step 8: Create `OrderCreatedEvent`**

Create `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OrderCreatedEvent.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import java.util.List;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long restaurantId,
        List<LineItem> lineItems) {

    public record LineItem(Long menuItemId, int quantity) {
    }
}
```

- [ ] **Step 9: Create `TicketService`**

Create `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TicketService {

    private final TicketRepository ticketRepository;
    private final ProcessedEventRepository processedEventRepository;

    public TicketService(TicketRepository ticketRepository, ProcessedEventRepository processedEventRepository) {
        this.ticketRepository = ticketRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));
        ticketRepository.save(new Ticket(event.orderId(), "CREATED"));
    }
}
```

- [ ] **Step 10: Run test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: PASS

- [ ] **Step 11: Create the Kafka listener adapter**

Create `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java`:

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.OrderCreatedEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
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
        ticketService.handleOrderCreated(event);
    }
}
```

- [ ] **Step 12: Add Kafka consumer config to `application.yml`**

Replace `ftgo-kitchen-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: ftgo-kitchen-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_kitchen
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: kitchen-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest

server:
  port: 8083
```

- [ ] **Step 13: Add the same Kafka consumer config to the test `application.yml`**

Replace `ftgo-kitchen-service/src/test/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MySQL
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: create-drop
    database-platform: org.hibernate.dialect.H2Dialect
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: kitchen-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
```

- [ ] **Step 14: Run the full kitchen-service test suite**

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: PASS. `FtgoKitchenServiceApplicationTests` (`@SpringBootTest`) now boots the `OrderEventListener`'s Kafka consumer container; with no broker reachable it logs connection-retry warnings in the background but does not fail context startup (matches existing `spring-kafka` behavior — `spring.kafka.listener.missing-topics-fatal` defaults to `false`).

- [ ] **Step 15: Commit**

```bash
git add ftgo-kitchen-service/build.gradle \
        ftgo-kitchen-service/src/main/resources/application.yml \
        ftgo-kitchen-service/src/test/resources/application.yml \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRepository.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEventRepository.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OrderCreatedEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OrderEventListener.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java
git commit -m "feat(kitchen-service): consume OrderCreated and create Ticket idempotently"
```

---

### Task 5: docker-compose — fix Kafka networking, add kitchen-service

The current `compose.yml` Kafka config advertises `PLAINTEXT://localhost:9092`, which only resolves correctly for host-side clients. Containers (order-service, kitchen-service) resolving `localhost` would hit themselves, not the Kafka broker. This task adds a dual-listener setup (internal Docker-network listener + external host listener) and wires the two application services to the internal one.

**Files:**
- Create: `ftgo-kitchen-service/Dockerfile`
- Modify: `compose.yml`

- [ ] **Step 1: Create `ftgo-kitchen-service/Dockerfile`**

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-kitchen-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-kitchen-service/build/libs/*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 2: Update `compose.yml`**

Replace the full file:

```yaml
services:

  mysql:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_USER: ftgo
      MYSQL_PASSWORD: ftgo
    ports:
      - "3306:3306"
    volumes:
      - mysql-data:/var/lib/mysql
      - ./infrastructure/mysql/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "ftgo", "-pftgo"]
      interval: 10s
      timeout: 5s
      retries: 5

  zookeeper:
    image: confluentinc/cp-zookeeper:7.9.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.9.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  restaurant-service:
    build:
      context: .
      dockerfile: ftgo-restaurant-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
    ports:
      - "8085:8085"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_restaurant

  order-service:
    build:
      context: .
      dockerfile: ftgo-order-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      restaurant-service:
        condition: service_started
    ports:
      - "8082:8082"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order
      RESTAURANT_SERVICE_BASE_URL: http://restaurant-service:8085
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092

  kitchen-service:
    build:
      context: .
      dockerfile: ftgo-kitchen-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
    ports:
      - "8083:8083"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_kitchen
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092

volumes:
  mysql-data:
```

- [ ] **Step 3: Rebuild and start the stack**

Run: `docker compose up -d --build`
Expected: all five containers (`mysql`, `zookeeper`, `kafka`, `restaurant-service`, `order-service`, `kitchen-service`) start; `docker compose ps` shows all `Up`.

- [ ] **Step 4: Commit**

```bash
git add ftgo-kitchen-service/Dockerfile compose.yml
git commit -m "feat: wire kitchen-service into docker compose, fix Kafka dual-listener networking"
```

---

### Task 6: manual end-to-end verification

Not a code change — confirms the outbox → Kafka → consumer pipeline actually works, and that at-least-once delivery is handled correctly.

- [ ] **Step 1: Start the stack fresh**

```bash
docker compose down
docker compose up -d --build
sleep 15
docker compose ps
```
Expected: all six containers `Up`.

- [ ] **Step 2: Place an order**

```bash
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 2}]}' | jq
```
Expected: `201`-shaped JSON body with an order `id`.

- [ ] **Step 3: Confirm the outbox row was written, then published**

```bash
docker compose exec mysql mysql -uftgo -pftgo ftgo_order -e "SELECT id, event_type, sent_at FROM outbox_events ORDER BY id DESC LIMIT 1;"
```
Expected immediately after the order: `sent_at` is `NULL`. Re-run a few seconds later (after the 2s poll interval): `sent_at` is populated.

- [ ] **Step 4: Confirm kitchen-service created a `Ticket`**

```bash
docker compose exec mysql mysql -uftgo -pftgo ftgo_kitchen -e "SELECT * FROM tickets;"
```
Expected: one row with the `order_id` matching Step 2's order.

- [ ] **Step 5: Verify duplicate delivery is deduped**

Reset the outbox row's `sent_at` to force a re-publish of the same event:
```bash
docker compose exec mysql mysql -uftgo -pftgo ftgo_order -e "UPDATE outbox_events SET sent_at = NULL ORDER BY id DESC LIMIT 1;"
```
Wait ~5 seconds for the poller to re-publish, then:
```bash
docker compose exec mysql mysql -uftgo -pftgo ftgo_kitchen -e "SELECT COUNT(*) FROM tickets;"
```
Expected: ticket count is unchanged (still 1) — `processed_events` deduped the redelivery.

- [ ] **Step 6: Update `CONTEXT.md` and session docs**

Update the Communication patterns checklist (mark messaging + transactional outbox as done), the services table, and add a session log entry noting this work and its date. Follow the existing format in `CONTEXT.md`.
