# Create Order Saga (Choreography) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Ch.4 Create Order saga using choreography: order-service, consumer-service, kitchen-service, and accounting-service coordinate purely via Kafka events (no orchestrator) to move an `Order` from `APPROVAL_PENDING` to `APPROVED` or `REJECTED`, with real compensating transactions for all three failure points.

**Architecture:** Each of the four services keeps its own outbox table and publishes to its own Kafka topic (`order.events`, `consumer.events`, `kitchen.events`, `accounting.events`), extending the Ch.3 transactional-outbox pattern uniformly. Every consumer dedupes via a local `processed_events` ledger. accounting-service's card-authorization step is a *join* — it waits for both `ConsumerVerified` and `TicketCreated` (in either order) via a local `saga_join_state` row before acting.

**Tech Stack:** Spring Boot 3.5 / Java 21, Spring Data JPA, Spring Kafka, MySQL 8.4 (H2 for tests), Jackson.

## Global Constraints

- Spec source: `docs/superpowers/specs/2026-07-17-ch4-create-order-saga-choreography-design.md`. Follow it for architecture/compensation flows; this plan resolves two gaps the spec left implicit:
  - **`consumerId` did not previously exist anywhere in the schema.** `CreateOrderRequest` → `Order` → `OrderCreatedEvent` all gain a required `consumerId` field (Task 1) so consumer-service has something to verify.
  - **The spec's "order total under a threshold" accounting rule assumed a price, which no saga event carries** (order-service validates prices against restaurant-service but never persists them). This plan substitutes **total line-item quantity** as the threshold input throughout: kitchen-service declines ticket creation above `KITCHEN_CAPACITY_LIMIT = 20`, accounting-service declines authorization above `AUTHORIZATION_QUANTITY_LIMIT = 10`. Distinct thresholds make both failure paths independently triggerable by order size alone during manual verification.
- New services (consumer-service, accounting-service) get **no** Eureka client and **no** synchronous REST calls — they participate purely via Kafka, consistent with the design's choreography scope.
- Every new JPA entity, repository, outbox/dedup table, and Kafka producer/consumer config must mirror the existing Ch.3 shapes in `ftgo-order-service` and `ftgo-kitchen-service` exactly (same field names, same `@Scheduled` outbox-poller shape, same `processed_events` dedup pattern) — do not introduce a new framework (no Eventuate Tram).
- All new unit tests use plain Mockito mocks in the existing style (see `OrderServiceTest`, `TicketServiceTest`, `OutboxPublisherTest`) — no `@DataJpaTest`/`@SpringBootTest` for business logic. Thin Kafka `@KafkaListener` classes that just parse-and-delegate are not unit tested individually, matching the existing convention (`OrderEventListener` in kitchen-service has no dedicated test) — they're covered by the Task 9 manual e2e pass.
- Databases `ftgo_consumer` and `ftgo_accounting` already exist (`infrastructure/mysql/init.sql`), and `ddl-auto: update` is already the project convention — no migration tooling needed.

---

## Task 1: order-service — `APPROVAL_PENDING` status and `consumerId`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderStatus.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/CreateOrderRequest.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderResponse.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`

**Interfaces:**
- Produces: `Order.getConsumerId(): Long`, `Order.markApproved(): void`, `Order.markRejected(): void`, `OrderStatus.APPROVAL_PENDING`, `OrderService.createOrder(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems): Order` (signature changes — `consumerId` is now the first parameter). Later tasks (Task 7) consume `Order.markApproved()`/`markRejected()` and the `APPROVAL_PENDING` status.

- [ ] **Step 1: Update `OrderServiceTest` to expect `APPROVAL_PENDING` and a `consumerId` — run it to see it fail**

Replace the file:

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
    void createsOrderInApprovalPendingWhenRestaurantAndMenuItemsAreValid() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Order order = orderService.createOrder(1L, 1L, List.of(new OrderLineItem(10L, 2)));

        assertThat(order.getConsumerId()).isEqualTo(1L);
        assertThat(order.getRestaurantId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
    }

    @Test
    void writesOutboxEventWhenOrderIsCreated() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orderService.createOrder(1L, 1L, List.of(new OrderLineItem(10L, 2)));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent savedEvent = captor.getValue();
        assertThat(savedEvent.getEventType()).isEqualTo("OrderCreated");
        assertThat(savedEvent.getPayload()).contains("\"consumerId\":1");
        assertThat(savedEvent.getPayload()).contains("\"restaurantId\":1");
        assertThat(savedEvent.isSent()).isFalse();
    }

    @Test
    void rejectsOrderWhenMenuItemDoesNotBelongToRestaurant() {
        assertThatThrownBy(() -> orderService.createOrder(1L, 1L, List.of(new OrderLineItem(999L, 1))))
                .isInstanceOf(MenuItemNotFoundException.class);
    }
}
```

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: FAIL (compile error — `createOrder` doesn't accept 3 args yet, `getConsumerId`/`APPROVAL_PENDING` don't exist).

- [ ] **Step 2: Add `APPROVAL_PENDING` to `OrderStatus`**

```java
package com.sanjay.ftgo.order.domain;

public enum OrderStatus {
    APPROVAL_PENDING,
    APPROVED,
    REJECTED
}
```

- [ ] **Step 3: Add `consumerId` and mutators to `Order`**

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

    public void markApproved() {
        this.status = OrderStatus.APPROVED;
    }

    public void markRejected() {
        this.status = OrderStatus.REJECTED;
    }
}
```

- [ ] **Step 4: Add `consumerId` to `OrderCreatedEvent`**

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        Long restaurantId,
        List<LineItem> lineItems) {

    public record LineItem(Long menuItemId, int quantity) {
    }

    public static OrderCreatedEvent from(Order order, String eventId) {
        List<LineItem> items = order.getLineItems().stream()
                .map(lineItem -> new LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
        return new OrderCreatedEvent(eventId, "OrderCreated", order.getId(), order.getConsumerId(), order.getRestaurantId(), items);
    }
}
```

- [ ] **Step 5: Update `OrderService.createOrder` to take `consumerId` and create in `APPROVAL_PENDING`**

In `OrderService.java`, replace the `createOrder` method:

```java
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
        OrderCreatedEvent event = OrderCreatedEvent.from(order, eventId);
        outboxEventRepository.save(new OutboxEvent(eventId, "OrderCreated", order.getId(), toJson(event)));

        return order;
    }
```

(The rest of the class — fields, constructor, `toJson` — is unchanged.)

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: PASS.

- [ ] **Step 6: Update `CreateOrderRequest`, `OrderController`, `OrderResponse`**

`CreateOrderRequest.java`:

```java
package com.sanjay.ftgo.order.api;

import java.util.List;

public record CreateOrderRequest(Long consumerId, Long restaurantId, List<LineItemRequest> lineItems) {

    public record LineItemRequest(Long menuItemId, int quantity) {
    }
}
```

`OrderController.java` — replace the `createOrder` method body:

```java
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
```

`OrderResponse.java`:

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.Order;

import java.util.List;

public record OrderResponse(Long id, Long consumerId, Long restaurantId, List<LineItemResponse> lineItems, String status) {

    public record LineItemResponse(Long menuItemId, int quantity) {
    }

    public static OrderResponse from(Order order) {
        List<LineItemResponse> items = order.getLineItems().stream()
                .map(lineItem -> new LineItemResponse(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
        return new OrderResponse(order.getId(), order.getConsumerId(), order.getRestaurantId(), items, order.getStatus().name());
    }
}
```

- [ ] **Step 7: Update `OrderControllerTest` to include `consumerId` — run to verify it passes**

Replace the file:

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

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
}
```

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (all order-service tests, including `OutboxPublisherTest`, `RestaurantServiceProxyTest`, `FtgoOrderServiceApplicationTests`, which are untouched by this task).

- [ ] **Step 8: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderStatus.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderCreatedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/CreateOrderRequest.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderResponse.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java
git commit -m "feat(order-service): add consumerId and APPROVAL_PENDING status for Create Order saga"
```

---

## Task 2: kitchen-service — outbox infrastructure + capacity-aware ticket creation

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRepository.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/FailedOrder.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/FailedOrderRepository.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEventRepository.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/KitchenEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KafkaProducerConfig.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisher.java`
- Modify: `ftgo-kitchen-service/src/main/resources/application.yml`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisherTest.java`

**Interfaces:**
- Consumes: `OrderCreatedEvent(eventId, eventType, orderId, restaurantId, lineItems)` (unchanged, already exists in this package).
- Produces: `Ticket.markAwaitingAcceptance()`, `Ticket.markCancelled()`, `TicketRepository.findByOrderId(Long): Optional<Ticket>` — used by Task 3 and Task 4. `KitchenEvent` record shape (`eventId, eventType, orderId, ticketId, totalQuantity, reason`) — used by Task 6 (accounting-service) and Task 7 (order-service) as the wire format for `kitchen.events`.

- [ ] **Step 1: Write the failing `TicketServiceTest` covering the new capacity/pre-failure behavior**

Replace the file:

```java
package com.sanjay.ftgo.kitchen.domain;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TicketServiceTest {

    private final TicketRepository ticketRepository = mock(TicketRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final FailedOrderRepository failedOrderRepository = mock(FailedOrderRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

    private final TicketService ticketService = new TicketService(
            ticketRepository, processedEventRepository, failedOrderRepository, outboxEventRepository, objectMapper);

    private final OrderCreatedEvent event = new OrderCreatedEvent(
            "event-1", "OrderCreated", 42L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 2)));

    @Test
    void createsTicketInCreatePendingOnFirstDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository).save(any());
        verify(ticketRepository).save(argThatStatusIs("CREATE_PENDING"));
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketCreated"));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(true);

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository, never()).save(any());
        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void createsTicketDirectlyAsCancelledWhenOrderAlreadyFailed() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(true);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(ticketRepository).save(argThatStatusIs("CANCELLED"));
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void rejectsTicketCreationWhenQuantityExceedsKitchenCapacity() {
        OrderCreatedEvent bigEvent = new OrderCreatedEvent(
                "event-2", "OrderCreated", 43L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 25)));
        when(processedEventRepository.existsById("event-2")).thenReturn(false);
        when(failedOrderRepository.existsById(43L)).thenReturn(false);

        ticketService.handleOrderCreated(bigEvent);

        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketCreationFailed"));
    }

    private Ticket argThatStatusIs(String status) {
        return org.mockito.ArgumentMatchers.argThat(t -> t != null && status.equals(t.getStatus()));
    }

    private OutboxEvent argThatEventTypeIs(String eventType) {
        return org.mockito.ArgumentMatchers.argThat(e -> e != null && eventType.equals(e.getEventType()));
    }
}
```

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: FAIL (compile error — `FailedOrderRepository`, `OutboxEventRepository`, 5-arg `TicketService` constructor don't exist yet).

- [ ] **Step 2: Add `FailedOrder` entity and repository**

```java
package com.sanjay.ftgo.kitchen.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "failed_orders")
public class FailedOrder {

    @Id
    private Long orderId;

    protected FailedOrder() {
    }

    public FailedOrder(Long orderId) {
        this.orderId = orderId;
    }

    public Long getOrderId() {
        return orderId;
    }
}
```

```java
package com.sanjay.ftgo.kitchen.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FailedOrderRepository extends JpaRepository<FailedOrder, Long> {
}
```

- [ ] **Step 3: Add `OutboxEvent` entity and repository (mirrors order-service's)**

```java
package com.sanjay.ftgo.kitchen.domain;

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

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "sent_at")
    private Instant sentAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String eventType, Long orderId, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
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

```java
package com.sanjay.ftgo.kitchen.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findBySentAtIsNullOrderByIdAsc();
}
```

- [ ] **Step 4: Add the `KitchenEvent` record**

```java
package com.sanjay.ftgo.kitchen.domain;

public record KitchenEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long ticketId,
        Integer totalQuantity,
        String reason) {
}
```

- [ ] **Step 5: Add `markAwaitingAcceptance()`/`markCancelled()` to `Ticket` and `findByOrderId` to `TicketRepository`**

Replace `Ticket.java`:

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

    public void markAwaitingAcceptance() {
        this.status = "AWAITING_ACCEPTANCE";
    }

    public void markCancelled() {
        this.status = "CANCELLED";
    }
}
```

Replace `TicketRepository.java`:

```java
package com.sanjay.ftgo.kitchen.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TicketRepository extends JpaRepository<Ticket, Long> {

    Optional<Ticket> findByOrderId(Long orderId);
}
```

- [ ] **Step 6: Rewrite `TicketService` — capacity check, pre-failure check, outbox publish**

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

        if (totalQuantity > KITCHEN_CAPACITY_LIMIT) {
            publishEvent("TicketCreationFailed", event.orderId(), null, totalQuantity,
                    "order exceeds kitchen capacity");
            return;
        }

        Ticket ticket = ticketRepository.save(new Ticket(event.orderId(), "CREATE_PENDING"));
        publishEvent("TicketCreated", event.orderId(), ticket.getId(), totalQuantity, null);
    }

    private void publishEvent(String eventType, Long orderId, Long ticketId, Integer totalQuantity, String reason) {
        String eventId = UUID.randomUUID().toString();
        KitchenEvent event = new KitchenEvent(eventId, eventType, orderId, ticketId, totalQuantity, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, toJson(event)));
    }

    private String toJson(KitchenEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + event.eventType() + " for order " + event.orderId(), e);
        }
    }
}
```

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: PASS.

- [ ] **Step 7: Add `KafkaProducerConfig` and `OutboxPublisher` (mirrors order-service's, topic `kitchen.events`, no conditional gating)**

```java
package com.sanjay.ftgo.kitchen.infrastructure;

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
    public ProducerFactory<String, String> kitchenEventProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kitchenEventKafkaTemplate(ProducerFactory<String, String> kitchenEventProducerFactory) {
        return new KafkaTemplate<>(kitchenEventProducerFactory);
    }
}
```

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.sanjay.ftgo.kitchen.domain.OutboxEvent;
import com.sanjay.ftgo.kitchen.domain.OutboxEventRepository;
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
    private static final String TOPIC = "kitchen.events";

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
                kafkaTemplate.send(TOPIC, String.valueOf(event.getOrderId()), event.getPayload()).get();
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }
}
```

- [ ] **Step 8: Write `OutboxPublisherTest` (mirrors order-service's)**

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.sanjay.ftgo.kitchen.domain.OutboxEvent;
import com.sanjay.ftgo.kitchen.domain.OutboxEventRepository;
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
        OutboxEvent event = new OutboxEvent("event-1", "TicketCreated", 100L, "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("kitchen.events"), eq("100"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isTrue();
        verify(outboxEventRepository).save(event);
    }

    @Test
    void leavesEventUnsentWhenPublishFails() {
        OutboxEvent event = new OutboxEvent("event-2", "TicketCreated", 200L, "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(eq("kitchen.events"), eq("200"), eq("{}"))).thenReturn(failed);

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isFalse();
        verify(outboxEventRepository, never()).save(any());
    }
}
```

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: PASS (all kitchen-service tests).

- [ ] **Step 9: Add outbox config to `application.yml`**

Append to `ftgo-kitchen-service/src/main/resources/application.yml`:

```yaml
outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20
```

- [ ] **Step 10: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketRepository.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/FailedOrder.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/FailedOrderRepository.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEventRepository.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/KitchenEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KafkaProducerConfig.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisher.java \
        ftgo-kitchen-service/src/main/resources/application.yml \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisherTest.java
git commit -m "feat(kitchen-service): publish TicketCreated/TicketCreationFailed via outbox, gated on kitchen capacity"
```

---

*Continued in Tasks 3–9 below — each is self-contained and independently testable.*

## Task 3: kitchen-service — react to `accounting.events` (confirm or cancel the ticket)

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/AccountingEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/AccountingEventListener.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`

**Interfaces:**
- Consumes: `AccountingEvent(eventId, eventType, orderId, reason)` on topic `accounting.events`, `eventType` one of `"CardAuthorized"` / `"CardAuthorizationFailed"` (produced by Task 6).
- Produces: `TicketService.handleAccountingEvent(String eventId, Long orderId, String eventType): void`.

- [ ] **Step 1: Add the failing test cases to `TicketServiceTest`**

Add these two `@Test` methods to the existing class (keep everything else in the file unchanged):

```java
    @Test
    void confirmsTicketWhenCardAuthorized() {
        Ticket ticket = new Ticket(42L, "CREATE_PENDING");
        when(processedEventRepository.existsById("acct-event-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-1", 42L, "CardAuthorized");

        assertThat(ticket.getStatus()).isEqualTo("AWAITING_ACCEPTANCE");
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketConfirmed"));
    }

    @Test
    void cancelsTicketWhenCardAuthorizationFailed() {
        Ticket ticket = new Ticket(42L, "CREATE_PENDING");
        when(processedEventRepository.existsById("acct-event-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-2", 42L, "CardAuthorizationFailed");

        assertThat(ticket.getStatus()).isEqualTo("CANCELLED");
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketCancelled"));
    }
```

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: FAIL (compile error — `handleAccountingEvent` doesn't exist).

- [ ] **Step 2: Add the `AccountingEvent` record**

```java
package com.sanjay.ftgo.kitchen.domain;

public record AccountingEvent(
        String eventId,
        String eventType,
        Long orderId,
        String reason) {
}
```

- [ ] **Step 3: Add `handleAccountingEvent` to `TicketService`**

Add this method to `TicketService` (alongside `handleOrderCreated`):

```java
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
```

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: PASS.

- [ ] **Step 4: Add `AccountingEventListener`**

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.AccountingEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.events", groupId = "kitchen-service")
    public void onMessage(String payload) {
        AccountingEvent event;
        try {
            event = objectMapper.readValue(payload, AccountingEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting event: {}", payload, e);
            return;
        }
        ticketService.handleAccountingEvent(event.eventId(), event.orderId(), event.eventType());
    }
}
```

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/AccountingEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/AccountingEventListener.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java
git commit -m "feat(kitchen-service): confirm or cancel ticket on accounting.events"
```

---

## Task 4: kitchen-service — react to `consumer.events` (cancel or pre-empt a ticket)

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ConsumerVerificationEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/ConsumerEventListener.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`

**Interfaces:**
- Consumes: `ConsumerVerificationEvent(eventId, eventType, orderId, consumerId, reason)` on topic `consumer.events`, only reacts to `eventType == "ConsumerVerificationFailed"` (produced by Task 5).
- Produces: `TicketService.handleConsumerVerificationFailed(String eventId, Long orderId): void`.

- [ ] **Step 1: Add failing test cases to `TicketServiceTest`**

Add these two `@Test` methods:

```java
    @Test
    void cancelsExistingTicketWhenConsumerVerificationFails() {
        Ticket ticket = new Ticket(42L, "CREATE_PENDING");
        when(processedEventRepository.existsById("cons-event-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleConsumerVerificationFailed("cons-event-1", 42L);

        assertThat(ticket.getStatus()).isEqualTo("CANCELLED");
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketCancelled"));
        verify(failedOrderRepository, never()).save(any());
    }

    @Test
    void recordsFailedOrderWhenNoTicketExistsYet() {
        when(processedEventRepository.existsById("cons-event-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(43L)).thenReturn(Optional.empty());

        ticketService.handleConsumerVerificationFailed("cons-event-2", 43L);

        verify(failedOrderRepository).save(any());
        verify(outboxEventRepository, never()).save(any());
    }
```

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: FAIL (compile error — `handleConsumerVerificationFailed` doesn't exist).

- [ ] **Step 2: Add the `ConsumerVerificationEvent` record**

```java
package com.sanjay.ftgo.kitchen.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
```

- [ ] **Step 3: Add `handleConsumerVerificationFailed` to `TicketService`**

```java
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
```

Run: `./gradlew :ftgo-kitchen-service:test --tests "com.sanjay.ftgo.kitchen.domain.TicketServiceTest"`
Expected: PASS.

- [ ] **Step 4: Add `ConsumerEventListener`**

```java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "kitchen-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        if ("ConsumerVerificationFailed".equals(event.eventType())) {
            ticketService.handleConsumerVerificationFailed(event.eventId(), event.orderId());
        }
    }
}
```

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ConsumerVerificationEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/ConsumerEventListener.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java
git commit -m "feat(kitchen-service): cancel or pre-empt ticket on ConsumerVerificationFailed"
```

---

## Task 5: consumer-service — first real code (verify consumer, publish `consumer.events`)

**Files:**
- Modify: `ftgo-consumer-service/build.gradle`
- Modify: `ftgo-consumer-service/src/main/resources/application.yml`
- Modify: `ftgo-consumer-service/src/test/resources/application.yml`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/Consumer.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerRepository.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ProcessedEvent.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ProcessedEventRepository.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OutboxEvent.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OutboxEventRepository.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OrderCreatedEvent.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationEvent.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/DataSeeder.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/KafkaProducerConfig.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisher.java`
- Create: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/OrderEventListener.java`
- Test: `ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationServiceTest.java`
- Test: `ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisherTest.java`

**Interfaces:**
- Consumes: `OrderCreatedEvent(eventId, eventType, orderId, consumerId)` on topic `order.events` (subset of order-service's fields — Jackson ignores the extra `restaurantId`/`lineItems` fields by default).
- Produces: `ConsumerVerificationEvent(eventId, eventType, orderId, consumerId, reason)` on topic `consumer.events`, consumed by Task 3, Task 4, and Task 7.

- [ ] **Step 1: Add `spring-kafka` dependency**

Replace `ftgo-consumer-service/build.gradle`:

```groovy
dependencies {
    implementation 'org.springframework.kafka:spring-kafka'
}
```

- [ ] **Step 2: Write the failing `ConsumerVerificationServiceTest`**

```java
package com.sanjay.ftgo.consumer.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConsumerVerificationServiceTest {

    private final ConsumerRepository consumerRepository = mock(ConsumerRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ConsumerVerificationService service = new ConsumerVerificationService(
            consumerRepository, processedEventRepository, outboxEventRepository, objectMapper);

    private final OrderCreatedEvent event = new OrderCreatedEvent("event-1", "OrderCreated", 42L, 1L);

    @Test
    void publishesConsumerVerifiedWhenConsumerIsActive() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Sanjay", true)));

        service.handleOrderCreated(event);

        verify(outboxEventRepository).save(argThat(e -> "ConsumerVerified".equals(e.getEventType())));
    }

    @Test
    void publishesConsumerVerificationFailedWhenConsumerIsInactive() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Blocked Consumer", false)));

        service.handleOrderCreated(event);

        verify(outboxEventRepository).save(argThat(e -> "ConsumerVerificationFailed".equals(e.getEventType())));
    }

    @Test
    void publishesConsumerVerificationFailedWhenConsumerNotFound() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.empty());

        service.handleOrderCreated(event);

        verify(outboxEventRepository).save(argThat(e -> "ConsumerVerificationFailed".equals(e.getEventType())));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(true);

        service.handleOrderCreated(event);

        verify(outboxEventRepository, never()).save(any());
        verify(consumerRepository, never()).findById(any());
    }
}
```

Run: `./gradlew :ftgo-consumer-service:test --tests "com.sanjay.ftgo.consumer.domain.ConsumerVerificationServiceTest"`
Expected: FAIL (compile error — none of the referenced classes exist yet).

- [ ] **Step 3: Add `Consumer` entity and repository**

```java
package com.sanjay.ftgo.consumer.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "consumers")
public class Consumer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private boolean active;

    protected Consumer() {
    }

    public Consumer(Long id, String name, boolean active) {
        this.id = id;
        this.name = name;
        this.active = active;
    }

    public Consumer(String name, boolean active) {
        this(null, name, active);
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean isActive() {
        return active;
    }
}
```

```java
package com.sanjay.ftgo.consumer.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsumerRepository extends JpaRepository<Consumer, Long> {
}
```

- [ ] **Step 4: Add `ProcessedEvent`/`ProcessedEventRepository` (mirrors kitchen-service's)**

```java
package com.sanjay.ftgo.consumer.domain;

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

```java
package com.sanjay.ftgo.consumer.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
```

- [ ] **Step 5: Add `OutboxEvent`/`OutboxEventRepository` (mirrors order-service's)**

```java
package com.sanjay.ftgo.consumer.domain;

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

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "sent_at")
    private Instant sentAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String eventType, Long orderId, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
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

```java
package com.sanjay.ftgo.consumer.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findBySentAtIsNullOrderByIdAsc();
}
```

- [ ] **Step 6: Add the `OrderCreatedEvent` and `ConsumerVerificationEvent` records**

```java
package com.sanjay.ftgo.consumer.domain;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId) {
}
```

```java
package com.sanjay.ftgo.consumer.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
```

- [ ] **Step 7: Add `ConsumerVerificationService`**

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

        Consumer consumer = consumerRepository.findById(event.consumerId()).orElse(null);
        if (consumer == null) {
            publishEvent("ConsumerVerificationFailed", event.orderId(), event.consumerId(), "consumer not found");
        } else if (!consumer.isActive()) {
            publishEvent("ConsumerVerificationFailed", event.orderId(), event.consumerId(), "consumer is not active");
        } else {
            publishEvent("ConsumerVerified", event.orderId(), event.consumerId(), null);
        }
    }

    private void publishEvent(String eventType, Long orderId, Long consumerId, String reason) {
        String eventId = UUID.randomUUID().toString();
        ConsumerVerificationEvent event = new ConsumerVerificationEvent(eventId, eventType, orderId, consumerId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, toJson(event)));
    }

    private String toJson(ConsumerVerificationEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + event.eventType() + " for order " + event.orderId(), e);
        }
    }
}
```

Run: `./gradlew :ftgo-consumer-service:test --tests "com.sanjay.ftgo.consumer.domain.ConsumerVerificationServiceTest"`
Expected: PASS.

- [ ] **Step 8: Add `DataSeeder` (mirrors restaurant-service's) — seeds consumer id 1 (active) and id 2 (inactive)**

```java
package com.sanjay.ftgo.consumer.infrastructure;

import com.sanjay.ftgo.consumer.domain.Consumer;
import com.sanjay.ftgo.consumer.domain.ConsumerRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class DataSeeder implements CommandLineRunner {

    private final ConsumerRepository consumerRepository;

    public DataSeeder(ConsumerRepository consumerRepository) {
        this.consumerRepository = consumerRepository;
    }

    @Override
    public void run(String... args) {
        if (consumerRepository.count() > 0) {
            return;
        }
        consumerRepository.save(new Consumer("Sanjay", true));
        consumerRepository.save(new Consumer("Blocked Consumer", false));
    }
}
```

- [ ] **Step 9: Add `KafkaProducerConfig` and `OutboxPublisher` (topic `consumer.events`)**

```java
package com.sanjay.ftgo.consumer.infrastructure;

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
    public ProducerFactory<String, String> consumerEventProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> consumerEventKafkaTemplate(ProducerFactory<String, String> consumerEventProducerFactory) {
        return new KafkaTemplate<>(consumerEventProducerFactory);
    }
}
```

```java
package com.sanjay.ftgo.consumer.infrastructure;

import com.sanjay.ftgo.consumer.domain.OutboxEvent;
import com.sanjay.ftgo.consumer.domain.OutboxEventRepository;
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
    private static final String TOPIC = "consumer.events";

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
                kafkaTemplate.send(TOPIC, String.valueOf(event.getOrderId()), event.getPayload()).get();
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }
}
```

- [ ] **Step 10: Write `OutboxPublisherTest`**

```java
package com.sanjay.ftgo.consumer.infrastructure;

import com.sanjay.ftgo.consumer.domain.OutboxEvent;
import com.sanjay.ftgo.consumer.domain.OutboxEventRepository;
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
        OutboxEvent event = new OutboxEvent("event-1", "ConsumerVerified", 100L, "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("consumer.events"), eq("100"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isTrue();
        verify(outboxEventRepository).save(event);
    }

    @Test
    void leavesEventUnsentWhenPublishFails() {
        OutboxEvent event = new OutboxEvent("event-2", "ConsumerVerified", 200L, "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(eq("consumer.events"), eq("200"), eq("{}"))).thenReturn(failed);

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isFalse();
        verify(outboxEventRepository, never()).save(any());
    }
}
```

- [ ] **Step 11: Add `OrderEventListener`**

```java
package com.sanjay.ftgo.consumer.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.consumer.domain.ConsumerVerificationService;
import com.sanjay.ftgo.consumer.domain.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final ConsumerVerificationService consumerVerificationService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(ConsumerVerificationService consumerVerificationService, ObjectMapper objectMapper) {
        this.consumerVerificationService = consumerVerificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.events", groupId = "consumer-service")
    public void onMessage(String payload) {
        OrderCreatedEvent event;
        try {
            event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed order event: {}", payload, e);
            return;
        }
        consumerVerificationService.handleOrderCreated(event);
    }
}
```

- [ ] **Step 12: Update `application.yml` (main and test) with Kafka config**

Replace `ftgo-consumer-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: ftgo-consumer-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_consumer
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: consumer-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest

server:
  port: 8081

outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20
```

Replace `ftgo-consumer-service/src/test/resources/application.yml`:

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
      group-id: consumer-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
```

Run: `./gradlew :ftgo-consumer-service:test`
Expected: PASS (all consumer-service tests, including the pre-existing `FtgoConsumerServiceApplicationTests` context-load test).

- [ ] **Step 13: Commit**

```bash
git add ftgo-consumer-service/
git commit -m "feat(consumer-service): verify consumer and publish ConsumerVerified/Failed via outbox"
```

---

## Task 6: accounting-service — first real code (join on consumer+kitchen events, authorize card)

**Files:**
- Modify: `ftgo-accounting-service/build.gradle`
- Modify: `ftgo-accounting-service/src/main/resources/application.yml`
- Modify: `ftgo-accounting-service/src/test/resources/application.yml`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/Authorization.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationRepository.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinState.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinStateRepository.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/ProcessedEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/ProcessedEventRepository.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/OutboxEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/OutboxEventRepository.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/ConsumerVerificationEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/KitchenEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AccountingEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KafkaProducerConfig.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisher.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/ConsumerEventListener.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java`
- Test: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java`
- Test: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisherTest.java`

**Interfaces:**
- Consumes: `ConsumerVerificationEvent` on `consumer.events` (Task 5), `KitchenEvent` on `kitchen.events` filtered to `TicketCreated`/`TicketCreationFailed` (Task 2).
- Produces: `AccountingEvent(eventId, eventType, orderId, reason)` on topic `accounting.events`, `eventType` one of `"CardAuthorized"`/`"CardAuthorizationFailed"` — consumed by Task 3 and Task 7.

- [ ] **Step 1: Add `spring-kafka` dependency**

Replace `ftgo-accounting-service/build.gradle`:

```groovy
dependencies {
    implementation 'org.springframework.kafka:spring-kafka'
}
```

- [ ] **Step 2: Write the failing `SagaJoinServiceTest`**

```java
package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaJoinServiceTest {

    private final SagaJoinStateRepository sagaJoinStateRepository = mock(SagaJoinStateRepository.class);
    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final SagaJoinService service = new SagaJoinService(
            sagaJoinStateRepository, authorizationRepository, processedEventRepository, outboxEventRepository, objectMapper);

    @Test
    void authorizesWhenConsumerVerifiedArrivesFirstThenTicketCreatedUnderLimit() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleConsumerEvent("e1", 42L, "ConsumerVerified");
        service.handleKitchenEvent("e2", 42L, "TicketCreated", 5);

        verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e -> "CardAuthorized".equals(e.getEventType())));
    }

    @Test
    void authorizesWhenTicketCreatedArrivesFirstThenConsumerVerified() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleKitchenEvent("e1", 42L, "TicketCreated", 5);
        service.handleConsumerEvent("e2", 42L, "ConsumerVerified");

        verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e -> "CardAuthorized".equals(e.getEventType())));
    }

    @Test
    void declinesWhenTotalQuantityExceedsLimit() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleConsumerEvent("e1", 42L, "ConsumerVerified");
        service.handleKitchenEvent("e2", 42L, "TicketCreated", 15);

        verify(authorizationRepository).save(argThat(a -> "DECLINED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e -> "CardAuthorizationFailed".equals(e.getEventType())));
    }

    @Test
    void abandonsJoinWhenConsumerVerificationFails() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleConsumerEvent("e1", 42L, "ConsumerVerificationFailed");
        service.handleKitchenEvent("e2", 42L, "TicketCreated", 5);

        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void abandonsJoinWhenTicketCreationFails() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleKitchenEvent("e1", 42L, "TicketCreationFailed", null);
        service.handleConsumerEvent("e2", 42L, "ConsumerVerified");

        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.handleConsumerEvent("e1", 42L, "ConsumerVerified");

        verify(sagaJoinStateRepository, never()).findById(any());
    }
}
```

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.SagaJoinServiceTest"`
Expected: FAIL (compile error — none of the referenced classes exist yet).

- [ ] **Step 3: Add `Authorization` entity and repository**

```java
package com.sanjay.ftgo.accounting.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "authorizations")
public class Authorization {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    private String status;

    protected Authorization() {
    }

    public Authorization(Long orderId, String status) {
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

```java
package com.sanjay.ftgo.accounting.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface AuthorizationRepository extends JpaRepository<Authorization, Long> {
}
```

- [ ] **Step 4: Add `SagaJoinState` entity and repository**

```java
package com.sanjay.ftgo.accounting.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "saga_join_state")
public class SagaJoinState {

    @Id
    private Long orderId;

    private boolean consumerVerified;
    private boolean ticketCreated;
    private boolean failed;
    private boolean resolved;
    private Integer totalQuantity;

    protected SagaJoinState() {
    }

    public SagaJoinState(Long orderId) {
        this.orderId = orderId;
        this.consumerVerified = false;
        this.ticketCreated = false;
        this.failed = false;
        this.resolved = false;
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

    public boolean isResolved() {
        return resolved;
    }

    public Integer getTotalQuantity() {
        return totalQuantity;
    }

    public void markConsumerVerified() {
        this.consumerVerified = true;
    }

    public void markTicketCreated(Integer totalQuantity) {
        this.ticketCreated = true;
        this.totalQuantity = totalQuantity;
    }

    public void markFailed() {
        this.failed = true;
    }

    public void markResolved() {
        this.resolved = true;
    }
}
```

```java
package com.sanjay.ftgo.accounting.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaJoinStateRepository extends JpaRepository<SagaJoinState, Long> {
}
```

- [ ] **Step 5: Add `ProcessedEvent`/`ProcessedEventRepository` and `OutboxEvent`/`OutboxEventRepository`**

```java
package com.sanjay.ftgo.accounting.domain;

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

```java
package com.sanjay.ftgo.accounting.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
```

```java
package com.sanjay.ftgo.accounting.domain;

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

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "sent_at")
    private Instant sentAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String eventType, Long orderId, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
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

```java
package com.sanjay.ftgo.accounting.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findBySentAtIsNullOrderByIdAsc();
}
```

- [ ] **Step 6: Add the `ConsumerVerificationEvent`, `KitchenEvent`, `AccountingEvent` records**

```java
package com.sanjay.ftgo.accounting.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
```

```java
package com.sanjay.ftgo.accounting.domain;

public record KitchenEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long ticketId,
        Integer totalQuantity,
        String reason) {
}
```

```java
package com.sanjay.ftgo.accounting.domain;

public record AccountingEvent(
        String eventId,
        String eventType,
        Long orderId,
        String reason) {
}
```

- [ ] **Step 7: Add `SagaJoinService`**

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

    private void tryResolve(SagaJoinState state) {
        if (!state.isConsumerVerified() || !state.isTicketCreated()) {
            return;
        }
        state.markResolved();
        sagaJoinStateRepository.save(state);

        boolean authorized = state.getTotalQuantity() <= AUTHORIZATION_QUANTITY_LIMIT;
        authorizationRepository.save(new Authorization(state.getOrderId(), authorized ? "AUTHORIZED" : "DECLINED"));

        if (authorized) {
            publishEvent("CardAuthorized", state.getOrderId(), null);
        } else {
            publishEvent("CardAuthorizationFailed", state.getOrderId(), "order quantity exceeds authorization limit");
        }
    }

    private void publishEvent(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        AccountingEvent event = new AccountingEvent(eventId, eventType, orderId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, toJson(event)));
    }

    private String toJson(AccountingEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + event.eventType() + " for order " + event.orderId(), e);
        }
    }
}
```

Run: `./gradlew :ftgo-accounting-service:test --tests "com.sanjay.ftgo.accounting.domain.SagaJoinServiceTest"`
Expected: PASS.

- [ ] **Step 8: Add `KafkaProducerConfig`, `OutboxPublisher` (topic `accounting.events`), and its test**

```java
package com.sanjay.ftgo.accounting.infrastructure;

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
    public ProducerFactory<String, String> accountingEventProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> accountingEventKafkaTemplate(ProducerFactory<String, String> accountingEventProducerFactory) {
        return new KafkaTemplate<>(accountingEventProducerFactory);
    }
}
```

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.sanjay.ftgo.accounting.domain.OutboxEvent;
import com.sanjay.ftgo.accounting.domain.OutboxEventRepository;
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
    private static final String TOPIC = "accounting.events";

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
                kafkaTemplate.send(TOPIC, String.valueOf(event.getOrderId()), event.getPayload()).get();
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }
}
```

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.sanjay.ftgo.accounting.domain.OutboxEvent;
import com.sanjay.ftgo.accounting.domain.OutboxEventRepository;
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
        OutboxEvent event = new OutboxEvent("event-1", "CardAuthorized", 100L, "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("accounting.events"), eq("100"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isTrue();
        verify(outboxEventRepository).save(event);
    }

    @Test
    void leavesEventUnsentWhenPublishFails() {
        OutboxEvent event = new OutboxEvent("event-2", "CardAuthorized", 200L, "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(eq("accounting.events"), eq("200"), eq("{}"))).thenReturn(failed);

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isFalse();
        verify(outboxEventRepository, never()).save(any());
    }
}
```

- [ ] **Step 9: Add `ConsumerEventListener` and `KitchenEventListener` (the latter filters to only `TicketCreated`/`TicketCreationFailed`)**

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final SagaJoinService sagaJoinService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(SagaJoinService sagaJoinService, ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "accounting-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        sagaJoinService.handleConsumerEvent(event.eventId(), event.orderId(), event.eventType());
    }
}
```

```java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.KitchenEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);
    private static final Set<String> RELEVANT_EVENT_TYPES = Set.of("TicketCreated", "TicketCreationFailed");

    private final SagaJoinService sagaJoinService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(SagaJoinService sagaJoinService, ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
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
        if (!RELEVANT_EVENT_TYPES.contains(event.eventType())) {
            return;
        }
        sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
    }
}
```

- [ ] **Step 10: Update `application.yml` (main and test) with Kafka config**

Replace `ftgo-accounting-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: ftgo-accounting-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_accounting
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: accounting-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest

server:
  port: 8084

outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20
```

Replace `ftgo-accounting-service/src/test/resources/application.yml`:

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
      group-id: accounting-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
```

Run: `./gradlew :ftgo-accounting-service:test`
Expected: PASS (all accounting-service tests).

- [ ] **Step 11: Commit**

```bash
git add ftgo-accounting-service/
git commit -m "feat(accounting-service): join on consumer+kitchen events and authorize card via outbox"
```

---

## Task 7: order-service — react to saga events (approve or reject the order)

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEventRepository.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ConsumerVerificationEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingEvent.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/ConsumerEventListener.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Modify: `ftgo-order-service/src/test/resources/application.yml`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java`

**Interfaces:**
- Consumes: `ConsumerVerificationEvent` on `consumer.events` (Task 5), `KitchenEvent` on `kitchen.events` (Task 2/3/4), `AccountingEvent` on `accounting.events` (Task 6).
- Produces: `Order` transitions to `APPROVED` (on `TicketConfirmed`) or `REJECTED` (on `ConsumerVerificationFailed`, `TicketCreationFailed`, or `CardAuthorizationFailed`) — this is the terminal state of the saga, verified in Task 9.

- [ ] **Step 1: Write the failing `OrderSagaServiceTest`**

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderSagaServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderSagaService orderSagaService = new OrderSagaService(orderRepository, processedEventRepository);

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
    }

    @Test
    void rejectsOrderInApprovalPending() {
        Order order = pendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.reject(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(orderRepository).save(order);
    }

    @Test
    void doesNotReapproveAnAlreadyRejectedOrder() {
        Order order = pendingOrder();
        order.markRejected();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.approve(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderSagaService.approve(42L, "e1");

        verify(orderRepository, never()).findById(any());
    }
}
```

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderSagaServiceTest"`
Expected: FAIL (compile error — `ProcessedEvent`/`ProcessedEventRepository`/`OrderSagaService` don't exist, and the `Order` 5-arg constructor from Task 1 is required here too — already added).

- [ ] **Step 2: Add `ProcessedEvent`/`ProcessedEventRepository`**

```java
package com.sanjay.ftgo.order.domain;

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

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
```

- [ ] **Step 3: Add `OrderSagaService`**

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderSagaService {

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;

    public OrderSagaService(OrderRepository orderRepository, ProcessedEventRepository processedEventRepository) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void approve(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.APPROVAL_PENDING) {
            return;
        }
        order.markApproved();
        orderRepository.save(order);
    }

    @Transactional
    public void reject(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.APPROVAL_PENDING) {
            return;
        }
        order.markRejected();
        orderRepository.save(order);
    }
}
```

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderSagaServiceTest"`
Expected: PASS.

- [ ] **Step 4: Add the `ConsumerVerificationEvent`, `KitchenEvent`, `AccountingEvent` records**

```java
package com.sanjay.ftgo.order.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
```

```java
package com.sanjay.ftgo.order.domain;

public record KitchenEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long ticketId,
        Integer totalQuantity,
        String reason) {
}
```

```java
package com.sanjay.ftgo.order.domain;

public record AccountingEvent(
        String eventId,
        String eventType,
        Long orderId,
        String reason) {
}
```

- [ ] **Step 5: Add the three saga listeners**

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final OrderSagaService orderSagaService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(OrderSagaService orderSagaService, ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "order-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        if ("ConsumerVerificationFailed".equals(event.eventType())) {
            orderSagaService.reject(event.orderId(), event.eventId());
        }
    }
}
```

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.KitchenEvent;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final OrderSagaService orderSagaService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(OrderSagaService orderSagaService, ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
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
            default -> { }
        }
    }
}
```

```java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.AccountingEvent;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final OrderSagaService orderSagaService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(OrderSagaService orderSagaService, ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
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
        if ("CardAuthorizationFailed".equals(event.eventType())) {
            orderSagaService.reject(event.orderId(), event.eventId());
        }
    }
}
```

- [ ] **Step 6: Add Kafka consumer config to `application.yml` (main and test)**

Add this block under `spring:` in `ftgo-order-service/src/main/resources/application.yml` (alongside the existing `kafka.bootstrap-servers` line):

```yaml
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
```

(This replaces the single existing `kafka: bootstrap-servers: localhost:9092` line.)

Do the same in `ftgo-order-service/src/test/resources/application.yml`, replacing its existing `kafka: bootstrap-servers: localhost:9092` line with:

```yaml
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
```

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (all order-service tests).

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEventRepository.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ConsumerVerificationEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/ConsumerEventListener.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenEventListener.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingEventListener.java \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/resources/application.yml \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java
git commit -m "feat(order-service): approve or reject Order based on saga events"
```

---

## Task 8: Docker wiring — Dockerfiles and compose.yml for consumer-service and accounting-service

**Files:**
- Create: `ftgo-consumer-service/Dockerfile`
- Create: `ftgo-accounting-service/Dockerfile`
- Modify: `compose.yml`

**Interfaces:**
- Consumes: nothing new — packages the code from Tasks 5 and 6 as running containers.
- Produces: `consumer-service` reachable at `localhost:8081`, `accounting-service` at `localhost:8084`, both wired into the same Kafka/MySQL infrastructure as order-service/kitchen-service. Task 9's manual verification depends on both being up.

- [ ] **Step 1: Add `ftgo-consumer-service/Dockerfile`**

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-consumer-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-consumer-service/build/libs/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 2: Add `ftgo-accounting-service/Dockerfile`**

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-accounting-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-accounting-service/build/libs/*.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: Add both services to `compose.yml`**

Insert these two service blocks after the existing `kitchen-service:` block (before the trailing `volumes:` section):

```yaml
  consumer-service:
    build:
      context: .
      dockerfile: ftgo-consumer-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
    ports:
      - "8081:8081"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_consumer
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092

  accounting-service:
    build:
      context: .
      dockerfile: ftgo-accounting-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
    ports:
      - "8084:8084"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_accounting
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

- [ ] **Step 4: Validate the compose file parses and builds**

Run: `docker compose config --quiet`
Expected: no output, exit code 0 (confirms YAML is well-formed and references resolve).

Run: `docker compose build consumer-service accounting-service`
Expected: both images build successfully.

- [ ] **Step 5: Commit**

```bash
git add ftgo-consumer-service/Dockerfile ftgo-accounting-service/Dockerfile compose.yml
git commit -m "feat: wire consumer-service and accounting-service into docker-compose"
```

---

## Task 9: Manual end-to-end verification via Docker

**Files:** none — this task runs the full stack and inspects behavior. No code changes.

**Interfaces:**
- Consumes: the complete saga built in Tasks 1–8.
- Produces: confirmation the saga works end-to-end — this is the acceptance gate for the whole plan.

- [ ] **Step 1: Bring up the full stack**

```bash
docker compose up -d --build
```

Wait for all containers healthy: `docker compose ps` — `mysql` should show `healthy`; `order-service`, `kitchen-service`, `consumer-service`, `accounting-service`, `service-registry` should show `running`/`Up`.

- [ ] **Step 2: Happy path — order reaches `APPROVED`, ticket reaches `AWAITING_ACCEPTANCE`**

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":2}]}'
```

Note the returned `id` (say `ORDER_ID`). Wait ~5s for the outbox pollers and saga to settle, then:

```bash
docker compose exec mysql mysql -uftgo -pftgo -e \
  "SELECT id, status FROM orders WHERE id=${ORDER_ID};" ftgo_order
docker compose exec mysql mysql -uftgo -pftgo -e \
  "SELECT order_id, status FROM tickets WHERE order_id=${ORDER_ID};" ftgo_kitchen
docker compose exec mysql mysql -uftgo -pftgo -e \
  "SELECT order_id, status FROM authorizations WHERE order_id=${ORDER_ID};" ftgo_accounting
```

Expected: `orders.status = APPROVED`, `tickets.status = AWAITING_ACCEPTANCE`, `authorizations.status = AUTHORIZED`.

- [ ] **Step 3: Case A — consumer verification fails (consumerId 2, seeded inactive)**

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":2,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}'
```

Wait ~5s, then check `orders.status` for the new `ORDER_ID` → expect `REJECTED`. Check `tickets` for that `order_id` → expect either no row, or `status = CANCELLED` depending on timing (kitchen-service may have already created `CREATE_PENDING` before the failure event arrives — either outcome is correct per the design as long as it ends at `CANCELLED` or absent, never `AWAITING_ACCEPTANCE`). Check `authorizations` for that `order_id` → expect no row (accounting never authorized).

- [ ] **Step 4: Case B — kitchen capacity exceeded (quantity > 20)**

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":25}]}'
```

Wait ~5s, then check `orders.status` → expect `REJECTED`. Check `tickets` for that `order_id` → expect no row (kitchen never created one). Check `authorizations` → expect no row.

- [ ] **Step 5: Case C — card authorization declined (quantity between 11 and 20)**

```bash
curl -s -X POST localhost:8082/orders -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":15}]}'
```

Wait ~5s, then check `orders.status` → expect `REJECTED`. Check `tickets` for that `order_id` → expect `status = CANCELLED` (kitchen created it, then compensated). Check `authorizations` → expect `status = DECLINED`.

- [ ] **Step 6: Redelivery/idempotency check**

Pick the `ORDER_ID` from the happy-path test (Step 2). Force a Kafka redelivery by resetting one already-sent outbox row in kitchen-service, e.g.:

```bash
docker compose exec mysql mysql -uftgo -pftgo -e \
  "UPDATE outbox_events SET sent_at = NULL WHERE order_id=${ORDER_ID} AND event_type='TicketCreated';" ftgo_kitchen
```

Wait ~5s for the poller to resend, then re-check `authorizations` and `tickets` for that `order_id` — row counts must be unchanged (still exactly one `AUTHORIZED` authorization, ticket still `AWAITING_ACCEPTANCE`), confirming `processed_events` dedup absorbed the redelivery in accounting-service.

- [ ] **Step 7: Out-of-order join arrival**

This is implicitly exercised by every order placed above, since `ConsumerVerified` and `TicketCreated` are published by independent services with no ordering guarantee between topics. Confirm from the Step 2 happy-path result alone that authorization fired exactly once (`SELECT COUNT(*) FROM authorizations WHERE order_id=${ORDER_ID};` → `1`) regardless of which event actually arrived first — no further action needed if Step 2 passed.

- [ ] **Step 8: Tear down**

```bash
docker compose down
```

- [ ] **Step 9: Update `CONTEXT.md` and `docs/session-*.md`, then commit**

Update `CONTEXT.md`'s "Current position", "Session log", and the "Data consistency" pattern checklist (mark choreography saga as implemented) to reflect this work, following the existing convention from prior sessions in the file.

```bash
git add CONTEXT.md docs/
git commit -m "docs: update CONTEXT.md — Ch.4 Create Order saga (choreography) implemented"
```
