# CQRS Order History Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `ftgo-order-history-service` — a new, dedicated CQRS read-side service that maintains a denormalized `order_views` table purely by consuming Kafka events from `order-service`/`kitchen-service`/`accounting-service`/`delivery-service`, and exposes `GET /order-views/{orderId}`. This is Ch.7's last sub-project; completing it flips Ch.7 to Done.

**Architecture:** A brand-new Spring Boot service (no Eureka, no synchronous calls to anyone) with 4 Kafka listeners funneling into one `OrderViewService`, which upserts (find-or-create by `orderId`) a single `OrderView` JPA entity per order. Status columns are set via direct `eventType`→status string maps — no reimplemented state machines. Coexists with, does not replace, `order-service`'s `GET /orders/{id}/view` from the prior sub-project.

**Tech Stack:** Java 21, Spring Boot 3.5.16, Spring Data JPA, Spring Kafka, `ftgo-common`'s shared `ProcessedEvent` dedup module, JUnit 5 + AssertJ + Mockito, MySQL 8.4 (test: H2 with `MODE=MySQL`).

## Global Constraints

- No Eureka client, no Resilience4j, no `spring-cloud-starter-loadbalancer` — this service makes no outbound synchronous calls to anything (spec decision 5).
- No restaurant name/menu denormalization — `restaurantId` stays an opaque foreign key (spec decision 2).
- Status columns are set via direct `eventType`→status string mapping, never a reimplemented guarded state machine (spec decision 3).
- Every listener upserts (find-or-create by `orderId`) — no listener may assume the row already exists, including the `OrderCreated` handler itself, which must also tolerate the row already existing from an earlier-arriving update event (spec decision 4).
- Every wire record is this service's own trimmed copy, per this codebase's established per-consumer convention — do not import another service's wire record.
- Every Kafka listener dedups via `ftgo-common`'s `ProcessedEventRepository` (`existsById`/`save`), matching every other consumer in this codebase.
- Code comments explain *why*, not *what*.
- TDD throughout: write the failing test before the implementation, for every task with a test step.
- Frequent commits: one commit per task, using this project's existing commit-message conventions (`feat:`, `fix:`, `docs:`, `refactor:`).
- This is a chapter-completion change (Ch.7 flips to Done once this merges) — per this project's `CLAUDE.md`, the docs task in this plan must be a full sweep (`docs/ARCHITECTURE.md` + every touched service's README), not just a per-change update.

---

## Codebase reference (read once, applies to every task below)

`ftgo-order-history-service` does not exist yet — everything is new. The exact event-type→status mappings below were verified against the real sealed-interface `permits` lists in `OrderDomainEvent`/`TicketDomainEvent`/`AuthorizationDomainEvent`/`DeliveryDomainEvent` and their publishers during spec-writing — treat them as authoritative, but if implementation reveals a mismatch against the actual current code, the real code wins; note the discrepancy in your task report.

`@Embeddable` records work as `@ElementCollection` element types in this codebase already (see `Order.java`'s `OrderLineItem`) — this plan's `OrderViewLineItem` follows the identical pattern.

---

### Task 1: Scaffold `ftgo-order-history-service`

**Files:**
- Create: `ftgo-order-history-service/build.gradle`
- Create: `ftgo-order-history-service/src/main/resources/application.yml`
- Create: `ftgo-order-history-service/src/test/resources/application.yml`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/FtgoOrderHistoryServiceApplication.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/config/PersistenceConfig.java`
- Create: `ftgo-order-history-service/Dockerfile`
- Create: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/FtgoOrderHistoryServiceApplicationTests.java`
- Modify: `settings.gradle`
- Modify: `infrastructure/mysql/init.sql`
- Modify: `compose.yml`

**Interfaces:**
- Produces: a bootable (but empty) Spring Boot service on port 8088, wired into Gradle/Docker Compose/MySQL init, ready for later tasks to add domain code into `com.sanjay.ftgo.orderhistory.domain`/`infrastructure`/`api`.

- [ ] **Step 1: Add to `settings.gradle`**

```groovy
// Add after the existing `include 'ftgo-service-registry'` line:
include 'ftgo-order-history-service'
```

- [ ] **Step 2: Add the database to `infrastructure/mysql/init.sql`**

```sql
-- Add alongside the existing CREATE DATABASE lines:
CREATE DATABASE IF NOT EXISTS ftgo_order_history;

-- Add alongside the existing GRANT lines:
GRANT ALL PRIVILEGES ON ftgo_order_history.* TO 'ftgo'@'%';
```

- [ ] **Step 3: Write `build.gradle`** (no Eureka/Resilience4j/LoadBalancer — this service has no outbound synchronous calls)

```groovy
// ftgo-order-history-service/build.gradle
dependencies {
    // spring-kafka comes transitively via ftgo-common's `api` dependency
    implementation project(':ftgo-common')
}
```

- [ ] **Step 4: Write `application.yml`**

```yaml
# ftgo-order-history-service/src/main/resources/application.yml
spring:
  application:
    name: ftgo-order-history-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_order_history
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-history-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest

server:
  port: 8088
```

(No `outbox:`/`saga:` blocks — this service never publishes and has no saga role.)

- [ ] **Step 5: Write test `application.yml`** — check `ftgo-delivery-service/src/test/resources/application.yml` first and match its exact H2/`MODE=MySQL` shape, substituting only `spring.application.name` and dropping the `outbox`/`saga` blocks (this service has neither).

- [ ] **Step 6: Write the application class**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/FtgoOrderHistoryServiceApplication.java
package com.sanjay.ftgo.orderhistory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FtgoOrderHistoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoOrderHistoryServiceApplication.class, args);
    }
}
```

- [ ] **Step 7: Write `PersistenceConfig`** (mirrors `ftgo-delivery-service`'s exactly)

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/config/PersistenceConfig.java
package com.sanjay.ftgo.orderhistory.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoOrderHistoryServiceApplication because @EntityScan/@EnableJpaRepositories
// placed directly on the @SpringBootApplication class bypass @WebMvcTest's slice filtering,
// which only excludes @Component/@Configuration beans discovered via scan — see order-service's
// PersistenceConfig for the concrete failure this pattern avoids.
// (ftgo-common's KafkaProducerConfig bean doesn't need scanning here — it's registered
// automatically via ftgo-common's own Spring Boot auto-configuration. This service never uses
// OutboxPublisher/OutboxEventRepository since it never publishes, only ProcessedEventRepository
// for consume-side dedup.)
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.orderhistory.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.orderhistory.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
```

- [ ] **Step 8: Write the Dockerfile**

```dockerfile
# ftgo-order-history-service/Dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-order-history-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-order-history-service/build/libs/*.jar app.jar
EXPOSE 8088
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 9: Wire into `compose.yml`** — add after the `delivery-service` block, before `volumes:` (no `service-registry` dependency, per spec decision 5):

```yaml
  order-history-service:
    build:
      context: .
      dockerfile: ftgo-order-history-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
    ports:
      - "8088:8088"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order_history
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

- [ ] **Step 10: Write the context-load test**

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/FtgoOrderHistoryServiceApplicationTests.java
package com.sanjay.ftgo.orderhistory;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class FtgoOrderHistoryServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

- [ ] **Step 11: Verify the module builds**

Run: `./gradlew :ftgo-order-history-service:build`
Expected: BUILD SUCCESSFUL

- [ ] **Step 12: Verify the Docker image builds**

Run: `docker compose build order-history-service`
Expected: image builds successfully.

- [ ] **Step 13: Commit**

```bash
git add ftgo-order-history-service/ settings.gradle infrastructure/mysql/init.sql compose.yml
git commit -m "feat: scaffold ftgo-order-history-service (Gradle, config, Docker, compose)"
```

---

### Task 2: `OrderView` entity, `OrderViewLineItem`, and `OrderViewRepository`

**Files:**
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewLineItem.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderView.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewRepository.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewTest.java`

**Interfaces:**
- Produces: `OrderView` (plain JPA entity, no guarded transitions — the read side doesn't enforce business rules), with `orderId` as its `@Id`, plain setters for every column, and `getLineItems(): List<OrderViewLineItem>` returning a live mutable list. `OrderViewRepository extends JpaRepository<OrderView, Long>` — no custom query methods needed, `orderId` already is the primary key. Consumed by `OrderViewService` (Tasks 3–6).

- [ ] **Step 1: Write the failing test**

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewTest.java
package com.sanjay.ftgo.orderhistory.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderViewTest {

    @Test
    void newOrderViewStartsWithNullStatusesAndEmptyLineItems() {
        OrderView view = new OrderView(42L);

        assertThat(view.getOrderId()).isEqualTo(42L);
        assertThat(view.getOrderStatus()).isNull();
        assertThat(view.getTicketStatus()).isNull();
        assertThat(view.getAuthorizationStatus()).isNull();
        assertThat(view.getDeliveryStatus()).isNull();
        assertThat(view.getCourierId()).isNull();
        assertThat(view.getLineItems()).isEmpty();
    }

    @Test
    void settersUpdateFields() {
        OrderView view = new OrderView(42L);

        view.setConsumerId(1L);
        view.setRestaurantId(7L);
        view.setOrderStatus("APPROVAL_PENDING");
        view.setLineItems(List.of(new OrderViewLineItem(10L, 2)));

        assertThat(view.getConsumerId()).isEqualTo(1L);
        assertThat(view.getRestaurantId()).isEqualTo(7L);
        assertThat(view.getOrderStatus()).isEqualTo("APPROVAL_PENDING");
        assertThat(view.getLineItems()).containsExactly(new OrderViewLineItem(10L, 2));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-history-service:test --tests OrderViewTest`
Expected: FAIL (compile error — `OrderView`/`OrderViewLineItem` don't exist yet)

- [ ] **Step 3: Write `OrderViewLineItem`** (mirrors `Order.java`'s `OrderLineItem` — an `@Embeddable` record, the established pattern for `@ElementCollection` elements in this codebase)

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewLineItem.java
package com.sanjay.ftgo.orderhistory.domain;

import jakarta.persistence.Embeddable;

@Embeddable
public record OrderViewLineItem(Long menuItemId, int quantity) {
}
```

- [ ] **Step 4: Write `OrderView`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderView.java
package com.sanjay.ftgo.orderhistory.domain;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "order_views")
public class OrderView {

    @Id
    private Long orderId;

    private Long consumerId;

    private Long restaurantId;

    private String orderStatus;

    private String ticketStatus;

    private String authorizationStatus;

    private String deliveryStatus;

    private Long courierId;

    @ElementCollection
    @CollectionTable(name = "order_view_line_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderViewLineItem> lineItems = new ArrayList<>();

    protected OrderView() {
    }

    // No status is set here on purpose - every field starts unset (null/empty) regardless of
    // which of the 4 event sources creates this row first (see OrderViewService's upsert
    // pattern), since Kafka gives no cross-topic ordering guarantee.
    public OrderView(Long orderId) {
        this.orderId = orderId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(Long consumerId) {
        this.consumerId = consumerId;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public void setRestaurantId(Long restaurantId) {
        this.restaurantId = restaurantId;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getTicketStatus() {
        return ticketStatus;
    }

    public void setTicketStatus(String ticketStatus) {
        this.ticketStatus = ticketStatus;
    }

    public String getAuthorizationStatus() {
        return authorizationStatus;
    }

    public void setAuthorizationStatus(String authorizationStatus) {
        this.authorizationStatus = authorizationStatus;
    }

    public String getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(String deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public Long getCourierId() {
        return courierId;
    }

    public void setCourierId(Long courierId) {
        this.courierId = courierId;
    }

    public List<OrderViewLineItem> getLineItems() {
        return lineItems;
    }

    public void setLineItems(List<OrderViewLineItem> lineItems) {
        this.lineItems = lineItems;
    }
}
```

- [ ] **Step 5: Write `OrderViewRepository`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewRepository.java
package com.sanjay.ftgo.orderhistory.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderViewRepository extends JpaRepository<OrderView, Long> {
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :ftgo-order-history-service:test --tests OrderViewTest`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewLineItem.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderView.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewRepository.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewTest.java
git commit -m "feat: add OrderView entity and repository"
```

---

### Task 3: `order.events` — `OrderEvent` wire record, `OrderViewService.handleOrderEvent`, `OrderEventListener`

**Files:**
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderEvent.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListener.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListenerTest.java`

**Interfaces:**
- Consumes: `OrderView`/`OrderViewLineItem`/`OrderViewRepository` (Task 2), `com.sanjay.ftgo.common.outbox.{ProcessedEvent,ProcessedEventRepository}`.
- Produces: `OrderViewService.handleOrderEvent(String eventId, String eventType, Long orderId, Long consumerId, Long restaurantId, List<OrderViewLineItem> lineItems)` — the first handler method on `OrderViewService`; Tasks 4–6 add 3 sibling methods to this same class.

This task establishes the upsert pattern every later task's handler follows: `orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId))`, mutate, save.

- [ ] **Step 1: Write the failing tests**

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java
package com.sanjay.ftgo.orderhistory.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderViewServiceTest {

    private final OrderViewRepository orderViewRepository = mock(OrderViewRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);

    private OrderViewService orderViewService;

    @BeforeEach
    void setUp() {
        orderViewService = new OrderViewService(orderViewRepository, processedEventRepository);
        when(orderViewRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    void orderCreatedCreatesFullRowWhenNoneExists() {
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.empty());
        List<OrderViewLineItem> lineItems = List.of(new OrderViewLineItem(10L, 2));

        orderViewService.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, lineItems);

        var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
        verify(orderViewRepository).save(captor.capture());
        OrderView saved = captor.getValue();
        assertThat(saved.getOrderId()).isEqualTo(42L);
        assertThat(saved.getConsumerId()).isEqualTo(1L);
        assertThat(saved.getRestaurantId()).isEqualTo(7L);
        assertThat(saved.getOrderStatus()).isEqualTo("APPROVAL_PENDING");
        assertThat(saved.getLineItems()).containsExactly(new OrderViewLineItem(10L, 2));
    }

    @Test
    void orderCreatedFillsInFieldsWhenRowAlreadyExists() {
        // Simulates the cross-topic race: a ticket/authorization/delivery event already
        // created a stub row before OrderCreated arrived.
        OrderView stub = new OrderView(42L);
        stub.setTicketStatus("CREATE_PENDING");
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(stub));
        List<OrderViewLineItem> lineItems = List.of(new OrderViewLineItem(10L, 2));

        orderViewService.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, lineItems);

        var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
        verify(orderViewRepository).save(captor.capture());
        OrderView saved = captor.getValue();
        assertThat(saved.getConsumerId()).isEqualTo(1L);
        assertThat(saved.getOrderStatus()).isEqualTo("APPROVAL_PENDING");
        assertThat(saved.getTicketStatus()).isEqualTo("CREATE_PENDING"); // untouched
    }

    @Test
    void orderApprovedUpdatesOrderStatusOnlyOnExistingRow() {
        OrderView existing = new OrderView(42L);
        existing.setOrderStatus("APPROVAL_PENDING");
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleOrderEvent("evt-2", "OrderApproved", 42L, null, null, null);

        assertThat(existing.getOrderStatus()).isEqualTo("APPROVED");
    }

    @Test
    void orderApprovedCreatesStubRowWhenNoneExists() {
        // Cross-topic race the other direction: OrderApproved somehow processed before
        // OrderCreated - shouldn't happen in practice (order-service publishes OrderCreated
        // first), but the upsert pattern must not NPE or drop the update either way.
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.empty());

        orderViewService.handleOrderEvent("evt-2", "OrderApproved", 42L, null, null, null);

        var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
        verify(orderViewRepository).save(captor.capture());
        assertThat(captor.getValue().getOrderStatus()).isEqualTo("APPROVED");
    }

    @Test
    void orderRevisionCompensationRequestedDoesNotChangeOrderStatus() {
        OrderView existing = new OrderView(42L);
        existing.setOrderStatus("REVISION_PENDING");
        when(processedEventRepository.existsById("evt-3")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleOrderEvent("evt-3", "OrderRevisionCompensationRequested", 42L, null, null, null);

        assertThat(existing.getOrderStatus()).isEqualTo("REVISION_PENDING");
    }

    @Test
    void dedupsOnEventId() {
        when(processedEventRepository.existsById("evt-1")).thenReturn(true);

        orderViewService.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, List.of());

        verify(orderViewRepository, org.mockito.Mockito.never()).findById(any());
    }
}
```

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListenerTest.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewLineItem;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final OrderEventListener listener = new OrderEventListener(orderViewService, new ObjectMapper());

    @Test
    void onOrderCreatedCallsHandleOrderEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"OrderCreated","orderId":42,"consumerId":1,"restaurantId":7,
                 "lineItems":[{"menuItemId":10,"quantity":2}]}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, List.of(new OrderViewLineItem(10L, 2)));
    }

    @Test
    void onOrderApprovedCallsHandleOrderEventWithNullOptionalFields() {
        String payload = """
                {"eventId":"evt-2","eventType":"OrderApproved","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleOrderEvent("evt-2", "OrderApproved", 42L, null, null, null);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,OrderEventListenerTest"`
Expected: FAIL (compile errors — none of `OrderEvent`/`OrderViewService`/`OrderEventListener` exist yet)

- [ ] **Step 3: Write `OrderEvent`** (trimmed to only the fields this service needs — matches `order-service`'s producer-side `OrderEvent` field-for-field on the subset used, per this codebase's established trimmed-copy convention)

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderEvent.java
package com.sanjay.ftgo.orderhistory.domain;

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

- [ ] **Step 4: Write `OrderViewService` with `handleOrderEvent`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java
package com.sanjay.ftgo.orderhistory.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class OrderViewService {

    private final OrderViewRepository orderViewRepository;
    private final ProcessedEventRepository processedEventRepository;

    public OrderViewService(OrderViewRepository orderViewRepository, ProcessedEventRepository processedEventRepository) {
        this.orderViewRepository = orderViewRepository;
        this.processedEventRepository = processedEventRepository;
    }

    // Upsert, not create-only: a ticket/authorization/delivery event can legitimately arrive
    // before this OrderCreated, since Kafka gives no ordering guarantee across topics. If a
    // stub row already exists (created by one of those), this fills in the fields OrderCreated
    // owns without disturbing whatever the stub already recorded.
    @Transactional
    public void handleOrderEvent(String eventId, String eventType, Long orderId,
                                  Long consumerId, Long restaurantId, List<OrderViewLineItem> lineItems) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

        switch (eventType) {
            case "OrderCreated" -> {
                view.setConsumerId(consumerId);
                view.setRestaurantId(restaurantId);
                view.setLineItems(lineItems);
                view.setOrderStatus("APPROVAL_PENDING");
            }
            case "OrderApproved" -> view.setOrderStatus("APPROVED");
            case "OrderRejected" -> view.setOrderStatus("REJECTED");
            case "OrderCancelled" -> view.setOrderStatus("CANCEL_PENDING");
            case "OrderCancelConfirmed" -> view.setOrderStatus("CANCELLED");
            case "OrderCancelRejected" -> view.setOrderStatus("APPROVED");
            case "OrderRevisionProposed" -> view.setOrderStatus("REVISION_PENDING");
            case "OrderRevised" -> view.setOrderStatus("APPROVED");
            case "OrderRevisionRejected" -> view.setOrderStatus("APPROVED");
            // OrderRevisionCompensationRequested: wire-only pseudo-event signalling kitchen to
            // undo a provisional revision - the order itself stays REVISION_PENDING at this
            // point, so no orderStatus change here (see Ch.6's event-sourcing design docs for
            // the full rationale behind this pseudo-event's existence).
            default -> { }
        }

        orderViewRepository.save(view);
    }
}
```

- [ ] **Step 5: Write `OrderEventListener`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListener.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewLineItem;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        OrderEvent event;
        try {
            event = objectMapper.readValue(payload, OrderEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed order event: {}", payload, e);
            return;
        }
        List<OrderViewLineItem> lineItems = event.lineItems() == null ? null
                : event.lineItems().stream().map(li -> new OrderViewLineItem(li.menuItemId(), li.quantity())).toList();
        orderViewService.handleOrderEvent(event.eventId(), event.eventType(), event.orderId(),
                event.consumerId(), event.restaurantId(), lineItems);
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,OrderEventListenerTest"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderEvent.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListener.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListenerTest.java
git commit -m "feat: consume order.events into the order-history read model"
```

---

### Task 4: `kitchen.events` — `KitchenEvent` wire record, `OrderViewService.handleKitchenEvent`, `KitchenEventListener`

**Files:**
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/KitchenEvent.java`
- Modify: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/KitchenEventListener.java`
- Modify: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/KitchenEventListenerTest.java`

**Interfaces:**
- Produces: `OrderViewService.handleKitchenEvent(String eventId, String eventType, Long orderId)` — new sibling method on the same class Task 3 started.

- [ ] **Step 1: Write the failing tests** — add to the existing `OrderViewServiceTest`:

```java
// New test methods added to OrderViewServiceTest.java

@Test
void ticketCreatedSetsTicketStatusOnExistingOrCreatesStub() {
    when(processedEventRepository.existsById("evt-4")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.empty());

    orderViewService.handleKitchenEvent("evt-4", "TicketCreated", 42L);

    var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
    verify(orderViewRepository).save(captor.capture());
    assertThat(captor.getValue().getTicketStatus()).isEqualTo("CREATE_PENDING");
}

@Test
void ticketAcceptedUpdatesExistingRow() {
    OrderView existing = new OrderView(42L);
    existing.setTicketStatus("AWAITING_ACCEPTANCE");
    when(processedEventRepository.existsById("evt-5")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleKitchenEvent("evt-5", "TicketAccepted", 42L);

    assertThat(existing.getTicketStatus()).isEqualTo("ACCEPTED");
}

@Test
void ticketCreationFailedDoesNotSetTicketStatus() {
    OrderView existing = new OrderView(42L);
    when(processedEventRepository.existsById("evt-6")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleKitchenEvent("evt-6", "TicketCreationFailed", 42L);

    assertThat(existing.getTicketStatus()).isNull();
}

@Test
void ticketQuantityRevisedDoesNotChangeTicketStatus() {
    OrderView existing = new OrderView(42L);
    existing.setTicketStatus("ACCEPTED");
    when(processedEventRepository.existsById("evt-7")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleKitchenEvent("evt-7", "TicketQuantityRevised", 42L);

    assertThat(existing.getTicketStatus()).isEqualTo("ACCEPTED");
}
```

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/KitchenEventListenerTest.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class KitchenEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final KitchenEventListener listener = new KitchenEventListener(orderViewService, new ObjectMapper());

    @Test
    void onTicketAcceptedCallsHandleKitchenEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"TicketAccepted","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleKitchenEvent("evt-1", "TicketAccepted", 42L);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,KitchenEventListenerTest"`
Expected: FAIL (compile error — `handleKitchenEvent`/`KitchenEvent`/`KitchenEventListener` don't exist yet)

- [ ] **Step 3: Write `KitchenEvent`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/KitchenEvent.java
package com.sanjay.ftgo.orderhistory.domain;

public record KitchenEvent(String eventId, String eventType, Long orderId) {
}
```

- [ ] **Step 4: Add `handleKitchenEvent` to `OrderViewService`**

```java
// Add to ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java

@Transactional
public void handleKitchenEvent(String eventId, String eventType, Long orderId) {
    if (processedEventRepository.existsById(eventId)) {
        return;
    }
    processedEventRepository.save(new ProcessedEvent(eventId));

    OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

    switch (eventType) {
        case "TicketCreated" -> view.setTicketStatus("CREATE_PENDING");
        case "TicketConfirmed" -> view.setTicketStatus("AWAITING_ACCEPTANCE");
        case "TicketAccepted" -> view.setTicketStatus("ACCEPTED");
        case "TicketPreparingStarted" -> view.setTicketStatus("PREPARING");
        case "TicketReadyForPickup" -> view.setTicketStatus("READY_FOR_PICKUP");
        case "TicketPickedUp" -> view.setTicketStatus("PICKED_UP");
        case "TicketCancelled" -> view.setTicketStatus("CANCELLED");
        // TicketCreationFailed: no ticket was ever created, nothing to record.
        // TicketCancellationRejected/TicketRevisionRejected/TicketRevisionUndone/
        // TicketQuantityRevised: none represent a new lifecycle state on their own -
        // TicketQuantityRevised changes quantity, not status, and this read model doesn't
        // track quantity at all (out of scope per the design).
        default -> { }
    }

    orderViewRepository.save(view);
}
```

- [ ] **Step 5: Write `KitchenEventListener`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/KitchenEventListener.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.KitchenEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        orderViewService.handleKitchenEvent(event.eventId(), event.eventType(), event.orderId());
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,KitchenEventListenerTest"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/KitchenEvent.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/KitchenEventListener.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/KitchenEventListenerTest.java
git commit -m "feat: consume kitchen.events into the order-history read model"
```

---

### Task 5: `accounting.events` — `AccountingEvent` wire record, `OrderViewService.handleAccountingEvent`, `AccountingEventListener`

**Files:**
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/AccountingEvent.java`
- Modify: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/AccountingEventListener.java`
- Modify: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/AccountingEventListenerTest.java`

**Interfaces:**
- Produces: `OrderViewService.handleAccountingEvent(String eventId, String eventType, Long orderId)` — new sibling method.

- [ ] **Step 1: Write the failing tests** — add to `OrderViewServiceTest`:

```java
// New test methods added to OrderViewServiceTest.java

@Test
void cardAuthorizedSetsAuthorizationStatus() {
    OrderView existing = new OrderView(42L);
    when(processedEventRepository.existsById("evt-8")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleAccountingEvent("evt-8", "CardAuthorized", 42L);

    assertThat(existing.getAuthorizationStatus()).isEqualTo("AUTHORIZED");
}

@Test
void cardAuthorizationFailedSetsDeclined() {
    OrderView existing = new OrderView(42L);
    when(processedEventRepository.existsById("evt-9")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleAccountingEvent("evt-9", "CardAuthorizationFailed", 42L);

    assertThat(existing.getAuthorizationStatus()).isEqualTo("DECLINED");
}

@Test
void authorizationReversedSetsReversed() {
    OrderView existing = new OrderView(42L);
    existing.setAuthorizationStatus("AUTHORIZED");
    when(processedEventRepository.existsById("evt-10")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleAccountingEvent("evt-10", "AuthorizationReversed", 42L);

    assertThat(existing.getAuthorizationStatus()).isEqualTo("REVERSED");
}

@Test
void authorizationRevisionRejectedDoesNotChangeStatus() {
    OrderView existing = new OrderView(42L);
    existing.setAuthorizationStatus("AUTHORIZED");
    when(processedEventRepository.existsById("evt-11")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleAccountingEvent("evt-11", "AuthorizationRevisionRejected", 42L);

    assertThat(existing.getAuthorizationStatus()).isEqualTo("AUTHORIZED");
}
```

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/AccountingEventListenerTest.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class AccountingEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final AccountingEventListener listener = new AccountingEventListener(orderViewService, new ObjectMapper());

    @Test
    void onCardAuthorizedCallsHandleAccountingEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"CardAuthorized","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleAccountingEvent("evt-1", "CardAuthorized", 42L);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,AccountingEventListenerTest"`
Expected: FAIL

- [ ] **Step 3: Write `AccountingEvent`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/AccountingEvent.java
package com.sanjay.ftgo.orderhistory.domain;

public record AccountingEvent(String eventId, String eventType, Long orderId) {
}
```

- [ ] **Step 4: Add `handleAccountingEvent` to `OrderViewService`**

```java
// Add to ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java

@Transactional
public void handleAccountingEvent(String eventId, String eventType, Long orderId) {
    if (processedEventRepository.existsById(eventId)) {
        return;
    }
    processedEventRepository.save(new ProcessedEvent(eventId));

    OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

    switch (eventType) {
        case "CardAuthorized" -> view.setAuthorizationStatus("AUTHORIZED");
        case "CardAuthorizationFailed" -> view.setAuthorizationStatus("DECLINED");
        case "AuthorizationReversed" -> view.setAuthorizationStatus("REVERSED");
        // Still authorized, just at a new quantity - this read model doesn't track quantity.
        case "AuthorizationRevised" -> view.setAuthorizationStatus("AUTHORIZED");
        // Decline of a revision attempt, not a new authorization state - the existing
        // authorization is untouched.
        default -> { }
    }

    orderViewRepository.save(view);
}
```

- [ ] **Step 5: Write `AccountingEventListener`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/AccountingEventListener.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.AccountingEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        AccountingEvent event;
        try {
            event = objectMapper.readValue(payload, AccountingEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting event: {}", payload, e);
            return;
        }
        orderViewService.handleAccountingEvent(event.eventId(), event.eventType(), event.orderId());
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,AccountingEventListenerTest"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/AccountingEvent.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/AccountingEventListener.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/AccountingEventListenerTest.java
git commit -m "feat: consume accounting.events into the order-history read model"
```

---

### Task 6: `delivery.events` — `DeliveryEvent` wire record, `OrderViewService.handleDeliveryEvent`, `DeliveryEventListener`

**Files:**
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/DeliveryEvent.java`
- Modify: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/DeliveryEventListener.java`
- Modify: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/DeliveryEventListenerTest.java`

**Interfaces:**
- Produces: `OrderViewService.handleDeliveryEvent(String eventId, String eventType, Long orderId, Long courierId)` — last of the 4 handler methods on `OrderViewService`; the class is feature-complete after this task.

- [ ] **Step 1: Write the failing tests** — add to `OrderViewServiceTest`:

```java
// New test methods added to OrderViewServiceTest.java

@Test
void deliveryScheduledSetsStatusAndCourierId() {
    OrderView existing = new OrderView(42L);
    when(processedEventRepository.existsById("evt-12")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleDeliveryEvent("evt-12", "DeliveryScheduled", 42L, 3L);

    assertThat(existing.getDeliveryStatus()).isEqualTo("SCHEDULED");
    assertThat(existing.getCourierId()).isEqualTo(3L);
}

@Test
void deliveryCancelledClearsCourierId() {
    OrderView existing = new OrderView(42L);
    existing.setDeliveryStatus("SCHEDULED");
    existing.setCourierId(3L);
    when(processedEventRepository.existsById("evt-13")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleDeliveryEvent("evt-13", "DeliveryCancelled", 42L, null);

    assertThat(existing.getDeliveryStatus()).isEqualTo("CANCELLED");
    assertThat(existing.getCourierId()).isNull();
}

@Test
void deliverySchedulingFailedDoesNotChangeStatus() {
    OrderView existing = new OrderView(42L);
    when(processedEventRepository.existsById("evt-14")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleDeliveryEvent("evt-14", "DeliverySchedulingFailed", 42L, null);

    assertThat(existing.getDeliveryStatus()).isNull();
}

@Test
void deliveryPickedUpAndDeliveredUpdateStatus() {
    OrderView existing = new OrderView(42L);
    existing.setDeliveryStatus("SCHEDULED");
    when(processedEventRepository.existsById("evt-15")).thenReturn(false);
    when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

    orderViewService.handleDeliveryEvent("evt-15", "DeliveryPickedUp", 42L, null);

    assertThat(existing.getDeliveryStatus()).isEqualTo("PICKED_UP");
}
```

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/DeliveryEventListenerTest.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final DeliveryEventListener listener = new DeliveryEventListener(orderViewService, new ObjectMapper());

    @Test
    void onDeliveryScheduledCallsHandleDeliveryEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42,"courierId":3}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleDeliveryEvent("evt-1", "DeliveryScheduled", 42L, 3L);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,DeliveryEventListenerTest"`
Expected: FAIL

- [ ] **Step 3: Write `DeliveryEvent`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/DeliveryEvent.java
package com.sanjay.ftgo.orderhistory.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId, Long courierId) {
}
```

- [ ] **Step 4: Add `handleDeliveryEvent` to `OrderViewService`**

```java
// Add to ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java

@Transactional
public void handleDeliveryEvent(String eventId, String eventType, Long orderId, Long courierId) {
    if (processedEventRepository.existsById(eventId)) {
        return;
    }
    processedEventRepository.save(new ProcessedEvent(eventId));

    OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

    switch (eventType) {
        case "DeliveryScheduled" -> {
            view.setDeliveryStatus("SCHEDULED");
            view.setCourierId(courierId);
        }
        case "DeliveryPickedUp" -> view.setDeliveryStatus("PICKED_UP");
        case "DeliveryDelivered" -> view.setDeliveryStatus("DELIVERED");
        case "DeliveryCancelled" -> {
            view.setDeliveryStatus("CANCELLED");
            view.setCourierId(null); // no courier is assigned anymore
        }
        // DeliverySchedulingFailed: nothing was ever scheduled, nothing to record.
        default -> { }
    }

    orderViewRepository.save(view);
}
```

- [ ] **Step 5: Write `DeliveryEventListener`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/DeliveryEventListener.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.DeliveryEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DeliveryEventListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public DeliveryEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(payload, DeliveryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery event: {}", payload, e);
            return;
        }
        orderViewService.handleDeliveryEvent(event.eventId(), event.eventType(), event.orderId(), event.courierId());
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-history-service:test --tests "OrderViewServiceTest,DeliveryEventListenerTest"`
Expected: PASS

- [ ] **Step 7: Run the full order-history-service test suite**

Run: `./gradlew :ftgo-order-history-service:test`
Expected: PASS (all tests from Tasks 2–6)

- [ ] **Step 8: Commit**

```bash
git add ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/DeliveryEvent.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/DeliveryEventListener.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/DeliveryEventListenerTest.java
git commit -m "feat: consume delivery.events into the order-history read model"
```

---

### Task 7: `OrderHistoryController` — `GET /order-views/{orderId}`

**Files:**
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/api/OrderViewResponse.java`
- Create: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/api/OrderHistoryController.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/api/OrderHistoryControllerTest.java`

**Interfaces:**
- Consumes: `OrderView`/`OrderViewLineItem`/`OrderViewRepository` (Task 2).
- Produces: `GET /order-views/{orderId}` — terminal endpoint for this sub-project.

- [ ] **Step 1: Write the failing test**

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/api/OrderHistoryControllerTest.java
package com.sanjay.ftgo.orderhistory.api;

import com.sanjay.ftgo.orderhistory.domain.OrderView;
import com.sanjay.ftgo.orderhistory.domain.OrderViewLineItem;
import com.sanjay.ftgo.orderhistory.domain.OrderViewRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderHistoryController.class)
class OrderHistoryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderViewRepository orderViewRepository;

    @Test
    void returnsOrderViewWhenFound() throws Exception {
        OrderView view = new OrderView(42L);
        view.setConsumerId(1L);
        view.setRestaurantId(7L);
        view.setOrderStatus("APPROVED");
        view.setTicketStatus("ACCEPTED");
        view.setAuthorizationStatus("AUTHORIZED");
        view.setDeliveryStatus("SCHEDULED");
        view.setCourierId(3L);
        view.setLineItems(List.of(new OrderViewLineItem(10L, 2)));
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(view));

        mockMvc.perform(get("/order-views/42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(42))
                .andExpect(jsonPath("$.orderStatus").value("APPROVED"))
                .andExpect(jsonPath("$.ticketStatus").value("ACCEPTED"))
                .andExpect(jsonPath("$.authorizationStatus").value("AUTHORIZED"))
                .andExpect(jsonPath("$.deliveryStatus").value("SCHEDULED"))
                .andExpect(jsonPath("$.courierId").value(3))
                .andExpect(jsonPath("$.lineItems[0].menuItemId").value(10));
    }

    @Test
    void returns404WhenNotFound() throws Exception {
        when(orderViewRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/order-views/99")).andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-history-service:test --tests OrderHistoryControllerTest`
Expected: FAIL (compile error — `OrderHistoryController`/`OrderViewResponse` don't exist yet)

- [ ] **Step 3: Write `OrderViewResponse`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/api/OrderViewResponse.java
package com.sanjay.ftgo.orderhistory.api;

import java.util.List;

public record OrderViewResponse(
        Long orderId,
        Long consumerId,
        Long restaurantId,
        String orderStatus,
        String ticketStatus,
        String authorizationStatus,
        String deliveryStatus,
        Long courierId,
        List<LineItemView> lineItems) {

    public record LineItemView(Long menuItemId, int quantity) {
    }
}
```

- [ ] **Step 4: Write `OrderHistoryController`**

```java
// ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/api/OrderHistoryController.java
package com.sanjay.ftgo.orderhistory.api;

import com.sanjay.ftgo.orderhistory.domain.OrderView;
import com.sanjay.ftgo.orderhistory.domain.OrderViewRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/order-views")
public class OrderHistoryController {

    private final OrderViewRepository orderViewRepository;

    public OrderHistoryController(OrderViewRepository orderViewRepository) {
        this.orderViewRepository = orderViewRepository;
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderViewResponse> view(@PathVariable Long orderId) {
        return orderViewRepository.findById(orderId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private OrderViewResponse toResponse(OrderView view) {
        return new OrderViewResponse(
                view.getOrderId(), view.getConsumerId(), view.getRestaurantId(),
                view.getOrderStatus(), view.getTicketStatus(), view.getAuthorizationStatus(),
                view.getDeliveryStatus(), view.getCourierId(),
                view.getLineItems().stream()
                        .map(li -> new OrderViewResponse.LineItemView(li.menuItemId(), li.quantity()))
                        .toList());
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-order-history-service:test --tests OrderHistoryControllerTest`
Expected: PASS

- [ ] **Step 6: Run the full order-history-service test suite**

Run: `./gradlew :ftgo-order-history-service:test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/api/OrderViewResponse.java \
        ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/api/OrderHistoryController.java \
        ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/api/OrderHistoryControllerTest.java
git commit -m "feat: add GET /order-views/{orderId} query endpoint"
```

---

### Task 8: Full workspace build check

**Files:** none (verification-only task)

**Interfaces:** none

- [ ] **Step 1: Build every module**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL across all 9 modules (the 8 existing plus `ftgo-order-history-service`)

- [ ] **Step 2: Build every Docker image**

Run: `docker compose build`
Expected: all images build successfully, including the newly-wired `order-history-service`

- [ ] **Step 3: If anything fails, fix it now** — do not proceed to Task 9 with a red build.

(No commit for this task unless Step 3 required a fix.)

---

### Task 9: Docs — full Ch.7 completion sweep

**Files:**
- Create: `ftgo-order-history-service/README.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `CONTEXT.md`
- Modify: any other `README.md` the sweep below finds stale

**Interfaces:** none — documentation only.

This is a **chapter-completion full sweep**, not a per-change update — Ch.7 flips to Done once this merges (both sub-projects, API composition and CQRS, complete). Per this project's `CLAUDE.md`: "Whenever a book chapter's status flips to Done in `CONTEXT.md`'s progress table, do a full documentation sweep as part of that same change... Grep for the chapter's own saga/pattern names across `*.md`... to catch anything the per-change rule missed."

- [ ] **Step 1: Read the current state of every doc this sweep touches** — `docs/ARCHITECTURE.md`, `CONTEXT.md`, and grep for "Ch.7", "API composition", "CQRS", "order-history" across `*.md` (excluding `docs/session-*.md`, `docs/superpowers/plans/`, `docs/superpowers/specs/`, which are point-in-time records and stay as-is) to find anything a per-change update might have missed.

- [ ] **Step 2: Write `ftgo-order-history-service/README.md`** — full treatment matching the depth of `ftgo-delivery-service/README.md` (Role / API / Events consumed / Domain model / Idempotency & reliability / Running standalone). Cover: no Eureka, no synchronous calls to anyone; consumes `order.events`/`kitchen.events`/`accounting.events`/`delivery.events`; `GET /order-views/{orderId}`; the upsert-on-any-event pattern and why (cross-topic ordering isn't guaranteed); `restaurantId` is opaque (no restaurant-service dependency, since restaurant-service publishes no events); port 8088.

- [ ] **Step 3: Update `docs/ARCHITECTURE.md`** — add a "CQRS" subsection alongside the existing "API composition" section (added in the prior sub-project), explicitly contrasting the two patterns (latency/freshness/failure-coupling tradeoffs — API composition pays request-time latency and couples availability to all 4 downstream services being up; CQRS reads are near-instant and decoupled from write-side availability, at the cost of eventual consistency). Update the Kafka topic catalog table: `order-history-service` becomes a 4th/5th consumer of `order.events`/`kitchen.events`/`accounting.events`/`delivery.events` (add to existing consumer lists in that table, don't create parallel rows).

- [ ] **Step 4: Update `CONTEXT.md`** — new services-table row for `ftgo-order-history-service`; `[x] CQRS (Ch.7)` checked off in the Querying section of the patterns reference (both `API composition` and `CQRS` now checked); flip Ch.7's row in the book-progress table to **Done**; session log entry. Move any Ch.7-related items out of "Needs more depth"/"Open questions" if present (check — per the per-change entries added by sub-projects 1–2, these sections may already be clean for Ch.7, but verify).

- [ ] **Step 5: Sweep for anything the per-change rule missed** — based on Step 1's grep, update any other stale reference (e.g. an older summary doc that still says "Ch.7 not started" or a services table elsewhere that's out of sync).

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-history-service/README.md docs/ARCHITECTURE.md CONTEXT.md
# plus any other files Step 5 touched
git commit -m "docs: full Ch.7 documentation sweep (queries: API composition + CQRS, chapter complete)"
```

---

### Task 10: Manual Docker e2e verification

**Files:** none

**Interfaces:** none

- [ ] **Step 1: Bring up the full stack**

Run: `docker compose up --build -d`
Verify: all containers (including the new `order-history-service`, 12 total) reach a running state.

- [ ] **Step 2: Place a real order and poll the read model across its lifecycle**

`POST /orders` on order-service, then `GET /order-views/{id}` on order-history-service (port 8088) immediately — expect `orderStatus="APPROVAL_PENDING"`, other statuses `null`. Poll again after a few seconds (Kafka consumption lag) — expect `orderStatus="APPROVED"`, `ticketStatus`/`authorizationStatus`/`deliveryStatus` all populated with real end states.

- [ ] **Step 3: Cross-check against sub-project 2's endpoint**

For the same order at the same moment (after the read model has caught up), call `GET /orders/{id}/view` on order-service (the API-composition endpoint) and confirm both endpoints agree on the order's real status — this is the direct payoff of building both patterns: they should describe the same reality through two different mechanisms.

- [ ] **Step 4: Verify a decline scenario**

Create an order that triggers a decline (e.g. consumerId 2, inactive), let the saga settle, `GET /order-views/{id}` — confirm the read model reflects the real compensated end state (matching whatever `GET /orders/{id}/view` shows for the same order).

- [ ] **Step 5: Verify 404 on a nonexistent order**

`GET /order-views/999999` → expect `404`.

- [ ] **Step 6: Verify redelivery/idempotency**

Force a Kafka redelivery per this project's established technique (reset an already-sent outbox row's `sent_at` to NULL in the producing service, wait for the next poll cycle) and confirm `processed_events`/the `order_views` row's field values are unchanged after redelivery — no double-processing side effects (there are none to observe beyond idempotent overwrites here, but confirm no errors/duplicate rows).

- [ ] **Step 7: Tear down**

Run: `docker compose down` (omit `-v` unless the user confirms the `mysql-data` volume can be discarded).

No commit for this task — it's verification only. If any scenario surfaces a bug, fix it in a new commit with a `fix:` message describing the bug and root cause, then re-run the affected scenario before continuing.

---

## Deferred (not in this plan)

- Any query beyond single-order-by-id (listing, filtering, pagination, consumer-scoped queries).
- Restaurant name/menu denormalization.
- Caching, read replicas, eventual-consistency SLA tracking.
