# Ch.11 §11.3.4 Application Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose Prometheus-scrapeable metrics (`GET /actuator/prometheus`) on all 9 runnable services, add hand-instrumented business counters to the 7 business services, and stand up a `prometheus` + `grafana` compose stack with 3 alert rules and one dashboard, verified by a new Cucumber e2e scenario plus manual Docker inspection.

**Architecture:** `micrometer-registry-prometheus` added to the same `actuatorModules` list in root `build.gradle` that already adds `spring-boot-starter-actuator` (9 services). Each business service gets `Counter` increments injected at the exact domain-transition call sites identified below — some corrected from the spec's loose table after reading the real code (see Global Constraints). `compose.yml` gains `prometheus`/`grafana` observer containers, not on any business service's `depends_on` critical path.

**Tech Stack:** Micrometer (`io.micrometer:micrometer-registry-prometheus`), Spring Boot Actuator (already present), Prometheus (`prom/prometheus` image, native alerting rules, no Alertmanager), Grafana (`grafana/grafana` image, provisioned datasource + dashboard), Cucumber/JUnit Platform (existing `ftgo-end-to-end-test` module).

## Global Constraints

- Spec source: `docs/superpowers/specs/2026-08-03-ch11-application-metrics-design.md`.
- All 9 services already have `spring-boot-starter-actuator` via the `actuatorModules` list in root `build.gradle:69-77`; this plan only adds the Prometheus registry dependency to the same block and flips `management.endpoints.web.exposure.include` from `health` to `health, prometheus` in each service's `application.yml` (every one of the 9 currently reads exactly `include: health`, confirmed by grep — no divergent formats to handle).
- **Correction vs. spec:** `ftgo-order-service`'s `OrderTransitions` interface has two implementations gated by the `persistence.mode` property (`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/JpaOrderTransitions.java`, `@ConditionalOnProperty(havingValue = "jpa", matchIfMissing = true)`, and `EventSourcedOrderTransitions.java`, `havingValue = "event-sourcing"`). `jpa` is the active default (`ftgo-order-service/src/main/resources/application.yml:40` sets `persistence.mode: jpa`). Only `JpaOrderTransitions` gets counters — `EventSourcedOrderTransitions` is out of scope for this sub-project (it's inactive by default; instrumenting it would double the work for a code path nothing currently runs).
- **Correction vs. spec:** the spec's table says counters live in "OrderService" — they actually belong in `JpaOrderTransitions`, since that's the class whose methods (`create`, `approve`, `reject`, `cancel`) are the real one-to-one mapping to `Order`'s domain transitions. `OrderService.createOrder()` only orchestrates restaurant validation and delegates to `orderTransitions.create(...)`; putting the counter there would miss `approve`/`reject`/`cancel`, which never pass through `OrderService` at all (they're driven by saga event handlers calling `OrderTransitions` directly).
- **`orders_cancelled` counts at `cancel()`, not `noteCancelled()`:** `Order.java:104-116` shows `cancel()` is the real state transition (`APPROVED` → `CANCEL_PENDING`, the point a cancellation is initiated) while `noteCancelled()` only confirms an already-in-flight cancellation (`CANCEL_PENDING` → `CANCELLED`). Counting at `cancel()` matches the spec's own pattern of counting the initiating action (`orders_placed` counts at `create()`, not at some later confirmation step).
- **kitchen-service counters split across two classes, not one:** `Ticket.java` has `confirm()` (`CREATE_PENDING`→`AWAITING_ACCEPTANCE`), `accept()` (`AWAITING_ACCEPTANCE`→`ACCEPTED`), `preparing()`, `readyForPickup()`, `pickedUp()`, `cancel()`. `TicketService.java` only calls `confirm()` and `cancel()` (from Kafka/saga handlers). `accept()`, `preparing()`, `readyForPickup()`, `pickedUp()` are called exclusively from `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java` (staff-facing REST endpoints) — confirmed by grep, they are reachable, not vestigial. So: `tickets_accepted`/`tickets_preparing`/`tickets_ready_for_pickup`/`tickets_picked_up` are injected in `TicketController`, and `tickets_cancelled` is injected in `TicketService.handleCancelTicketCommand`/`handleOrderCancelled`/`handleConsumerVerificationFailed`/`handleAccountingEvent` (all four call sites that invoke `ticket.cancel()`) — no correction needed to the counter names themselves, only to which class owns which counter.
- **delivery-service counters split across two classes too, mirroring kitchen:** `Delivery.java` has `pickUp()` and `deliver()`, both called only from `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java` (`/deliveries/{id}/picked-up`, `/deliveries/{id}/delivered`, courier-facing). `DeliveryService.java`'s `schedule()` (private, called from `handleOrderCreated`/`handleScheduleDeliveryCommand`) is where a delivery is first created — inject `deliveries_scheduled` there. `release()`/`handleReleaseDeliveryCommand()` in `DeliveryService` both call `delivery.cancel()` — inject `deliveries_cancelled` there, guarded by the same `if (!events.isEmpty())` check those methods already use (a delivery already `CANCELLED` from a racing caller must not double-count).
- **accounting-service:** `Authorization.authorize(...)`/`Authorization.decline(...)` (`Authorization.java:38,43`) are static factories called from `SagaJoinService.handleAuthorizeCardCommand` and `SagaJoinService.tryResolve` (`ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`) — both call sites branch on the same `authorized` boolean, so both need the counter increment. No `authorizations_reversed` call site exists in the current codebase reachable from a distinct "reversal" trigger beyond what's already covered — `AuthorizationCancelService.reverseForChoreography`/`reverseForCommand` both call `authorization.reverse()`; inject `authorizations_reversed` there (2 call sites, both needed for choreography vs. orchestration saga modes).
- No spec requirement changes for restaurant-service (`restaurants_created`, in `RestaurantController.createRestaurant`), consumer-service (`consumers_created`, in `ConsumerController.createConsumer`), or order-history-service (`order_views_updated`, in `OrderEventListener.onMessage` via `OrderViewService.handleOrderEvent` — see Task 7) — these match the spec's table exactly.
- Prometheus alert rules use Prometheus's native rule format, no Alertmanager (per spec, out of scope).
- Documentation lands per-change in the same commits as the code they describe (project `CLAUDE.md` convention) — Task 11 is not a full chapter-completion sweep (§11.3 as a whole isn't Done; see spec's Documentation section).

---

### Task 1: Add Prometheus registry + actuator exposure to all 9 services

**Files:**
- Modify: `build.gradle:73-77` (the `actuatorModules` `configure` block)
- Modify: `ftgo-order-service/src/main/resources/application.yml:79`
- Modify: `ftgo-kitchen-service/src/main/resources/application.yml` (find the `include: health` line)
- Modify: `ftgo-consumer-service/src/main/resources/application.yml:38`
- Modify: `ftgo-restaurant-service/src/main/resources/application.yml` (find the `include: health` line)
- Modify: `ftgo-accounting-service/src/main/resources/application.yml:45`
- Modify: `ftgo-delivery-service/src/main/resources/application.yml` (find the `include: health` line)
- Modify: `ftgo-order-history-service/src/main/resources/application.yml` (find the `include: health` line)
- Modify: `ftgo-mobile-gateway/src/main/resources/application.yml:78`
- Modify: `ftgo-public-gateway/src/main/resources/application.yml:83`

**Interfaces:**
- Produces: `GET /actuator/prometheus` on all 9 services (port map: order-service 8082, kitchen-service 8083, consumer-service 8081, restaurant-service 8085, accounting-service 8084, delivery-service 8086, order-history-service 8088, mobile-gateway 8090, public-gateway 8091), a `MeterRegistry` bean (Spring Boot autoconfigures `PrometheusMeterRegistry` once the dependency is present) available for `@Autowired`/constructor injection in Tasks 2-7.

- [ ] **Step 1: Add the Micrometer Prometheus dependency to the actuator block**

In `build.gradle`, change:
```groovy
configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
    }
}
```
to:
```groovy
configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        implementation 'io.micrometer:micrometer-registry-prometheus'
    }
}
```

- [ ] **Step 2: Expose the prometheus endpoint in each of the 9 services**

In each of the 9 `application.yml` files listed above, change:
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health
```
to:
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, prometheus
```
Leave the rest of each file's `management:` block (e.g. `endpoint:` sub-keys) untouched — only the `include` value changes.

- [ ] **Step 3: Build to confirm the dependency resolves**

Run: `./gradlew :ftgo-order-service:compileJava :ftgo-kitchen-service:compileJava :ftgo-consumer-service:compileJava :ftgo-restaurant-service:compileJava :ftgo-accounting-service:compileJava :ftgo-delivery-service:compileJava :ftgo-order-history-service:compileJava :ftgo-mobile-gateway:compileJava :ftgo-public-gateway:compileJava`
Expected: BUILD SUCCESSFUL, no dependency resolution errors.

- [ ] **Step 4: Manually verify one service exposes the endpoint**

Run: `./gradlew :ftgo-order-service:bootRun &` then, once it's up, `curl -s http://localhost:8082/actuator/prometheus | head -20` — expect Prometheus text-exposition-format output (lines like `# HELP jvm_memory_used_bytes ...`). Stop the process afterward (`kill %1` or `fg` then Ctrl-C).

- [ ] **Step 5: Commit**

```bash
git add build.gradle ftgo-order-service/src/main/resources/application.yml \
  ftgo-kitchen-service/src/main/resources/application.yml \
  ftgo-consumer-service/src/main/resources/application.yml \
  ftgo-restaurant-service/src/main/resources/application.yml \
  ftgo-accounting-service/src/main/resources/application.yml \
  ftgo-delivery-service/src/main/resources/application.yml \
  ftgo-order-history-service/src/main/resources/application.yml \
  ftgo-mobile-gateway/src/main/resources/application.yml \
  ftgo-public-gateway/src/main/resources/application.yml
git commit -m "feat: expose Prometheus metrics endpoint on all 9 services"
```

---

### Task 2: order-service custom counters

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/JpaOrderTransitions.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/JpaOrderTransitionsTest.java`

**Interfaces:**
- Consumes: `io.micrometer.core.instrument.MeterRegistry` (Spring-provided bean from Task 1).
- Produces: counters `orders_placed`, `orders_approved`, `orders_rejected`, `orders_cancelled` (plain `Counter`, no tags), incremented once per successful `create()`/`approve()`/`reject()`/`cancel()` call.

- [ ] **Step 1: Write the failing tests**

Add to `JpaOrderTransitionsTest.java` (new imports: `io.micrometer.core.instrument.simple.SimpleMeterRegistry`):
```java
    @Test
    void createIncrementsOrdersPlacedCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        JpaOrderTransitions withMetrics = new JpaOrderTransitions(orderRepository, domainEventPublisher, meterRegistry);
        when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        withMetrics.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThat(meterRegistry.counter("orders_placed").count()).isEqualTo(1.0);
    }

    @Test
    void approveIncrementsOrdersApprovedCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        JpaOrderTransitions withMetrics = new JpaOrderTransitions(orderRepository, domainEventPublisher, meterRegistry);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVAL_PENDING)));
        when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        withMetrics.approve(42L, "evt-1");

        assertThat(meterRegistry.counter("orders_approved").count()).isEqualTo(1.0);
    }

    @Test
    void rejectIncrementsOrdersRejectedCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        JpaOrderTransitions withMetrics = new JpaOrderTransitions(orderRepository, domainEventPublisher, meterRegistry);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVAL_PENDING)));
        when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        withMetrics.reject(42L, "evt-1");

        assertThat(meterRegistry.counter("orders_rejected").count()).isEqualTo(1.0);
    }

    @Test
    void cancelIncrementsOrdersCancelledCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        JpaOrderTransitions withMetrics = new JpaOrderTransitions(orderRepository, domainEventPublisher, meterRegistry);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVED)));
        when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        withMetrics.cancel(42L, "evt-1");

        assertThat(meterRegistry.counter("orders_cancelled").count()).isEqualTo(1.0);
    }
```
`orderIn(OrderStatus)` is the existing private helper at the top of the test class — reuse it, do not redefine it. `OrderStatus.APPROVAL_PENDING` is the status `Order`'s constructor accepts that lets `noteApproved()`/`noteRejected()` succeed (see `Order.java:88-100`); `OrderStatus.APPROVED` is required for `cancel()` to succeed (`Order.java:104-108`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-service:test --tests JpaOrderTransitionsTest -i`
Expected: FAIL — compile error, `JpaOrderTransitions(OrderRepository, OrderDomainEventPublisher, MeterRegistry)` constructor does not exist yet.

- [ ] **Step 3: Add the MeterRegistry field and counter increments**

In `JpaOrderTransitions.java`, add the import and field, and update the constructor:
```java
import io.micrometer.core.instrument.MeterRegistry;
```
```java
    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final MeterRegistry meterRegistry;

    public JpaOrderTransitions(OrderRepository orderRepository, OrderDomainEventPublisher domainEventPublisher,
                                MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.meterRegistry = meterRegistry;
    }
```
Update the four transition methods:
```java
    @Override
    @Transactional
    public Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId) {
        Order order = orderRepository.save(new Order(consumerId, restaurantId, lineItems, OrderStatus.APPROVAL_PENDING));
        meterRegistry.counter("orders_placed").increment();
        return order;
    }
```
```java
    @Override
    @Transactional
    public TransitionResult cancel(Long orderId, String eventId) {
        Order order = findOrThrow(orderId);
        List<OrderDomainEvent> events = order.cancel();
        orderRepository.save(order);
        meterRegistry.counter("orders_cancelled").increment();
        return new TransitionResult(order, events);
    }
```
```java
    @Override
    @Transactional
    public void approve(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteApproved, "approve");
        meterRegistry.counter("orders_approved").increment();
    }

    @Override
    @Transactional
    public void reject(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteRejected, "reject");
        meterRegistry.counter("orders_rejected").increment();
    }
```
Note: `applyBestEffort` silently no-ops (via `UnsupportedStateTransitionException` catch, `JpaOrderTransitions.java:106-118`) when the order is missing or already in a state that rejects the transition — the counter increments unconditionally here, same best-effort semantics as the rest of the method. This matches `cancel()`, which also unconditionally increments once `order.cancel()` has been called (it throws `UnsupportedStateTransitionException` if invalid, which propagates and prevents the increment — consistent "count only on an actual attempted domain call" behavior).

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests JpaOrderTransitionsTest -i`
Expected: PASS, all tests including the 4 new ones and all pre-existing ones (constructor signature change requires no other call-site edits — Spring injects the new `MeterRegistry` param automatically; the `@InjectMocks` pre-existing tests need a `@Mock private MeterRegistry meterRegistry;` field added alongside the other `@Mock` fields for `@InjectMocks` to satisfy the 3-arg constructor).

Add this to the top of `JpaOrderTransitionsTest.java`'s existing `@Mock` fields:
```java
    @Mock
    private MeterRegistry meterRegistry;
```
(with `import io.micrometer.core.instrument.MeterRegistry;` added). This mock is unused by the counter-specific tests above (which construct their own `SimpleMeterRegistry`-backed instance directly) but is required so the pre-existing `@InjectMocks private JpaOrderTransitions transitions;` field still resolves a 3-arg constructor for the other, already-passing tests in this class — Mockito's `@Mock` returns a no-op stub for `.counter(...).increment()` chains by default (a mocked `MeterRegistry.counter(String)` returns `null` unless stubbed — a raw `@Mock` will NPE on `.increment()`), so stub it: add `@BeforeEach` setup or inline `when(meterRegistry.counter(anyString())).thenReturn(mock(Counter.class));` — simplest is a single `@BeforeEach` method:
```java
    @BeforeEach
    void stubMeterRegistry() {
        lenient().when(meterRegistry.counter(anyString())).thenReturn(mock(io.micrometer.core.instrument.Counter.class));
    }
```
with `import org.junit.jupiter.api.BeforeEach;`, `import static org.mockito.ArgumentMatchers.anyString;`, `import static org.mockito.Mockito.lenient;`, `import static org.mockito.Mockito.mock;` added to the existing import block.

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/JpaOrderTransitions.java \
  ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/JpaOrderTransitionsTest.java
git commit -m "feat: add orders_placed/approved/rejected/cancelled counters to order-service"
```

---

### Task 3: kitchen-service custom counters

**Files:**
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`
- Test: create `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerMetricsTest.java`

**Interfaces:**
- Consumes: `io.micrometer.core.instrument.MeterRegistry`.
- Produces: counters `tickets_accepted`, `tickets_preparing`, `tickets_ready_for_pickup`, `tickets_picked_up` (in `TicketController`), `tickets_cancelled` (in `TicketService`).

- [ ] **Step 1: Write the failing test for TicketService's cancellation counter**

Read `TicketServiceTest.java` first to find its existing constructor-mocking pattern for `TicketService` (it mocks `TicketRepository`, `ProcessedEventRepository`, `FailedOrderRepository`, `OutboxEventRepository`, `TicketDomainEventPublisher`, `ObjectMapper` — 6 args). Add one test using `SimpleMeterRegistry` directly, following the same construction style as Task 2's tests (bypass `@InjectMocks` for this specific test, wire a 7-arg constructor manually):
```java
    @Test
    void handleCancelTicketCommandIncrementsTicketsCancelledCounter() {
        io.micrometer.core.instrument.simple.SimpleMeterRegistry meterRegistry =
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        TicketService withMetrics = new TicketService(ticketRepository, processedEventRepository,
                failedOrderRepository, outboxEventRepository, domainEventPublisher, objectMapper, meterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        when(ticketRepository.findByOrderId(42L)).thenReturn(java.util.Optional.of(ticket));

        withMetrics.handleCancelTicketCommand("evt-1", 42L, "CreateOrder");

        assertThat(meterRegistry.counter("tickets_cancelled").count()).isEqualTo(1.0);
    }
```
Add this test method to `TicketServiceTest.java`, matching whatever field names the existing mocks in that class already use for `ticketRepository`/`processedEventRepository`/`failedOrderRepository`/`outboxEventRepository`/`domainEventPublisher`/`objectMapper` (read the file to confirm exact field names before writing this — do not guess).

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-kitchen-service:test --tests TicketServiceTest -i`
Expected: FAIL — compile error, no 7-arg `TicketService` constructor.

- [ ] **Step 3: Add the MeterRegistry field to TicketService and increment on cancellation**

In `TicketService.java`, add the import and field:
```java
import io.micrometer.core.instrument.MeterRegistry;
```
```java
    private final TicketDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public TicketService(TicketRepository ticketRepository,
                          ProcessedEventRepository processedEventRepository,
                          FailedOrderRepository failedOrderRepository,
                          OutboxEventRepository outboxEventRepository,
                          TicketDomainEventPublisher domainEventPublisher,
                          ObjectMapper objectMapper,
                          MeterRegistry meterRegistry) {
        this.ticketRepository = ticketRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedOrderRepository = failedOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }
```
Add `meterRegistry.counter("tickets_cancelled").increment();` immediately after each of the four `ticket.cancel()` call sites that currently succeed (i.e. inside the `try` block after `ticketRepository.save(ticket)`, not before — a cancel that throws must not increment):
- `handleAccountingEvent`: after the `case "CardAuthorizationFailed" -> ticket.cancel();` branch resolves and `ticketRepository.save(ticket)` runs (the existing code already does this generically for both branches via the shared `events`/`domainEventPublisher.publish` — add the increment guarded so it only fires for the cancel branch: introduce a local `boolean cancelled = "CardAuthorizationFailed".equals(eventType);` before the `switch`, then `if (cancelled) { meterRegistry.counter("tickets_cancelled").increment(); }` right after `ticketRepository.save(ticket);`).
- `handleConsumerVerificationFailed`: after `ticketRepository.save(ticket);` inside the `if (ticket != null)` branch.
- `handleCancelTicketCommand`: after `ticketRepository.save(ticket);` inside the `try` block (before `publishReply("TicketCancelled", ...)`).
- `handleOrderCancelled`: after `ticketRepository.save(ticket);` inside the `try` block (before `domainEventPublisher.publish(ticket, events);`).

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests TicketServiceTest -i`
Expected: PASS. Also add `@Mock private MeterRegistry meterRegistry;` plus the same `lenient().when(meterRegistry.counter(anyString())).thenReturn(mock(Counter.class));` `@BeforeEach` stub as Task 2 to every pre-existing `@InjectMocks`-based test in this class, matching whatever `@BeforeEach`/setup pattern the file already uses (add one if none exists).

- [ ] **Step 5: Write the failing test for TicketController's four staff-driven counters**

Read `TicketController.java` fully first to learn its constructor dependencies and the exact response types of `accept`/`preparing`/`readyForPickup`/`pickedUp` endpoints. Create `TicketControllerMetricsTest.java`:
```java
package com.sanjay.ftgo.kitchen.api;

import com.sanjay.ftgo.kitchen.domain.Ticket;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisher;
import com.sanjay.ftgo.kitchen.domain.TicketRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TicketControllerMetricsTest {

    @Mock
    private TicketRepository ticketRepository;

    @Mock
    private TicketDomainEventPublisher domainEventPublisher;

    private SimpleMeterRegistry meterRegistry;
    private TicketController controller;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        controller = new TicketController(ticketRepository, domainEventPublisher, meterRegistry);
    }

    @Test
    void acceptIncrementsTicketsAcceptedCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.accept(1L, new AcceptTicketRequest(ZonedDateTime.now().plusMinutes(20)));

        assertThat(meterRegistry.counter("tickets_accepted").count()).isEqualTo(1.0);
    }

    @Test
    void preparingIncrementsTicketsPreparingCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(20));
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.preparing(1L);

        assertThat(meterRegistry.counter("tickets_preparing").count()).isEqualTo(1.0);
    }

    @Test
    void readyForPickupIncrementsTicketsReadyForPickupCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(20));
        ticket.preparing();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.readyForPickup(1L);

        assertThat(meterRegistry.counter("tickets_ready_for_pickup").count()).isEqualTo(1.0);
    }

    @Test
    void pickedUpIncrementsTicketsPickedUpCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(20));
        ticket.preparing();
        ticket.readyForPickup();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.pickedUp(1L);

        assertThat(meterRegistry.counter("tickets_picked_up").count()).isEqualTo(1.0);
    }
}
```
This test's exact request/response types (`AcceptTicketRequest`, method signatures for `accept`/`preparing`/`readyForPickup`/`pickedUp`, and `ticketRepository.save(...)` mocking needs, if `findById` alone isn't sufficient given `TicketController`'s `apply()` helper) must be reconciled against what you actually read in `TicketController.java` in this step — adjust field/method names to match the real class exactly; the shape above is the intended pattern, not verbatim-guaranteed API.

- [ ] **Step 6: Run test to verify it fails**

Run: `./gradlew :ftgo-kitchen-service:test --tests TicketControllerMetricsTest -i`
Expected: FAIL — compile error, no `MeterRegistry`-accepting `TicketController` constructor.

- [ ] **Step 7: Add MeterRegistry to TicketController and increment on each transition**

Add `MeterRegistry` as a constructor param (same pattern as Tasks 2-3), and add one `meterRegistry.counter("tickets_accepted").increment();` (etc., matching each endpoint's transition name) immediately after each `apply(ticket, ticket.xxx())` call inside `accept`/`preparing`/`readyForPickup`/`pickedUp`, before the `return ResponseEntity.ok().build();`.

- [ ] **Step 8: Run tests to verify they pass**

Run: `./gradlew :ftgo-kitchen-service:test --tests TicketServiceTest,TicketControllerMetricsTest,TicketControllerTest -i`
Expected: PASS (include `TicketControllerTest` — the pre-existing controller test — to confirm the constructor change didn't break it; if it uses `@InjectMocks`, add the same `@Mock`+stub pattern as Task 2 Step 4).

- [ ] **Step 9: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java \
  ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java \
  ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java \
  ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerMetricsTest.java \
  ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerTest.java
git commit -m "feat: add ticket lifecycle counters to kitchen-service"
```

---

### Task 4: accounting-service custom counters

**Files:**
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelService.java`
- Test: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java`
- Test: create `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelServiceMetricsTest.java`

**Interfaces:**
- Consumes: `io.micrometer.core.instrument.MeterRegistry`.
- Produces: counters `authorizations_approved`, `authorizations_declined` (in `SagaJoinService`), `authorizations_reversed` (in `AuthorizationCancelService`).

- [ ] **Step 1: Write failing tests for SagaJoinService**

Read `SagaJoinServiceTest.java` first to learn its exact mock field names and constructor argument order. Add two tests following the pattern from Task 2 (construct a fresh instance with `SimpleMeterRegistry`, bypassing `@InjectMocks`):
```java
    @Test
    void handleAuthorizeCardCommandIncrementsAuthorizationsApprovedCounterWhenAuthorized() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        SagaJoinService withMetrics = new SagaJoinService(/* existing mocked deps in this class's order */, meterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);

        withMetrics.handleAuthorizeCardCommand("evt-1", 42L, 5);

        assertThat(meterRegistry.counter("authorizations_approved").count()).isEqualTo(1.0);
    }

    @Test
    void handleAuthorizeCardCommandIncrementsAuthorizationsDeclinedCounterWhenOverLimit() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        SagaJoinService withMetrics = new SagaJoinService(/* existing mocked deps */, meterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);

        withMetrics.handleAuthorizeCardCommand("evt-1", 42L, 999999);

        assertThat(meterRegistry.counter("authorizations_declined").count()).isEqualTo(1.0);
    }
```
Read `SagaJoinService.isAuthorized(int totalQuantity)` and the existing `SagaJoinServiceTest.java` to find the exact quantity threshold/mock setup already used to force an approved vs. declined outcome in existing tests — reuse those exact values instead of guessing `999999`; substitute the real threshold-crossing values once read.

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-accounting-service:test --tests SagaJoinServiceTest -i`
Expected: FAIL — compile error, constructor signature mismatch.

- [ ] **Step 3: Add MeterRegistry to SagaJoinService**

Add `MeterRegistry meterRegistry` as a new constructor param (import `io.micrometer.core.instrument.MeterRegistry`), store as a field. In `handleAuthorizeCardCommand`, after:
```java
        boolean authorized = isAuthorized(totalQuantity);
        AuthorizationResult result = authorized
                ? Authorization.authorize(orderId, totalQuantity)
                : Authorization.decline(orderId, "order quantity exceeds authorization limit", totalQuantity);
        authorizationRepository.save(result.authorization());
```
add:
```java
        meterRegistry.counter(authorized ? "authorizations_approved" : "authorizations_declined").increment();
```
Apply the identical addition in `tryResolve` right after its analogous `authorizationRepository.save(result.authorization());` line (same `authorized` boolean already in scope there).

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests SagaJoinServiceTest -i`
Expected: PASS. Add the `@Mock private MeterRegistry meterRegistry;` + `lenient()` stub pattern from Task 2 Step 4 for this class's pre-existing `@InjectMocks`-based tests.

- [ ] **Step 5: Write failing test for AuthorizationCancelService**

Read `AuthorizationCancelServiceTest.java` if it exists (else check for other tests covering this class) for constructor/mock conventions; if none exists, follow the constructor-mocking style established in `SagaJoinServiceTest.java`. Create `AuthorizationCancelServiceMetricsTest.java`:
```java
package com.sanjay.ftgo.accounting.domain;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizationCancelServiceMetricsTest {

    @Mock
    private AuthorizationRepository authorizationRepository;
    @Mock
    private ProcessedEventRepository processedEventRepository;
    @Mock
    private AuthorizationDomainEventPublisher domainEventPublisher;
    @Mock
    private OutboxEventRepository outboxEventRepository;

    private SimpleMeterRegistry meterRegistry;
    private AuthorizationCancelService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new AuthorizationCancelService(authorizationRepository, processedEventRepository,
                domainEventPublisher, outboxEventRepository, new ObjectMapper(), meterRegistry);
    }

    @Test
    void reverseForChoreographyIncrementsAuthorizationsReversedCounter() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reverseForChoreography("evt-1", 42L);

        assertThat(meterRegistry.counter("authorizations_reversed").count()).isEqualTo(1.0);
    }

    @Test
    void reverseForCommandIncrementsAuthorizationsReversedCounter() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reverseForCommand("evt-1", 42L, "CancelOrder");

        assertThat(meterRegistry.counter("authorizations_reversed").count()).isEqualTo(1.0);
    }
}
```
Verify `Authorization.authorize(orderId, quantity)` returns an `AuthorizationResult` with an `.authorization()` accessor by re-checking `Authorization.java` (already read: `AuthorizationResult` wraps `authorization` + `events`) — adjust the helper call if the accessor name differs from `.authorization()`.

- [ ] **Step 6: Run test to verify it fails**

Run: `./gradlew :ftgo-accounting-service:test --tests AuthorizationCancelServiceMetricsTest -i`
Expected: FAIL — compile error, no 6-arg `AuthorizationCancelService` constructor.

- [ ] **Step 7: Add MeterRegistry to AuthorizationCancelService**

Add `MeterRegistry meterRegistry` as a new constructor param, store as a field. In both `reverseForChoreography` and `reverseForCommand`, immediately after `authorizationRepository.save(authorization);`, add:
```java
        meterRegistry.counter("authorizations_reversed").increment();
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests SagaJoinServiceTest,AuthorizationCancelServiceMetricsTest -i`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java \
  ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelService.java \
  ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java \
  ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/AuthorizationCancelServiceMetricsTest.java
git commit -m "feat: add authorization counters to accounting-service"
```

---

### Task 5: delivery-service custom counters

**Files:**
- Modify: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryService.java`
- Modify: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryServiceTest.java`
- Test: create `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerMetricsTest.java`

**Interfaces:**
- Consumes: `io.micrometer.core.instrument.MeterRegistry`.
- Produces: counters `deliveries_scheduled`, `deliveries_cancelled` (in `DeliveryService`), `deliveries_picked_up`, `deliveries_delivered` (in `DeliveryController`).

- [ ] **Step 1: Write failing tests for DeliveryService**

Read `DeliveryServiceTest.java` first for exact mock field names/constructor order. Add two tests (following Task 2's `SimpleMeterRegistry`-direct-construction pattern):
```java
    @Test
    void handleOrderCreatedIncrementsDeliveriesScheduledCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        DeliveryService withMetrics = new DeliveryService(deliveryRepository, courierRepository,
                processedEventRepository, failedOrderRepository, outboxEventRepository,
                domainEventPublisher, objectMapper, meterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.of(new Courier()));

        withMetrics.handleOrderCreated("evt-1", 42L, 7L);

        assertThat(meterRegistry.counter("deliveries_scheduled").count()).isEqualTo(1.0);
    }

    @Test
    void releaseIncrementsDeliveriesCancelledCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        DeliveryService withMetrics = new DeliveryService(deliveryRepository, courierRepository,
                processedEventRepository, failedOrderRepository, outboxEventRepository,
                domainEventPublisher, objectMapper, meterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        Delivery delivery = Delivery.schedule(42L, 7L, 1L).delivery();
        when(deliveryRepository.findForUpdateByOrderId(42L)).thenReturn(Optional.of(delivery));

        withMetrics.release("evt-1", 42L);

        assertThat(meterRegistry.counter("deliveries_cancelled").count()).isEqualTo(1.0);
    }
```
Verify `Courier`'s no-arg constructor and setter conventions, and `Delivery.schedule(orderId, restaurantId, courierId)`'s exact return-type accessor (`.delivery()`), by reading `Courier.java` and `Delivery.java` — adjust the test fixture construction to match what's actually there if it differs (e.g. `Courier` may require a constructor arg).

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryServiceTest -i`
Expected: FAIL — compile error, no 8-arg `DeliveryService` constructor.

- [ ] **Step 3: Add MeterRegistry to DeliveryService**

Add `MeterRegistry meterRegistry` as a new constructor param, store as field. In private `schedule(Long orderId, Long restaurantId)`, after `Delivery delivery = deliveryRepository.save(result.delivery());`, add:
```java
        meterRegistry.counter("deliveries_scheduled").increment();
```
In `handleScheduleDeliveryCommand`, after `deliveryRepository.save(result.delivery());`, add the same increment (this is the orchestration-mode equivalent of `schedule()` — it does not call the private `schedule()` helper, so needs its own increment).
In `release`, inside `if (!events.isEmpty())`, add `meterRegistry.counter("deliveries_cancelled").increment();` alongside the existing `releaseCourier(delivery); domainEventPublisher.publish(delivery, events);` lines.
In `handleReleaseDeliveryCommand`, inside its `if (!events.isEmpty())`, add the same increment alongside `releaseCourier(delivery); publishReply("DeliveryCancelled", ...)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryServiceTest -i`
Expected: PASS. Add the `@Mock`+`lenient()` stub pattern from Task 2 Step 4 to this class's existing `@InjectMocks` tests.

- [ ] **Step 5: Write failing test for DeliveryController**

Create `DeliveryControllerMetricsTest.java`, reading `DeliveryController.java` and any existing `DeliveryControllerTest.java` first for exact constructor/mock conventions:
```java
package com.sanjay.ftgo.delivery.api;

import com.sanjay.ftgo.delivery.domain.Delivery;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEventPublisher;
import com.sanjay.ftgo.delivery.domain.DeliveryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeliveryControllerMetricsTest {

    @Mock
    private DeliveryRepository deliveryRepository;
    @Mock
    private DeliveryDomainEventPublisher domainEventPublisher;

    private SimpleMeterRegistry meterRegistry;
    private DeliveryController controller;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        controller = new DeliveryController(deliveryRepository, domainEventPublisher, meterRegistry);
    }

    @Test
    void pickedUpIncrementsDeliveriesPickedUpCounter() {
        Delivery delivery = Delivery.schedule(42L, 7L, 1L).delivery();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        controller.pickedUp(1L);

        assertThat(meterRegistry.counter("deliveries_picked_up").count()).isEqualTo(1.0);
    }

    @Test
    void deliveredIncrementsDeliveriesDeliveredCounter() {
        Delivery delivery = Delivery.schedule(42L, 7L, 1L).delivery();
        delivery.pickUp();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        controller.delivered(1L);

        assertThat(meterRegistry.counter("deliveries_delivered").count()).isEqualTo(1.0);
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryControllerMetricsTest -i`
Expected: FAIL — compile error, no `MeterRegistry`-accepting `DeliveryController` constructor.

- [ ] **Step 7: Add MeterRegistry to DeliveryController**

Add `MeterRegistry meterRegistry` as a new constructor param. In `pickedUp`, after `apply(delivery, delivery.pickUp());`, add `meterRegistry.counter("deliveries_picked_up").increment();` before `return ResponseEntity.ok().build();`. In `delivered`, after `apply(delivery, delivery.deliver());`, add `meterRegistry.counter("deliveries_delivered").increment();` before its `return`.

- [ ] **Step 8: Run tests to verify they pass**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryServiceTest,DeliveryControllerMetricsTest,DeliveryControllerTest -i`
Expected: PASS (include the pre-existing `DeliveryControllerTest` to confirm the constructor change doesn't break it — apply the `@Mock`+stub fix there too if it uses `@InjectMocks`).

- [ ] **Step 9: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryService.java \
  ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java \
  ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryServiceTest.java \
  ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerMetricsTest.java \
  ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java
git commit -m "feat: add delivery lifecycle counters to delivery-service"
```

---

### Task 6: restaurant-service and consumer-service custom counters

**Files:**
- Modify: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantController.java`
- Modify: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/api/ConsumerController.java`
- Test: `ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/api/RestaurantControllerTest.java`
- Test: `ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/api/ConsumerControllerTest.java`

**Interfaces:**
- Consumes: `io.micrometer.core.instrument.MeterRegistry`.
- Produces: counter `restaurants_created` (`RestaurantController`), counter `consumers_created` (`ConsumerController`).

- [ ] **Step 1: Write failing test for RestaurantController**

Read `RestaurantControllerTest.java` and `RestaurantController.java` fully first (constructor args, field names, response type for `createRestaurant`). Add:
```java
    @Test
    void createRestaurantIncrementsRestaurantsCreatedCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RestaurantController withMetrics = new RestaurantController(restaurantRepository, meterRegistry);
        when(restaurantRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        withMetrics.createRestaurant(new CreateRestaurantRequest("Test", List.of()));

        assertThat(meterRegistry.counter("restaurants_created").count()).isEqualTo(1.0);
    }
```
Adjust `RestaurantController`'s constructor arg list and `CreateRestaurantRequest`'s exact constructor signature to match what you read — `RestaurantController.java` line 44 (`restaurantRepository.save(new Restaurant(request.name(), menuItems))`) confirms `restaurantRepository` is the field name; confirm any other dependencies (e.g. an event publisher) that the real constructor may also take.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-restaurant-service:test --tests RestaurantControllerTest -i`
Expected: FAIL — compile error, constructor mismatch.

- [ ] **Step 3: Add MeterRegistry to RestaurantController**

Add `MeterRegistry meterRegistry` as a new constructor param. After the existing `Restaurant restaurant = restaurantRepository.save(new Restaurant(request.name(), menuItems));` line, add:
```java
        meterRegistry.counter("restaurants_created").increment();
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :ftgo-restaurant-service:test --tests RestaurantControllerTest -i`
Expected: PASS. Apply the `@Mock`+stub pattern from Task 2 Step 4 to this class's other pre-existing tests if it uses `@InjectMocks`.

- [ ] **Step 5: Write failing test for ConsumerController**

Read `ConsumerControllerTest.java` and `ConsumerController.java` fully first. Add:
```java
    @Test
    void createConsumerIncrementsConsumersCreatedCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ConsumerController withMetrics = new ConsumerController(consumerRepository, meterRegistry);
        when(consumerRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        withMetrics.createConsumer(new CreateConsumerRequest("Test", true));

        assertThat(meterRegistry.counter("consumers_created").count()).isEqualTo(1.0);
    }
```
Adjust to the real constructor/field/request-record signatures found in `ConsumerController.java` (line 27: `consumerRepository.save(new Consumer(request.name(), request.active()))` confirms `consumerRepository` field name and `CreateConsumerRequest(name, active)` shape).

- [ ] **Step 6: Run test to verify it fails**

Run: `./gradlew :ftgo-consumer-service:test --tests ConsumerControllerTest -i`
Expected: FAIL — compile error, constructor mismatch.

- [ ] **Step 7: Add MeterRegistry to ConsumerController**

Add `MeterRegistry meterRegistry` as a new constructor param. After `Consumer consumer = consumerRepository.save(new Consumer(request.name(), request.active()));`, add:
```java
        meterRegistry.counter("consumers_created").increment();
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `./gradlew :ftgo-restaurant-service:test :ftgo-consumer-service:test -i`
Expected: PASS. Apply `@Mock`+stub pattern to remaining pre-existing tests if needed.

- [ ] **Step 9: Commit**

```bash
git add ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantController.java \
  ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/api/ConsumerController.java \
  ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/api/RestaurantControllerTest.java \
  ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/api/ConsumerControllerTest.java
git commit -m "feat: add restaurants_created/consumers_created counters"
```

---

### Task 7: order-history-service custom counter

**Files:**
- Modify: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java`

**Interfaces:**
- Consumes: `io.micrometer.core.instrument.MeterRegistry`.
- Produces: counter `order_views_updated`, incremented once per successful `OrderViewService.handleOrderEvent(...)` call that results in an `OrderView` row being created or updated.

- [ ] **Step 1: Read OrderViewService.java to find the exact upsert call site**

Before writing any code, read `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java` fully (referenced by `OrderEventListener.onMessage` → `orderViewService.handleOrderEvent(...)`, per `OrderEventListener.java` already shown above). Identify the exact repository-save call(s) inside `handleOrderEvent` — there may be more than one branch (create vs. update vs. ignore-unknown-event-type). The counter must increment on every branch that actually persists a change, and must NOT increment on a branch that no-ops (e.g. an already-processed event, or an unrecognized `eventType`).

- [ ] **Step 2: Write the failing test**

Read `OrderViewServiceTest.java` first for its exact mock/constructor conventions. Add:
```java
    @Test
    void handleOrderEventIncrementsOrderViewsUpdatedCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        OrderViewService withMetrics = new OrderViewService(/* existing mocked deps in this class's order */, meterRegistry);
        // Arrange whatever mock state makes handleOrderEvent take its "OrderCreated"
        // (or equivalent persisting) branch, matching this test class's existing
        // "successful create" test case setup exactly.

        withMetrics.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, List.of());

        assertThat(meterRegistry.counter("order_views_updated").count()).isEqualTo(1.0);
    }
```
Match the exact method signature of `handleOrderEvent` (parameter names/order/types) from the real file — `OrderEventListener.java`'s call (`orderViewService.handleOrderEvent(event.eventId(), event.eventType(), event.orderId(), event.consumerId(), event.restaurantId(), lineItems)`) gives the parameter order but you must confirm exact types by reading `OrderViewService.java` directly.

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :ftgo-order-history-service:test --tests OrderViewServiceTest -i`
Expected: FAIL — compile error, constructor mismatch.

- [ ] **Step 4: Add MeterRegistry and the increment**

Add `MeterRegistry meterRegistry` as a new constructor param (import `io.micrometer.core.instrument.MeterRegistry`), store as field. Add `meterRegistry.counter("order_views_updated").increment();` immediately after each persisting branch's save call identified in Step 1 — do not add it to any no-op/early-return branch.

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-order-history-service:test --tests OrderViewServiceTest -i`
Expected: PASS. Apply the `@Mock`+stub pattern from Task 2 Step 4 to this class's other pre-existing `@InjectMocks` tests.

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java \
  ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/domain/OrderViewServiceTest.java
git commit -m "feat: add order_views_updated counter to order-history-service"
```

---

### Task 8: Prometheus compose service, scrape config, and alert rules

**Files:**
- Modify: `compose.yml` (add `prometheus` service block)
- Create: `prometheus/prometheus.yml`
- Create: `prometheus/alert_rules.yml`

**Interfaces:**
- Consumes: `/actuator/prometheus` on all 9 services (from Task 1).
- Produces: a running `prometheus` container on host port `9090`, scraping all 9 services every 5s, with 3 loaded alert rules.

- [ ] **Step 1: Create the scrape config**

Create `prometheus/prometheus.yml`:
```yaml
global:
  scrape_interval: 5s

rule_files:
  - /etc/prometheus/alert_rules.yml

scrape_configs:
  - job_name: order-service
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['order-service:8082']
  - job_name: kitchen-service
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['kitchen-service:8083']
  - job_name: consumer-service
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['consumer-service:8081']
  - job_name: restaurant-service
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['restaurant-service:8085']
  - job_name: accounting-service
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['accounting-service:8084']
  - job_name: delivery-service
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['delivery-service:8086']
  - job_name: order-history-service
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['order-history-service:8088']
  - job_name: mobile-gateway
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['mobile-gateway:8090']
  - job_name: public-gateway
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['public-gateway:8091']
```
Targets use compose service names (`order-service:8082`, etc.), not `localhost` — Prometheus resolves these via Docker's internal DNS on the compose network, the same way every other inter-service reference in `compose.yml` already works (e.g. `SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order`).

- [ ] **Step 2: Create the alert rules file**

Create `prometheus/alert_rules.yml`:
```yaml
groups:
  - name: ftgo-alerts
    rules:
      - alert: ServiceDown
        expr: up == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.job }} instance {{ $labels.instance }} is down"
          description: "Prometheus has failed to scrape {{ $labels.job }} for at least 30 seconds."

      - alert: HighOrderRejectionRate
        expr: rate(orders_rejected_total[5m]) / rate(orders_placed_total[5m]) > 0.5
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High order rejection rate"
          description: "More than 50% of orders placed in the last 5 minutes have been rejected."

      - alert: HighAuthorizationDeclineRate
        expr: rate(authorizations_declined_total[5m]) / (rate(authorizations_approved_total[5m]) + rate(authorizations_declined_total[5m])) > 0.5
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High authorization decline rate"
          description: "More than 50% of card authorizations in the last 5 minutes have been declined."
```
`_total` suffix: Micrometer's Prometheus registry appends `_total` to `Counter` metric names in exposition format (standard Prometheus counter naming convention) — the raw Java-side names (`orders_rejected`, `orders_placed`, `authorizations_declined`, `authorizations_approved`) become `orders_rejected_total` etc. once scraped. Verify this by checking Task 1 Step 4's `curl` output for the exact `_total` suffix pattern once real counters exist (after Tasks 2-7 land) — if Micrometer's actual exposition format differs from this assumption, correct the rule expressions to match the real metric names before this task's Step 4 verification.

- [ ] **Step 3: Add the prometheus service to compose.yml**

Find the last service block in `compose.yml` (after `public-gateway`) and add, matching the file's existing 2-space indentation style:
```yaml
  prometheus:
    image: prom/prometheus:v2.55.1
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./prometheus/alert_rules.yml:/etc/prometheus/alert_rules.yml:ro
    ports:
      - "9090:9090"
```
No `depends_on` on any business service, and no business service depends on `prometheus` — it's a pure observer per the spec's architecture.

- [ ] **Step 4: Manually verify Prometheus scrapes and loads rules**

Run: `docker compose up -d prometheus order-service kitchen-service consumer-service restaurant-service accounting-service delivery-service order-history-service mobile-gateway public-gateway service-registry mysql zookeeper kafka authorization-server` (or `docker compose up -d` for the full stack), wait ~60s for services to register with Eureka and become scrapeable, then open `http://localhost:9090/targets` and confirm all 9 targets show `State: UP`, and `http://localhost:9090/alerts` and confirm all 3 rules loaded (their state will be `Inactive` under normal load — that's expected, not a failure). Run `docker compose down` afterward if this was a full-stack bring-up solely for this check.

- [ ] **Step 5: Commit**

```bash
git add compose.yml prometheus/prometheus.yml prometheus/alert_rules.yml
git commit -m "feat: add Prometheus compose service with scrape config and alert rules"
```

---

### Task 9: Grafana compose service with provisioned datasource and dashboard

**Files:**
- Modify: `compose.yml` (add `grafana` service block)
- Create: `grafana/provisioning/datasources/prometheus.yml`
- Create: `grafana/provisioning/dashboards/dashboard.yml`
- Create: `grafana/dashboards/ftgo-overview.json`

**Interfaces:**
- Consumes: the `prometheus` compose service from Task 8 (`http://prometheus:9090` on the compose network).
- Produces: a running `grafana` container on host port `3000`, with the Prometheus datasource and one dashboard auto-provisioned (no manual UI setup required).

- [ ] **Step 1: Create the datasource provisioning file**

Create `grafana/provisioning/datasources/prometheus.yml`:
```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

- [ ] **Step 2: Create the dashboard provisioning pointer**

Create `grafana/provisioning/dashboards/dashboard.yml`:
```yaml
apiVersion: 1

providers:
  - name: FTGO
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/dashboards
```

- [ ] **Step 3: Create the dashboard JSON**

Create `grafana/dashboards/ftgo-overview.json`:
```json
{
  "title": "FTGO Overview",
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "10s",
  "panels": [
    {
      "id": 1,
      "title": "Service Up/Down",
      "type": "stat",
      "gridPos": { "h": 6, "w": 24, "x": 0, "y": 0 },
      "targets": [{ "expr": "up", "legendFormat": "{{job}}" }]
    },
    {
      "id": 2,
      "title": "JVM Heap Used (bytes)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 6 },
      "targets": [{ "expr": "jvm_memory_used_bytes{area=\"heap\"}", "legendFormat": "{{job}}" }]
    },
    {
      "id": 3,
      "title": "HTTP Request Rate",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 6 },
      "targets": [{ "expr": "rate(http_server_requests_seconds_count[1m])", "legendFormat": "{{job}} {{uri}}" }]
    },
    {
      "id": 4,
      "title": "Orders (rate/min)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 14 },
      "targets": [
        { "expr": "rate(orders_placed_total[1m])", "legendFormat": "placed" },
        { "expr": "rate(orders_approved_total[1m])", "legendFormat": "approved" },
        { "expr": "rate(orders_rejected_total[1m])", "legendFormat": "rejected" },
        { "expr": "rate(orders_cancelled_total[1m])", "legendFormat": "cancelled" }
      ]
    },
    {
      "id": 5,
      "title": "Tickets (rate/min)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 14 },
      "targets": [
        { "expr": "rate(tickets_accepted_total[1m])", "legendFormat": "accepted" },
        { "expr": "rate(tickets_preparing_total[1m])", "legendFormat": "preparing" },
        { "expr": "rate(tickets_ready_for_pickup_total[1m])", "legendFormat": "ready_for_pickup" },
        { "expr": "rate(tickets_picked_up_total[1m])", "legendFormat": "picked_up" },
        { "expr": "rate(tickets_cancelled_total[1m])", "legendFormat": "cancelled" }
      ]
    },
    {
      "id": 6,
      "title": "Authorizations (rate/min)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 22 },
      "targets": [
        { "expr": "rate(authorizations_approved_total[1m])", "legendFormat": "approved" },
        { "expr": "rate(authorizations_declined_total[1m])", "legendFormat": "declined" },
        { "expr": "rate(authorizations_reversed_total[1m])", "legendFormat": "reversed" }
      ]
    },
    {
      "id": 7,
      "title": "Deliveries (rate/min)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 22 },
      "targets": [
        { "expr": "rate(deliveries_scheduled_total[1m])", "legendFormat": "scheduled" },
        { "expr": "rate(deliveries_picked_up_total[1m])", "legendFormat": "picked_up" },
        { "expr": "rate(deliveries_delivered_total[1m])", "legendFormat": "delivered" },
        { "expr": "rate(deliveries_cancelled_total[1m])", "legendFormat": "cancelled" }
      ]
    },
    {
      "id": 8,
      "title": "Restaurants / Consumers / Order Views (rate/min)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 30 },
      "targets": [
        { "expr": "rate(restaurants_created_total[1m])", "legendFormat": "restaurants_created" },
        { "expr": "rate(consumers_created_total[1m])", "legendFormat": "consumers_created" },
        { "expr": "rate(order_views_updated_total[1m])", "legendFormat": "order_views_updated" }
      ]
    }
  ]
}
```

- [ ] **Step 4: Add the grafana service to compose.yml**

Add after the `prometheus` block from Task 8:
```yaml
  grafana:
    image: grafana/grafana:11.3.1
    depends_on:
      - prometheus
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/etc/grafana/dashboards:ro
    ports:
      - "3000:3000"
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
```
`GF_AUTH_ANONYMOUS_ENABLED` avoids requiring a login for local/learning use — this is a local Docker Compose learning project with no external exposure, consistent with the rest of this repo's compose services which run without auth hardening beyond what each Spring service itself enforces.

- [ ] **Step 5: Manually verify the dashboard renders**

Run: `docker compose up -d grafana` (with `prometheus` and the business services already up from Task 8's verification, or bring up the full stack), open `http://localhost:3000`, confirm no login is required, navigate to Dashboards → FTGO Overview, confirm the "Service Up/Down" panel shows all 9 targets and the JVM/HTTP panels render data (the business-counter panels will show flat zero lines until Task 10's e2e scenario or manual traffic generates data — that's expected at this point). `docker compose down` afterward if this was a standalone check.

- [ ] **Step 6: Commit**

```bash
git add compose.yml grafana/provisioning/datasources/prometheus.yml \
  grafana/provisioning/dashboards/dashboard.yml grafana/dashboards/ftgo-overview.json
git commit -m "feat: add Grafana compose service with provisioned FTGO dashboard"
```

---

### Task 10: Cucumber e2e scenario verifying business counters

**Files:**
- Modify: `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`
- Modify: `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/PlaceReviseCancelOrderStepDefinitions.java`

**Interfaces:**
- Consumes: the existing `postWithRetry`/`pollForFinalStatus`/`OrderIdHolder`/`TokenClient` helpers already in `PlaceReviseCancelOrderStepDefinitions.java`; order-service's `GET /actuator/prometheus` on `http://localhost:8082` (from Task 1/Task 2).
- Produces: a new Cucumber scenario asserting `orders_placed_total` and `orders_approved_total` both read back ≥ 1 after an order is placed and approved.

- [ ] **Step 1: Add the new scenario to the feature file**

Append to `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`:
```gherkin
  Scenario: Placing and approving an order increments the order-service Prometheus counters
    Given a restaurant "Ajanta Metrics E2E" with a menu item "Lamb Biryani" priced at 14.00
    And an active consumer "Metrics E2E Consumer"
    When the consumer places an order for 1 of the menu item at the restaurant
    Then the order is eventually approved
    And the order-service Prometheus counters "orders_placed_total" and "orders_approved_total" both eventually read at least 1
```
The `Given`/`When`/`Then` steps up through "the order is eventually approved" reuse existing step definitions verbatim — only the final `And` step is new.

- [ ] **Step 2: Add the new step definition**

Add to `PlaceReviseCancelOrderStepDefinitions.java` (new import: `io.cucumber.java.en.And` — check if it's already imported; if `Then`-annotated steps in Cucumber-Java also match `And` steps by convention in this codebase, confirm by checking how the existing `.feature` file's `And`-prefixed Given/steps map to `@Given`/`@When`/`@Then` annotations — Cucumber-Java's `And`/`But` keywords match whichever step-definition annotation was used for the preceding Given/When/Then in the same block, so this new `And` step needs `@Then`, not a distinct `@And` annotation):
```java
    @Then("the order-service Prometheus counters {string} and {string} both eventually read at least 1")
    public void theOrderServicePrometheusCountersBothEventuallyReadAtLeastOne(String counterA, String counterB) throws Exception {
        assertTrue(counterEventuallyAtLeastOne(counterA), "Counter " + counterA + " did not reach >= 1 within 30s");
        assertTrue(counterEventuallyAtLeastOne(counterB), "Counter " + counterB + " did not reach >= 1 within 30s");
    }

    private boolean counterEventuallyAtLeastOne(String counterName) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (Instant.now().isBefore(deadline)) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8082/actuator/prometheus"))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            for (String line : response.body().split("\n")) {
                if (line.startsWith(counterName + " ")) {
                    double value = Double.parseDouble(line.substring(counterName.length()).trim());
                    if (value >= 1.0) {
                        return true;
                    }
                }
            }
            Thread.sleep(500);
        }
        return false;
    }
```
Add `import static org.junit.jupiter.api.Assertions.assertTrue;` to the existing static-import block if not already present (the class currently imports `assertEquals` and `fail`).

- [ ] **Step 3: Run the e2e test**

Run: `./gradlew :ftgo-end-to-end-test:e2eTest --tests "*PlaceReviseCancelOrder*"`
Expected: PASS. This brings the full Docker Compose stack up via the `docker-compose` Gradle plugin (`ftgo-end-to-end-test/build.gradle`'s `dockerCompose { isRequiredBy(tasks.named('e2eTest')) }`), runs both scenarios in `PlaceReviseCancelOrder.feature` (the original 3-step scenario plus the new one), and tears the stack down.

- [ ] **Step 4: Commit**

```bash
git add ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature \
  ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/PlaceReviseCancelOrderStepDefinitions.java
git commit -m "test: add e2e scenario verifying order-service Prometheus counters"
```

---

### Task 11: Documentation sweep (per-change, not full chapter sweep)

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Modify: `ftgo-order-service/README.md`
- Modify: `ftgo-kitchen-service/README.md`
- Modify: `ftgo-accounting-service/README.md`
- Modify: `ftgo-delivery-service/README.md`
- Modify: `ftgo-restaurant-service/README.md`
- Modify: `ftgo-consumer-service/README.md`
- Modify: `ftgo-order-history-service/README.md`
- Modify: `README.md`
- Modify: `CONTEXT.md`

**Interfaces:**
- Consumes: nothing (documentation only).
- Produces: nothing consumed by later tasks — this is the terminal task.

- [ ] **Step 1: Add an Application metrics section to docs/ARCHITECTURE.md**

Read `docs/ARCHITECTURE.md` first to find where the health-check-API section (§11.3.1) lives, and add a new "Application metrics (Ch.11, §11.3.4)" section immediately after it, matching that section's existing depth/format. Cover: the Micrometer + Prometheus + Grafana architecture, the full counter table from the spec (reproduced with the corrected class-name attributions from this plan's Global Constraints — e.g. "`orders_placed`/`orders_approved`/`orders_rejected`/`orders_cancelled` — `JpaOrderTransitions`" instead of the spec's looser "OrderService"), the 3 alert rules, and a note that Alertmanager/real notification delivery is out of scope.

- [ ] **Step 2: Add a Metrics subsection to each of the 7 business-service READMEs**

For each of `ftgo-order-service/README.md`, `ftgo-kitchen-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-delivery-service/README.md`, `ftgo-restaurant-service/README.md`, `ftgo-consumer-service/README.md`, `ftgo-order-history-service/README.md`: read the file first to find its existing section structure (likely an "API" or "Endpoints" section to sit alongside), then add a "## Metrics" section listing that service's `GET /actuator/prometheus` endpoint and its specific custom counters (from the table in this plan's tasks, not the spec's uncorrected table) with a one-line description of when each increments.

- [ ] **Step 3: Update root README.md**

Read `README.md`'s tech stack list and "Book progress" table. Add Micrometer/Prometheus/Grafana to the tech stack list (following its existing format, e.g. alongside Spring Boot Actuator's existing entry from the health-check sub-project). In the Book progress table, update the Ch.11 row's status/notes to mention application metrics is done (do not mark the whole §11.3 or Ch.11 row "Done" — other §11.3 patterns remain unstarted, per this plan's Global Constraints).

- [ ] **Step 4: Update CONTEXT.md**

Read `CONTEXT.md`'s "Current position" and "Patterns reference" sections (both already read and summarized in this project's prior session — "Current position" around line 47, "Patterns reference" checklist around lines 190-198). Update "Current position" to describe the application-metrics sub-project as done and name whichever §11.3 sub-project is logically next (log aggregation, distributed tracing, exception tracking, or audit logging — do not pick one for the user, just list them as remaining, matching how "Remaining §11.1 topics... not yet covered" was phrased for the security sub-project). Add an "Application metrics (§11.3.4)" checked-off line to the "Patterns reference" checklist, next to "Health check API". Add a session-log entry for this sub-project (following the existing session-log entry format used for the security sub-project) summarizing the 11 tasks completed.

- [ ] **Step 5: Commit**

```bash
git add docs/ARCHITECTURE.md ftgo-order-service/README.md ftgo-kitchen-service/README.md \
  ftgo-accounting-service/README.md ftgo-delivery-service/README.md ftgo-restaurant-service/README.md \
  ftgo-consumer-service/README.md ftgo-order-history-service/README.md README.md CONTEXT.md
git commit -m "docs: document Ch.11 §11.3.4 application metrics across ARCHITECTURE/READMEs/CONTEXT"
```

---

## Self-Review

**Spec coverage:**
- Micrometer + Prometheus on all 9 services → Task 1. ✅
- 7 services' custom counters per the spec's table (with documented corrections) → Tasks 2-7. ✅
- `prometheus`/`grafana` compose services, scrape config, alert rules → Tasks 8-9. ✅
- Grafana dashboard → Task 9. ✅
- Cucumber e2e scenario → Task 10. ✅
- Manual Docker verification (targets up, alerts load, dashboard renders) → Task 8 Step 4, Task 9 Step 5. ✅
- Per-change documentation (not full chapter sweep) → Task 11. ✅
- Out-of-scope items (Alertmanager, per-endpoint custom timers, tagged counters, other §11.3 patterns) → none of the 11 tasks touch these. ✅

**Placeholder scan:** No "TBD"/"TODO"/"similar to Task N" found. Several steps explicitly instruct the implementer to *read the real file first* before finalizing an exact signature (e.g. Task 3 Step 5's `AcceptTicketRequest` shape, Task 6's `CreateRestaurantRequest`/`CreateConsumerRequest` constructors, Task 7's `handleOrderEvent` signature) rather than guessing — this is a deliberate research instruction with a concrete fallback shape provided, not a content gap; each such step also states what must not vary (the counter name, the increment placement) so review can hold the implementer to those invariants even if the surrounding scaffolding needed adjusting.

**Type consistency:** `MeterRegistry` (constructor-injected, `io.micrometer.core.instrument.MeterRegistry`) is the consistent injection type across all 7 counter tasks — no task introduces a different metrics API. Counter names are used identically between each task's implementation code and Task 8's alert rules / Task 9's dashboard queries (`orders_placed`/`orders_approved`/`orders_rejected`/`orders_cancelled`, `tickets_accepted`/`tickets_preparing`/`tickets_ready_for_pickup`/`tickets_picked_up`/`tickets_cancelled`, `authorizations_approved`/`authorizations_declined`/`authorizations_reversed`, `deliveries_scheduled`/`deliveries_picked_up`/`deliveries_delivered`/`deliveries_cancelled`, `restaurants_created`, `consumers_created`, `order_views_updated`) — cross-checked against the dashboard JSON's panel targets and the alert rules' `expr` fields in Tasks 8-9, all consistent with the `_total` Prometheus-exposition suffix convention noted in Task 8 Step 2.
