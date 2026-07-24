# API Composition Order View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `GET /orders/{id}/view` on `order-service` — a composite query (Ch.7's API composition pattern) that assembles `Order` (local) plus `Restaurant`/`Ticket`/`Authorization`/`Delivery` (fetched in parallel from their owning services), with each remote section independently degrading to "not found" or "unavailable" rather than failing the whole request.

**Architecture:** `kitchen-service`, `accounting-service`, and `delivery-service` each gain Eureka registration (mirroring `restaurant-service`'s existing setup) plus a small new `GET .../order/{orderId}` read endpoint. `order-service` gains 3 new circuit-breaker-wrapped proxies (mirroring the existing `RestaurantServiceProxy` exactly) plus a `findRestaurantForView` addition to the existing restaurant proxy, all called in parallel via `CompletableFuture` on a virtual-thread executor from a new `OrderViewController`.

**Tech Stack:** Java 21, Spring Boot 3.5.16, Spring Cloud (Eureka client, LoadBalancer), Resilience4j circuit breakers, `RestClient`, WireMock (existing test dependency in order-service), JUnit 5 + AssertJ + Mockito.

## Global Constraints

- No schema changes anywhere — every new endpoint reads via an already-existing `findByOrderId` repository method.
- A 404 from any of the 3 new read endpoints is NOT an error from the calling proxy's perspective — it returns `SectionResult.NotFound<>()` directly, without throwing, so it is never recorded as a circuit-breaker failure.
- Only genuine downstream failures (5xx, timeout, connection refused) reach each proxy's `@CircuitBreaker` fallback, which always returns `SectionResult.Unavailable<>(reason)`.
- `GET /orders/{id}/view` itself 404s only if the order doesn't exist; every other section degrades independently inside a 200 response.
- Every new circuit breaker instance mirrors `restaurantService`'s existing Resilience4j settings exactly (sliding-window-size 5, failure-rate-threshold 50, wait-duration-in-open-state 5s, permitted-number-of-calls-in-half-open-state 3, automatic-transition-from-open-to-half-open-enabled true) — no `ignore-exceptions` needed on the new instances, since `NotFound` is handled before any exception is thrown.
- `RestaurantServiceProxy.findRestaurant` (existing, throw-on-404, used by order creation) is left completely unchanged — the new `findRestaurantForView` is a separate, additive method reusing the same `RestClient` bean and the same `restaurantService` circuit breaker instance.
- Code comments explain *why*, not *what*.
- TDD throughout: write the failing test before the implementation, for every task with a test step.
- Frequent commits: one commit per task, using this project's existing commit-message conventions (`feat:`, `fix:`, `docs:`, `refactor:`).

---

## Codebase reference (read once, applies to every task below)

Only `restaurant-service` and `order-service` currently register with Eureka. `kitchen-service`'s `TicketController`, `delivery-service`'s `DeliveryController` have zero `GET` endpoints today. `accounting-service`'s `api` package is completely empty (no controller at all). `spring-boot-starter-web` is already on every service's classpath (declared once in the root `build.gradle`'s blanket `subprojects` block), so no build.gradle change is needed anywhere just to add a controller — only Eureka-client wiring needs adding.

`Ticket`/`Authorization`/`Delivery` already have `findByOrderId(Long): Optional<X>` on their repositories (`TicketRepository`, `AuthorizationRepository`, `DeliveryRepository`) — every new read endpoint is a thin wrapper around that existing method.

---

### Task 1: `kitchen-service` — Eureka registration + `GET /tickets/order/{orderId}`

**Files:**
- Modify: `ftgo-kitchen-service/build.gradle`
- Modify: `ftgo-kitchen-service/src/main/resources/application.yml`
- Modify: `compose.yml`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketInfo.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerTest.java`

**Interfaces:**
- Produces: `Ticket.getReadyBy(): ZonedDateTime` (new getter). `TicketInfo(Long id, Long orderId, String status, ZonedDateTime readyBy)` — the wire response shape `order-service`'s `KitchenServiceProxy` (Task 6) will deserialize.

- [ ] **Step 1: Add the Eureka client dependency** (mirrors `ftgo-restaurant-service/build.gradle` exactly)

```groovy
// ftgo-kitchen-service/build.gradle
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

dependencies {
    // spring-kafka comes transitively via ftgo-common's `api` dependency
    implementation project(':ftgo-common')
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
}
```

- [ ] **Step 2: Add Eureka config to `application.yml`** (mirrors `ftgo-restaurant-service`'s exactly)

```yaml
# Add to ftgo-kitchen-service/src/main/resources/application.yml, after the existing `saga:` block:

eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true
```

- [ ] **Step 3: Wire into `compose.yml`** — add `service-registry` to `kitchen-service`'s `depends_on` and the Eureka env var, matching `restaurant-service`'s block:

```yaml
  kitchen-service:
    build:
      context: .
      dockerfile: ftgo-kitchen-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      service-registry:
        condition: service_started
    ports:
      - "8083:8083"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_kitchen
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SAGA_MODE: ${SAGA_MODE:-choreography}
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/
```

- [ ] **Step 4: Verify the module still builds with the new dependency**

Run: `./gradlew :ftgo-kitchen-service:build`
Expected: BUILD SUCCESSFUL (existing tests still pass — this step adds no new behavior yet)

- [ ] **Step 5: Commit the Eureka registration**

```bash
git add ftgo-kitchen-service/build.gradle ftgo-kitchen-service/src/main/resources/application.yml compose.yml
git commit -m "feat: register kitchen-service with Eureka"
```

- [ ] **Step 6: Write the failing test for the new endpoint** (mirrors `TicketControllerTest`'s existing style — read that file first to match its exact fixture/assertion conventions before adding these)

```java
// Add to ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerTest.java

@Test
void viewByOrderIdReturnsTicketInfo() throws Exception {
    Ticket ticket = Ticket.createTicket(42L, 3).ticket();
    ticket.confirm();
    ZonedDateTime readyBy = ZonedDateTime.parse("2026-07-20T18:00:00Z");
    ticket.accept(readyBy);
    when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

    mockMvc.perform(get("/tickets/order/42"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.orderId").value(42))
            .andExpect(jsonPath("$.status").value("ACCEPTED"))
            .andExpect(jsonPath("$.readyBy").value("2026-07-20T18:00:00Z"));
}

@Test
void viewByOrderIdReturns404WhenNoTicketForOrder() throws Exception {
    when(ticketRepository.findByOrderId(99L)).thenReturn(Optional.empty());

    mockMvc.perform(get("/tickets/order/99"))
            .andExpect(status().isNotFound());
}
```

Add the necessary imports if not already present: `java.time.ZonedDateTime`, `java.util.Optional`, `static org.mockito.Mockito.when`, `static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get`, `static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath`.

- [ ] **Step 7: Run test to verify it fails**

Run: `./gradlew :ftgo-kitchen-service:test --tests TicketControllerTest`
Expected: FAIL (compile error — `Ticket.getReadyBy()` and the `/tickets/order/{orderId}` route don't exist yet)

- [ ] **Step 8: Add `Ticket.getReadyBy()`**

```java
// Add to ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java, alongside the existing getters:

public ZonedDateTime getReadyBy() {
    return readyBy;
}
```

- [ ] **Step 9: Write `TicketInfo`**

```java
// ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketInfo.java
package com.sanjay.ftgo.kitchen.api;

import java.time.ZonedDateTime;

public record TicketInfo(Long id, Long orderId, String status, ZonedDateTime readyBy) {
}
```

- [ ] **Step 10: Add the endpoint to `TicketController`**

```java
// Add to ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java

@GetMapping("/order/{orderId}")
public ResponseEntity<TicketInfo> viewByOrderId(@PathVariable Long orderId) {
    return ticketRepository.findByOrderId(orderId)
            .map(ticket -> new TicketInfo(ticket.getId(), ticket.getOrderId(), ticket.getState().name(), ticket.getReadyBy()))
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
}
```

Add the `@GetMapping` import (`org.springframework.web.bind.annotation.GetMapping`) alongside the existing `@PostMapping` import.

- [ ] **Step 11: Run test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests TicketControllerTest`
Expected: PASS

- [ ] **Step 12: Run the full kitchen-service test suite**

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: PASS

- [ ] **Step 13: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/Ticket.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketInfo.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/api/TicketController.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/api/TicketControllerTest.java
git commit -m "feat: add GET /tickets/order/{orderId} read endpoint"
```

---

### Task 2: `accounting-service` — Eureka registration + first-ever `AuthorizationController`

**Files:**
- Modify: `ftgo-accounting-service/build.gradle`
- Modify: `ftgo-accounting-service/src/main/resources/application.yml`
- Modify: `compose.yml`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/api/AuthorizationInfo.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/api/AuthorizationController.java`
- Create: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/api/AuthorizationControllerTest.java`

**Interfaces:**
- Consumes: `AuthorizationRepository.findByOrderId(Long): Optional<Authorization>` (existing), `Authorization.getId()/getOrderId()/getStatus()` (existing).
- Produces: `AuthorizationInfo(Long id, Long orderId, String status)` — the wire response shape `order-service`'s `AccountingServiceProxy` (Task 7) will deserialize.

- [ ] **Step 1: Add the Eureka client dependency**

```groovy
// ftgo-accounting-service/build.gradle
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

dependencies {
    // spring-kafka comes transitively via ftgo-common's `api` dependency
    implementation project(':ftgo-common')
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
}
```

- [ ] **Step 2: Add Eureka config to `application.yml`**

```yaml
# Add to ftgo-accounting-service/src/main/resources/application.yml, after the existing `saga:` block:

eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true
```

- [ ] **Step 3: Wire into `compose.yml`**

```yaml
  accounting-service:
    build:
      context: .
      dockerfile: ftgo-accounting-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      service-registry:
        condition: service_started
    ports:
      - "8084:8084"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_accounting
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SAGA_MODE: ${SAGA_MODE:-choreography}
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/
```

- [ ] **Step 4: Verify the module still builds**

Run: `./gradlew :ftgo-accounting-service:build`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Commit the Eureka registration**

```bash
git add ftgo-accounting-service/build.gradle ftgo-accounting-service/src/main/resources/application.yml compose.yml
git commit -m "feat: register accounting-service with Eureka"
```

- [ ] **Step 6: Write the failing test** (this is accounting-service's first-ever controller test — mirror `TicketControllerTest`'s `@WebMvcTest` shape)

```java
// ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/api/AuthorizationControllerTest.java
package com.sanjay.ftgo.accounting.api;

import com.sanjay.ftgo.accounting.domain.Authorization;
import com.sanjay.ftgo.accounting.domain.AuthorizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AuthorizationController.class)
class AuthorizationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AuthorizationRepository authorizationRepository;

    @Test
    void viewByOrderIdReturnsAuthorizationInfo() throws Exception {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        mockMvc.perform(get("/authorizations/order/42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(42))
                .andExpect(jsonPath("$.status").value("AUTHORIZED"));
    }

    @Test
    void viewByOrderIdReturns404WhenNoAuthorizationForOrder() throws Exception {
        when(authorizationRepository.findByOrderId(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/authorizations/order/99")).andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `./gradlew :ftgo-accounting-service:test --tests AuthorizationControllerTest`
Expected: FAIL (compile error — `AuthorizationController`/`AuthorizationInfo` don't exist yet)

- [ ] **Step 8: Write `AuthorizationInfo`**

```java
// ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/api/AuthorizationInfo.java
package com.sanjay.ftgo.accounting.api;

public record AuthorizationInfo(Long id, Long orderId, String status) {
}
```

- [ ] **Step 9: Write `AuthorizationController`** (mirrors `TicketController`'s read-only structure — no `@Transactional` needed since this is a single read with no state change)

```java
// ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/api/AuthorizationController.java
package com.sanjay.ftgo.accounting.api;

import com.sanjay.ftgo.accounting.domain.AuthorizationRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/authorizations")
public class AuthorizationController {

    private final AuthorizationRepository authorizationRepository;

    public AuthorizationController(AuthorizationRepository authorizationRepository) {
        this.authorizationRepository = authorizationRepository;
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<AuthorizationInfo> viewByOrderId(@PathVariable Long orderId) {
        return authorizationRepository.findByOrderId(orderId)
                .map(authorization -> new AuthorizationInfo(
                        authorization.getId(), authorization.getOrderId(), authorization.getStatus().name()))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
```

- [ ] **Step 10: Run test to verify it passes**

Run: `./gradlew :ftgo-accounting-service:test --tests AuthorizationControllerTest`
Expected: PASS

- [ ] **Step 11: Run the full accounting-service test suite**

Run: `./gradlew :ftgo-accounting-service:test`
Expected: PASS

- [ ] **Step 12: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/api/AuthorizationInfo.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/api/AuthorizationController.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/api/AuthorizationControllerTest.java
git commit -m "feat: add GET /authorizations/order/{orderId} read endpoint (accounting-service's first controller)"
```

---

### Task 3: `delivery-service` — Eureka registration + `GET /deliveries/order/{orderId}`

**Files:**
- Modify: `ftgo-delivery-service/build.gradle`
- Modify: `ftgo-delivery-service/src/main/resources/application.yml`
- Modify: `compose.yml`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryInfo.java`
- Modify: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java`
- Modify: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java`

**Interfaces:**
- Produces: `DeliveryInfo(Long id, Long orderId, String status, Long courierId)` — the wire response shape `order-service`'s `DeliveryServiceProxy` (Task 8) will deserialize.

- [ ] **Step 1: Add the Eureka client dependency**

```groovy
// ftgo-delivery-service/build.gradle
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

dependencies {
    // spring-kafka comes transitively via ftgo-common's `api` dependency
    implementation project(':ftgo-common')
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
}
```

- [ ] **Step 2: Add Eureka config to `application.yml`**

```yaml
# Add to ftgo-delivery-service/src/main/resources/application.yml, after the existing `saga:` block:

eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true
```

- [ ] **Step 3: Wire into `compose.yml`**

```yaml
  delivery-service:
    build:
      context: .
      dockerfile: ftgo-delivery-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      service-registry:
        condition: service_started
    ports:
      - "8086:8086"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_delivery
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SAGA_MODE: ${SAGA_MODE:-choreography}
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/
```

- [ ] **Step 4: Verify the module still builds**

Run: `./gradlew :ftgo-delivery-service:build`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Commit the Eureka registration**

```bash
git add ftgo-delivery-service/build.gradle ftgo-delivery-service/src/main/resources/application.yml compose.yml
git commit -m "feat: register delivery-service with Eureka"
```

- [ ] **Step 6: Write the failing test** (read `DeliveryControllerTest.java` first to match its exact existing structure before adding these)

```java
// Add to ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java

@Test
void viewByOrderIdReturnsDeliveryInfo() throws Exception {
    Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
    when(deliveryRepository.findByOrderId(42L)).thenReturn(Optional.of(delivery));

    mockMvc.perform(get("/deliveries/order/42"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.orderId").value(42))
            .andExpect(jsonPath("$.status").value("SCHEDULED"))
            .andExpect(jsonPath("$.courierId").value(3));
}

@Test
void viewByOrderIdReturns404WhenNoDeliveryForOrder() throws Exception {
    when(deliveryRepository.findByOrderId(99L)).thenReturn(Optional.empty());

    mockMvc.perform(get("/deliveries/order/99")).andExpect(status().isNotFound());
}
```

Add the necessary imports if not already present: `static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get`, `static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath`.

- [ ] **Step 7: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryControllerTest`
Expected: FAIL (compile error — the `/deliveries/order/{orderId}` route and `DeliveryInfo` don't exist yet)

- [ ] **Step 8: Write `DeliveryInfo`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryInfo.java
package com.sanjay.ftgo.delivery.api;

public record DeliveryInfo(Long id, Long orderId, String status, Long courierId) {
}
```

- [ ] **Step 9: Add the endpoint to `DeliveryController`**

```java
// Add to ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java

@GetMapping("/order/{orderId}")
public ResponseEntity<DeliveryInfo> viewByOrderId(@PathVariable Long orderId) {
    return deliveryRepository.findByOrderId(orderId)
            .map(delivery -> new DeliveryInfo(
                    delivery.getId(), delivery.getOrderId(), delivery.getStatus().name(), delivery.getCourierId()))
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
}
```

Add the `@GetMapping` import alongside the existing `@PostMapping` import.

- [ ] **Step 10: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryControllerTest`
Expected: PASS

- [ ] **Step 11: Run the full delivery-service test suite**

Run: `./gradlew :ftgo-delivery-service:test`
Expected: PASS

- [ ] **Step 12: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryInfo.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java
git commit -m "feat: add GET /deliveries/order/{orderId} read endpoint"
```

**All 3 downstream services are now query-ready.** The remaining tasks touch only `order-service`.

---

### Task 4: `order-service` — `SectionResult` + composite response DTOs

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SectionResult.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Found.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/NotFound.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Unavailable.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/TicketInfo.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AuthorizationInfo.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryInfo.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderSummary.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderViewResponse.java`

**Interfaces:**
- Produces: the full `SectionResult<T>`/`Found`/`NotFound`/`Unavailable` sealed hierarchy and the 3 client-side info records — consumed by Tasks 5–9 (proxies and the controller). No test file for this task — these are pure data carriers with no behavior, consistent with how this codebase's other wire-format records (`KitchenCommand`, `TicketCreatedEvent`, etc.) are never independently unit-tested.

- [ ] **Step 1: Write the sealed result hierarchy** (domain package, since these represent domain-level outcomes of a downstream lookup, not wire DTOs themselves)

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SectionResult.java
package com.sanjay.ftgo.order.domain;

public sealed interface SectionResult<T> permits Found, NotFound, Unavailable {
}
```

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Found.java
package com.sanjay.ftgo.order.domain;

public record Found<T>(T data) implements SectionResult<T> {
}
```

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/NotFound.java
package com.sanjay.ftgo.order.domain;

public record NotFound<T>() implements SectionResult<T> {
}
```

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Unavailable.java
package com.sanjay.ftgo.order.domain;

public record Unavailable<T>(String reason) implements SectionResult<T> {
}
```

- [ ] **Step 2: Write the 3 client-side info records** (each is `order-service`'s own copy of the deserialization target, matching Tasks 1–3's producer shapes field-for-field — per this codebase's established per-service wire-record convention, e.g. `KitchenCommand` having a separate copy in every consuming service)

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/TicketInfo.java
package com.sanjay.ftgo.order.domain;

import java.time.ZonedDateTime;

public record TicketInfo(Long id, Long orderId, String status, ZonedDateTime readyBy) {
}
```

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AuthorizationInfo.java
package com.sanjay.ftgo.order.domain;

public record AuthorizationInfo(Long id, Long orderId, String status) {
}
```

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryInfo.java
package com.sanjay.ftgo.order.domain;

public record DeliveryInfo(Long id, Long orderId, String status, Long courierId) {
}
```

- [ ] **Step 3: Write the composite response records** (api package, since these are the outward-facing response shape of the new endpoint)

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderSummary.java
package com.sanjay.ftgo.order.api;

import java.util.List;

public record OrderSummary(Long id, String status, Long consumerId, Long restaurantId, List<LineItemView> lineItems) {

    public record LineItemView(Long menuItemId, int quantity) {
    }
}
```

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderViewResponse.java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;

public record OrderViewResponse(
        OrderSummary order,
        SectionResult<RestaurantInfo> restaurant,
        SectionResult<TicketInfo> ticket,
        SectionResult<AuthorizationInfo> authorization,
        SectionResult<DeliveryInfo> delivery) {
}
```

- [ ] **Step 4: Verify the module compiles**

Run: `./gradlew :ftgo-order-service:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SectionResult.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Found.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/NotFound.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Unavailable.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/TicketInfo.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AuthorizationInfo.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryInfo.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderSummary.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderViewResponse.java
git commit -m "feat: add SectionResult and composite order-view response DTOs"
```

---

### Task 5: `order-service` — `RestaurantServiceProxy.findRestaurantForView`

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantServicePort.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxy.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxyTest.java`

**Interfaces:**
- Consumes: `SectionResult`/`Found`/`NotFound`/`Unavailable`, `RestaurantInfo` (existing) (Task 4).
- Produces: `RestaurantServicePort.findRestaurantForView(Long restaurantId): SectionResult<RestaurantInfo>` — consumed by Task 9's `OrderViewController`.

- [ ] **Step 1: Read the existing test file first** to confirm its WireMock port (8089) and fixture conventions before adding new tests.

- [ ] **Step 2: Write the failing tests** — add these to the existing `RestaurantServiceProxyTest` class:

```java
// New test methods added to RestaurantServiceProxyTest.java

@Test
void findRestaurantForViewReturnsFoundOnSuccess() {
    wireMockServer.stubFor(get(urlEqualTo("/restaurants/1"))
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("""
                            {"id":1,"name":"Ajanta Indian Cuisine","menuItems":[]}
                            """)));

    SectionResult<RestaurantInfo> result = restaurantServiceProxy.findRestaurantForView(1L);

    assertThat(result).isInstanceOf(Found.class);
    assertThat(((Found<RestaurantInfo>) result).data().name()).isEqualTo("Ajanta Indian Cuisine");
}

@Test
void findRestaurantForViewReturnsNotFoundOn404() {
    wireMockServer.stubFor(get(urlEqualTo("/restaurants/99"))
            .willReturn(aResponse().withStatus(404)));

    SectionResult<RestaurantInfo> result = restaurantServiceProxy.findRestaurantForView(99L);

    assertThat(result).isInstanceOf(NotFound.class);
}

@Test
void findRestaurantForViewReturnsUnavailableWhenCircuitOpen() {
    wireMockServer.stop();

    SectionResult<RestaurantInfo> result = null;
    for (int i = 0; i < 4; i++) {
        result = restaurantServiceProxy.findRestaurantForView(1L);
    }

    assertThat(result).isInstanceOf(Unavailable.class);
}
```

Add imports: `com.sanjay.ftgo.order.domain.Found`, `com.sanjay.ftgo.order.domain.NotFound`, `com.sanjay.ftgo.order.domain.SectionResult`, `com.sanjay.ftgo.order.domain.Unavailable`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-service:test --tests RestaurantServiceProxyTest`
Expected: FAIL (compile error — `findRestaurantForView` doesn't exist yet)

- [ ] **Step 4: Add `findRestaurantForView` to `RestaurantServicePort`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantServicePort.java
package com.sanjay.ftgo.order.domain;

public interface RestaurantServicePort {

    RestaurantInfo findRestaurant(Long restaurantId);

    SectionResult<RestaurantInfo> findRestaurantForView(Long restaurantId);
}
```

- [ ] **Step 5: Implement it in `RestaurantServiceProxy`**

```java
// Add to ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxy.java

@Override
@CircuitBreaker(name = "restaurantService", fallbackMethod = "findRestaurantForViewFallback")
public SectionResult<RestaurantInfo> findRestaurantForView(Long restaurantId) {
    try {
        RestaurantInfo info = restClient.get()
                .uri("/restaurants/{id}", restaurantId)
                .retrieve()
                .body(RestaurantInfo.class);
        return new Found<>(info);
    } catch (HttpClientErrorException.NotFound e) {
        return new NotFound<>();
    }
}

@SuppressWarnings("unused")
private SectionResult<RestaurantInfo> findRestaurantForViewFallback(Long restaurantId, Throwable throwable) {
    return new Unavailable<>(throwable.getMessage());
}
```

Add imports: `com.sanjay.ftgo.order.domain.Found`, `com.sanjay.ftgo.order.domain.NotFound`, `com.sanjay.ftgo.order.domain.SectionResult`, `com.sanjay.ftgo.order.domain.Unavailable`.

Note: `findRestaurantForView` deliberately reuses the same `restaurantService` circuit breaker instance name as the existing `findRestaurant` — both methods target the same downstream service via the same `RestClient`, so their breaker state should trip together, not be tracked independently.

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests RestaurantServiceProxyTest`
Expected: PASS (all existing + 3 new tests). Note: the circuit-breaker test needs a moment for the breaker to reset between test runs if run repeatedly — this mirrors the existing `tripsCircuitBreakerAfterRepeatedFailures` test's behavior, which the shared `restaurantService` instance also exercises.

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantServicePort.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxy.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxyTest.java
git commit -m "feat: add non-throwing findRestaurantForView to RestaurantServiceProxy"
```

---

### Task 6: `order-service` — `KitchenServicePort`/`KitchenServiceProxy`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenServicePort.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenServiceProxy.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/KitchenServiceProxyTest.java`

**Interfaces:**
- Consumes: `SectionResult`/`Found`/`NotFound`/`Unavailable`, `TicketInfo` (Task 4).
- Produces: `KitchenServicePort.findTicket(Long orderId): SectionResult<TicketInfo>` — consumed by Task 9's `OrderViewController`.

- [ ] **Step 1: Write the failing test** (mirrors `RestaurantServiceProxyTest`'s WireMock pattern, on a distinct port — 8090 — to avoid colliding with the restaurant proxy's test if both run in the same JVM)

```java
// ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/KitchenServiceProxyTest.java
package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;
import com.sanjay.ftgo.order.domain.Unavailable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KitchenServiceProxyTest {

    private WireMockServer wireMockServer;

    @Autowired
    private KitchenServiceProxy kitchenServiceProxy;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(8090);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    void returnsFoundOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/tickets/order/42"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"orderId":42,"status":"ACCEPTED","readyBy":"2026-07-20T18:00:00Z"}
                                """)));

        SectionResult<TicketInfo> result = kitchenServiceProxy.findTicket(42L);

        assertThat(result).isInstanceOf(Found.class);
        assertThat(((Found<TicketInfo>) result).data().status()).isEqualTo("ACCEPTED");
    }

    @Test
    void returnsNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/tickets/order/99"))
                .willReturn(aResponse().withStatus(404)));

        SectionResult<TicketInfo> result = kitchenServiceProxy.findTicket(99L);

        assertThat(result).isInstanceOf(NotFound.class);
    }

    @Test
    void returnsUnavailableWhenCircuitOpen() {
        wireMockServer.stop();

        SectionResult<TicketInfo> result = null;
        for (int i = 0; i < 4; i++) {
            result = kitchenServiceProxy.findTicket(42L);
        }

        assertThat(result).isInstanceOf(Unavailable.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests KitchenServiceProxyTest`
Expected: FAIL (compile error — `KitchenServiceProxy` doesn't exist yet)

- [ ] **Step 3: Write `KitchenServicePort`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenServicePort.java
package com.sanjay.ftgo.order.domain;

public interface KitchenServicePort {

    SectionResult<TicketInfo> findTicket(Long orderId);
}
```

- [ ] **Step 4: Add the `kitchenServiceRestClient` bean** (mirrors `restaurantServiceRestClient` exactly, base URL matches kitchen-service's `spring.application.name`)

```java
// Add to ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java

@Bean
public RestClient kitchenServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
    ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
            .withConnectTimeout(Duration.ofSeconds(2))
            .withReadTimeout(Duration.ofSeconds(2));
    ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

    return loadBalancedRestClientBuilder
            .baseUrl("http://ftgo-kitchen-service")
            .requestFactory(requestFactory)
            .build();
}
```

- [ ] **Step 5: Add the `kitchenService` circuit breaker instance to `application.yml`**

```yaml
# Add under the existing resilience4j.circuitbreaker.instances key, alongside restaurantService:

      kitchenService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
```

- [ ] **Step 6: Write `KitchenServiceProxy`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenServiceProxy.java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.KitchenServicePort;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;
import com.sanjay.ftgo.order.domain.Unavailable;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class KitchenServiceProxy implements KitchenServicePort {

    private final RestClient restClient;

    public KitchenServiceProxy(RestClient kitchenServiceRestClient) {
        this.restClient = kitchenServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "kitchenService", fallbackMethod = "findTicketFallback")
    public SectionResult<TicketInfo> findTicket(Long orderId) {
        try {
            TicketInfo info = restClient.get()
                    .uri("/tickets/order/{orderId}", orderId)
                    .retrieve()
                    .body(TicketInfo.class);
            return new Found<>(info);
        } catch (HttpClientErrorException.NotFound e) {
            return new NotFound<>();
        }
    }

    @SuppressWarnings("unused")
    private SectionResult<TicketInfo> findTicketFallback(Long orderId, Throwable throwable) {
        return new Unavailable<>(throwable.getMessage());
    }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests KitchenServiceProxyTest`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/KitchenServicePort.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KitchenServiceProxy.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/KitchenServiceProxyTest.java
git commit -m "feat: add KitchenServiceProxy for the composite order view"
```

---

### Task 7: `order-service` — `AccountingServicePort`/`AccountingServiceProxy`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingServicePort.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingServiceProxy.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/AccountingServiceProxyTest.java`

**Interfaces:**
- Consumes: `SectionResult`/`Found`/`NotFound`/`Unavailable`, `AuthorizationInfo` (Task 4).
- Produces: `AccountingServicePort.findAuthorization(Long orderId): SectionResult<AuthorizationInfo>` — consumed by Task 9's `OrderViewController`.

**Note:** `order-service`'s own domain already has a class named `AccountingCommand`/`AccountingEvent` in the saga-participation code — this new `AccountingServicePort`/`AccountingServiceProxy` is unrelated (a query-side proxy, not a saga command publisher) but shares the "Accounting" prefix; this is intentional and consistent with the project's existing naming (e.g. `KitchenCommand` vs. the new `KitchenServiceProxy` in Task 6 — same prefix, different concern, no collision since the class names themselves are fully distinct).

- [ ] **Step 1: Write the failing test** (mirrors `KitchenServiceProxyTest`, port 8091)

```java
// ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/AccountingServiceProxyTest.java
package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class AccountingServiceProxyTest {

    private WireMockServer wireMockServer;

    @Autowired
    private AccountingServiceProxy accountingServiceProxy;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(8091);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    void returnsFoundOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/authorizations/order/42"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"orderId":42,"status":"AUTHORIZED"}
                                """)));

        SectionResult<AuthorizationInfo> result = accountingServiceProxy.findAuthorization(42L);

        assertThat(result).isInstanceOf(Found.class);
        assertThat(((Found<AuthorizationInfo>) result).data().status()).isEqualTo("AUTHORIZED");
    }

    @Test
    void returnsNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/authorizations/order/99"))
                .willReturn(aResponse().withStatus(404)));

        SectionResult<AuthorizationInfo> result = accountingServiceProxy.findAuthorization(99L);

        assertThat(result).isInstanceOf(NotFound.class);
    }

    @Test
    void returnsUnavailableWhenCircuitOpen() {
        wireMockServer.stop();

        SectionResult<AuthorizationInfo> result = null;
        for (int i = 0; i < 4; i++) {
            result = accountingServiceProxy.findAuthorization(42L);
        }

        assertThat(result).isInstanceOf(Unavailable.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests AccountingServiceProxyTest`
Expected: FAIL (compile error — `AccountingServiceProxy` doesn't exist yet)

- [ ] **Step 3: Write `AccountingServicePort`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingServicePort.java
package com.sanjay.ftgo.order.domain;

public interface AccountingServicePort {

    SectionResult<AuthorizationInfo> findAuthorization(Long orderId);
}
```

- [ ] **Step 4: Add the `accountingServiceRestClient` bean**

```java
// Add to ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java

@Bean
public RestClient accountingServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
    ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
            .withConnectTimeout(Duration.ofSeconds(2))
            .withReadTimeout(Duration.ofSeconds(2));
    ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

    return loadBalancedRestClientBuilder
            .baseUrl("http://ftgo-accounting-service")
            .requestFactory(requestFactory)
            .build();
}
```

- [ ] **Step 5: Add the `accountingService` circuit breaker instance to `application.yml`**

```yaml
# Add under resilience4j.circuitbreaker.instances, alongside restaurantService/kitchenService:

      accountingService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
```

- [ ] **Step 6: Write `AccountingServiceProxy`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingServiceProxy.java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.AccountingServicePort;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class AccountingServiceProxy implements AccountingServicePort {

    private final RestClient restClient;

    public AccountingServiceProxy(RestClient accountingServiceRestClient) {
        this.restClient = accountingServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "accountingService", fallbackMethod = "findAuthorizationFallback")
    public SectionResult<AuthorizationInfo> findAuthorization(Long orderId) {
        try {
            AuthorizationInfo info = restClient.get()
                    .uri("/authorizations/order/{orderId}", orderId)
                    .retrieve()
                    .body(AuthorizationInfo.class);
            return new Found<>(info);
        } catch (HttpClientErrorException.NotFound e) {
            return new NotFound<>();
        }
    }

    @SuppressWarnings("unused")
    private SectionResult<AuthorizationInfo> findAuthorizationFallback(Long orderId, Throwable throwable) {
        return new Unavailable<>(throwable.getMessage());
    }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests AccountingServiceProxyTest`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/AccountingServicePort.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/AccountingServiceProxy.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/AccountingServiceProxyTest.java
git commit -m "feat: add AccountingServiceProxy for the composite order view"
```

---

### Task 8: `order-service` — `DeliveryServicePort`/`DeliveryServiceProxy`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryServicePort.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/DeliveryServiceProxy.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/DeliveryServiceProxyTest.java`

**Interfaces:**
- Consumes: `SectionResult`/`Found`/`NotFound`/`Unavailable`, `DeliveryInfo` (Task 4).
- Produces: `DeliveryServicePort.findDelivery(Long orderId): SectionResult<DeliveryInfo>` — consumed by Task 9's `OrderViewController`.

**Note:** `order-service` already has classes named `DeliveryCommand`/`DeliveryEvent` from the Ch.7 sub-project 1 saga work — same naming-overlap situation as Task 7's `AccountingServiceProxy`, intentional and non-colliding (fully distinct class names).

- [ ] **Step 1: Write the failing test** (mirrors the prior two proxy tests, port 8092)

```java
// ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/DeliveryServiceProxyTest.java
package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DeliveryServiceProxyTest {

    private WireMockServer wireMockServer;

    @Autowired
    private DeliveryServiceProxy deliveryServiceProxy;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(8092);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    void returnsFoundOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/deliveries/order/42"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"orderId":42,"status":"SCHEDULED","courierId":3}
                                """)));

        SectionResult<DeliveryInfo> result = deliveryServiceProxy.findDelivery(42L);

        assertThat(result).isInstanceOf(Found.class);
        assertThat(((Found<DeliveryInfo>) result).data().courierId()).isEqualTo(3L);
    }

    @Test
    void returnsNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/deliveries/order/99"))
                .willReturn(aResponse().withStatus(404)));

        SectionResult<DeliveryInfo> result = deliveryServiceProxy.findDelivery(99L);

        assertThat(result).isInstanceOf(NotFound.class);
    }

    @Test
    void returnsUnavailableWhenCircuitOpen() {
        wireMockServer.stop();

        SectionResult<DeliveryInfo> result = null;
        for (int i = 0; i < 4; i++) {
            result = deliveryServiceProxy.findDelivery(42L);
        }

        assertThat(result).isInstanceOf(Unavailable.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests DeliveryServiceProxyTest`
Expected: FAIL (compile error — `DeliveryServiceProxy` doesn't exist yet)

- [ ] **Step 3: Write `DeliveryServicePort`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryServicePort.java
package com.sanjay.ftgo.order.domain;

public interface DeliveryServicePort {

    SectionResult<DeliveryInfo> findDelivery(Long orderId);
}
```

- [ ] **Step 4: Add the `deliveryServiceRestClient` bean**

```java
// Add to ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java

@Bean
public RestClient deliveryServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
    ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
            .withConnectTimeout(Duration.ofSeconds(2))
            .withReadTimeout(Duration.ofSeconds(2));
    ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

    return loadBalancedRestClientBuilder
            .baseUrl("http://ftgo-delivery-service")
            .requestFactory(requestFactory)
            .build();
}
```

- [ ] **Step 5: Add the `deliveryService` circuit breaker instance to `application.yml`**

```yaml
# Add under resilience4j.circuitbreaker.instances, alongside the other 3 instances:

      deliveryService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
```

- [ ] **Step 6: Write `DeliveryServiceProxy`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/DeliveryServiceProxy.java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.DeliveryServicePort;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class DeliveryServiceProxy implements DeliveryServicePort {

    private final RestClient restClient;

    public DeliveryServiceProxy(RestClient deliveryServiceRestClient) {
        this.restClient = deliveryServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "deliveryService", fallbackMethod = "findDeliveryFallback")
    public SectionResult<DeliveryInfo> findDelivery(Long orderId) {
        try {
            DeliveryInfo info = restClient.get()
                    .uri("/deliveries/order/{orderId}", orderId)
                    .retrieve()
                    .body(DeliveryInfo.class);
            return new Found<>(info);
        } catch (HttpClientErrorException.NotFound e) {
            return new NotFound<>();
        }
    }

    @SuppressWarnings("unused")
    private SectionResult<DeliveryInfo> findDeliveryFallback(Long orderId, Throwable throwable) {
        return new Unavailable<>(throwable.getMessage());
    }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests DeliveryServiceProxyTest`
Expected: PASS

- [ ] **Step 8: Run the full order-service test suite** (all 4 proxy tests together, to confirm the distinct WireMock ports don't collide)

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryServicePort.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/DeliveryServiceProxy.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/DeliveryServiceProxyTest.java
git commit -m "feat: add DeliveryServiceProxy for the composite order view"
```

---

### Task 9: `order-service` — `VirtualThreadExecutorConfig` + `OrderViewController`

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/VirtualThreadExecutorConfig.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderViewController.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderViewControllerTest.java`

**Interfaces:**
- Consumes: `RestaurantServicePort.findRestaurantForView` (Task 5), `KitchenServicePort.findTicket` (Task 6), `AccountingServicePort.findAuthorization` (Task 7), `DeliveryServicePort.findDelivery` (Task 8), `OrderRepository`/`OrderNotFoundException`/`Order` (existing), `OrderSummary`/`OrderViewResponse` (Task 4).
- Produces: `GET /orders/{id}/view` — terminal endpoint for this sub-project.

- [ ] **Step 1: Write the failing test**

```java
// ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderViewControllerTest.java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.AccountingServicePort;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.DeliveryServicePort;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.KitchenServicePort;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantServicePort;
import com.sanjay.ftgo.order.domain.TicketInfo;
import com.sanjay.ftgo.order.domain.Unavailable;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderViewController.class)
class OrderViewControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderRepository orderRepository;

    @MockitoBean
    private RestaurantServicePort restaurantServicePort;

    @MockitoBean
    private KitchenServicePort kitchenServicePort;

    @MockitoBean
    private AccountingServicePort accountingServicePort;

    @MockitoBean
    private DeliveryServicePort deliveryServicePort;

    @MockitoBean
    private ExecutorService orderViewExecutor;

    @org.springframework.boot.test.context.TestConfiguration
    static class RealExecutorConfig {
        @org.springframework.context.annotation.Bean
        ExecutorService orderViewExecutor() {
            return Executors.newVirtualThreadPerTaskExecutor();
        }
    }

    @Test
    void returnsAllFoundSections() throws Exception {
        Order order = new Order(1L, 42L, 7L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(restaurantServicePort.findRestaurantForView(7L))
                .thenReturn(new Found<>(new RestaurantInfo(7L, "Ajanta", List.of())));
        when(kitchenServicePort.findTicket(1L))
                .thenReturn(new Found<>(new TicketInfo(1L, 1L, "ACCEPTED", null)));
        when(accountingServicePort.findAuthorization(1L))
                .thenReturn(new Found<>(new AuthorizationInfo(1L, 1L, "AUTHORIZED")));
        when(deliveryServicePort.findDelivery(1L))
                .thenReturn(new Found<>(new DeliveryInfo(1L, 1L, "SCHEDULED", 3L)));

        mockMvc.perform(get("/orders/1/view"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.order.id").value(1))
                .andExpect(jsonPath("$.restaurant.data.name").value("Ajanta"))
                .andExpect(jsonPath("$.ticket.data.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.authorization.data.status").value("AUTHORIZED"))
                .andExpect(jsonPath("$.delivery.data.courierId").value(3));
    }

    @Test
    void returns404WhenOrderNotFound() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/orders/99/view")).andExpect(status().isNotFound());
    }

    @Test
    void degradesIndividualSectionsIndependently() throws Exception {
        Order order = new Order(1L, 42L, 7L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(restaurantServicePort.findRestaurantForView(7L))
                .thenReturn(new Found<>(new RestaurantInfo(7L, "Ajanta", List.of())));
        when(kitchenServicePort.findTicket(1L)).thenReturn(new NotFound<>());
        when(accountingServicePort.findAuthorization(1L)).thenReturn(new NotFound<>());
        when(deliveryServicePort.findDelivery(1L)).thenReturn(new Unavailable<>("timeout"));

        mockMvc.perform(get("/orders/1/view"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.restaurant.data.name").value("Ajanta"))
                .andExpect(jsonPath("$.ticket.data").doesNotExist())
                .andExpect(jsonPath("$.authorization.data").doesNotExist())
                .andExpect(jsonPath("$.delivery.reason").value("timeout"));
    }
}
```

Note: `OrderViewControllerTest`'s use of `@MockitoBean private ExecutorService orderViewExecutor` plus a nested `@TestConfiguration` supplying a real virtual-thread executor is deliberate — the executor needs to actually run submitted tasks for the controller's `CompletableFuture`s to complete, but the bean still needs to exist as a mockable-then-overridden Spring bean for the `@WebMvcTest` slice to wire in. If this pattern proves awkward during implementation, an acceptable alternative is to skip mocking `orderViewExecutor` entirely and just declare `VirtualThreadExecutorConfig` in a `@Import` on the test class instead — pick whichever compiles cleanly and keeps the test fast; note the choice in the implementation report.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests OrderViewControllerTest`
Expected: FAIL (compile error — `OrderViewController`/`VirtualThreadExecutorConfig` don't exist yet)

- [ ] **Step 3: Write `VirtualThreadExecutorConfig`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/VirtualThreadExecutorConfig.java
package com.sanjay.ftgo.order.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Virtual threads are a natural fit for the composite order-view query: 4 short-lived,
// blocking, I/O-bound downstream calls fired concurrently, with no pool-size tuning decision
// to make or justify (unlike a fixed-size ExecutorService).
@Configuration
public class VirtualThreadExecutorConfig {

    @Bean
    public ExecutorService orderViewExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
```

- [ ] **Step 4: Write `OrderViewController`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderViewController.java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.AccountingServicePort;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.DeliveryServicePort;
import com.sanjay.ftgo.order.domain.KitchenServicePort;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantServicePort;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@RestController
@RequestMapping("/orders")
public class OrderViewController {

    private final OrderRepository orderRepository;
    private final RestaurantServicePort restaurantServicePort;
    private final KitchenServicePort kitchenServicePort;
    private final AccountingServicePort accountingServicePort;
    private final DeliveryServicePort deliveryServicePort;
    private final ExecutorService orderViewExecutor;

    public OrderViewController(OrderRepository orderRepository,
                                RestaurantServicePort restaurantServicePort,
                                KitchenServicePort kitchenServicePort,
                                AccountingServicePort accountingServicePort,
                                DeliveryServicePort deliveryServicePort,
                                ExecutorService orderViewExecutor) {
        this.orderRepository = orderRepository;
        this.restaurantServicePort = restaurantServicePort;
        this.kitchenServicePort = kitchenServicePort;
        this.accountingServicePort = accountingServicePort;
        this.deliveryServicePort = deliveryServicePort;
        this.orderViewExecutor = orderViewExecutor;
    }

    @GetMapping("/{id}/view")
    public ResponseEntity<OrderViewResponse> view(@PathVariable Long id) {
        Order order = orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));

        CompletableFuture<SectionResult<RestaurantInfo>> restaurantFuture =
                CompletableFuture.supplyAsync(() -> restaurantServicePort.findRestaurantForView(order.getRestaurantId()), orderViewExecutor);
        CompletableFuture<SectionResult<TicketInfo>> ticketFuture =
                CompletableFuture.supplyAsync(() -> kitchenServicePort.findTicket(id), orderViewExecutor);
        CompletableFuture<SectionResult<AuthorizationInfo>> authorizationFuture =
                CompletableFuture.supplyAsync(() -> accountingServicePort.findAuthorization(id), orderViewExecutor);
        CompletableFuture<SectionResult<DeliveryInfo>> deliveryFuture =
                CompletableFuture.supplyAsync(() -> deliveryServicePort.findDelivery(id), orderViewExecutor);

        CompletableFuture.allOf(restaurantFuture, ticketFuture, authorizationFuture, deliveryFuture).join();

        OrderSummary summary = new OrderSummary(
                order.getId(),
                order.getStatus().name(),
                order.getConsumerId(),
                order.getRestaurantId(),
                order.getLineItems().stream()
                        .map(item -> new OrderSummary.LineItemView(item.menuItemId(), item.quantity()))
                        .toList());

        OrderViewResponse response = new OrderViewResponse(
                summary, restaurantFuture.join(), ticketFuture.join(), authorizationFuture.join(), deliveryFuture.join());
        return ResponseEntity.ok(response);
    }

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<String> handleNotFound(OrderNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests OrderViewControllerTest`
Expected: PASS

- [ ] **Step 6: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/VirtualThreadExecutorConfig.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderViewController.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderViewControllerTest.java
git commit -m "feat: add GET /orders/{id}/view composite query endpoint"
```

---

### Task 10: Full workspace build check

**Files:** none (verification-only task)

**Interfaces:** none

- [ ] **Step 1: Build every module**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL across all 8 modules

- [ ] **Step 2: Build every Docker image**

Run: `docker compose build`
Expected: all images build successfully

- [ ] **Step 3: If anything fails, fix it now** — do not proceed to Task 11 with a red build.

(No commit for this task unless Step 3 required a fix — if it did, commit that fix separately with a `fix:` message.)

---

### Task 11: Docs — order-service/kitchen-service/accounting-service/delivery-service READMEs, `docs/ARCHITECTURE.md`, `CONTEXT.md`

**Files:**
- Modify: `ftgo-order-service/README.md`
- Modify: `ftgo-kitchen-service/README.md`
- Modify: `ftgo-accounting-service/README.md`
- Modify: `ftgo-delivery-service/README.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `CONTEXT.md`

**Interfaces:** none — documentation only, per this project's `CLAUDE.md` "documentation updates land in the same change as the code they describe" rule and the spec's own Docs section.

- [ ] **Step 1: Read each file's current content before editing** — match each file's existing structure, heading levels, and tone exactly.

- [ ] **Step 2: Update `ftgo-order-service/README.md`** — add the new `GET /orders/{id}/view` endpoint to its API section, describing the response shape (`OrderSummary` + 4 `SectionResult` sections) and the parallel-fan-out/circuit-breaker approach. Note the 3 new proxy dependencies (kitchen/accounting/delivery-service, all via Eureka).

- [ ] **Step 3: Update `ftgo-kitchen-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-delivery-service/README.md`** — each gets its new `GET .../order/{orderId}` endpoint documented, plus a note that the service now registers with Eureka (`accounting-service`'s update also notes this is its first-ever REST controller).

- [ ] **Step 4: Update `docs/ARCHITECTURE.md`** — add a new "API composition" section (this project's first query pattern), documenting: the parallel-fan-out-via-virtual-threads approach, the per-proxy circuit breaker pattern (reusing `restaurantService`'s existing settings), and the 3-state `SectionResult` (found/not-found/unavailable) design. Update any service-discovery diagram/table to show `kitchen-service`/`accounting-service`/`delivery-service` as now-registered Eureka clients alongside `restaurant-service`.

- [ ] **Step 5: Update `CONTEXT.md`** — update the "Services to build" table rows for `ftgo-kitchen-service`/`ftgo-accounting-service`/`ftgo-delivery-service` (new read endpoint + Eureka registration) and `ftgo-order-service` (new composite query). Add a session log entry. Check off `[x] API composition (Ch.7)` in the "Querying" section of the patterns reference (CQRS stays unchecked — sub-project 3 not started). Do NOT mark Ch.7 "Done" in the progress table yet — sub-project 3 (CQRS) is still pending.

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-service/README.md ftgo-kitchen-service/README.md ftgo-accounting-service/README.md \
        ftgo-delivery-service/README.md docs/ARCHITECTURE.md CONTEXT.md
git commit -m "docs: document the API composition order-view query across all touched services"
```

---

### Task 12: Manual Docker e2e verification

**Files:** none

**Interfaces:** none

- [ ] **Step 1: Bring up the full stack**

Run: `docker compose up --build -d`
Verify: all 10 containers (including `kitchen-service`/`accounting-service`/`delivery-service` now depending on `service-registry`) reach a running state.

- [ ] **Step 2: Verify all 4 services register with Eureka**

Check `http://localhost:8761` (or `curl http://localhost:8761/eureka/apps`) and confirm `FTGO-RESTAURANT-SERVICE`, `FTGO-KITCHEN-SERVICE`, `FTGO-ACCOUNTING-SERVICE`, `FTGO-DELIVERY-SERVICE` all appear as registered instances (note: `order-service` itself has `register-with-eureka: false`, so it won't appear — that's expected, unchanged from before this sub-project).

- [ ] **Step 3: Call the composite view at several points in a real order's lifecycle**

Create an order (`POST /orders`), then immediately call `GET /orders/{id}/view` — expect `ticket`/`authorization`/`delivery` all `NotFound` (nothing has processed yet), `restaurant` `Found`. Wait for the saga to settle (a few seconds), call it again — expect all 4 sections `Found` with statuses matching the order's real end state (e.g. `Order.APPROVED` → `Ticket.AWAITING_ACCEPTANCE`, `Authorization.AUTHORIZED`, `Delivery.SCHEDULED`).

- [ ] **Step 4: Verify a decline scenario**

Create an order that triggers a decline (e.g. consumerId 2, inactive), let the saga settle, call `GET /orders/{id}/view` — expect `Order.REJECTED`, and confirm each section reflects the real compensated end state (e.g. `Ticket.CANCELLED` if one existed, `Authorization`/`Delivery` `NotFound` if the saga failed before those legs resolved).

- [ ] **Step 5: Verify unavailable-section degradation**

Stop one downstream container mid-test (e.g. `docker compose stop kitchen-service`), call `GET /orders/{id}/view` for an order with a real ticket — verify the response still returns `200` with `ticket` as `Unavailable` (some reason string) while `restaurant`/`authorization`/`delivery` still return normally. Restart the container (`docker compose start kitchen-service`) and confirm the section returns to `Found` after the circuit breaker's wait-duration-in-open-state (5s) elapses.

- [ ] **Step 6: Verify 404 on a nonexistent order**

`GET /orders/999999/view` → expect `404`.

- [ ] **Step 7: Tear down**

Run: `docker compose down` (omit `-v` unless the user confirms the `mysql-data` volume can be discarded).

No commit for this task — it's verification only. If any scenario surfaces a bug, fix it in a new commit with a `fix:` message describing the bug and root cause, then re-run the affected scenario before continuing.

---

## Deferred (not in this plan)

- **Sub-project 3**: CQRS read model — a dedicated read-side service/table fed by Kafka events from all five services. Separate future brainstorm → spec → plan cycle.
- Pagination, filtering, or any query beyond single-order-by-id.
- Caching the composite response.
