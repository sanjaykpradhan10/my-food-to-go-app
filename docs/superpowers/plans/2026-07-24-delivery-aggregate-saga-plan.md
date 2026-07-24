# Delivery Aggregate + Saga Participation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up `ftgo-delivery-service` as a real `Delivery` DDD aggregate and a full Create/Cancel Order saga participant (both choreography and orchestration modes), so a future composite order-view query (Ch.7, sub-projects 2–3) has real delivery data to compose.

**Architecture:** `Delivery` follows this codebase's established aggregate shape (`Ticket`/`Authorization`): a JPA entity with a guarded state machine returning class-per-event domain events, published via a dedicated `DeliveryDomainEventPublisher` through the shared `ftgo-common` transactional outbox. A small seeded `Courier` pool backs a simple "first available" matching rule. `delivery-service` becomes the Create Order saga's 3rd parallel-join leg (alongside consumer verification and ticket creation) and a new step in the Cancel Order saga (kitchen → delivery-release → accounting-reversal), in both saga modes, mirroring the exact choreography/orchestration split already used by `Ticket`/`Authorization`.

**Tech Stack:** Java 17, Spring Boot 3.5.16, Spring Data JPA, Spring Kafka, `ftgo-common`'s shared outbox/dedup module, JUnit 5 + AssertJ + Mockito, MySQL 8.4 (test: H2 with `MODE=MySQL`).

## Global Constraints

- No event sourcing for `Delivery` — plain JPA only (spec decision 7; Ch.6's event-sourcing exercise was scoped to `Order` alone).
- No `Delivery` row persisted on a scheduling decline (spec decision 3) — matches `Ticket`'s `TicketCreationFailed` precedent exactly.
- Courier matching is "first available, no matching algorithm" (spec decision 2) — no geo/routing/ETA logic anywhere in this plan.
- `Revise Order` saga is untouched (spec decision 6) — no task in this plan touches `OrderReviseSagaOrchestrator`/`OrderReviseSagaService`/kitchen's revision handlers.
- Every new record/entity mirrors the exact field-naming and wire-format conventions already used by `Ticket`/`Authorization`/`KitchenCommand`/`SagaReply` — do not invent new conventions.
- Code comments explain *why*, not *what* (project `CLAUDE.md`).
- TDD throughout: write the failing test before the implementation, for every task with a test step.
- Frequent commits: one commit per task, using this project's existing commit-message conventions (`feat:`, `fix:`, `docs:`, `refactor:`).

---

## Codebase reference (read once, applies to every task below)

`ftgo-delivery-service` is currently an *empty stub*: `FtgoDeliveryServiceApplication.java`, empty `api/`/`domain/`/`infrastructure/` packages (`.gitkeep` only), a bare `application.yml`, and a `build.gradle` with no dependencies. It is already listed in `settings.gradle` and `ftgo_delivery` already exists in `infrastructure/mysql/init.sql` — no changes needed to either file.

Every new file in this plan mirrors an existing file 1:1. The task steps below name the exact file being mirrored each time (e.g. "mirrors `ftgo-kitchen-service`'s `TicketService.java`").

---

### Task 1: Scaffold `ftgo-delivery-service` (Gradle, config, Docker, compose)

**Files:**
- Modify: `ftgo-delivery-service/build.gradle`
- Modify: `ftgo-delivery-service/src/main/resources/application.yml`
- Modify: `ftgo-delivery-service/src/test/resources/application.yml`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/config/PersistenceConfig.java`
- Create: `ftgo-delivery-service/Dockerfile`
- Modify: `compose.yml`

**Interfaces:**
- Produces: a bootable (but empty) Spring Boot service on port 8086, wired into Docker Compose, ready for later tasks to add domain code into `com.sanjay.ftgo.delivery.domain`/`infrastructure`/`api`.

- [ ] **Step 1: Add the `ftgo-common` dependency**

```groovy
// ftgo-delivery-service/build.gradle
dependencies {
    // spring-kafka comes transitively via ftgo-common's `api` dependency
    implementation project(':ftgo-common')
}
```

- [ ] **Step 2: Write `application.yml`** (mirrors `ftgo-kitchen-service/src/main/resources/application.yml`; port 8086 is the next free port — 8081–8085 and 8761/8087 are already taken)

```yaml
# ftgo-delivery-service/src/main/resources/application.yml
spring:
  application:
    name: ftgo-delivery-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_delivery
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: delivery-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest

outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20

saga:
  mode: choreography

server:
  port: 8086
```

- [ ] **Step 3: Write test `application.yml`** — check the existing file first (`ftgo-delivery-service/src/test/resources/application.yml`) and match kitchen-service's test config exactly (H2 `MODE=MySQL`, same `outbox`/`saga` defaults), e.g.:

```yaml
# ftgo-delivery-service/src/test/resources/application.yml
spring:
  application:
    name: ftgo-delivery-service
  datasource:
    url: jdbc:h2:mem:testdb;MODE=MySQL
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092

outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20

saga:
  mode: choreography
```

(If `ftgo-kitchen-service/src/test/resources/application.yml` differs from this, copy its exact contents instead, substituting only the `spring.application.name` — consistency with the established pattern matters more than this snippet.)

- [ ] **Step 4: Write `PersistenceConfig`** (mirrors `ftgo-kitchen-service`'s `config/PersistenceConfig.java` exactly)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/config/PersistenceConfig.java
package com.sanjay.ftgo.delivery.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoDeliveryServiceApplication because @EntityScan/@EnableJpaRepositories
// placed directly on the @SpringBootApplication class bypass @WebMvcTest's slice filtering —
// see ftgo-kitchen-service's PersistenceConfig for the concrete failure this pattern avoids.
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.delivery.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.delivery.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
```

- [ ] **Step 5: Write the Dockerfile** (mirrors `ftgo-kitchen-service/Dockerfile`)

```dockerfile
# ftgo-delivery-service/Dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-delivery-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-delivery-service/build/libs/*.jar app.jar
EXPOSE 8086
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 6: Wire into `compose.yml`** — add a new service block after `accounting-service`, before the `volumes:` key:

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
    ports:
      - "8086:8086"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_delivery
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SAGA_MODE: ${SAGA_MODE:-choreography}
```

- [ ] **Step 7: Verify the module builds**

Run: `./gradlew :ftgo-delivery-service:build`
Expected: BUILD SUCCESSFUL (the existing `FtgoDeliveryServiceApplicationTests` context-load test still passes now that `ftgo-common` and `PersistenceConfig` are wired in).

- [ ] **Step 8: Verify the Docker image builds**

Run: `docker compose build delivery-service`
Expected: image builds successfully.

- [ ] **Step 9: Commit**

```bash
git add ftgo-delivery-service/build.gradle ftgo-delivery-service/src/main/resources/application.yml \
        ftgo-delivery-service/src/test/resources/application.yml \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/config/PersistenceConfig.java \
        ftgo-delivery-service/Dockerfile compose.yml
git commit -m "feat: scaffold ftgo-delivery-service (Gradle, config, Docker, compose)"
```

---

### Task 2: `Courier` entity, repository, and seed data

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/Courier.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/CourierRepository.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/CourierSeeder.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/CourierTest.java`

**Interfaces:**
- Produces: `Courier(String name)` constructor (starts `available=true`), `getId()`/`getName()`/`isAvailable()`/`setAvailable(boolean)`; `CourierRepository.findFirstByAvailableTrue(): Optional<Courier>`. Later tasks (`DeliveryService`, Task 5) depend on both.

- [ ] **Step 1: Write the failing test**

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/CourierTest.java
package com.sanjay.ftgo.delivery.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CourierTest {

    @Test
    void newCourierStartsAvailable() {
        Courier courier = new Courier("Alex");

        assertThat(courier.getName()).isEqualTo("Alex");
        assertThat(courier.isAvailable()).isTrue();
    }

    @Test
    void setAvailableTogglesFlag() {
        Courier courier = new Courier("Alex");

        courier.setAvailable(false);

        assertThat(courier.isAvailable()).isFalse();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests CourierTest`
Expected: FAIL (compile error — `Courier` doesn't exist yet)

- [ ] **Step 3: Write `Courier`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/Courier.java
package com.sanjay.ftgo.delivery.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "couriers")
public class Courier {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private boolean available;

    protected Courier() {
    }

    public Courier(String name) {
        this.name = name;
        this.available = true;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }
}
```

- [ ] **Step 4: Write `CourierRepository`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/CourierRepository.java
package com.sanjay.ftgo.delivery.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface CourierRepository extends JpaRepository<Courier, Long> {

    Optional<Courier> findFirstByAvailableTrue();
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests CourierTest`
Expected: PASS

- [ ] **Step 6: Write the seed data component** (mirrors `ftgo-restaurant-service`'s `infrastructure/DataSeeder.java`)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/CourierSeeder.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.sanjay.ftgo.delivery.domain.Courier;
import com.sanjay.ftgo.delivery.domain.CourierRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CourierSeeder implements CommandLineRunner {

    private final CourierRepository courierRepository;

    public CourierSeeder(CourierRepository courierRepository) {
        this.courierRepository = courierRepository;
    }

    @Override
    public void run(String... args) {
        if (courierRepository.count() > 0) {
            return;
        }
        courierRepository.save(new Courier("Alex"));
        courierRepository.save(new Courier("Bailey"));
        courierRepository.save(new Courier("Casey"));
    }
}
```

- [ ] **Step 7: Run the full module test suite**

Run: `./gradlew :ftgo-delivery-service:test`
Expected: PASS (no other tests yet besides `CourierTest` and the context-load test)

- [ ] **Step 8: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/Courier.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/CourierRepository.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/CourierSeeder.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/CourierTest.java
git commit -m "feat: add Courier entity, repository, and seed data to delivery-service"
```

---

### Task 3: `DeliveryStatus` enum and `Delivery` aggregate

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryStatus.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/UnsupportedStateTransitionException.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryScheduledEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliverySchedulingFailedEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryPickedUpEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDeliveredEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryCancelledEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryScheduleResult.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/Delivery.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Delivery.schedule(Long orderId, Long restaurantId, Long courierId): DeliveryScheduleResult` (static factory); instance methods `pickUp()`, `deliver()`, `cancel()`, each returning `List<DeliveryDomainEvent>`; `getId()`/`getOrderId()`/`getRestaurantId()`/`getCourierId()`/`getStatus()`. `DeliveryScheduleResult(Delivery delivery, List<DeliveryDomainEvent> events)`. All consumed by `DeliveryDomainEventPublisher` (Task 4) and `DeliveryService` (Task 5).

**Design note (deviates from the spec's literal enum text):** the spec's `DeliveryStatus` listed a transient `PENDING` value "that never appears in the database." Per spec decision 3 (no `Delivery` row persisted on decline), no code path ever constructs a `Delivery` in `PENDING` — it would be dead code. This plan omits it: `DeliveryStatus` has exactly `SCHEDULED, PICKED_UP, DELIVERED, CANCELLED`, and `Delivery.schedule(...)` constructs directly into `SCHEDULED`, exactly like `Ticket.createTicket(...)` constructs directly into its own valid starting state.

- [ ] **Step 1: Write the failing test**

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryTest.java
package com.sanjay.ftgo.delivery.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryTest {

    @Test
    void scheduleStartsInScheduledAndEmitsDeliveryScheduled() {
        DeliveryScheduleResult result = Delivery.schedule(42L, 7L, 3L);

        assertThat(result.delivery().getStatus()).isEqualTo(DeliveryStatus.SCHEDULED);
        assertThat(result.delivery().getOrderId()).isEqualTo(42L);
        assertThat(result.delivery().getRestaurantId()).isEqualTo(7L);
        assertThat(result.delivery().getCourierId()).isEqualTo(3L);
        assertThat(result.events()).containsExactly(new DeliveryScheduledEvent(42L, 3L));
    }

    @Test
    void pickUpMovesFromScheduledToPickedUp() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        List<DeliveryDomainEvent> events = delivery.pickUp();

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.PICKED_UP);
        assertThat(events).containsExactly(new DeliveryPickedUpEvent(42L));
    }

    @Test
    void pickUpFromWrongStateThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();

        assertThatThrownBy(delivery::pickUp).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void deliverMovesFromPickedUpToDelivered() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();

        List<DeliveryDomainEvent> events = delivery.deliver();

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.DELIVERED);
        assertThat(events).containsExactly(new DeliveryDeliveredEvent(42L));
    }

    @Test
    void deliverFromWrongStateThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        assertThatThrownBy(delivery::deliver).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelMovesFromScheduledToCancelled() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        List<DeliveryDomainEvent> events = delivery.cancel();

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        assertThat(events).containsExactly(new DeliveryCancelledEvent(42L));
    }

    @Test
    void cancelFromPickedUpThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();

        assertThatThrownBy(delivery::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromDeliveredThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();
        delivery.deliver();

        assertThatThrownBy(delivery::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromCancelledThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.cancel();

        assertThatThrownBy(delivery::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryTest`
Expected: FAIL (compile error — none of `Delivery`/`DeliveryStatus`/etc. exist yet)

- [ ] **Step 3: Write `DeliveryStatus`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryStatus.java
package com.sanjay.ftgo.delivery.domain;

public enum DeliveryStatus {
    SCHEDULED,
    PICKED_UP,
    DELIVERED,
    CANCELLED
}
```

- [ ] **Step 4: Write `UnsupportedStateTransitionException`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/UnsupportedStateTransitionException.java
package com.sanjay.ftgo.delivery.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(DeliveryStatus status) {
        super("Unsupported transition from state " + status);
    }
}
```

- [ ] **Step 5: Write the domain event records and sealed interface**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEvent.java
package com.sanjay.ftgo.delivery.domain;

public sealed interface DeliveryDomainEvent
        permits DeliveryScheduledEvent, DeliverySchedulingFailedEvent, DeliveryPickedUpEvent,
                DeliveryDeliveredEvent, DeliveryCancelledEvent {

    Long orderId();
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryScheduledEvent.java
package com.sanjay.ftgo.delivery.domain;

public record DeliveryScheduledEvent(Long orderId, Long courierId) implements DeliveryDomainEvent {
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliverySchedulingFailedEvent.java
package com.sanjay.ftgo.delivery.domain;

public record DeliverySchedulingFailedEvent(Long orderId, String reason) implements DeliveryDomainEvent {
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryPickedUpEvent.java
package com.sanjay.ftgo.delivery.domain;

public record DeliveryPickedUpEvent(Long orderId) implements DeliveryDomainEvent {
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDeliveredEvent.java
package com.sanjay.ftgo.delivery.domain;

public record DeliveryDeliveredEvent(Long orderId) implements DeliveryDomainEvent {
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryCancelledEvent.java
package com.sanjay.ftgo.delivery.domain;

public record DeliveryCancelledEvent(Long orderId) implements DeliveryDomainEvent {
}
```

- [ ] **Step 6: Write `DeliveryScheduleResult`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryScheduleResult.java
package com.sanjay.ftgo.delivery.domain;

import java.util.List;

public record DeliveryScheduleResult(Delivery delivery, List<DeliveryDomainEvent> events) {
}
```

- [ ] **Step 7: Write `Delivery`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/Delivery.java
package com.sanjay.ftgo.delivery.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.List;

@Entity
@Table(name = "deliveries")
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    private Long restaurantId;

    private Long courierId;

    @Enumerated(EnumType.STRING)
    private DeliveryStatus status;

    protected Delivery() {
    }

    private Delivery(Long orderId, Long restaurantId, Long courierId, DeliveryStatus status) {
        this.orderId = orderId;
        this.restaurantId = restaurantId;
        this.courierId = courierId;
        this.status = status;
    }

    // No Delivery is ever constructed outside SCHEDULED - a decline never persists a row
    // (mirrors Ticket.createTicket/TicketCreationFailed), so there's no separate "pending"
    // starting state to guard against here.
    public static DeliveryScheduleResult schedule(Long orderId, Long restaurantId, Long courierId) {
        Delivery delivery = new Delivery(orderId, restaurantId, courierId, DeliveryStatus.SCHEDULED);
        return new DeliveryScheduleResult(delivery, List.of(new DeliveryScheduledEvent(orderId, courierId)));
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public Long getCourierId() {
        return courierId;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public List<DeliveryDomainEvent> pickUp() {
        if (status != DeliveryStatus.SCHEDULED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = DeliveryStatus.PICKED_UP;
        return List.of(new DeliveryPickedUpEvent(orderId));
    }

    public List<DeliveryDomainEvent> deliver() {
        if (status != DeliveryStatus.PICKED_UP) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = DeliveryStatus.DELIVERED;
        return List.of(new DeliveryDeliveredEvent(orderId));
    }

    // Legal only from SCHEDULED - reused for both a real Cancel Order request and every
    // Create Order compensation path (consumer/kitchen/accounting failure), same as
    // Ticket.cancel() serving both roles.
    public List<DeliveryDomainEvent> cancel() {
        if (status != DeliveryStatus.SCHEDULED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = DeliveryStatus.CANCELLED;
        return List.of(new DeliveryCancelledEvent(orderId));
    }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryTest`
Expected: PASS (8 tests)

- [ ] **Step 9: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryStatus.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/UnsupportedStateTransitionException.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryScheduledEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliverySchedulingFailedEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryPickedUpEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDeliveredEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryCancelledEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryScheduleResult.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/Delivery.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryTest.java
git commit -m "feat: add Delivery DDD aggregate with guarded state machine"
```

---

### Task 4: `DeliveryEvent` wire record and `DeliveryDomainEventPublisher`

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEventPublisher.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEventPublisherTest.java`

**Interfaces:**
- Consumes: `Delivery`, `DeliveryDomainEvent` and its 5 implementations (Task 3), `com.sanjay.ftgo.common.outbox.OutboxEvent`/`OutboxEventRepository` (`ftgo-common`).
- Produces: `DeliveryDomainEventPublisher.publish(Delivery delivery, List<DeliveryDomainEvent> events)` and `publishSchedulingFailed(DeliverySchedulingFailedEvent event)` — both used by `DeliveryService` (Task 5).

- [ ] **Step 1: Write the failing test** (mirrors `ftgo-kitchen-service`'s `TicketDomainEventPublisherTest.java` shape — if that exact file doesn't exist, mirror `TicketServiceTest`'s outbox-assertion style instead, asserting on `OutboxEventRepository` interactions via Mockito)

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEventPublisherTest.java
package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeliveryDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final DeliveryDomainEventPublisher publisher =
            new DeliveryDomainEventPublisher(outboxEventRepository, new ObjectMapper());

    @Test
    void publishesDeliveryScheduledToDeliveryEventsTopic() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        publisher.publish(delivery, List.of(new DeliveryScheduledEvent(42L, 3L)));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("DeliveryScheduled");
        assertThat(saved.getAggregateId()).isEqualTo(42L);
        assertThat(saved.getTopic()).isEqualTo("delivery.events");
        assertThat(saved.getPayload()).contains("\"courierId\":3");
    }

    @Test
    void publishesSchedulingFailedWithNoDeliveryEntity() {
        publisher.publishSchedulingFailed(new DeliverySchedulingFailedEvent(42L, "no courier available"));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("DeliverySchedulingFailed");
        assertThat(saved.getPayload()).contains("\"reason\":\"no courier available\"");
    }
}
```

(If `OutboxEvent`'s getter names differ from `getEventType`/`getAggregateId`/`getTopic`/`getPayload`, check `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxEvent.java` first and use its actual accessor names — this test must compile against the real class.)

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryDomainEventPublisherTest`
Expected: FAIL (compile error — `DeliveryEvent`/`DeliveryDomainEventPublisher` don't exist yet)

- [ ] **Step 3: Write `DeliveryEvent`** (mirrors `KitchenEvent`'s shape — `ticketId`/`totalQuantity` become `deliveryId`/`courierId`)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryEvent.java
package com.sanjay.ftgo.delivery.domain;

public record DeliveryEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long deliveryId,
        Long courierId,
        String reason) {
}
```

- [ ] **Step 4: Write `DeliveryDomainEventPublisher`** (mirrors `TicketDomainEventPublisher` exactly)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEventPublisher.java
package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class DeliveryDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public DeliveryDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publish(Delivery delivery, List<DeliveryDomainEvent> events) {
        events.forEach(event -> publishEvent(delivery.getId(), event));
    }

    public void publishSchedulingFailed(DeliverySchedulingFailedEvent event) {
        publishEvent(null, event);
    }

    private void publishEvent(Long deliveryId, DeliveryDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        DeliveryEvent wireEvent = toWireEvent(eventId, deliveryId, event);
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "delivery.events", toJson(wireEvent)));
    }

    private DeliveryEvent toWireEvent(String eventId, Long deliveryId, DeliveryDomainEvent event) {
        return switch (event) {
            case DeliveryScheduledEvent e ->
                    new DeliveryEvent(eventId, "DeliveryScheduled", e.orderId(), deliveryId, e.courierId(), null);
            case DeliverySchedulingFailedEvent e ->
                    new DeliveryEvent(eventId, "DeliverySchedulingFailed", e.orderId(), deliveryId, null, e.reason());
            case DeliveryPickedUpEvent e ->
                    new DeliveryEvent(eventId, "DeliveryPickedUp", e.orderId(), deliveryId, null, null);
            case DeliveryDeliveredEvent e ->
                    new DeliveryEvent(eventId, "DeliveryDelivered", e.orderId(), deliveryId, null, null);
            case DeliveryCancelledEvent e ->
                    new DeliveryEvent(eventId, "DeliveryCancelled", e.orderId(), deliveryId, null, null);
        };
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize delivery event", e);
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryDomainEventPublisherTest`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEventPublisher.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryDomainEventPublisherTest.java
git commit -m "feat: add DeliveryEvent wire record and DeliveryDomainEventPublisher"
```

---

### Task 5: `DeliveryRepository`, `FailedOrder`, and `DeliveryService`

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryRepository.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryNotFoundException.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/FailedOrder.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/FailedOrderRepository.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryService.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryServiceTest.java`

**Interfaces:**
- Consumes: `Delivery`/`Courier`/`CourierRepository`/`DeliveryDomainEventPublisher` (Tasks 2–4), `com.sanjay.ftgo.common.outbox.{OutboxEvent,OutboxEventRepository,ProcessedEvent,ProcessedEventRepository}`.
- Produces (used by later tasks — Tasks 7–9 listeners, Task 6 controller):
  - `DeliveryService.handleOrderCreated(String eventId, Long orderId, Long restaurantId)` — choreography, own-leg schedule.
  - `DeliveryService.release(String eventId, Long orderId)` — choreography, all compensation/cancellation triggers.
  - `DeliveryService.handleScheduleDeliveryCommand(String eventId, Long orderId, Long restaurantId)` — orchestration, replies on `saga.replies`.
  - `DeliveryService.handleReleaseDeliveryCommand(String eventId, Long orderId, String sagaType)` — orchestration, replies on `saga.replies`.
  - `DeliveryRepository.findByOrderId(Long orderId): Optional<Delivery>` — also used directly by `DeliveryController` (Task 6)? No — Task 6 uses `findById` like `TicketController` does; `findByOrderId` is only used inside `DeliveryService`.

- [ ] **Step 1: Write the failing test**

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryServiceTest.java
package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeliveryServiceTest {

    private final DeliveryRepository deliveryRepository = mock(DeliveryRepository.class);
    private final CourierRepository courierRepository = mock(CourierRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final FailedOrderRepository failedOrderRepository = mock(FailedOrderRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final DeliveryDomainEventPublisher domainEventPublisher = mock(DeliveryDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private DeliveryService deliveryService;

    @BeforeEach
    void setUp() {
        deliveryService = new DeliveryService(deliveryRepository, courierRepository, processedEventRepository,
                failedOrderRepository, outboxEventRepository, domainEventPublisher, objectMapper);
    }

    @Test
    void handleOrderCreatedSchedulesWhenCourierAvailable() {
        Courier courier = new Courier("Alex");
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.of(courier));
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(deliveryRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        deliveryService.handleOrderCreated("evt-1", 42L, 7L);

        assertThat(courier.isAvailable()).isFalse();
        verify(courierRepository).save(courier);
        verify(deliveryRepository).save(any(Delivery.class));
        verify(domainEventPublisher).publish(any(Delivery.class), any());
    }

    @Test
    void handleOrderCreatedPublishesSchedulingFailedWhenNoCourierAvailable() {
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.empty());
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);

        deliveryService.handleOrderCreated("evt-1", 42L, 7L);

        verify(deliveryRepository, never()).save(any());
        verify(domainEventPublisher).publishSchedulingFailed(new DeliverySchedulingFailedEvent(42L, "no courier available"));
    }

    @Test
    void handleOrderCreatedSkipsSchedulingWhenOrderAlreadyFailed() {
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(true);

        deliveryService.handleOrderCreated("evt-1", 42L, 7L);

        verify(courierRepository, never()).findFirstByAvailableTrue();
        verify(deliveryRepository, never()).save(any());
    }

    @Test
    void releaseCancelsDeliveryAndFreesCourier() {
        Courier courier = new Courier("Alex");
        courier.setAvailable(false);
        Delivery delivery = Delivery.schedule(42L, 7L, 9L).delivery();
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(deliveryRepository.findByOrderId(42L)).thenReturn(Optional.of(delivery));
        when(courierRepository.findById(9L)).thenReturn(Optional.of(courier));

        deliveryService.release("evt-2", 42L);

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        assertThat(courier.isAvailable()).isTrue();
        verify(courierRepository).save(courier);
        verify(domainEventPublisher).publish(delivery, java.util.List.of(new DeliveryCancelledEvent(42L)));
    }

    @Test
    void releaseRecordsFailedOrderWhenDeliveryNotYetScheduled() {
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(deliveryRepository.findByOrderId(42L)).thenReturn(Optional.empty());

        deliveryService.release("evt-2", 42L);

        verify(failedOrderRepository).save(any(FailedOrder.class));
        verify(domainEventPublisher, never()).publish(any(), any());
    }

    @Test
    void handleScheduleDeliveryCommandRepliesDeliveryScheduled() {
        Courier courier = new Courier("Alex");
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.of(courier));
        when(processedEventRepository.existsById("evt-3")).thenReturn(false);
        when(deliveryRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        deliveryService.handleScheduleDeliveryCommand("evt-3", 42L, 7L);

        verify(outboxEventRepository, times(1)).save(any());
    }

    @Test
    void handleReleaseDeliveryCommandRepliesDeliveryCancelled() {
        Delivery delivery = Delivery.schedule(42L, 7L, 9L).delivery();
        Courier courier = new Courier("Alex");
        courier.setAvailable(false);
        when(processedEventRepository.existsById("evt-4")).thenReturn(false);
        when(deliveryRepository.findByOrderId(42L)).thenReturn(Optional.of(delivery));
        when(courierRepository.findById(9L)).thenReturn(Optional.of(courier));

        deliveryService.handleReleaseDeliveryCommand("evt-4", 42L, "CreateOrder");

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        assertThat(courier.isAvailable()).isTrue();
        verify(outboxEventRepository, times(1)).save(any());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryServiceTest`
Expected: FAIL (compile error — `DeliveryService`/`DeliveryRepository`/`FailedOrder`/`FailedOrderRepository` don't exist yet)

- [ ] **Step 3: Write `DeliveryRepository`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryRepository.java
package com.sanjay.ftgo.delivery.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface DeliveryRepository extends JpaRepository<Delivery, Long> {

    Optional<Delivery> findByOrderId(Long orderId);
}
```

- [ ] **Step 4: Write `DeliveryNotFoundException`** (used by Task 6's controller)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryNotFoundException.java
package com.sanjay.ftgo.delivery.domain;

public class DeliveryNotFoundException extends RuntimeException {

    public DeliveryNotFoundException(Long deliveryId) {
        super("Delivery not found: " + deliveryId);
    }
}
```

- [ ] **Step 5: Write `FailedOrder`/`FailedOrderRepository`** (mirrors `ftgo-kitchen-service`'s pair exactly — same race this codebase already solved for kitchen: a compensation trigger (`ConsumerVerificationFailed`/`TicketCreationFailed`) arriving before `OrderCreated` is processed)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/FailedOrder.java
package com.sanjay.ftgo.delivery.domain;

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
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/FailedOrderRepository.java
package com.sanjay.ftgo.delivery.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FailedOrderRepository extends JpaRepository<FailedOrder, Long> {
}
```

- [ ] **Step 6: Write `DeliveryService`**

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryService.java
package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final CourierRepository courierRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final FailedOrderRepository failedOrderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final DeliveryDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public DeliveryService(DeliveryRepository deliveryRepository,
                            CourierRepository courierRepository,
                            ProcessedEventRepository processedEventRepository,
                            FailedOrderRepository failedOrderRepository,
                            OutboxEventRepository outboxEventRepository,
                            DeliveryDomainEventPublisher domainEventPublisher,
                            ObjectMapper objectMapper) {
        this.deliveryRepository = deliveryRepository;
        this.courierRepository = courierRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedOrderRepository = failedOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    // Choreography: delivery-service's own parallel-join leg, triggered directly by OrderCreated
    // (not gated on the other two legs, exactly like kitchen's ticket creation).
    @Transactional
    public void handleOrderCreated(String eventId, Long orderId, Long restaurantId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (failedOrderRepository.existsById(orderId)) {
            return;
        }

        schedule(orderId, restaurantId);
    }

    // Orchestration equivalent of handleOrderCreated: replies on saga.replies instead of
    // broadcasting on delivery.events, since the orchestrator (not a peer listener) is waiting.
    @Transactional
    public void handleScheduleDeliveryCommand(String eventId, Long orderId, Long restaurantId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Optional<Courier> available = courierRepository.findFirstByAvailableTrue();
        if (available.isEmpty()) {
            publishReply("DeliverySchedulingFailed", orderId, "no courier available", "CreateOrder");
            return;
        }
        Courier courier = available.get();
        courier.setAvailable(false);
        courierRepository.save(courier);

        DeliveryScheduleResult result = Delivery.schedule(orderId, restaurantId, courier.getId());
        deliveryRepository.save(result.delivery());
        publishReply("DeliveryScheduled", orderId, null, "CreateOrder");
    }

    private void schedule(Long orderId, Long restaurantId) {
        Optional<Courier> available = courierRepository.findFirstByAvailableTrue();
        if (available.isEmpty()) {
            domainEventPublisher.publishSchedulingFailed(new DeliverySchedulingFailedEvent(orderId, "no courier available"));
            return;
        }
        Courier courier = available.get();
        courier.setAvailable(false);
        courierRepository.save(courier);

        DeliveryScheduleResult result = Delivery.schedule(orderId, restaurantId, courier.getId());
        Delivery delivery = deliveryRepository.save(result.delivery());
        domainEventPublisher.publish(delivery, result.events());
    }

    // Choreography: single entry point for every release trigger - a real Cancel Order request
    // (kitchen's TicketCancelled) and every Create Order compensation path (ConsumerVerificationFailed,
    // TicketCreationFailed, CardAuthorizationFailed all funnel here). If the delivery hasn't been
    // scheduled yet (the compensation trigger raced ahead of this service's own OrderCreated
    // processing), record FailedOrder so the eventual schedule() call becomes a no-op instead of
    // assigning a courier to an order that's already doomed - same race this codebase already
    // solved for kitchen's Ticket.
    @Transactional
    public void release(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Delivery delivery = deliveryRepository.findByOrderId(orderId).orElse(null);
        if (delivery == null) {
            failedOrderRepository.save(new FailedOrder(orderId));
            return;
        }
        List<DeliveryDomainEvent> events = delivery.cancel();
        deliveryRepository.save(delivery);
        releaseCourier(delivery);
        domainEventPublisher.publish(delivery, events);
    }

    // Orchestration equivalent of release: replies on saga.replies. Unconditional once the
    // orchestrator sends this command (spec decision 5 - no decline path for releasing a
    // courier), so no FailedOrder handling is needed here: the orchestrator only ever sends
    // ReleaseDelivery after it already has confirmation the delivery was scheduled.
    @Transactional
    public void handleReleaseDeliveryCommand(String eventId, Long orderId, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Delivery delivery = deliveryRepository.findByOrderId(orderId).orElse(null);
        if (delivery == null) {
            return;
        }
        delivery.cancel();
        deliveryRepository.save(delivery);
        releaseCourier(delivery);
        publishReply("DeliveryCancelled", orderId, null, sagaType);
    }

    private void releaseCourier(Delivery delivery) {
        courierRepository.findById(delivery.getCourierId()).ifPresent(courier -> {
            courier.setAvailable(true);
            courierRepository.save(courier);
        });
    }

    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "delivery", eventType, orderId, reason, sagaType);
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

Note: `DeliveryService` references `SagaReply`, written in Task 9 (needed for orchestration). Since Task 9 comes after this task in the plan but `DeliveryService` needs it to compile, **write `SagaReply` now** as part of this step (it's a 1-line record, zero risk of rework):

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/SagaReply.java
package com.sanjay.ftgo.delivery.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason, String sagaType) {
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryServiceTest`
Expected: PASS (7 tests)

- [ ] **Step 8: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryRepository.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryNotFoundException.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/FailedOrder.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/FailedOrderRepository.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryService.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/SagaReply.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryServiceTest.java
git commit -m "feat: add DeliveryService with schedule/release for both saga modes"
```

---

### Task 6: `DeliveryController` (courier-facing REST API)

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java`

**Interfaces:**
- Consumes: `Delivery`/`DeliveryRepository`/`DeliveryDomainEventPublisher`/`DeliveryNotFoundException`/`UnsupportedStateTransitionException` (Tasks 3–5).
- Produces: `POST /deliveries/{id}/picked-up`, `POST /deliveries/{id}/delivered` — terminal for this sub-project (no other task depends on this controller).

- [ ] **Step 1: Write the failing test** (mirrors `TicketControllerTest` exactly)

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java
package com.sanjay.ftgo.delivery.api;

import com.sanjay.ftgo.delivery.domain.Delivery;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEventPublisher;
import com.sanjay.ftgo.delivery.domain.DeliveryRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DeliveryController.class)
class DeliveryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DeliveryRepository deliveryRepository;

    @MockitoBean
    private DeliveryDomainEventPublisher domainEventPublisher;

    @Test
    void movesScheduledDeliveryToPickedUp() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/picked-up")).andExpect(status().isOk());
    }

    @Test
    void returns404WhenDeliveryNotFoundOnPickedUp() throws Exception {
        when(deliveryRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/deliveries/99/picked-up")).andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenPickingUpAlreadyPickedUpDelivery() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/picked-up")).andExpect(status().isConflict());
    }

    @Test
    void movesPickedUpDeliveryToDelivered() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/delivered")).andExpect(status().isOk());
    }

    @Test
    void returns409WhenDeliveringScheduledDelivery() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/delivered")).andExpect(status().isConflict());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryControllerTest`
Expected: FAIL (compile error — `DeliveryController` doesn't exist yet)

- [ ] **Step 3: Write `DeliveryController`** (mirrors `TicketController` exactly)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java
package com.sanjay.ftgo.delivery.api;

import com.sanjay.ftgo.delivery.domain.Delivery;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEvent;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEventPublisher;
import com.sanjay.ftgo.delivery.domain.DeliveryNotFoundException;
import com.sanjay.ftgo.delivery.domain.DeliveryRepository;
import com.sanjay.ftgo.delivery.domain.UnsupportedStateTransitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/deliveries")
@Transactional
public class DeliveryController {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryDomainEventPublisher domainEventPublisher;

    public DeliveryController(DeliveryRepository deliveryRepository, DeliveryDomainEventPublisher domainEventPublisher) {
        this.deliveryRepository = deliveryRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @PostMapping("/{deliveryId}/picked-up")
    public ResponseEntity<Void> pickedUp(@PathVariable Long deliveryId) {
        Delivery delivery = findDelivery(deliveryId);
        apply(delivery, delivery.pickUp());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{deliveryId}/delivered")
    public ResponseEntity<Void> delivered(@PathVariable Long deliveryId) {
        Delivery delivery = findDelivery(deliveryId);
        apply(delivery, delivery.deliver());
        return ResponseEntity.ok().build();
    }

    private Delivery findDelivery(Long deliveryId) {
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new DeliveryNotFoundException(deliveryId));
    }

    private void apply(Delivery delivery, List<DeliveryDomainEvent> events) {
        deliveryRepository.save(delivery);
        domainEventPublisher.publish(delivery, events);
    }

    @ExceptionHandler(DeliveryNotFoundException.class)
    public ResponseEntity<String> handleNotFound(DeliveryNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(UnsupportedStateTransitionException.class)
    public ResponseEntity<String> handleConflict(UnsupportedStateTransitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryControllerTest`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/api/DeliveryController.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java
git commit -m "feat: add courier-facing DeliveryController REST API"
```

---

### Task 7: Choreography inbound — `OrderEventListener` (own leg: `OrderCreated` → schedule)

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/OrderCreatedEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/OrderEventListener.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/OrderEventListenerTest.java`

**Interfaces:**
- Consumes: `DeliveryService.handleOrderCreated(String, Long, Long)` (Task 5).
- Produces: nothing consumed by later tasks — this is delivery-service's own leaf listener for its own join leg.

- [ ] **Step 1: Write the failing test** (a plain unit test on the listener's dispatch logic — mirrors this codebase's convention of testing listeners via direct method calls with a mocked service, not a full Kafka integration test; check `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/` for an existing listener test to match exactly — if none exists there, use this shape)

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/OrderEventListenerTest.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final OrderEventListener listener = new OrderEventListener(deliveryService, new ObjectMapper());

    @Test
    void onOrderCreatedSchedulesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"OrderCreated","orderId":42,"restaurantId":7}
                """;

        listener.onMessage(payload);

        verify(deliveryService).handleOrderCreated("evt-1", 42L, 7L);
    }

    @Test
    void ignoresOtherEventTypes() {
        String payload = """
                {"eventId":"evt-1","eventType":"OrderCancelled","orderId":42,"restaurantId":7}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(deliveryService);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests OrderEventListenerTest`
Expected: FAIL (compile error — `OrderCreatedEvent`/`OrderEventListener` don't exist yet)

- [ ] **Step 3: Write `OrderCreatedEvent`** (delivery-service's own copy, trimmed to only the fields it needs — Spring's default Jackson config (`FAIL_ON_UNKNOWN_PROPERTIES=false`) already tolerates order-service's `OrderEvent` carrying extra fields like `consumerId`/`lineItems`, confirmed by kitchen-service's own trimmed copy working the same way)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/OrderCreatedEvent.java
package com.sanjay.ftgo.delivery.domain;

public record OrderCreatedEvent(String eventId, String eventType, Long orderId, Long restaurantId) {
}
```

- [ ] **Step 4: Write `OrderEventListener`** (mirrors `ftgo-kitchen-service`'s `OrderEventListener`, but only needs the one `"OrderCreated"` case — `"OrderCancelled"` is handled by delivery-service's own `KitchenEventListener` in Task 8, reacting to kitchen's `TicketCancelled`, not order-service's `OrderCancelled` directly, matching the spec's kitchen-then-delivery Cancel Order sequencing)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/OrderEventListener.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import com.sanjay.ftgo.delivery.domain.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.events", groupId = "delivery-service")
    public void onMessage(String payload) {
        OrderCreatedEvent event;
        try {
            event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed order event: {}", payload, e);
            return;
        }
        if ("OrderCreated".equals(event.eventType())) {
            deliveryService.handleOrderCreated(event.eventId(), event.orderId(), event.restaurantId());
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests OrderEventListenerTest`
Expected: PASS (3 tests)

- [ ] **Step 6: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/OrderCreatedEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/OrderEventListener.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/OrderEventListenerTest.java
git commit -m "feat: schedule delivery on OrderCreated (choreography)"
```

---

### Task 8: Choreography compensation listeners (`ConsumerEventListener`, `KitchenEventListener`, `AccountingEventListener`)

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/ConsumerVerificationEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/KitchenEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/AccountingEvent.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/ConsumerEventListener.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/KitchenEventListener.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/AccountingEventListener.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/ConsumerEventListenerTest.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/KitchenEventListenerTest.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/AccountingEventListenerTest.java`

**Interfaces:**
- Consumes: `DeliveryService.release(String, Long)` (Task 5).
- Produces: nothing consumed by later tasks.

This task wires the full choreography compensation matrix from the delivery side: `ConsumerVerificationFailed` (consumer.events), `TicketCreationFailed` **and** `TicketCancelled` (kitchen.events — the former is Create Order compensation, the latter is the real Cancel Order trigger), and `CardAuthorizationFailed` (accounting.events) all release the courier via the same `DeliveryService.release` entry point, mirroring exactly how kitchen-service's `Ticket` already reacts to every sibling participant's failure/cancellation event.

- [ ] **Step 1: Write the failing tests**

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/ConsumerEventListenerTest.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ConsumerEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final ConsumerEventListener listener = new ConsumerEventListener(deliveryService, new ObjectMapper());

    @Test
    void onConsumerVerificationFailedReleasesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"ConsumerVerificationFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-1", 42L);
    }

    @Test
    void ignoresConsumerVerified() {
        String payload = """
                {"eventId":"evt-1","eventType":"ConsumerVerified","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }
}
```

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/KitchenEventListenerTest.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class KitchenEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final KitchenEventListener listener = new KitchenEventListener(deliveryService, new ObjectMapper());

    @Test
    void onTicketCreationFailedReleasesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"TicketCreationFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-1", 42L);
    }

    @Test
    void onTicketCancelledReleasesDelivery() {
        String payload = """
                {"eventId":"evt-2","eventType":"TicketCancelled","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-2", 42L);
    }

    @Test
    void ignoresTicketCreated() {
        String payload = """
                {"eventId":"evt-1","eventType":"TicketCreated","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }
}
```

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/AccountingEventListenerTest.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class AccountingEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final AccountingEventListener listener = new AccountingEventListener(deliveryService, new ObjectMapper());

    @Test
    void onCardAuthorizationFailedReleasesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"CardAuthorizationFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-1", 42L);
    }

    @Test
    void ignoresCardAuthorized() {
        String payload = """
                {"eventId":"evt-1","eventType":"CardAuthorized","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ftgo-delivery-service:test --tests "com.sanjay.ftgo.delivery.infrastructure.*EventListenerTest"`
Expected: FAIL (compile errors — none of the wire records or listeners exist yet)

- [ ] **Step 3: Write the wire records** (each trimmed to only what delivery-service needs, mirroring kitchen-service's own trimmed `ConsumerVerificationEvent`/`AccountingEvent` copies)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/ConsumerVerificationEvent.java
package com.sanjay.ftgo.delivery.domain;

public record ConsumerVerificationEvent(String eventId, String eventType, Long orderId) {
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/KitchenEvent.java
package com.sanjay.ftgo.delivery.domain;

public record KitchenEvent(String eventId, String eventType, Long orderId) {
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/AccountingEvent.java
package com.sanjay.ftgo.delivery.domain;

public record AccountingEvent(String eventId, String eventType, Long orderId) {
}
```

- [ ] **Step 4: Write the three listeners** (each mirrors `ftgo-kitchen-service`'s corresponding listener)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/ConsumerEventListener.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "delivery-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        if ("ConsumerVerificationFailed".equals(event.eventType())) {
            deliveryService.release(event.eventId(), event.orderId());
        }
    }
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/KitchenEventListener.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import com.sanjay.ftgo.delivery.domain.KitchenEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Both cases funnel into the same DeliveryService.release: TicketCreationFailed is Create Order
// compensation (delivery may or may not have scheduled yet), TicketCancelled is the real Cancel
// Order trigger (delivery is always scheduled by then, since Cancel is only reachable from an
// already-APPROVED order). release() handles both uniformly - see DeliveryService's own comment.
@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "delivery-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "TicketCreationFailed", "TicketCancelled" -> deliveryService.release(event.eventId(), event.orderId());
            default -> { }
        }
    }
}
```

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/AccountingEventListener.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.AccountingEvent;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.events", groupId = "delivery-service")
    public void onMessage(String payload) {
        AccountingEvent event;
        try {
            event = objectMapper.readValue(payload, AccountingEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting event: {}", payload, e);
            return;
        }
        if ("CardAuthorizationFailed".equals(event.eventType())) {
            deliveryService.release(event.eventId(), event.orderId());
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew :ftgo-delivery-service:test --tests "com.sanjay.ftgo.delivery.infrastructure.*EventListenerTest"`
Expected: PASS (7 tests total)

- [ ] **Step 6: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/ConsumerVerificationEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/KitchenEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/AccountingEvent.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/ConsumerEventListener.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/KitchenEventListener.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/AccountingEventListener.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/ConsumerEventListenerTest.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/KitchenEventListenerTest.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/AccountingEventListenerTest.java
git commit -m "feat: release delivery on every compensation/cancellation trigger (choreography)"
```

---

### Task 9: Orchestration inbound — `DeliveryCommand` and `DeliveryCommandListener`

**Files:**
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryCommand.java`
- Create: `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/DeliveryCommandListener.java`
- Test: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/DeliveryCommandListenerTest.java`

**Interfaces:**
- Consumes: `DeliveryService.handleScheduleDeliveryCommand`/`handleReleaseDeliveryCommand` (Task 5).
- Produces: `DeliveryCommand(String eventId, String commandType, Long orderId, Long restaurantId, String sagaType)` — also depended on by Task 12 (order-service's orchestrator, which sends this same shape — order-service keeps its own copy, per this codebase's established per-service wire-record convention).

- [ ] **Step 1: Write the failing test**

```java
// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/DeliveryCommandListenerTest.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeliveryCommandListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final DeliveryCommandListener listener = new DeliveryCommandListener(deliveryService, new ObjectMapper());

    @Test
    void onScheduleDeliveryCommandCallsHandleScheduleDeliveryCommand() {
        String payload = """
                {"eventId":"evt-1","commandType":"ScheduleDelivery","orderId":42,"restaurantId":7,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(deliveryService).handleScheduleDeliveryCommand("evt-1", 42L, 7L);
    }

    @Test
    void onReleaseDeliveryCommandCallsHandleReleaseDeliveryCommand() {
        String payload = """
                {"eventId":"evt-2","commandType":"ReleaseDelivery","orderId":42,"restaurantId":null,"sagaType":"CancelOrder"}
                """;

        listener.onMessage(payload);

        verify(deliveryService).handleReleaseDeliveryCommand("evt-2", 42L, "CancelOrder");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryCommandListenerTest`
Expected: FAIL (compile error — `DeliveryCommand`/`DeliveryCommandListener` don't exist yet)

- [ ] **Step 3: Write `DeliveryCommand`** (mirrors `KitchenCommand`'s shape — `totalQuantity` becomes `restaurantId`; reused for both `ScheduleDelivery` and `ReleaseDelivery`, same as `KitchenCommand` carrying both `CreateTicket` and `CancelTicket`)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryCommand.java
package com.sanjay.ftgo.delivery.domain;

public record DeliveryCommand(String eventId, String commandType, Long orderId, Long restaurantId, String sagaType) {
}
```

- [ ] **Step 4: Write `DeliveryCommandListener`** (mirrors `KitchenCommandListener`)

```java
// ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/DeliveryCommandListener.java
package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryCommand;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class DeliveryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryCommandListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public DeliveryCommandListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.commands", groupId = "delivery-service")
    public void onMessage(String payload) {
        DeliveryCommand command;
        try {
            command = objectMapper.readValue(payload, DeliveryCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery command: {}", payload, e);
            return;
        }
        switch (command.commandType()) {
            case "ScheduleDelivery" ->
                    deliveryService.handleScheduleDeliveryCommand(command.eventId(), command.orderId(), command.restaurantId());
            case "ReleaseDelivery" ->
                    deliveryService.handleReleaseDeliveryCommand(command.eventId(), command.orderId(), command.sagaType());
            default -> log.warn("Unknown delivery command type: {}", command.commandType());
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-delivery-service:test --tests DeliveryCommandListenerTest`
Expected: PASS

- [ ] **Step 6: Run the full delivery-service test suite**

Run: `./gradlew :ftgo-delivery-service:test`
Expected: PASS (all tests from Tasks 2–9)

- [ ] **Step 7: Commit**

```bash
git add ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/DeliveryCommand.java \
        ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/infrastructure/DeliveryCommandListener.java \
        ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/infrastructure/DeliveryCommandListenerTest.java
git commit -m "feat: handle ScheduleDelivery/ReleaseDelivery commands (orchestration)"
```

**delivery-service is now feature-complete for this sub-project.** The remaining tasks touch `accounting-service`, `kitchen-service`, and `order-service`.

---

### Task 10: `accounting-service` — 3-way `SagaJoinState` join

**Files:**
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinState.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Test: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java` (extend existing tests — read the file first to match its exact structure/mocks before adding new cases)

**Interfaces:**
- Consumes: nothing new from this plan (this task only touches `accounting-service`'s existing join machinery).
- Produces: `SagaJoinState.markDeliveryScheduled(Long)`, `SagaJoinState.isDeliveryScheduled()`; `SagaJoinService.handleDeliveryEvent(String eventId, Long orderId, String eventType)` — consumed by Task 11's new `DeliveryEventListener`.

- [ ] **Step 1: Read the existing test file to match its structure**

Run: `cat ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java` and note its exact mock setup, `@BeforeEach`, and assertion style before writing new tests — this task must extend it consistently, not duplicate its scaffolding.

- [ ] **Step 2: Write the failing tests** — add these cases to the existing `SagaJoinServiceTest` class (adapting to whatever constructor/mock names the file already uses):

```java
// New test methods added to SagaJoinServiceTest.java

@Test
void resolvesOnlyAfterAllThreeLegs() {
    sagaJoinService.handleConsumerEvent("evt-1", 42L, "ConsumerVerified");
    sagaJoinService.handleKitchenEvent("evt-2", 42L, "TicketCreated", 5);
    // Not resolved yet - delivery leg still missing
    verify(authorizationRepository, never()).save(any());

    sagaJoinService.handleDeliveryEvent("evt-3", 42L, "DeliveryScheduled");

    verify(authorizationRepository, times(1)).save(any());
}

@Test
void deliverySchedulingFailedMarksJoinFailed() {
    sagaJoinService.handleDeliveryEvent("evt-1", 42L, "DeliverySchedulingFailed");

    sagaJoinService.handleConsumerEvent("evt-2", 42L, "ConsumerVerified");
    sagaJoinService.handleKitchenEvent("evt-3", 42L, "TicketCreated", 5);

    // Join already marked failed by the delivery leg - never authorizes
    verify(authorizationRepository, never()).save(any());
}
```

(Match these to the existing test file's exact mock field names — e.g. it may be `sagaJoinStateRepository`/`authorizationRepository`/`sagaJoinService` with specific `when(...)` stubs already set up in a shared `@BeforeEach`. Read the file fully before writing these additions so they compile against the real fixture.)

- [ ] **Step 3: Run tests to verify the new ones fail**

Run: `./gradlew :ftgo-accounting-service:test --tests SagaJoinServiceTest`
Expected: FAIL (compile error — `handleDeliveryEvent` doesn't exist yet)

- [ ] **Step 4: Extend `SagaJoinState`**

```java
// Add to ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinState.java,
// alongside the existing consumerVerified/ticketCreated fields:

private boolean deliveryScheduled;

public boolean isDeliveryScheduled() {
    return deliveryScheduled;
}

public void markDeliveryScheduled() {
    this.deliveryScheduled = true;
}
```

- [ ] **Step 5: Extend `SagaJoinService`**

```java
// Add to ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java

@Transactional
public void handleDeliveryEvent(String eventId, Long orderId, String eventType) {
    if (processedEventRepository.existsById(eventId)) {
        return;
    }
    processedEventRepository.save(new ProcessedEvent(eventId));

    SagaJoinState state = sagaJoinStateRepository.findById(orderId).orElseGet(() -> new SagaJoinState(orderId));
    if (state.isResolved() || state.isFailed()) {
        return;
    }

    if ("DeliverySchedulingFailed".equals(eventType)) {
        state.markFailed();
        sagaJoinStateRepository.save(state);
        return;
    }

    state.markDeliveryScheduled();
    sagaJoinStateRepository.save(state);
    tryResolve(state);
}
```

And change `tryResolve`'s guard from a 2-leg check to a 3-leg check:

```java
// Replace in tryResolve:
if (!state.isConsumerVerified() || !state.isTicketCreated()) {
    return;
}
// with:
if (!state.isConsumerVerified() || !state.isTicketCreated() || !state.isDeliveryScheduled()) {
    return;
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests SagaJoinServiceTest`
Expected: PASS (all existing tests plus the 2 new ones — the existing 2-leg happy-path tests must be updated to also send a `DeliveryScheduled` event, or they will now hang at "not yet resolved"; update them accordingly as part of this step)

- [ ] **Step 7: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinState.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java
git commit -m "feat: extend Create Order saga join to a 3-way join (consumer/kitchen/delivery)"
```

---

### Task 11: `accounting-service` — new `DeliveryEventListener`, retarget Cancel Order's reversal trigger

**Files:**
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/DeliveryEvent.java`
- Create: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/DeliveryEventListener.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java`
- Test: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/DeliveryEventListenerTest.java`
- Test: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListenerTest.java` (if it exists — modify to remove the `"TicketCancelled"` assertion; read first)

**Interfaces:**
- Consumes: `SagaJoinService.handleDeliveryEvent` (Task 10), the existing `AuthorizationCancelService.reverseForChoreography` (unchanged signature).

- [ ] **Step 1: Write the failing test**

```java
// ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/DeliveryEventListenerTest.java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final SagaJoinService sagaJoinService = mock(SagaJoinService.class);
    private final AuthorizationCancelService authorizationCancelService = mock(AuthorizationCancelService.class);
    private final DeliveryEventListener listener =
            new DeliveryEventListener(sagaJoinService, authorizationCancelService, new ObjectMapper());

    @Test
    void deliveryScheduledFeedsTheJoin() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleDeliveryEvent("evt-1", 42L, "DeliveryScheduled");
        verifyNoInteractions(authorizationCancelService);
    }

    @Test
    void deliverySchedulingFailedFeedsTheJoin() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliverySchedulingFailed","orderId":42,"reason":"no courier available"}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleDeliveryEvent("evt-1", 42L, "DeliverySchedulingFailed");
    }

    @Test
    void deliveryCancelledTriggersReversal() {
        String payload = """
                {"eventId":"evt-2","eventType":"DeliveryCancelled","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verify(authorizationCancelService).reverseForChoreography("evt-2", 42L);
        verifyNoInteractions(sagaJoinService);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-accounting-service:test --tests DeliveryEventListenerTest`
Expected: FAIL (compile error — `DeliveryEvent`/`DeliveryEventListener` don't exist yet)

- [ ] **Step 3: Write `DeliveryEvent`**

```java
// ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/DeliveryEvent.java
package com.sanjay.ftgo.accounting.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId, String reason) {
}
```

- [ ] **Step 4: Write `DeliveryEventListener`** (mirrors `KitchenEventListener`'s dual role: feeds the Create Order join AND triggers Cancel Order's reversal — the trigger moves here from `KitchenEventListener`'s `"TicketCancelled"` case, per spec decision 5's kitchen → delivery-release → accounting sequencing)

```java
// ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/DeliveryEventListener.java
package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.DeliveryEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class DeliveryEventListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventListener.class);

    private final SagaJoinService sagaJoinService;
    private final AuthorizationCancelService authorizationCancelService;
    private final ObjectMapper objectMapper;

    public DeliveryEventListener(SagaJoinService sagaJoinService,
                                  AuthorizationCancelService authorizationCancelService,
                                  ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.authorizationCancelService = authorizationCancelService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.events", groupId = "accounting-service")
    public void onMessage(String payload) {
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(payload, DeliveryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "DeliveryScheduled", "DeliverySchedulingFailed" ->
                    sagaJoinService.handleDeliveryEvent(event.eventId(), event.orderId(), event.eventType());
            case "DeliveryCancelled" -> authorizationCancelService.reverseForChoreography(event.eventId(), event.orderId());
            default -> { }
        }
    }
}
```

- [ ] **Step 5: Remove the `"TicketCancelled"` case from `KitchenEventListener`** — read the existing test file first (`ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListenerTest.java`) and delete/update whichever test asserted `TicketCancelled -> reverseForChoreography`, since that trigger has moved to `DeliveryEventListener`.

```java
// ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java
// Change the switch from:
switch (event.eventType()) {
    case "TicketCreated", "TicketCreationFailed" ->
            sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
    case "TicketCancelled" -> authorizationCancelService.reverseForChoreography(event.eventId(), event.orderId());
    case "TicketQuantityRevised" ->
            authorizationReviseService.reviseForChoreography(event.eventId(), event.orderId(), event.totalQuantity());
    default -> { }
}
// to (TicketCancelled case removed - DeliveryEventListener now owns that trigger):
switch (event.eventType()) {
    case "TicketCreated", "TicketCreationFailed" ->
            sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
    case "TicketQuantityRevised" ->
            authorizationReviseService.reviseForChoreography(event.eventId(), event.orderId(), event.totalQuantity());
    default -> { }
}
```

If, after this change, `authorizationCancelService` is no longer referenced anywhere in `KitchenEventListener`, remove its field/constructor parameter and update `KitchenEventListenerTest`'s constructor calls accordingly — check this before finishing the step.

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew :ftgo-accounting-service:test --tests "DeliveryEventListenerTest,KitchenEventListenerTest"`
Expected: PASS

- [ ] **Step 7: Run the full accounting-service test suite**

Run: `./gradlew :ftgo-accounting-service:test`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/DeliveryEvent.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/DeliveryEventListener.java \
        ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListener.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/DeliveryEventListenerTest.java \
        ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/KitchenEventListenerTest.java
git commit -m "feat: retarget Cancel Order's reversal trigger to DeliveryCancelled"
```

---

### Task 12: `kitchen-service` — compensate ticket on `DeliverySchedulingFailed`

**Files:**
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/DeliveryEvent.java`
- Create: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/DeliveryEventListener.java`
- Test: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/DeliveryEventListenerTest.java`

**Interfaces:**
- Consumes: `TicketService.handleConsumerVerificationFailed(String, Long)` (Task 5's spec analysis showed this is exactly the right method to reuse — it already does "cancel ticket if it exists, else record FailedOrder," precisely the compensation semantics needed here).

- [ ] **Step 1: Write the failing test**

```java
// ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/DeliveryEventListenerTest.java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final TicketService ticketService = mock(TicketService.class);
    private final DeliveryEventListener listener = new DeliveryEventListener(ticketService, new ObjectMapper());

    @Test
    void deliverySchedulingFailedCancelsTicket() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliverySchedulingFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleConsumerVerificationFailed("evt-1", 42L);
    }

    @Test
    void ignoresDeliveryScheduled() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(ticketService);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-kitchen-service:test --tests DeliveryEventListenerTest`
Expected: FAIL (compile error — `DeliveryEvent`/`DeliveryEventListener` don't exist yet)

- [ ] **Step 3: Write `DeliveryEvent`**

```java
// ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/DeliveryEvent.java
package com.sanjay.ftgo.kitchen.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId) {
}
```

- [ ] **Step 4: Write `DeliveryEventListener`**

Reusing `TicketService.handleConsumerVerificationFailed` here is a deliberate name reuse, not a copy-paste — its existing behavior ("cancel the ticket if one exists, otherwise record `FailedOrder` for the race where this arrives before `OrderCreated`") is exactly what a `DeliverySchedulingFailed` compensation needs too. No new `TicketService` method is required.

```java
// ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/DeliveryEventListener.java
package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.DeliveryEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class DeliveryEventListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public DeliveryEventListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.events", groupId = "kitchen-service")
    public void onMessage(String payload) {
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(payload, DeliveryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery event: {}", payload, e);
            return;
        }
        if ("DeliverySchedulingFailed".equals(event.eventType())) {
            ticketService.handleConsumerVerificationFailed(event.eventId(), event.orderId());
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-kitchen-service:test --tests DeliveryEventListenerTest`
Expected: PASS

- [ ] **Step 6: Run the full kitchen-service test suite**

Run: `./gradlew :ftgo-kitchen-service:test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/DeliveryEvent.java \
        ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/DeliveryEventListener.java \
        ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/DeliveryEventListenerTest.java
git commit -m "feat: cancel ticket on DeliverySchedulingFailed (choreography compensation)"
```

---

### Task 13: `order-service` — wire records, `CreateOrderSagaInstance` 3rd leg, choreography reject-on-delivery-failure

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryCommand.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryEvent.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstance.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/DeliveryEventListener.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstanceTest.java` (extend if it exists, else create — check first)
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/DeliveryEventListenerTest.java`

**Interfaces:**
- Consumes: `OrderSagaService.reject(Long, String)` (existing).
- Produces: `CreateOrderSagaInstance.markDeliveryScheduled()`/`isDeliveryScheduled()` — consumed by Task 14's `CreateOrderSagaOrchestrator` changes. `DeliveryCommand`/`DeliveryEvent` wire records — consumed by Tasks 14 and 16.

- [ ] **Step 1: Write `DeliveryCommand`/`DeliveryEvent`** (order-service's own copies — same field shape as delivery-service's, per this codebase's per-service wire-record convention)

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryCommand.java
package com.sanjay.ftgo.order.domain;

public record DeliveryCommand(String eventId, String commandType, Long orderId, Long restaurantId, String sagaType) {
}
```

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryEvent.java
package com.sanjay.ftgo.order.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId, String reason) {
}
```

- [ ] **Step 2: Write the failing test for `CreateOrderSagaInstance`'s new leg**

```java
// Add to ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstanceTest.java
// (read the existing file first if present, to match its style; if it doesn't exist, create it
// fresh with just this one test plus the constructor/getter coverage the existing 2-leg fields
// already implicitly have via CreateOrderSagaOrchestratorTest)

@Test
void marksDeliveryScheduled() {
    CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 3);

    instance.markDeliveryScheduled();

    assertThat(instance.isDeliveryScheduled()).isTrue();
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests CreateOrderSagaInstanceTest`
Expected: FAIL (compile error — `markDeliveryScheduled`/`isDeliveryScheduled` don't exist yet)

- [ ] **Step 4: Extend `CreateOrderSagaInstance`**

```java
// Add to ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstance.java,
// alongside the existing consumerVerified/ticketCreated fields:

private boolean deliveryScheduled;

public boolean isDeliveryScheduled() {
    return deliveryScheduled;
}

public void markDeliveryScheduled() {
    this.deliveryScheduled = true;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests CreateOrderSagaInstanceTest`
Expected: PASS

- [ ] **Step 6: Write the failing test for the new choreography listener**

```java
// ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/DeliveryEventListenerTest.java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final OrderSagaService orderSagaService = mock(OrderSagaService.class);
    private final DeliveryEventListener listener = new DeliveryEventListener(orderSagaService, new ObjectMapper());

    @Test
    void deliverySchedulingFailedRejectsOrder() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliverySchedulingFailed","orderId":42,"reason":"no courier available"}
                """;

        listener.onMessage(payload);

        verify(orderSagaService).reject(42L, "evt-1");
    }

    @Test
    void ignoresDeliveryScheduled() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(orderSagaService);
    }
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests DeliveryEventListenerTest`
Expected: FAIL (compile error — `DeliveryEventListener` doesn't exist yet)

- [ ] **Step 8: Write `DeliveryEventListener`** (order-service only needs to react to the failure case — `DeliveryScheduled` doesn't move `Order` at all, matching how order-service's existing `KitchenEventListener` only reacts to `TicketCreationFailed`, not `TicketCreated`, for rejection purposes)

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/DeliveryEventListener.java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.DeliveryEvent;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class DeliveryEventListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventListener.class);

    private final OrderSagaService orderSagaService;
    private final ObjectMapper objectMapper;

    public DeliveryEventListener(OrderSagaService orderSagaService, ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.events", groupId = "order-service")
    public void onMessage(String payload) {
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(payload, DeliveryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery event: {}", payload, e);
            return;
        }
        if ("DeliverySchedulingFailed".equals(event.eventType())) {
            orderSagaService.reject(event.orderId(), event.eventId());
        }
    }
}
```

- [ ] **Step 9: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests DeliveryEventListenerTest`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryCommand.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/DeliveryEvent.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstance.java \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/DeliveryEventListener.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaInstanceTest.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/DeliveryEventListenerTest.java
git commit -m "feat: order-service wire records, 3rd join leg, and choreography reject-on-delivery-failure"
```

---

### Task 14: `order-service` — `CreateOrderSagaOrchestrator` becomes a 3-way join

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java` (read first, extend to match existing structure)

**Interfaces:**
- Consumes: `CreateOrderSagaInstance.markDeliveryScheduled()`/`isDeliveryScheduled()` (Task 13), `DeliveryCommand` (Task 13), `SagaCommandPublisher.publish` (existing).

- [ ] **Step 1: Read the existing orchestrator test file to match its structure**

Run: `cat ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java` and note its mock setup for `sagaCommandPublisher`/`orderTransitions`/`sagaInstanceRepository` before writing new/updated tests.

- [ ] **Step 2: Write the failing tests** — add/update these cases (adapt exact mock field names to the real file):

```java
// New/updated test methods for CreateOrderSagaOrchestratorTest.java

@Test
void startSendsThreeParallelCommands() {
    Order order = /* build a 1-line-item order, id 42, restaurantId 7, per existing test fixtures */;

    orchestrator.start(order);

    verify(sagaCommandPublisher).publish(eq("consumer.commands"), any(), eq("VerifyConsumerCommand"), eq(42L), any());
    verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CreateTicket"), eq(42L), any());
    verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ScheduleDelivery"), eq(42L), any());
}

@Test
void authorizesOnlyAfterAllThreeReplies() {
    Order order = /* same fixture */;
    orchestrator.start(order);
    when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instanceWith(42L, false, false, false)));

    orchestrator.handleReply("evt-a", "consumer", "ConsumerVerified", 42L, null);
    orchestrator.handleReply("evt-b", "kitchen", "TicketCreated", 42L, null);
    // Not yet - delivery leg still missing
    verify(sagaCommandPublisher, never()).publish(eq("accounting.commands"), any(), any(), any(), any());

    orchestrator.handleReply("evt-c", "delivery", "DeliveryScheduled", 42L, null);
    verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("AuthorizeCard"), eq(42L), any());
}

@Test
void deliverySchedulingFailedCompensatesTicketIfCreated() {
    // Arrange an instance where ticketCreated=true, deliveryScheduled=false, failed=false
    when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instanceWith(42L, true, true, false)));

    orchestrator.handleReply("evt-d", "delivery", "DeliverySchedulingFailed", 42L, "no courier available");

    verify(orderTransitions).reject(eq(42L), any());
    verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
}

@Test
void ticketCreationFailedCompensatesDeliveryIfScheduled() {
    when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instanceWith(42L, true, false, true)));

    orchestrator.handleReply("evt-e", "kitchen", "TicketCreationFailed", 42L, "order exceeds kitchen capacity");

    verify(orderTransitions).reject(eq(42L), any());
    verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L), any());
}

@Test
void accountingDeclineCompensatesBothTicketAndDelivery() {
    when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instanceWith(42L, true, true, true)));

    orchestrator.handleReply("evt-f", "accounting", "CardAuthorizationFailed", 42L, "order quantity exceeds authorization limit");

    verify(orderTransitions).reject(eq(42L), any());
    verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
    verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L), any());
}
```

Add a small private test helper `instanceWith(Long orderId, boolean ticketCreated, boolean consumerVerified, boolean deliveryScheduled)` that constructs a `CreateOrderSagaInstance` and calls the corresponding `mark*` methods, matching whatever helper style the existing test file already uses (or add this one if none exists).

- [ ] **Step 3: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-service:test --tests CreateOrderSagaOrchestratorTest`
Expected: FAIL (compile errors / assertion failures — `delivery.commands` never sent, 3-way join not implemented)

- [ ] **Step 4: Rewrite `CreateOrderSagaOrchestrator`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class CreateOrderSagaOrchestrator {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository;
    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public CreateOrderSagaOrchestrator(CreateOrderSagaInstanceRepository sagaInstanceRepository,
                                        OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
    }

    @Transactional
    public void start(Order order) {
        int totalQuantity = totalQuantity(order.getLineItems());
        sagaInstanceRepository.save(new CreateOrderSagaInstance(order.getId(), totalQuantity));

        String verifyEventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("consumer.commands", verifyEventId, "VerifyConsumerCommand", order.getId(),
                new VerifyConsumerCommand(verifyEventId, order.getId(), order.getConsumerId()));

        String createTicketEventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", createTicketEventId, "CreateTicket", order.getId(),
                new KitchenCommand(createTicketEventId, "CreateTicket", order.getId(), totalQuantity, "CreateOrder"));

        String scheduleDeliveryEventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("delivery.commands", scheduleDeliveryEventId, "ScheduleDelivery", order.getId(),
                new DeliveryCommand(scheduleDeliveryEventId, "ScheduleDelivery", order.getId(), order.getRestaurantId(), "CreateOrder"));
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
            // Late-arriving success replies after this instance already failed still need their
            // own compensation, since fail() only compensates whichever legs it already knew
            // about at the moment it ran.
            if ("kitchen".equals(participant) && "TicketCreated".equals(eventType)) {
                sendCancelTicket(orderId);
            } else if ("delivery".equals(participant) && "DeliveryScheduled".equals(eventType)) {
                sendReleaseDelivery(orderId);
            }
            return;
        }

        switch (participant) {
            case "consumer" -> handleConsumerReply(instance, eventType);
            case "kitchen" -> handleKitchenReply(instance, eventType);
            case "delivery" -> handleDeliveryReply(instance, eventType);
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

    private void handleDeliveryReply(CreateOrderSagaInstance instance, String eventType) {
        if ("DeliverySchedulingFailed".equals(eventType)) {
            fail(instance);
            return;
        }
        instance.markDeliveryScheduled();
        sagaInstanceRepository.save(instance);
        tryAuthorize(instance);
    }

    private void handleAccountingReply(CreateOrderSagaInstance instance, String eventType) {
        Long orderId = instance.getOrderId();
        if ("CardAuthorized".equals(eventType)) {
            orderTransitions.approve(orderId, UUID.randomUUID().toString());
            String eventId = UUID.randomUUID().toString();
            sagaCommandPublisher.publish("kitchen.commands", eventId, "ConfirmTicket", orderId,
                    new KitchenCommand(eventId, "ConfirmTicket", orderId, null, "CreateOrder"));
        } else {
            orderTransitions.reject(orderId, UUID.randomUUID().toString());
            sendCancelTicket(orderId);
            sendReleaseDelivery(orderId);
        }
    }

    private void tryAuthorize(CreateOrderSagaInstance instance) {
        if (!instance.isConsumerVerified() || !instance.isTicketCreated() || !instance.isDeliveryScheduled()) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("accounting.commands", eventId, "AuthorizeCard", instance.getOrderId(),
                new AccountingCommand(eventId, "AuthorizeCard", instance.getOrderId(), instance.getTotalQuantity(), "CreateOrder"));
    }

    private void fail(CreateOrderSagaInstance instance) {
        instance.markFailed();
        sagaInstanceRepository.save(instance);

        orderTransitions.reject(instance.getOrderId(), UUID.randomUUID().toString());

        if (instance.isTicketCreated()) {
            sendCancelTicket(instance.getOrderId());
        }
        if (instance.isDeliveryScheduled()) {
            sendReleaseDelivery(instance.getOrderId());
        }
    }

    private void sendCancelTicket(Long orderId) {
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", eventId, "CancelTicket", orderId,
                new KitchenCommand(eventId, "CancelTicket", orderId, null, "CreateOrder"));
    }

    private void sendReleaseDelivery(Long orderId) {
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("delivery.commands", eventId, "ReleaseDelivery", orderId,
                new DeliveryCommand(eventId, "ReleaseDelivery", orderId, null, "CreateOrder"));
    }

    private int totalQuantity(List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }
}
```

Note the one behavior refinement beyond a literal 2-leg-to-3-leg generalization: the original `handleReply`'s `instance.isFailed()` branch only re-checked for a late `"TicketCreated"` reply. This rewrite adds the symmetric `"delivery"`/`"DeliveryScheduled"` case, since a late-arriving successful delivery schedule after the instance already failed needs releasing too, exactly like a late-arriving ticket needs cancelling. This is a direct consequence of adding a 3rd leg, not a new design decision — the existing 2-leg code already had this exact pattern for kitchen; delivery gets the same treatment for the same reason.

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests CreateOrderSagaOrchestratorTest`
Expected: PASS (all existing + new tests)

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java
git commit -m "feat: extend CreateOrderSagaOrchestrator to a 3-way parallel join with delivery"
```

---

### Task 15: `order-service` — `CancelOrderSagaOrchestrator` inserts the delivery-release step

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestrator.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java` (read first, extend to match existing structure)

**Interfaces:**
- Consumes: `DeliveryCommand` (Task 13), `SagaCommandPublisher.publish` (existing).

- [ ] **Step 1: Read the existing orchestrator test file to match its structure**

Run: `cat ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java`

- [ ] **Step 2: Write the failing tests** — add/update these cases:

```java
// New/updated test methods for CancelOrderSagaOrchestratorTest.java

@Test
void kitchenConfirmedCancellableSendsReleaseDeliveryNotReverseAuthorization() {
    orchestrator.handleReply("evt-1", "kitchen", "TicketCancelled", 42L, null);

    verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L), any());
    verify(sagaCommandPublisher, never()).publish(eq("accounting.commands"), any(), any(), any(), any());
}

@Test
void deliveryReleasedSendsReverseAuthorization() {
    orchestrator.handleReply("evt-2", "delivery", "DeliveryCancelled", 42L, null);

    verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("ReverseAuthorization"), eq(42L), any());
}

@Test
void accountingReversedMarksOrderCancelled() {
    orchestrator.handleReply("evt-3", "accounting", "AuthorizationReversed", 42L, null);

    verify(orderTransitions).noteCancelled(eq(42L), any());
}
```

Remove/update whatever existing test asserted the old direct `"kitchen" -> "TicketCancelled"` → immediate `ReverseAuthorization` send, since that intermediate hop no longer exists.

- [ ] **Step 3: Run tests to verify they fail**

Run: `./gradlew :ftgo-order-service:test --tests CancelOrderSagaOrchestratorTest`
Expected: FAIL

- [ ] **Step 4: Rewrite `CancelOrderSagaOrchestrator`**

```java
// ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestrator.java
package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// Deliberately stateless, unlike CreateOrderSagaOrchestrator: Cancel Order is a strict
// linear pipeline (kitchen cancel -> delivery release -> accounting reversal -> order
// cancelled) with no parallel replies to join, so there's no need for a persisted saga
// instance table.
@Service
public class CancelOrderSagaOrchestrator {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public CancelOrderSagaOrchestrator(OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
    }

    @Transactional
    public void start(Order order) {
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", eventId, "CancelTicket", order.getId(),
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
            case "delivery" -> handleDeliveryReply(eventType, orderId);
            case "accounting" -> handleAccountingReply(eventType, orderId);
            default -> { }
        }
    }

    private void handleKitchenReply(String eventType, Long orderId) {
        if ("TicketCancellationRejected".equals(eventType)) {
            orderTransitions.undoCancel(orderId, UUID.randomUUID().toString());
            return;
        }
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("delivery.commands", eventId, "ReleaseDelivery", orderId,
                new DeliveryCommand(eventId, "ReleaseDelivery", orderId, null, "CancelOrder"));
    }

    private void handleDeliveryReply(String eventType, Long orderId) {
        if (!"DeliveryCancelled".equals(eventType)) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("accounting.commands", eventId, "ReverseAuthorization", orderId,
                new AccountingCommand(eventId, "ReverseAuthorization", orderId, null, "CancelOrder"));
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        if ("AuthorizationReversed".equals(eventType)) {
            orderTransitions.noteCancelled(orderId, UUID.randomUUID().toString());
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew :ftgo-order-service:test --tests CancelOrderSagaOrchestratorTest`
Expected: PASS

- [ ] **Step 6: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (this exercises every touched file across Tasks 13–15, plus regression coverage for `ReviseOrderSagaOrchestrator`/`OrderReviseSagaService`, which this plan does not touch)

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestrator.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java
git commit -m "feat: insert delivery-release step into Cancel Order saga orchestration"
```

---

### Task 16: Full workspace build check

**Files:** none (verification-only task)

**Interfaces:** none

- [ ] **Step 1: Build every module**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL across all 8 modules (`ftgo-common`, `ftgo-consumer-service`, `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-accounting-service`, `ftgo-restaurant-service`, `ftgo-delivery-service`, `ftgo-service-registry`)

- [ ] **Step 2: Build every Docker image**

Run: `docker compose build`
Expected: all images build successfully, including the newly-wired `delivery-service`

- [ ] **Step 3: If anything fails, fix it now** — this task exists specifically to catch cross-module regressions (e.g. a forgotten import, an accidentally-broken shared test fixture) before manual e2e verification. Do not proceed to Task 17 with a red build.

(No commit for this task unless Step 3 required a fix — if it did, commit that fix separately with a `fix:` message describing exactly what broke.)

---

### Task 17: Docs — `ftgo-delivery-service/README.md`, per-service updates, `ARCHITECTURE.md`, `CONTEXT.md`

**Files:**
- Modify: `ftgo-delivery-service/README.md` (full rewrite from stub)
- Modify: `ftgo-order-service/README.md`
- Modify: `ftgo-accounting-service/README.md`
- Modify: `ftgo-kitchen-service/README.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `CONTEXT.md`

**Interfaces:** none — documentation only, per this project's `CLAUDE.md` "documentation updates land in the same change as the code they describe" rule and the spec's own Docs section.

- [ ] **Step 1: Read each file's current content before editing**

Run: `cat ftgo-delivery-service/README.md ftgo-order-service/README.md ftgo-accounting-service/README.md ftgo-kitchen-service/README.md docs/ARCHITECTURE.md` — match each file's existing structure, heading levels, and tone exactly (this plan cannot dictate their exact prose without first seeing the live content, especially `docs/ARCHITECTURE.md`'s saga sequence-diagram sections, which must gain the delivery participant without disrupting existing content).

- [ ] **Step 2: Rewrite `ftgo-delivery-service/README.md`**

Full rewrite matching `ftgo-kitchen-service/README.md`'s depth and section structure (Role / API / Events published+consumed / Domain model / Idempotency & reliability / Running standalone). Cover:
- Role: schedules and tracks courier deliveries; Create Order saga's 3rd parallel-join leg; Cancel Order saga's delivery-release step.
- API: `POST /deliveries/{id}/picked-up`, `POST /deliveries/{id}/delivered`.
- Events published (`delivery.events`, choreography): `DeliveryScheduled`, `DeliverySchedulingFailed`, `DeliveryPickedUp`, `DeliveryDelivered`, `DeliveryCancelled`.
- Events published (`saga.replies`, orchestration): same 5 outcomes as replies.
- Events consumed: `order.events` (`OrderCreated`), `consumer.events` (`ConsumerVerificationFailed`), `kitchen.events` (`TicketCreationFailed`, `TicketCancelled`), `accounting.events` (`CardAuthorizationFailed`); `delivery.commands` (`ScheduleDelivery`, `ReleaseDelivery`) in orchestration mode.
- Domain model: `DeliveryStatus` (`SCHEDULED → PICKED_UP → DELIVERED`, or `CANCELLED` from `SCHEDULED`), `Courier` (seeded pool of 3, `available` flag).
- Idempotency & reliability: `processed_events` dedup, `FailedOrder` race-handling (link to the exact race this codebase already solved for kitchen's `Ticket`).
- Running standalone: port 8086, `SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `SAGA_MODE`.

- [ ] **Step 3: Update `ftgo-order-service/README.md`**

Update the "Saga participants" section to describe the Create Order saga as a 3-way parallel join (consumer/kitchen/delivery → accounting) and the Cancel Order saga as kitchen → delivery-release → accounting-reversal. Update whichever "Events consumed"/"Events published" tables list `delivery.commands`/`delivery.events` as new entries.

- [ ] **Step 4: Update `ftgo-accounting-service/README.md`**

Update "The join (Create Order, choreography only)" section to describe the 3-leg join (`consumerVerified`/`ticketCreated`/`deliveryScheduled`). Update the Cancel Order description to note the reversal trigger is now `DeliveryCancelled` (`delivery.events`), not `TicketCancelled` (`kitchen.events`).

- [ ] **Step 5: Update `ftgo-kitchen-service/README.md`**

Note the new `delivery.events` consumption (`DeliverySchedulingFailed` → ticket cancellation, reusing the existing `ConsumerVerificationFailed` compensation path).

- [ ] **Step 6: Update `docs/ARCHITECTURE.md`**

Add `delivery.events`/`delivery.commands` to the Kafka topic catalog table. Update the Create Order saga's sequence diagrams (both choreography and orchestration) to show delivery as a 3rd parallel participant. Update the Cancel Order saga's sequence diagrams (both modes) to show the kitchen → delivery-release → accounting-reversal chain.

- [ ] **Step 7: Update `CONTEXT.md`**

Update the "Services to build" table's `ftgo-delivery-service` row (from "Ready to scaffold" to a full description matching the other services' entries). Add a session log entry summarizing this sub-project (mirroring the existing session log's style and level of detail — see the 2026-07-21 Cancel Order saga entry for the right depth). Do NOT mark Ch.7 as "Done" in the progress table — this sub-project is prerequisite work only; sub-projects 2–3 (API composition, CQRS) are still pending.

- [ ] **Step 8: Commit**

```bash
git add ftgo-delivery-service/README.md ftgo-order-service/README.md ftgo-accounting-service/README.md \
        ftgo-kitchen-service/README.md docs/ARCHITECTURE.md CONTEXT.md
git commit -m "docs: document Delivery aggregate and saga participation across all touched services"
```

---

### Task 18: Manual Docker e2e verification

**Files:** none

**Interfaces:** none

This task is manual verification, not automated testing — follow the spec's own "Testing" section scenario list. Perform each scenario in **both** `SAGA_MODE=choreography` (default) and `SAGA_MODE=orchestration`.

- [ ] **Step 1: Bring up the full stack**

Run: `docker compose up --build -d` (choreography mode — default env vars)
Verify: all containers reach a running/healthy state (`docker compose ps`).

- [ ] **Step 2: Create Order happy path**

`POST /orders` with a small line-item total (≤10, within both kitchen's 20-item and accounting's 10-item limits). Verify via each service's DB or logs: `Order.APPROVED`, `Ticket.AWAITING_ACCEPTANCE`, `Authorization.AUTHORIZED`, `Delivery.SCHEDULED` with a `courierId` set, and that `Courier.available` flipped to `false` for the assigned courier.

- [ ] **Step 3: No-courier-available decline**

Manually exhaust all 3 seeded couriers (create 3 orders that each successfully schedule a delivery), then place a 4th order. Verify: `DeliverySchedulingFailed` fires, `Order.REJECTED`, `Ticket.CANCELLED` (compensated), and no `Authorization` row is ever created for that order (accounting's join never resolves — it received `DeliverySchedulingFailed` and marked the join failed before the other legs could authorize).

- [ ] **Step 4: Re-verify the 3 pre-existing decline paths with delivery now in the mix**

- Consumer inactive (use consumerId 2): verify `Ticket.CANCELLED` **and** the assigned courier's `available` flips back to `true`.
- Kitchen capacity exceeded (line-item total > 20): verify the assigned courier's `available` flips back to `true` (delivery already scheduled by the time kitchen declines, since both are parallel legs).
- Accounting declines (line-item total between 11–20): verify **both** `Ticket.CANCELLED` and the courier released.

- [ ] **Step 5: Cancel Order releasing the courier end-to-end**

Create and approve an order (courier assigned, `available=false`), then `POST /orders/{id}/cancel`. Verify: `Ticket.CANCELLED`, `Delivery.CANCELLED`, courier's `available` flips back to `true`, and `Authorization.REVERSED` — confirm via logs/DB that the reversal happened only *after* the delivery release (not concurrently), proving the new kitchen → delivery → accounting sequencing.

- [ ] **Step 6: Redelivery/idempotency**

Restart `delivery-service` (`docker compose restart delivery-service`) mid-flow or force a Kafka redelivery per this project's established method (see prior session log entries for the exact technique used, e.g. resetting `sent_at` on an outbox row); confirm `processed_events` and `Courier.available` counts are unchanged after redelivery — no double-release, no double-schedule.

- [ ] **Step 7: Repeat Steps 2–6 with `SAGA_MODE=orchestration`**

Run: `SAGA_MODE=orchestration docker compose up --build -d` and repeat every scenario above. Pay particular attention to Step 5 (Cancel Order): confirm via `order-service` logs that `ReverseAuthorization` is only sent to `accounting.commands` *after* `DeliveryCancelled` is received on `saga.replies`, not immediately after `TicketCancelled`.

- [ ] **Step 8: Tear down**

Run: `docker compose down -v` (only if the user's local environment doesn't need the data preserved — check before running `-v`, which deletes the `mysql-data` volume).

No commit for this task — it's verification only. If any scenario surfaces a bug, fix it in a new commit with a `fix:` message describing the bug and root cause, following this project's established convention (see the Ch.6 session log's `replayable` flag fix for the right level of detail in a fix commit message), then re-run the affected scenario before continuing.

---

## Deferred (not in this plan)

- **Sub-project 2**: API composition — `GET /orders/{id}/view` composing Order/Ticket/Authorization/Restaurant/Delivery via synchronous REST calls. Separate future brainstorm → spec → plan cycle.
- **Sub-project 3**: CQRS read model — a dedicated read-side service/table fed by Kafka events from all five services. Separate future brainstorm → spec → plan cycle.
- Any courier-facing UI/notification layer, geo/routing/ETA logic, or delivery-time estimation.
