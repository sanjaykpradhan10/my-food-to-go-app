# Shared `ftgo-common` Outbox Module — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the byte-for-byte-duplicated transactional-outbox/idempotent-receiver infrastructure (`OutboxEvent`, `ProcessedEvent`, their repositories, `OutboxPublisher`, `KafkaProducerConfig`) out of `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-accounting-service` into one new Gradle module, `ftgo-common`, with zero behavior change.

**Architecture:** New module `ftgo-common` under package `com.sanjay.ftgo.common.outbox`, built as a plain library (not an executable Spring Boot jar) via `bootJar { enabled = false }` / `jar { enabled = true }`, exposing JPA + Kafka types via the `api` configuration (requires applying `java-library` in its own `build.gradle`, layered on top of the `java` plugin every subproject already gets). Each of the four saga services depends on it via `implementation project(':ftgo-common')`, deletes its own local copies of the six files, updates imports, and adds `@EntityScan`/`@EnableJpaRepositories` listing both its own domain package and the shared package (Spring Boot only autoscans the application class's own package once `basePackages` is set explicitly on either annotation).

**Tech Stack:** Java 21, Spring Boot 3.5.3, Spring Data JPA, Spring Kafka, Gradle multi-module (matches every other module in this repo — no new tooling).

## Global Constraints

- Zero behavior change: every saga scenario (choreography + orchestration, happy path + all compensation cases) must reach identical `Order`/`Ticket`/`Authorization` end states after this refactor, verified via Docker per the spec's testing section.
- Infra only — do not touch the saga wire-format records (`SagaReply`, `OrderCreatedEvent`, `ConsumerVerificationEvent`, `KitchenEvent`, `AccountingEvent`, `VerifyConsumerCommand`, `KitchenCommand`, `AuthorizeCardCommand`). They stay per-service.
- Do not touch `ftgo-restaurant-service`, `ftgo-service-registry`, `ftgo-delivery-service` — none use the outbox pattern.
- No schema changes — `outbox_events`/`processed_events` table/column names are unchanged.
- Shared package is `com.sanjay.ftgo.common.outbox`, outside every service's own base package (`com.sanjay.ftgo.order`, `.kitchen`, `.consumer`, `.accounting`).
- `OutboxPublisher`'s `@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)` gate must be preserved verbatim in the shared class.
- Documentation (`CONTEXT.md`, `README.md`, per-service `docs/` READMEs if they reference the duplicated-infra note) must be updated in the same change, per `CLAUDE.md`.

---

## File Structure

**Create:**
- `ftgo-common/build.gradle`
- `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxEvent.java`
- `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxEventRepository.java`
- `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/ProcessedEvent.java`
- `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/ProcessedEventRepository.java`
- `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxPublisher.java`
- `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/KafkaProducerConfig.java`
- `ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxPublisherTest.java`
- `ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxPublisherConditionalTest.java`

**Modify:**
- `settings.gradle` — add `include 'ftgo-common'`
- Each of `ftgo-{order,kitchen,consumer,accounting}-service/build.gradle` — add `implementation project(':ftgo-common')`
- Each of the four services' `@SpringBootApplication` class — add `@EntityScan`/`@EnableJpaRepositories`
- Each of the six files identified below that reference the moved types by simple name (same-package references that need a new `import` line) or by the old fully-qualified package
- `CONTEXT.md`, `README.md` — architecture decisions section, per `CLAUDE.md`'s doc-sync rule

**Delete:**
- The six duplicated files × 4 services (24 files) from `src/main`
- The `OutboxPublisherTest`/`OutboxPublisherConditionalTest` duplicates × 4 services (5 files: order has both, kitchen/consumer/accounting have one each)

---

## Task 1: Create the `ftgo-common` module skeleton and build wiring

**Files:**
- Create: `ftgo-common/build.gradle`
- Modify: `settings.gradle`

**Interfaces:**
- Produces: a Gradle module `:ftgo-common` that other modules can depend on via `implementation project(':ftgo-common')`, exposing `spring-boot-starter-data-jpa` and `spring-kafka` transitively via `api`.

- [ ] **Step 1: Add the module to `settings.gradle`**

Edit `settings.gradle` to read:

```groovy
rootProject.name = 'my-food-to-go-app'

include 'ftgo-common'
include 'ftgo-consumer-service'
include 'ftgo-order-service'
include 'ftgo-kitchen-service'
include 'ftgo-accounting-service'
include 'ftgo-restaurant-service'
include 'ftgo-delivery-service'
include 'ftgo-service-registry'
```

- [ ] **Step 2: Create `ftgo-common/build.gradle`**

The root `build.gradle`'s `subprojects {}` block applies `org.springframework.boot` to every module (including this one), which normally produces an executable boot jar and disables the plain `jar` task. This module needs the opposite: a plain library jar other modules can compile against. Also apply `java-library` so the `api` configuration exists (plain `java`, which `subprojects{}` already applies, only has `implementation`/`compileOnly`, neither of which is exposed to consumers at compile time — and consumers need `OutboxEventRepository extends JpaRepository<...>` and `ProcessedEventRepository extends JpaRepository<...>`'s inherited methods, e.g. `.save(...)`, to resolve, which requires `JpaRepository` itself on their compile classpath transitively).

```groovy
apply plugin: 'java-library'

bootJar {
    enabled = false
}

jar {
    enabled = true
}

dependencies {
    api 'org.springframework.boot:spring-boot-starter-data-jpa'
    api 'org.springframework.kafka:spring-kafka'
}
```

- [ ] **Step 3: Verify the module builds (empty, no source yet)**

Run: `./gradlew :ftgo-common:build`
Expected: `BUILD SUCCESSFUL` (no source files yet, so effectively a no-op compile).

- [ ] **Step 4: Commit**

```bash
git add settings.gradle ftgo-common/build.gradle
git commit -m "build: scaffold ftgo-common module (empty)"
```

---

## Task 2: Move `OutboxEvent`/`OutboxEventRepository` into `ftgo-common`

**Files:**
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxEvent.java`
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxEventRepository.java`
- Delete (later tasks, once all four services are migrated): the four per-service copies

**Interfaces:**
- Produces: `com.sanjay.ftgo.common.outbox.OutboxEvent` (JPA entity, table `outbox_events`) with constructor `OutboxEvent(String eventId, String eventType, Long orderId, String topic, String payload)` and accessors `getId()`, `getEventId()`, `getEventType()`, `getOrderId()`, `getTopic()`, `getPayload()`, `getSentAt()`, `isSent()`, `markSent()`.
- Produces: `com.sanjay.ftgo.common.outbox.OutboxEventRepository extends JpaRepository<OutboxEvent, Long>` with `List<OutboxEvent> findBySentAtIsNullOrderByIdAsc()`.

- [ ] **Step 1: Create `OutboxEvent.java`** (verbatim copy of `ftgo-order-service`'s version, package changed)

```java
package com.sanjay.ftgo.common.outbox;

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

- [ ] **Step 2: Create `OutboxEventRepository.java`**

```java
package com.sanjay.ftgo.common.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findBySentAtIsNullOrderByIdAsc();
}
```

- [ ] **Step 3: Verify it compiles**

Run: `./gradlew :ftgo-common:compileJava`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Commit**

```bash
git add ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxEvent.java ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxEventRepository.java
git commit -m "feat(ftgo-common): add shared OutboxEvent entity and repository"
```

---

## Task 3: Move `ProcessedEvent`/`ProcessedEventRepository` into `ftgo-common`

**Files:**
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/ProcessedEvent.java`
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/ProcessedEventRepository.java`

**Interfaces:**
- Produces: `com.sanjay.ftgo.common.outbox.ProcessedEvent` (JPA entity, table `processed_events`) with constructor `ProcessedEvent(String eventId)` and accessors `getEventId()`, `getProcessedAt()`.
- Produces: `com.sanjay.ftgo.common.outbox.ProcessedEventRepository extends JpaRepository<ProcessedEvent, String>`.

- [ ] **Step 1: Create `ProcessedEvent.java`**

```java
package com.sanjay.ftgo.common.outbox;

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

- [ ] **Step 2: Create `ProcessedEventRepository.java`**

```java
package com.sanjay.ftgo.common.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
```

- [ ] **Step 3: Verify it compiles**

Run: `./gradlew :ftgo-common:compileJava`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Commit**

```bash
git add ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/ProcessedEvent.java ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/ProcessedEventRepository.java
git commit -m "feat(ftgo-common): add shared ProcessedEvent entity and repository"
```

---

## Task 4: Move `KafkaProducerConfig` and `OutboxPublisher` into `ftgo-common`, with tests

**Files:**
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/KafkaProducerConfig.java`
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxPublisher.java`
- Create: `ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxPublisherTest.java`
- Create: `ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxPublisherConditionalTest.java`

**Interfaces:**
- Consumes: `OutboxEvent`, `OutboxEventRepository` (Task 2), `ProcessedEvent`/`ProcessedEventRepository` not directly used by these two classes.
- Produces: `com.sanjay.ftgo.common.outbox.KafkaProducerConfig` (`@Configuration`, beans `eventProducerFactory`/`eventKafkaTemplate` — renamed from the per-service `orderEventProducerFactory` style since the prefix was never load-bearing, see spec).
- Produces: `com.sanjay.ftgo.common.outbox.OutboxPublisher` (`@Component`, `@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)`), constructor `OutboxPublisher(OutboxEventRepository, KafkaTemplate<String, String>, int batchSize)`, method `publishPendingEvents()`.

- [ ] **Step 1: Create `KafkaProducerConfig.java`** (generic bean names)

```java
package com.sanjay.ftgo.common.outbox;

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
    public ProducerFactory<String, String> eventProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> eventKafkaTemplate(ProducerFactory<String, String> eventProducerFactory) {
        return new KafkaTemplate<>(eventProducerFactory);
    }
}
```

- [ ] **Step 2: Create `OutboxPublisher.java`**

```java
package com.sanjay.ftgo.common.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

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
                kafkaTemplate.send(event.getTopic(), String.valueOf(event.getOrderId()), event.getPayload()).get();
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }
}
```

- [ ] **Step 3: Create `OutboxPublisherTest.java`** (consolidates the four near-identical per-service copies)

```java
package com.sanjay.ftgo.common.outbox;

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
        OutboxEvent event = new OutboxEvent("event-1", "SomethingHappened", 100L, "some.topic", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("some.topic"), eq("100"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isTrue();
        verify(outboxEventRepository).save(event);
    }

    @Test
    void leavesEventUnsentWhenPublishFails() {
        OutboxEvent event = new OutboxEvent("event-2", "SomethingHappened", 200L, "some.topic", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(eq("some.topic"), eq("200"), eq("{}"))).thenReturn(failed);

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isFalse();
        verify(outboxEventRepository, never()).save(any());
    }
}
```

- [ ] **Step 4: Create `OutboxPublisherConditionalTest.java`** (moved from `ftgo-order-service`, the only service that had it)

```java
package com.sanjay.ftgo.common.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class OutboxPublisherConditionalTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withBean(PropertySourcesPlaceholderConfigurer.class)
            .withUserConfiguration(CollaboratorConfig.class, OutboxPublisher.class);

    @Test
    void beanExistsWhenPublishModeIsPolling() {
        contextRunner.withPropertyValues("outbox.publish-mode=polling")
                .run(context -> assertThat(context).hasSingleBean(OutboxPublisher.class));
    }

    @Test
    void beanExistsWhenPublishModeIsUnset() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(OutboxPublisher.class));
    }

    @Test
    void beanAbsentWhenPublishModeIsCdc() {
        contextRunner.withPropertyValues("outbox.publish-mode=cdc")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxPublisher.class));
    }

    @Configuration
    static class CollaboratorConfig {

        @Bean
        OutboxEventRepository outboxEventRepository() {
            return mock(OutboxEventRepository.class);
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }
    }
}
```

- [ ] **Step 5: Run the new tests**

Run: `./gradlew :ftgo-common:test`
Expected: `BUILD SUCCESSFUL`, 5 tests passed (2 in `OutboxPublisherTest`, 3 in `OutboxPublisherConditionalTest`).

- [ ] **Step 6: Commit**

```bash
git add ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/KafkaProducerConfig.java ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxPublisher.java ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxPublisherTest.java ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxPublisherConditionalTest.java
git commit -m "feat(ftgo-common): add shared OutboxPublisher and KafkaProducerConfig"
```

---

## Task 5: Migrate `ftgo-order-service` onto `ftgo-common`

**Files:**
- Modify: `ftgo-order-service/build.gradle`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/FtgoOrderServiceApplication.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTrigger.java`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestrator.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/ChoreographyOrderCreationSagaTriggerTest.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java`
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderSagaServiceTest.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEvent.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEventRepository.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEvent.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEventRepository.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KafkaProducerConfig.java`
- Delete: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherTest.java`
- Delete: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherConditionalTest.java`

**Interfaces:**
- Consumes: `com.sanjay.ftgo.common.outbox.{OutboxEvent, OutboxEventRepository, ProcessedEvent, ProcessedEventRepository, OutboxPublisher, KafkaProducerConfig}` (Tasks 2–4).

- [ ] **Step 1: Add the module dependency**

Edit `ftgo-order-service/build.gradle`, add inside the existing `dependencies { ... }` block:

```groovy
    implementation project(':ftgo-common')
```

- [ ] **Step 2: Add explicit component scan to the application class**

Edit `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/FtgoOrderServiceApplication.java`:

```java
package com.sanjay.ftgo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EntityScan(basePackages = {"com.sanjay.ftgo.order.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.order.domain", "com.sanjay.ftgo.common.outbox"})
public class FtgoOrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoOrderServiceApplication.class, args);
    }
}
```

- [ ] **Step 3: Delete the six duplicated files**

```bash
rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEvent.java
rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxEventRepository.java
rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEvent.java
rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/ProcessedEventRepository.java
rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java
rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/KafkaProducerConfig.java
rm ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherTest.java
rm ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherConditionalTest.java
```

- [ ] **Step 4: Add imports in the three main files that referenced the moved types by same-package (no-import) reference**

In `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderSagaService.java`, `ChoreographyOrderCreationSagaTrigger.java`, and `CreateOrderSagaOrchestrator.java`, add these two import lines after the existing `package com.sanjay.ftgo.order.domain;` line (alongside whatever imports already exist in each file — do not remove any existing import):

```java
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
```

Only add the ones each file actually references — check each file:
- `OrderSagaService.java`: add both (uses `OutboxEventRepository` and `ProcessedEventRepository`).
- `ChoreographyOrderCreationSagaTrigger.java`: add whichever of the two it references (inspect the file; add only the ones used).
- `CreateOrderSagaOrchestrator.java`: add both (uses `OutboxEventRepository` and `ProcessedEventRepository`).

- [ ] **Step 5: Add the same imports to the three test files**

In `ChoreographyOrderCreationSagaTriggerTest.java`, `CreateOrderSagaOrchestratorTest.java`, `OrderSagaServiceTest.java` (package `com.sanjay.ftgo.order.domain`), add the same two import lines, only for the types each file actually mocks/references (inspect each file — typically these tests `mock(OutboxEventRepository.class)` and/or `mock(ProcessedEventRepository.class)`).

- [ ] **Step 6: Build and run order-service's test suite**

Run: `./gradlew :ftgo-order-service:build`
Expected: `BUILD SUCCESSFUL`. If compilation fails with "cannot find symbol" for `OutboxEventRepository`/`ProcessedEventRepository`/`OutboxEvent`/`ProcessedEvent`, that file is missing one of the two import lines from Step 4/5 — add it and re-run.

- [ ] **Step 7: Commit**

```bash
git add -A ftgo-order-service settings.gradle
git commit -m "refactor(order-service): migrate onto shared ftgo-common outbox module"
```

---

## Task 6: Migrate `ftgo-kitchen-service` onto `ftgo-common`

**Files:**
- Modify: `ftgo-kitchen-service/build.gradle`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/FtgoKitchenServiceApplication.java`
- Modify: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`
- Modify: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/domain/TicketServiceTest.java`
- Delete: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEvent.java`
- Delete: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEventRepository.java`
- Delete: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEvent.java`
- Delete: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEventRepository.java`
- Delete: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisher.java`
- Delete: `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KafkaProducerConfig.java`
- Delete: `ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisherTest.java`

**Interfaces:**
- Consumes: `com.sanjay.ftgo.common.outbox.{OutboxEvent, OutboxEventRepository, ProcessedEvent, ProcessedEventRepository, OutboxPublisher, KafkaProducerConfig}` (Tasks 2–4).

- [ ] **Step 1: Add the module dependency**

Edit `ftgo-kitchen-service/build.gradle` to read:

```groovy
dependencies {
    implementation 'org.springframework.kafka:spring-kafka'
    implementation project(':ftgo-common')
}
```

- [ ] **Step 2: Add explicit component scan to the application class**

Edit `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/FtgoKitchenServiceApplication.java`:

```java
package com.sanjay.ftgo.kitchen;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EntityScan(basePackages = {"com.sanjay.ftgo.kitchen.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.kitchen.domain", "com.sanjay.ftgo.common.outbox"})
public class FtgoKitchenServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoKitchenServiceApplication.class, args);
    }
}
```

- [ ] **Step 3: Delete the six duplicated files**

```bash
rm ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEvent.java
rm ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/OutboxEventRepository.java
rm ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEvent.java
rm ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/ProcessedEventRepository.java
rm ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisher.java
rm ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KafkaProducerConfig.java
rm ftgo-kitchen-service/src/test/java/com/sanjay/ftgo/kitchen/infrastructure/OutboxPublisherTest.java
```

- [ ] **Step 4: Add imports to `TicketService.java`**

In `ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/TicketService.java`, add after `package com.sanjay.ftgo.kitchen.domain;`:

```java
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
```

(This file constructs `new OutboxEvent(...)` too — check whether it references `OutboxEvent` directly, e.g. inside `publishEvent`/`publishReply` helper methods; if so, also add `import com.sanjay.ftgo.common.outbox.OutboxEvent;`. `ProcessedEvent` is referenced the same way via `new ProcessedEvent(eventId)` — add `import com.sanjay.ftgo.common.outbox.ProcessedEvent;` too.)

- [ ] **Step 5: Add the same imports to `TicketServiceTest.java`**

Add whichever of `OutboxEvent`, `OutboxEventRepository`, `ProcessedEvent`, `ProcessedEventRepository` the test file references (mocks/constructs), same package rule as Step 4.

- [ ] **Step 6: Build and run kitchen-service's test suite**

Run: `./gradlew :ftgo-kitchen-service:build`
Expected: `BUILD SUCCESSFUL`. Same "cannot find symbol" troubleshooting as Task 5 Step 6 if imports are incomplete.

- [ ] **Step 7: Commit**

```bash
git add -A ftgo-kitchen-service
git commit -m "refactor(kitchen-service): migrate onto shared ftgo-common outbox module"
```

---

## Task 7: Migrate `ftgo-consumer-service` onto `ftgo-common`

**Files:**
- Modify: `ftgo-consumer-service/build.gradle`
- Modify: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/FtgoConsumerServiceApplication.java`
- Modify: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java`
- Modify: `ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationServiceTest.java`
- Delete: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OutboxEvent.java`
- Delete: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OutboxEventRepository.java`
- Delete: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ProcessedEvent.java`
- Delete: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ProcessedEventRepository.java`
- Delete: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisher.java`
- Delete: `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/KafkaProducerConfig.java`
- Delete: `ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisherTest.java`

**Interfaces:**
- Consumes: `com.sanjay.ftgo.common.outbox.{OutboxEvent, OutboxEventRepository, ProcessedEvent, ProcessedEventRepository, OutboxPublisher, KafkaProducerConfig}` (Tasks 2–4).

- [ ] **Step 1: Add the module dependency**

Edit `ftgo-consumer-service/build.gradle` to read:

```groovy
dependencies {
    implementation 'org.springframework.kafka:spring-kafka'
    implementation project(':ftgo-common')
}
```

- [ ] **Step 2: Add explicit component scan to the application class**

Edit `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/FtgoConsumerServiceApplication.java`:

```java
package com.sanjay.ftgo.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EntityScan(basePackages = {"com.sanjay.ftgo.consumer.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.consumer.domain", "com.sanjay.ftgo.common.outbox"})
public class FtgoConsumerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoConsumerServiceApplication.class, args);
    }
}
```

- [ ] **Step 3: Delete the six duplicated files**

```bash
rm ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OutboxEvent.java
rm ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/OutboxEventRepository.java
rm ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ProcessedEvent.java
rm ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ProcessedEventRepository.java
rm ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisher.java
rm ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/infrastructure/KafkaProducerConfig.java
rm ftgo-consumer-service/src/test/java/com/sanjay/ftgo/consumer/infrastructure/OutboxPublisherTest.java
```

- [ ] **Step 4: Add imports to `ConsumerVerificationService.java`**

In `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/domain/ConsumerVerificationService.java`, add after `package com.sanjay.ftgo.consumer.domain;`, for whichever of these four types the file references directly (it constructs `new OutboxEvent(...)` and `new ProcessedEvent(...)` in its `publishEvent`/`publishReply`/dedup-check methods, and takes `OutboxEventRepository`/`ProcessedEventRepository` as constructor params):

```java
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
```

- [ ] **Step 5: Add the same imports to `ConsumerVerificationServiceTest.java`**

Add whichever of the four types the test file references.

- [ ] **Step 6: Build and run consumer-service's test suite**

Run: `./gradlew :ftgo-consumer-service:build`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 7: Commit**

```bash
git add -A ftgo-consumer-service
git commit -m "refactor(consumer-service): migrate onto shared ftgo-common outbox module"
```

---

## Task 8: Migrate `ftgo-accounting-service` onto `ftgo-common`

**Files:**
- Modify: `ftgo-accounting-service/build.gradle`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/FtgoAccountingServiceApplication.java`
- Modify: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`
- Modify: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/domain/SagaJoinServiceTest.java`
- Delete: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/OutboxEvent.java`
- Delete: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/OutboxEventRepository.java`
- Delete: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/ProcessedEvent.java`
- Delete: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/ProcessedEventRepository.java`
- Delete: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisher.java`
- Delete: `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KafkaProducerConfig.java`
- Delete: `ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisherTest.java`

**Interfaces:**
- Consumes: `com.sanjay.ftgo.common.outbox.{OutboxEvent, OutboxEventRepository, ProcessedEvent, ProcessedEventRepository, OutboxPublisher, KafkaProducerConfig}` (Tasks 2–4).

- [ ] **Step 1: Add the module dependency**

Edit `ftgo-accounting-service/build.gradle` to read:

```groovy
dependencies {
    implementation 'org.springframework.kafka:spring-kafka'
    implementation project(':ftgo-common')
}
```

- [ ] **Step 2: Add explicit component scan to the application class**

Edit `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/FtgoAccountingServiceApplication.java`:

```java
package com.sanjay.ftgo.accounting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EntityScan(basePackages = {"com.sanjay.ftgo.accounting.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.accounting.domain", "com.sanjay.ftgo.common.outbox"})
public class FtgoAccountingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoAccountingServiceApplication.class, args);
    }
}
```

- [ ] **Step 3: Delete the six duplicated files**

```bash
rm ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/OutboxEvent.java
rm ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/OutboxEventRepository.java
rm ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/ProcessedEvent.java
rm ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/ProcessedEventRepository.java
rm ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisher.java
rm ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/infrastructure/KafkaProducerConfig.java
rm ftgo-accounting-service/src/test/java/com/sanjay/ftgo/accounting/infrastructure/OutboxPublisherTest.java
```

- [ ] **Step 4: Add imports to `SagaJoinService.java`**

In `ftgo-accounting-service/src/main/java/com/sanjay/ftgo/accounting/domain/SagaJoinService.java`, add after `package com.sanjay.ftgo.accounting.domain;`, for whichever of these four types the file references directly:

```java
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
```

- [ ] **Step 5: Add the same imports to `SagaJoinServiceTest.java`**

Add whichever of the four types the test file references.

- [ ] **Step 6: Build and run accounting-service's test suite**

Run: `./gradlew :ftgo-accounting-service:build`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 7: Commit**

```bash
git add -A ftgo-accounting-service
git commit -m "refactor(accounting-service): migrate onto shared ftgo-common outbox module"
```

---

## Task 9: Full-repo build, Docker e2e verification, and documentation sync

**Files:**
- Modify: `CONTEXT.md` (architecture decisions section — remove the now-stale "no shared common module yet" note; add the `ftgo-common` decision; add a session log entry)
- Modify: `README.md` (architecture/tech-stack section, if it references the duplication)

**Interfaces:**
- None — this task is verification + documentation, no new code interfaces.

- [ ] **Step 1: Full-repo build**

Run: `./gradlew build`
Expected: `BUILD SUCCESSFUL` across all 8 modules (`ftgo-common` + the 7 pre-existing modules).

- [ ] **Step 2: Bring up the full stack and verify choreography happy path**

```bash
docker compose up -d --build
```

Wait for all services healthy (check `docker compose ps`), then:

```bash
curl -s -X POST http://localhost:8082/orders \
  -H 'Content-Type: application/json' \
  -d '{"consumerId": 1, "restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 2}]}'
```

Expected: `201` with the order in `APPROVAL_PENDING`, then (after the outbox poll interval, ~2s) transitioning to `APPROVED` — verify via `GET http://localhost:8082/orders/{id}` or a direct MySQL query (`SELECT status FROM orders WHERE id = ...` in order-service's schema) that it reaches `APPROVED`, and that `SELECT * FROM outbox_events WHERE sent_at IS NOT NULL` shows rows published (proves the shared `OutboxPublisher` is running under its new package/module).

- [ ] **Step 3: Verify orchestration happy path**

```bash
docker compose down
SAGA_MODE=orchestration docker compose up -d --build
```

Repeat the same `POST /orders` call. Expected: same end state (`Order.APPROVED`, `Ticket.AWAITING_ACCEPTANCE`, `Authorization.AUTHORIZED`), confirming `CreateOrderSagaOrchestrator` (which depends on the shared `OutboxEventRepository`/`ProcessedEventRepository`) still works correctly.

- [ ] **Step 4: Verify one compensation case**

With `SAGA_MODE=orchestration` still running, place an order with `consumerId: 2` (seeded as inactive per `CONTEXT.md`'s services table). Expected: `Order.REJECTED`, `Ticket.CANCELLED` — confirms the shared `OutboxPublisher` correctly publishes compensating commands (`CancelTicket`) from the shared outbox table.

- [ ] **Step 5: Verify redelivery/idempotency**

Force redelivery of one already-processed Kafka message (e.g. reset one row's `sent_at` to `NULL` in `outbox_events` and wait for the next poll, or replay via `kafka-console-producer` against the recorded payload) and confirm no duplicate side effect occurs (e.g. `Authorization` row count unchanged) — confirms the shared `ProcessedEvent` dedup ledger still works.

```bash
docker compose down
```

- [ ] **Step 6: Update `CONTEXT.md`**

In the "Architecture decisions made" section, replace:

```
- No shared common module yet — Ch.4's saga now duplicates OutboxEvent/ProcessedEvent/OutboxPublisher/KafkaProducerConfig near-verbatim across all four services (order/kitchen/consumer/accounting), plus each saga event record (ConsumerVerificationEvent, KitchenEvent, AccountingEvent) is copy-pasted into every consuming service. This was a deliberate, reviewed choice for the Ch.4 pass — extraction into a shared module is the natural next step if a 5th service joins a saga
```

with:

```
- Shared `ftgo-common` module (2026-07-18) — extracted the generic outbox/dedup infrastructure (OutboxEvent, ProcessedEvent, their repositories, OutboxPublisher, KafkaProducerConfig) into a new Gradle module, `com.sanjay.ftgo.common.outbox`, depended on by all four saga services. The saga wire-format records (SagaReply, KitchenCommand, ConsumerVerificationEvent, etc.) remain per-service, copy-pasted into every producer/consumer — deliberately out of scope for this pass, since those carry business meaning specific to who produces/consumes them rather than being generic plumbing.
```

Add a line to the "Session log" section:

```
- 2026-07-18 · Claude Code · Extracted the 4x-duplicated outbox/dedup infrastructure (OutboxEvent, ProcessedEvent, OutboxPublisher, KafkaProducerConfig) into a new ftgo-common Gradle module via a full brainstorm → spec → plan → subagent-driven-development cycle; each of the four saga services (order/kitchen/consumer/accounting) now depends on it via `implementation project(':ftgo-common')` and declares explicit @EntityScan/@EnableJpaRepositories for the shared package (Spring Boot only autoscans a service's own base package by default). The shared module is built as a plain library (bootJar disabled) with java-library's `api` configuration so JPA/Kafka types are visible transitively to consumers. Zero behavior change — re-verified via Docker that both saga styles (choreography and orchestration) still reach identical Order/Ticket/Authorization end states for the happy path, one compensation case, and redelivery/idempotency. Saga wire-format records deliberately stayed per-service, out of scope for this pass.
```

- [ ] **Step 7: Update `README.md` if it references the duplication note**

Search for the phrase referenced in Step 6 or similar wording in `README.md`; if present, update it to match the `ftgo-common` decision. If `README.md` doesn't mention this (it may only be in `CONTEXT.md`), skip.

```bash
grep -n "shared common module\|duplicat" README.md
```

- [ ] **Step 8: Commit documentation**

```bash
git add CONTEXT.md README.md
git commit -m "docs: record ftgo-common shared outbox module extraction"
```

---

## Self-Review Notes

- **Spec coverage**: Infra-only scope (Task 2–4 vs. wire-format records left alone) ✓; new module not executable (Task 1 `bootJar.enabled=false`) ✓; `java-library`/`api` for transitive JPA/Kafka visibility (Task 1) ✓; package `com.sanjay.ftgo.common.outbox` outside service base packages, with explicit `@EntityScan`/`@EnableJpaRepositories` on all four services (Tasks 5–8) ✓; `OutboxPublisher` conditional gate preserved verbatim (Task 4) ✓; generic `KafkaProducerConfig` bean names (Task 4) ✓; no schema change (all entities keep the same `@Table` names) ✓; test migration — `OutboxPublisherTest`/`OutboxPublisherConditionalTest` moved to `ftgo-common`, duplicates deleted from all four services (Tasks 4–8) ✓; `SchedulingEnabledTest` untouched per spec (still lives per-service, not referenced by this plan since it doesn't touch outbox types) ✓; Docker e2e verification for both saga modes + one compensation case + redelivery (Task 9) ✓; docs sync per `CLAUDE.md` (Task 9) ✓; restaurant/registry/delivery services untouched (never referenced in any task) ✓.
- **Placeholder scan**: no TBD/TODO; every step shows real code or an exact command with expected output.
- **Type consistency**: `OutboxEventRepository.findBySentAtIsNullOrderByIdAsc()`, `ProcessedEventRepository extends JpaRepository<ProcessedEvent, String>`, `OutboxPublisher(OutboxEventRepository, KafkaTemplate<String,String>, int)` are used identically across Tasks 2, 3, 4 and referenced identically in Tasks 5–8's import guidance.
