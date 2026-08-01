# Ch.10 Sub-Project 2: Component Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an out-of-process Cucumber component test for order-service's Place Order flow, running the real containerized service against real MySQL/Kafka with Restaurant Service stubbed via a WireMock container and Consumer/Kitchen/Delivery/Accounting stubbed via a custom Kafka-based `SagaParticipantStub`, per `docs/superpowers/specs/2026-07-31-ch10-component-tests-design.md`.

**Architecture:** A new `compose-component-test.yml` (project root) brings up mysql, zookeeper, kafka, a WireMock container (statically stubbing `GET /restaurants/1`), and order-service itself (built from its existing `Dockerfile`), running with `SAGA_MODE=orchestration` via a new `componenttest` Spring profile that disables Eureka and points order-service's `RestClientConfig` at the WireMock container through `SimpleDiscoveryClient`. A new `componentTest` Gradle source set on `ftgo-order-service` runs Cucumber scenarios (JUnit Platform engine) against this stack from the host JVM: HTTP calls hit `http://localhost:8082` (order-service's mapped port), and a `SagaParticipantStub` helper (plain `KafkaConsumer`/`KafkaProducer`, connecting to `localhost:9092`) watches the four `*.commands` topics and publishes replies onto `saga.replies` to drive the saga to completion.

**Tech Stack:** Cucumber 7 (`cucumber-java`, `cucumber-junit-platform-engine`), JUnit Platform Suite, `com.avast.gradle.docker-compose` Gradle plugin, WireMock (`wiremock/wiremock:3.9.2` Docker image, file-based stub mappings — no Java WireMock library dependency needed), `org.apache.kafka:kafka-clients` (transitively available via `spring-kafka`, already an `implementation` dependency of `ftgo-order-service` through `ftgo-common`).

## Global Constraints

- Only Place Order (orchestration mode, JPA persistence mode) is covered — Revise/Cancel sagas, choreography mode, other services' component tests, and event-sourced persistence are explicitly out of scope and deferred (see spec's "Deferred to a future sub-project" section). Do not implement them here.
- The `componentTest` task must stay out of the default `test` task (requires Docker, slow) — mirror how `contractTest` (sub-project 1) is already a separate task.
- The existing sub-project-1 Kafka bridge (`KafkaMessageVerifierSender`/`KafkaMessageVerifierReceiver` in `ftgo-common`'s testFixtures) is **not** reused — confirmed unsuited to a long-running, multi-topic stub (see spec's Architecture section). `SagaParticipantStub` is a new, independent class.
- Docker Compose teardown (`composeDown`) must run after the Cucumber task regardless of pass/fail — wire via the docker-compose Gradle plugin's `isRequiredBy`, not manual scripting.
- No Eureka container in `compose-component-test.yml` — service resolution is bypassed via `eureka.client.enabled=false` + `SimpleDiscoveryClient`, not a running registry.
- Per this repo's `CLAUDE.md`, any change that alters documented behavior updates `README.md` / `CONTEXT.md` / relevant per-service `README.md` in the same change (this chapter is not yet flipping to Done, so only the per-change rule applies, not the full chapter-completion sweep).

---

## File Structure

- `compose-component-test.yml` (new, project root) — slimmed compose stack: mysql, zookeeper, kafka, wiremock, order-service.
- `ftgo-order-service/src/componentTest/resources/wiremock/mappings/find-restaurant.json` (new) — static WireMock stub for `GET /restaurants/1`.
- `ftgo-order-service/src/main/resources/application-componenttest.yml` (new) — Spring profile: disables Eureka, points `SimpleDiscoveryClient` at the WireMock container, sets `saga.mode=orchestration`.
- `ftgo-order-service/build.gradle` (modify) — new `componentTest` source set + task, Cucumber deps, docker-compose plugin wiring.
- `settings.gradle` (modify) — declare `com.avast.gradle.docker-compose` plugin version, `apply false` at root.
- `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/SagaParticipantStub.java` (new) — Kafka-based saga participant stub.
- `ftgo-order-service/src/componentTest/resources/features/PlaceOrder.feature` (new) — the two Gherkin scenarios.
- `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/ComponentTestRunner.java` (new) — Cucumber JUnit Platform Suite runner.
- `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java` (new) — step definitions.
- `ftgo-order-service/README.md` (modify) — document the new component-test suite.
- `CONTEXT.md` (modify) — session log entry, note sub-project 2 shipped and sub-project 3 (+ the deferred sub-project 4) still open.

---

### Task 1: Compose stack, WireMock stub, and `componenttest` Spring profile

**Files:**
- Create: `compose-component-test.yml`
- Create: `ftgo-order-service/src/componentTest/resources/wiremock/mappings/find-restaurant.json`
- Create: `ftgo-order-service/src/main/resources/application-componenttest.yml`

**Interfaces:**
- Produces: order-service container reachable at `http://localhost:8082`; Kafka reachable at `localhost:9092`; WireMock container reachable at `http://localhost:8080` (host) / `http://wiremock:8080` (compose network); mysql `ftgo_order` schema at `localhost:3306`.
- Produces: `GET /restaurants/1` (via WireMock) returns `{"id":1,"name":"Ajanta","menuItems":[{"id":1,"name":"Vindaloo","price":12.50}]}`.

- [ ] **Step 1: Create the WireMock stub mapping**

`ftgo-order-service/src/componentTest/resources/wiremock/mappings/find-restaurant.json`:

```json
{
  "request": {
    "method": "GET",
    "url": "/restaurants/1"
  },
  "response": {
    "status": 200,
    "jsonBody": {
      "id": 1,
      "name": "Ajanta",
      "menuItems": [
        { "id": 1, "name": "Vindaloo", "price": 12.50 }
      ]
    },
    "headers": {
      "Content-Type": "application/json"
    }
  }
}
```

- [ ] **Step 2: Create the `componenttest` Spring profile**

`ftgo-order-service/src/main/resources/application-componenttest.yml`:

```yaml
eureka:
  client:
    enabled: false

spring:
  cloud:
    discovery:
      client:
        simple:
          instances:
            ftgo-restaurant-service:
              - uri: http://wiremock:8080

saga:
  mode: orchestration
```

This is a profile-specific file (`application-componenttest.yml`), so it only takes effect when `SPRING_PROFILES_ACTIVE=componenttest` is set; it ships harmlessly in the production jar like any other Spring Boot profile file and is never active outside this test suite.

- [ ] **Step 3: Create the compose stack**

`compose-component-test.yml` (project root):

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

  wiremock:
    image: wiremock/wiremock:3.9.2
    ports:
      - "8080:8080"
    volumes:
      - ./ftgo-order-service/src/componentTest/resources/wiremock:/home/wiremock

  order-service:
    build:
      context: .
      dockerfile: ftgo-order-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      wiremock:
        condition: service_started
    ports:
      - "8082:8082"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SPRING_PROFILES_ACTIVE: componenttest
```

No `volumes:` top-level key is needed here (unlike the root `compose.yml`) — mysql data doesn't need to persist across component-test runs; omitting a named volume means Compose uses an anonymous, disposable one.

- [ ] **Step 4: Manually verify the stack boots and the stub responds**

Run:
```bash
docker compose -f compose-component-test.yml up --build -d
sleep 20
curl -s http://localhost:8080/restaurants/1
curl -s -X POST http://localhost:8082/orders -H "Content-Type: application/json" -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":1,"quantity":2}]}'
docker compose -f compose-component-test.yml down -v
```
Expected: the `curl` to `:8080/restaurants/1` returns the WireMock JSON body above; the `curl` to `:8082/orders` returns HTTP 201 with `"status":"APPROVAL_PENDING"` (it will stay `APPROVAL_PENDING` forever at this point — no saga participant stub exists yet, that's Task 3 — this step only confirms order-service starts, reaches WireMock through `SimpleDiscoveryClient`, and accepts the create-order request).

- [ ] **Step 5: Commit**

```bash
git add compose-component-test.yml ftgo-order-service/src/componentTest/resources/wiremock/mappings/find-restaurant.json ftgo-order-service/src/main/resources/application-componenttest.yml
git commit -m "feat: add component-test compose stack, WireMock stub, and componenttest Spring profile"
```

---

### Task 2: `componentTest` Gradle source set and docker-compose plugin wiring

**Files:**
- Modify: `settings.gradle`
- Modify: `ftgo-order-service/build.gradle`

**Interfaces:**
- Consumes: nothing from Task 1 directly (Gradle wiring is independent of the compose file's contents, only its path).
- Produces: `./gradlew :ftgo-order-service:componentTest` — runs `composeUp` against `compose-component-test.yml`, then any tests on the `componentTest` source set, then `composeDown`, regardless of pass/fail.

- [ ] **Step 1: Declare the docker-compose plugin in `settings.gradle`**

In `settings.gradle`, add to the existing `plugins { ... }` block (alongside the existing `org.springframework.cloud.contract` entry):

```gradle
plugins {
    id 'org.springframework.boot' version '3.5.16' apply false
    id 'io.spring.dependency-management' version '1.1.7' apply false
    id 'org.springframework.cloud.contract' version '4.3.4' apply false
    // Brings up/tears down compose-component-test.yml around the componentTest task on
    // ftgo-order-service (Ch.10 sub-project 2) — the same plugin the book uses for its own
    // out-of-process component-test example.
    id 'com.avast.gradle.docker-compose' version '0.17.12' apply false
}
```

- [ ] **Step 2: Add the `componentTest` source set, Cucumber deps, and the `componentTest` task to `ftgo-order-service/build.gradle`**

Append to `ftgo-order-service/build.gradle` (after the existing `dependencies { ... }` block, which stays untouched):

```gradle
apply plugin: 'com.avast.gradle.docker-compose'

sourceSets {
    componentTest {
        java.srcDir 'src/componentTest/java'
        resources.srcDir 'src/componentTest/resources'
        compileClasspath += sourceSets.main.output
        runtimeClasspath += sourceSets.main.output
    }
}

configurations {
    componentTestImplementation.extendsFrom implementation
    componentTestRuntimeOnly.extendsFrom runtimeOnly
}

dependencies {
    componentTestImplementation 'io.cucumber:cucumber-java:7.20.1'
    componentTestImplementation 'io.cucumber:cucumber-junit-platform-engine:7.20.1'
    componentTestImplementation 'org.junit.platform:junit-platform-suite'
    componentTestImplementation 'org.junit.jupiter:junit-jupiter-api'
    // jackson-databind and kafka-clients both come transitively through `implementation`
    // (spring-boot-starter-web and spring-kafka respectively), inherited via
    // componentTestImplementation.extendsFrom implementation above.
}

tasks.register('componentTest', Test) {
    description = 'Runs out-of-process Cucumber component tests against a Dockerized order-service'
    group = 'verification'
    testClassesDirs = sourceSets.componentTest.output.classesDirs
    classpath = sourceSets.componentTest.runtimeClasspath
    useJUnitPlatform()
    // Compose state (fresh containers/data each run) makes result caching meaningless here.
    outputs.upToDateWhen { false }
}

dockerCompose {
    useComposeFiles = ["${rootDir}/compose-component-test.yml"]
    projectName = 'ftgo-component-test'
    isRequiredBy(tasks.componentTest)
    waitForTcpPorts = true
}
```

- [ ] **Step 3: Verify the task is registered and the empty source set compiles**

Run: `./gradlew :ftgo-order-service:componentTest --dry-run`
Expected: task graph prints `:ftgo-order-service:composeUp`, `:ftgo-order-service:componentTest`, `:ftgo-order-service:composeDown` in order, with `BUILD SUCCESSFUL` (dry-run doesn't execute tasks, just validates wiring). If `composeUp`/`composeDown` don't appear in the graph, `isRequiredBy` isn't wired correctly — re-check Step 2.

- [ ] **Step 4: Commit**

```bash
git add settings.gradle ftgo-order-service/build.gradle
git commit -m "feat: wire componentTest Gradle source set and docker-compose plugin"
```

---

### Task 3: `SagaParticipantStub`

**Files:**
- Create: `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/SagaParticipantStub.java`

**Interfaces:**
- Consumes: Kafka topics `consumer.commands`, `kitchen.commands`, `delivery.commands`, `accounting.commands` (JSON payloads shaped like `com.sanjay.ftgo.order.domain.VerifyConsumerCommand` / `KitchenCommand` / `DeliveryCommand` / `AccountingCommand`, all carrying an `orderId` field; `KitchenCommand`/`DeliveryCommand`/`AccountingCommand` also carry `commandType`).
- Produces: publishes to Kafka topic `saga.replies`, JSON shaped `{"eventId": String, "participant": String, "eventType": String, "orderId": Long, "reason": String|null, "sagaType": "CreateOrder"}` — this exact shape matches `com.sanjay.ftgo.order.domain.SagaReply` (and its four sibling-service copies), consumed by `OrchestratorReplyListener`.
- Produces (public API used by Task 5's step definitions): `new SagaParticipantStub(String bootstrapServers)`, `void setAccountingShouldApprove(boolean approve)`, `void close()`.

- [ ] **Step 1: Write `SagaParticipantStub.java`**

```java
package com.sanjay.ftgo.order.componenttest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Stands in for the Consumer, Kitchen, Delivery, and Accounting services during the Place Order
 * component test. CreateOrderSagaOrchestrator fans out VerifyConsumerCommand/KitchenCommand
 * (CreateTicket)/DeliveryCommand (ScheduleDelivery) in parallel, then AccountingCommand
 * (AuthorizeCard) only once all three replies succeed — so this stub always replies success for
 * consumer/kitchen/delivery, and only the accounting reply is configurable per scenario.
 */
public class SagaParticipantStub implements AutoCloseable {

    private static final String CONSUMER_COMMANDS = "consumer.commands";
    private static final String KITCHEN_COMMANDS = "kitchen.commands";
    private static final String DELIVERY_COMMANDS = "delivery.commands";
    private static final String ACCOUNTING_COMMANDS = "accounting.commands";
    private static final String SAGA_REPLIES = "saga.replies";
    private static final String SAGA_TYPE = "CreateOrder";

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;
    private volatile boolean accountingShouldApprove = true;

    public SagaParticipantStub(String bootstrapServers) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "saga-participant-stub-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(CONSUMER_COMMANDS, KITCHEN_COMMANDS, DELIVERY_COMMANDS, ACCOUNTING_COMMANDS));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        executor.submit(this::pollLoop);
    }

    public void setAccountingShouldApprove(boolean approve) {
        this.accountingShouldApprove = approve;
    }

    private void pollLoop() {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    handleCommand(record.topic(), record.value());
                } catch (Exception e) {
                    // A malformed or not-yet-understood command shouldn't kill the poll loop —
                    // the test itself will time out and fail if a reply never arrives.
                }
            }
        }
    }

    private void handleCommand(String topic, String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        long orderId = node.get("orderId").asLong();
        switch (topic) {
            case CONSUMER_COMMANDS -> reply("consumer", "ConsumerVerified", orderId);
            case KITCHEN_COMMANDS -> {
                if ("CreateTicket".equals(node.get("commandType").asText())) {
                    reply("kitchen", "TicketCreated", orderId);
                }
            }
            case DELIVERY_COMMANDS -> {
                if ("ScheduleDelivery".equals(node.get("commandType").asText())) {
                    reply("delivery", "DeliveryScheduled", orderId);
                }
            }
            case ACCOUNTING_COMMANDS -> {
                if ("AuthorizeCard".equals(node.get("commandType").asText())) {
                    reply("accounting", accountingShouldApprove ? "CardAuthorized" : "CardAuthorizationFailed", orderId);
                }
            }
            default -> { }
        }
    }

    private void reply(String participant, String eventType, long orderId) {
        String eventId = UUID.randomUUID().toString();
        String json = String.format(
                "{\"eventId\":\"%s\",\"participant\":\"%s\",\"eventType\":\"%s\",\"orderId\":%d,\"reason\":null,\"sagaType\":\"%s\"}",
                eventId, participant, eventType, orderId, SAGA_TYPE);
        try {
            producer.send(new ProducerRecord<>(SAGA_REPLIES, String.valueOf(orderId), json)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish saga reply for order " + orderId, e);
        }
    }

    @Override
    public void close() {
        running = false;
        executor.shutdown();
        consumer.close();
        producer.close();
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `./gradlew :ftgo-order-service:compileComponentTestJava`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/SagaParticipantStub.java
git commit -m "feat: add SagaParticipantStub for Ch.10 component tests"
```

---

### Task 4: Cucumber runner and feature file

**Files:**
- Create: `ftgo-order-service/src/componentTest/resources/features/PlaceOrder.feature`
- Create: `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/ComponentTestRunner.java`
- Create: `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java` (skeleton only — Task 5 fills in bodies)

**Interfaces:**
- Consumes: `SagaParticipantStub` from Task 3 (`new SagaParticipantStub(String)`, `setAccountingShouldApprove(boolean)`, `close()`).
- Produces: nothing consumed elsewhere — this is the test entry point.

- [ ] **Step 1: Write the feature file**

`ftgo-order-service/src/componentTest/resources/features/PlaceOrder.feature`:

```gherkin
Feature: Place Order

  Background:
    Given the Restaurant Service stub is serving restaurant 1 with menu item 1 priced at 12.50

  Scenario: Order authorized
    Given the saga participant stub will approve the accounting authorization
    When a consumer places an order for 2 of menu item 1 from restaurant 1
    Then the order is eventually approved

  Scenario: Order rejected due to expired credit card
    Given the saga participant stub will decline the accounting authorization
    When a consumer places an order for 2 of menu item 1 from restaurant 1
    Then the order is eventually rejected
```

- [ ] **Step 2: Write the Cucumber JUnit Platform Suite runner**

`ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/ComponentTestRunner.java`:

```java
package com.sanjay.ftgo.order.componenttest;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;

@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.sanjay.ftgo.order.componenttest")
public class ComponentTestRunner {
}
```

- [ ] **Step 3: Write a skeleton step definitions class (undefined-step stubs only)**

`ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java`:

```java
package com.sanjay.ftgo.order.componenttest;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class OrderServiceComponentTestStepDefinitions {

    private SagaParticipantStub sagaParticipantStub;

    @Before
    public void setUp() {
        sagaParticipantStub = new SagaParticipantStub("localhost:9092");
    }

    @After
    public void tearDown() {
        sagaParticipantStub.close();
    }

    @Given("the Restaurant Service stub is serving restaurant {int} with menu item {int} priced at {double}")
    public void theRestaurantServiceStubIsServing(int restaurantId, int menuItemId, double price) {
        throw new io.cucumber.java.PendingException();
    }

    @Given("the saga participant stub will approve the accounting authorization")
    public void theSagaParticipantStubWillApprove() {
        sagaParticipantStub.setAccountingShouldApprove(true);
    }

    @Given("the saga participant stub will decline the accounting authorization")
    public void theSagaParticipantStubWillDecline() {
        sagaParticipantStub.setAccountingShouldApprove(false);
    }

    @When("a consumer places an order for {int} of menu item {int} from restaurant {int}")
    public void aConsumerPlacesAnOrder(int quantity, int menuItemId, int restaurantId) {
        throw new io.cucumber.java.PendingException();
    }

    @Then("the order is eventually approved")
    public void theOrderIsEventuallyApproved() {
        throw new io.cucumber.java.PendingException();
    }

    @Then("the order is eventually rejected")
    public void theOrderIsEventuallyRejected() {
        throw new io.cucumber.java.PendingException();
    }
}
```

- [ ] **Step 4: Verify Cucumber discovers both scenarios (without running Docker yet)**

Run: `./gradlew :ftgo-order-service:compileComponentTestJava`
Expected: `BUILD SUCCESSFUL`. (Full scenario discovery is verified in Task 5 once steps are implemented and the compose stack is actually running — running `componentTest` now would bring up Docker and immediately hit the `PendingException` stubs, which is expected but not yet informative; skip running the task itself until Task 5.)

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/src/componentTest/resources/features/PlaceOrder.feature ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/ComponentTestRunner.java ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java
git commit -m "feat: add PlaceOrder.feature, Cucumber runner, and step-definition skeleton"
```

---

### Task 5: Implement step definitions and run the full component-test suite

**Files:**
- Modify: `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java`

**Interfaces:**
- Consumes: order-service REST API — `POST /orders` (`CreateOrderRequest` JSON: `{"consumerId": Long, "restaurantId": Long, "lineItems": [{"menuItemId": Long, "quantity": int}]}`, returns 201 with `OrderResponse` JSON `{"id": Long, "consumerId": Long, "restaurantId": Long, "lineItems": [...], "status": String}`) and `GET /orders/{id}` (returns 200 with the same `OrderResponse` shape). `status` is one of `OrderStatus`'s enum names: `APPROVAL_PENDING`, `APPROVED`, `REJECTED`, `CANCEL_PENDING`, `CANCELLED`, `REVISION_PENDING`.
- Consumes: WireMock container's `GET /restaurants/{id}` at `http://localhost:8080` (host-mapped port from Task 1), returning `RestaurantInfo` JSON `{"id": Long, "name": String, "menuItems": [{"id": Long, "name": String, "price": BigDecimal}]}`.

- [ ] **Step 1: Replace the skeleton with full implementations**

Full contents of `ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java`:

```java
package com.sanjay.ftgo.order.componenttest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class OrderServiceComponentTestStepDefinitions {

    private static final String ORDER_SERVICE_BASE_URL = "http://localhost:8082";
    private static final String WIREMOCK_BASE_URL = "http://localhost:8080";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private SagaParticipantStub sagaParticipantStub;
    private Long placedOrderId;

    @Before
    public void setUp() {
        sagaParticipantStub = new SagaParticipantStub(KAFKA_BOOTSTRAP_SERVERS);
    }

    @After
    public void tearDown() {
        sagaParticipantStub.close();
    }

    @Given("the Restaurant Service stub is serving restaurant {int} with menu item {int} priced at {double}")
    public void theRestaurantServiceStubIsServing(int restaurantId, int menuItemId, double price) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(WIREMOCK_BASE_URL + "/restaurants/" + restaurantId))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Restaurant Service stub is not serving restaurant " + restaurantId);
        JsonNode body = objectMapper.readTree(response.body());
        assertEquals(menuItemId, body.get("menuItems").get(0).get("id").asInt());
        assertEquals(price, body.get("menuItems").get(0).get("price").asDouble(), 0.001);
    }

    @Given("the saga participant stub will approve the accounting authorization")
    public void theSagaParticipantStubWillApprove() {
        sagaParticipantStub.setAccountingShouldApprove(true);
    }

    @Given("the saga participant stub will decline the accounting authorization")
    public void theSagaParticipantStubWillDecline() {
        sagaParticipantStub.setAccountingShouldApprove(false);
    }

    @When("a consumer places an order for {int} of menu item {int} from restaurant {int}")
    public void aConsumerPlacesAnOrder(int quantity, int menuItemId, int restaurantId) throws Exception {
        String requestBody = String.format(
                "{\"consumerId\":1,\"restaurantId\":%d,\"lineItems\":[{\"menuItemId\":%d,\"quantity\":%d}]}",
                restaurantId, menuItemId, quantity);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ORDER_SERVICE_BASE_URL + "/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode(), "Order creation failed: " + response.body());
        JsonNode body = objectMapper.readTree(response.body());
        placedOrderId = body.get("id").asLong();
        assertEquals("APPROVAL_PENDING", body.get("status").asText());
    }

    @Then("the order is eventually approved")
    public void theOrderIsEventuallyApproved() throws Exception {
        assertEquals("APPROVED", pollForFinalStatus());
    }

    @Then("the order is eventually rejected")
    public void theOrderIsEventuallyRejected() throws Exception {
        assertEquals("REJECTED", pollForFinalStatus());
    }

    private String pollForFinalStatus() throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        String lastStatus = "APPROVAL_PENDING";
        while (Instant.now().isBefore(deadline)) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ORDER_SERVICE_BASE_URL + "/orders/" + placedOrderId))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = objectMapper.readTree(response.body());
            lastStatus = body.get("status").asText();
            if (!"APPROVAL_PENDING".equals(lastStatus)) {
                return lastStatus;
            }
            Thread.sleep(500);
        }
        fail("Order " + placedOrderId + " did not leave APPROVAL_PENDING within 10s; last status: " + lastStatus);
        return lastStatus;
    }
}
```

- [ ] **Step 2: Run the full component-test suite**

Run: `./gradlew :ftgo-order-service:componentTest`
Expected: `BUILD SUCCESSFUL`, both scenarios pass. This single Gradle invocation brings up `compose-component-test.yml` (Task 1), runs both Cucumber scenarios end-to-end (real order-service container, real MySQL, real Kafka, WireMock-stubbed Restaurant Service, `SagaParticipantStub`-driven saga participants), and tears the stack down afterward — success here is the sub-project's own acceptance test.

If it fails, check in this order: (a) `docker compose -f compose-component-test.yml logs order-service` for startup errors (most likely a `SimpleDiscoveryClient`/profile misconfiguration from Task 1 Step 2); (b) whether `SAGA_MODE`/`saga.mode` actually resolved to `orchestration` (log line from `OrchestratorReplyListener`'s `@ConditionalOnProperty` — if absent, the bean didn't activate); (c) whether the `SagaParticipantStub`'s consumer group ever received messages (add a temporary log line in `handleCommand` if needed, but remove before committing).

- [ ] **Step 3: Commit**

```bash
git add ftgo-order-service/src/componentTest/java/com/sanjay/ftgo/order/componenttest/OrderServiceComponentTestStepDefinitions.java
git commit -m "feat: implement Ch.10 component-test step definitions, both scenarios passing"
```

---

### Task 6: Documentation

**Files:**
- Modify: `ftgo-order-service/README.md`
- Modify: `CONTEXT.md`

**Interfaces:**
- Consumes: nothing — documentation only, reflecting Tasks 1-5's shipped behavior.

- [ ] **Step 1: Document the component-test suite in `ftgo-order-service/README.md`**

Read the current `ftgo-order-service/README.md` first (in full) and add a new section (placed near any existing testing/contract-test documentation, matching that section's existing heading level and style) covering:
- What `./gradlew :ftgo-order-service:componentTest` does and requires (Docker running locally).
- The stack it brings up (`compose-component-test.yml`: mysql, zookeeper, kafka, WireMock, order-service) and what's stubbed vs. real.
- That it covers Place Order only, in orchestration mode, JPA persistence mode — link to (reference by path) `docs/superpowers/specs/2026-07-31-ch10-component-tests-design.md` for the full design and its "Deferred to a future sub-project" list.

- [ ] **Step 2: Update `CONTEXT.md`**

Read the current `CONTEXT.md` first (in full). Update:
- The "Services to build" / progress table entry for order-service or Ch.10, if one exists, to reflect sub-project 2 (component tests) as done.
- Append a session log entry (following the existing session log's established format) noting: Ch.10 sub-project 2 (component tests) shipped — out-of-process Cucumber + Docker Compose component test for order-service's Place Order flow (orchestration mode); sub-project 3 (end-to-end tests) and the deferred sub-project 4 (Revise/Cancel saga component tests, choreography-mode component-test coverage, other services' component tests, event-sourced persistence coverage) remain open.

- [ ] **Step 3: Commit**

```bash
git add ftgo-order-service/README.md CONTEXT.md
git commit -m "docs: document Ch.10 component-test suite in order-service README and CONTEXT.md"
```
