# Ch.10 sub-project 1 — Consumer-driven contract tests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Spring Cloud Contract-based consumer-driven contract tests for
one REST interaction (API Gateway ↔ Order Service) and two Kafka messaging
interactions (Order Service → Order History Service pub/sub, Order Service ↔
Kitchen Service async request/response), closing the gap found in the Ch.10
audit: this project has hand-written WireMock stubs but no contract
mechanism tying consumer expectations to provider behavior.

**Architecture:** Two new Gradle modules (`ftgo-order-service-contracts`,
`ftgo-kitchen-service-contracts`) hold Groovy contract definitions and get
consumed by both the provider (via the Spring Cloud Contract Gradle plugin,
which code-generates provider-side tests at build time) and the consumer
(via Stub Runner, which resolves the published contract JAR to configure
WireMock or messaging stubs). Because this project uses raw `spring-kafka`
rather than Spring Cloud Stream, the two messaging contracts need a custom
`MessageVerifierSender`/`MessageVerifierReceiver` bridge backed by an
embedded Kafka broker — built once as a shared JUnit test fixture in
`ftgo-common` and reused by both messaging contracts.

**Tech Stack:** Spring Cloud Contract Verifier + Gradle plugin (REST +
custom messaging), Spring Cloud Contract WireMock + Stub Runner (REST
consumer side), `spring-kafka-test`'s `EmbeddedKafkaBroker` (messaging
bridge), Gradle `java-test-fixtures` plugin (sharing the bridge).

## Global Constraints

- Spring Boot `3.5.16`, Spring Cloud `2025.0.3` (both already pinned at the
  root/per-module `build.gradle` — do not change).
- Java `21` (`sourceCompatibility` at the root — do not change).
- No production code changes anywhere in this plan — test/build-infrastructure
  only, per the spec's Out of Scope section.
- Every new Gradle module follows the existing per-module `build.gradle`
  convention: override `spring-cloud-dependencies` to `2025.0.3` in the
  module's own `dependencyManagement` block (see `ftgo-gateway-common/build.gradle`
  for the established pattern and its documented rationale).
- All three contracts are test/build-infra only; JSON shapes used in
  contracts must exactly match the real wire types already in production
  code (`OrderResponse`, `OrderEvent`, `KitchenCommand`, `SagaReply`) — no
  new production DTOs.

---

### Task 1: Shared Kafka contract-testing bridge in `ftgo-common`

**Files:**
- Modify: `ftgo-common/build.gradle`
- Create: `ftgo-common/src/testFixtures/java/com/sanjay/ftgo/common/contracttest/KafkaContractTestSupport.java`
- Create: `ftgo-common/src/testFixtures/java/com/sanjay/ftgo/common/contracttest/KafkaMessageVerifierSender.java`
- Create: `ftgo-common/src/testFixtures/java/com/sanjay/ftgo/common/contracttest/KafkaMessageVerifierReceiver.java`
- Test: `ftgo-common/src/testFixtures/test/java/com/sanjay/ftgo/common/contracttest/KafkaMessageVerifierRoundTripTest.java` (a plain test that lives alongside the fixtures to prove the bridge itself works, independent of any later contract)

**Interfaces:**
- Produces: `KafkaContractTestSupport` — a `@TestConfiguration` class providing
  an `EmbeddedKafkaBroker` (via `@EmbeddedKafka`), a `KafkaTemplate<String,String>`
  wired to that broker, and beans for `KafkaMessageVerifierSender` and
  `KafkaMessageVerifierReceiver`, both of type
  `org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender`
  / `MessageVerifierReceiver` (exact generic parameter and method
  signatures must be confirmed against the resolved
  `spring-cloud-contract-verifier` jar — see Step 1 below — before
  finalizing the implementation).
- Consumes: nothing from earlier tasks (this is the first task).

Every service that needs a messaging contract test (order-service,
order-history-service, kitchen-service) will later add
`testImplementation testFixtures(project(':ftgo-common'))` and import
`KafkaContractTestSupport` into its own `@SpringBootTest`.

- [ ] **Step 1: Confirm the exact `MessageVerifierSender`/`MessageVerifierReceiver` interface signatures**

Spring Cloud Contract 4.x (the version compatible with Spring Cloud
2025.0.3 — confirm the exact artifact version by checking what
`org.springframework.cloud:spring-cloud-contract-dependencies` resolves to
for `2025.0.3`, e.g. via `./gradlew :ftgo-common:dependencies --configuration testFixturesCompileClasspath`
after Step 2 adds the dependency) replaced its built-in stubbed-Kafka
support with a manual-integration model: you implement
`MessageVerifierSender<T>` and `MessageVerifierReceiver<T>` yourself,
backed by whatever real broker you choose (this plan uses
`spring-kafka-test`'s `EmbeddedKafkaBroker`, following Spring Cloud
Contract's own documented migration guidance for this exact situation).

Before writing Step 3's implementation, decompile/inspect the actual
interfaces on the resolved classpath:

```bash
./gradlew :ftgo-common:dependencies --configuration testFixturesCompileClasspath | grep spring-cloud-contract-verifier
find ~/.gradle/caches/modules-2 -name "spring-cloud-contract-verifier-*.jar" | head -1
javap -classpath "$(find ~/.gradle/caches/modules-2 -name 'spring-cloud-contract-verifier-*.jar' | head -1)" \
  org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender \
  org.springframework.cloud.contract.verifier.messaging.MessageVerifierReceiver
```

Record the exact method signatures (e.g. `send(T message, String
destination, YamlContract contract)`, `receive(String destination, long
timeout, TimeUnit timeUnit, YamlContract contract)` — confirm the actual
names/parameter order/types from `javap`'s output, they may differ
slightly across point releases) and use them verbatim in Step 3.

- [ ] **Step 2: Add dependencies to `ftgo-common/build.gradle`**

```groovy
apply plugin: 'java-library'
apply plugin: 'java-test-fixtures'

bootJar {
    enabled = false
}

jar {
    enabled = true
}

dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

dependencies {
    api 'org.springframework.boot:spring-boot-starter-data-jpa'
    api 'org.springframework.kafka:spring-kafka'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'

    testFixturesApi 'org.springframework.cloud:spring-cloud-contract-verifier'
    testFixturesApi 'org.springframework.kafka:spring-kafka-test'
    testFixturesApi 'org.springframework.boot:spring-boot-starter-test'
}
```

- [ ] **Step 3: Implement the bridge**

```java
// ftgo-common/src/testFixtures/java/com/sanjay/ftgo/common/contracttest/KafkaMessageVerifierSender.java
package com.sanjay.ftgo.common.contracttest;

import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierMessage;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

// Bridges Spring Cloud Contract's generic messaging-verification model onto this project's
// plain spring-kafka setup. Spring Cloud Contract 4.x dropped its own stubbed-Kafka support in
// favor of "bring your own broker" (Testcontainers or, as here, an embedded broker) plus a
// hand-written MessageVerifierSender/Receiver pair -- there is no off-the-shelf Kafka
// integration for a project (like this one) that isn't on Spring Cloud Stream.
public class KafkaMessageVerifierSender implements MessageVerifierSender<ContractVerifierMessage> {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaMessageVerifierSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(ContractVerifierMessage message, String destination, YamlContract contract) {
        Object payload = message.getPayload();
        kafkaTemplate.send(destination, payload == null ? null : payload.toString());
    }

    @Override
    public void send(Object payload, Map<String, Object> headers, String destination, YamlContract contract) {
        kafkaTemplate.send(destination, payload == null ? null : payload.toString());
    }
}
```

```java
// ftgo-common/src/testFixtures/java/com/sanjay/ftgo/common/contracttest/KafkaMessageVerifierReceiver.java
package com.sanjay.ftgo.common.contracttest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierReceiver;
import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierMessage;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class KafkaMessageVerifierReceiver implements MessageVerifierReceiver<ContractVerifierMessage> {

    private final Consumer<String, String> consumer;

    public KafkaMessageVerifierReceiver(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public ContractVerifierMessage receive(String destination, long timeout, TimeUnit timeUnit, YamlContract contract) {
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer, destination, Duration.ofMillis(timeUnit.toMillis(timeout)));
        return new ContractVerifierMessage(record.value(), java.util.Map.of());
    }

    @Override
    public ContractVerifierMessage receive(String destination, YamlContract contract) {
        return receive(destination, 5, TimeUnit.SECONDS, contract);
    }
}
```

**If `javap` (Step 1) shows different method names, parameter types, or an
additional required method** (e.g. `YamlContract` might be a different
type, or the interface might not be generic over `ContractVerifierMessage`
at all in this version), adjust both classes to match exactly — the code
above is this plan's best-confidence draft, not a guarantee of the exact
API surface; getting it to compile against the real interface is this
task's actual acceptance bar, not textual fidelity to this listing.

```java
// ftgo-common/src/testFixtures/java/com/sanjay/ftgo/common/contracttest/KafkaContractTestSupport.java
package com.sanjay.ftgo.common.contracttest;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

@TestConfiguration
@EmbeddedKafka(partitions = 1, topics = {"order.events", "kitchen.commands", "saga.replies"})
public class KafkaContractTestSupport {

    @Bean
    public KafkaTemplate<String, String> contractTestKafkaTemplate(EmbeddedKafkaBroker broker) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @Bean
    public Consumer<String, String> contractTestKafkaConsumer(EmbeddedKafkaBroker broker) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("contract-test-group", "true", broker);
        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps);
        broker.consumeFromAllEmbeddedTopics(consumer);
        return consumer;
    }

    @Bean
    public KafkaMessageVerifierSender kafkaMessageVerifierSender(KafkaTemplate<String, String> contractTestKafkaTemplate) {
        return new KafkaMessageVerifierSender(contractTestKafkaTemplate);
    }

    @Bean
    public KafkaMessageVerifierReceiver kafkaMessageVerifierReceiver(Consumer<String, String> contractTestKafkaConsumer) {
        return new KafkaMessageVerifierReceiver(contractTestKafkaConsumer);
    }
}
```

The `@EmbeddedKafka` topic list (`order.events`, `kitchen.commands`,
`saga.replies`) covers all three interactions this plan implements —
Task 1 sets this up once so Tasks 4 and 6 don't need to touch it.

- [ ] **Step 4: Write a round-trip proof test for the bridge itself**

```java
// ftgo-common/src/testFixtures/test/java/com/sanjay/ftgo/common/contracttest/KafkaMessageVerifierRoundTripTest.java
package com.sanjay.ftgo.common.contracttest;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = KafkaMessageVerifierRoundTripTest.TestConfig.class)
class KafkaMessageVerifierRoundTripTest {

    @org.springframework.context.annotation.Configuration
    @Import(KafkaContractTestSupport.class)
    static class TestConfig {
    }

    @org.springframework.beans.factory.annotation.Autowired
    private KafkaTemplate<String, String> contractTestKafkaTemplate;

    @org.springframework.beans.factory.annotation.Autowired
    private KafkaMessageVerifierReceiver kafkaMessageVerifierReceiver;

    @Test
    void sentMessageIsReceivedOnTheSameTopic() {
        contractTestKafkaTemplate.send("order.events", "{\"eventType\":\"OrderCreated\"}");

        var received = kafkaMessageVerifierReceiver.receive("order.events", null);

        assertEquals("{\"eventType\":\"OrderCreated\"}", received.getPayload());
    }
}
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :ftgo-common:testFixturesTest --tests KafkaMessageVerifierRoundTripTest`
Expected: PASS. If the test module name for `testFixtures` sources differs
(Gradle's `java-test-fixtures` plugin source-set/task naming can vary by
Gradle version — confirm with `./gradlew :ftgo-common:tasks --all | grep -i fixture`
if the above task name isn't found), adjust the run command accordingly;
the fixture code and test itself do not need to change.

- [ ] **Step 6: Commit**

```bash
git add ftgo-common/build.gradle ftgo-common/src/testFixtures
git commit -m "test: add shared Kafka bridge for Spring Cloud Contract messaging tests"
```

---

### Task 2: REST contract — `ftgo-order-service-contracts` module + provider-side test

**Files:**
- Modify: `settings.gradle`
- Create: `ftgo-order-service-contracts/build.gradle`
- Create: `ftgo-order-service-contracts/src/main/resources/contracts/order/shouldReturnOrderById.groovy`
- Modify: `ftgo-order-service/build.gradle`
- Create: `ftgo-order-service/src/contractTest/java/com/sanjay/ftgo/order/contracttest/HttpBase.java` (base class Spring Cloud Contract's Gradle plugin code-generates provider tests to extend — path convention set by the plugin's `baseClassForTests`/package-naming config, configured in this step)

**Interfaces:**
- Consumes: `OrderController.getOrder(Long id)` (`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java:56`, existing, unchanged), `OrderResponse.from(Order)` (`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderResponse.java`, existing, unchanged).
- Produces: the published `ftgo-order-service-contracts` artifact, consumed by Task 3's consumer-side test via Stub Runner coordinates `com.sanjay.ftgo:ftgo-order-service-contracts`.

- [ ] **Step 1: Register the new module**

```groovy
// settings.gradle — add after the existing include lines
include 'ftgo-order-service-contracts'
```

- [ ] **Step 2: Create the contracts module**

```groovy
// ftgo-order-service-contracts/build.gradle
apply plugin: 'java-library'

bootJar {
    enabled = false
}

jar {
    enabled = true
}
```

- [ ] **Step 3: Write the REST contract**

```groovy
// ftgo-order-service-contracts/src/main/resources/contracts/order/shouldReturnOrderById.groovy
package contracts.order

import org.springframework.cloud.contract.spec.Contract

Contract.make {
    description "should return an existing order by id"
    request {
        method GET()
        url '/orders/1223232'
    }
    response {
        status 200
        headers {
            header('Content-Type', 'application/json')
        }
        body(
                id: 1223232,
                consumerId: 1,
                restaurantId: 1,
                lineItems: [
                        [menuItemId: 10, quantity: 2]
                ],
                status: "APPROVAL_PENDING"
        )
    }
}
```

This mirrors `OrderResponse`'s exact field names
(`id`/`consumerId`/`restaurantId`/`lineItems`/`status`, with each line item
having `menuItemId`/`quantity`) — see `OrderResponse.java` above. No new
production DTO; this is the wire shape of the existing record.

- [ ] **Step 4: Wire up Spring Cloud Contract in `ftgo-order-service/build.gradle`**

```groovy
// ftgo-order-service/build.gradle — add plugin declaration and contractTest config
apply plugin: 'org.springframework.cloud.contract'

dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

contracts {
    baseClassForTests = 'com.sanjay.ftgo.order.contracttest.HttpBase'
    packageWithBaseClasses = 'com.sanjay.ftgo.order.contracttest'
}

dependencies {
    implementation project(':ftgo-common')
    implementation 'org.springframework.boot:spring-boot-starter-aop'
    implementation 'io.github.resilience4j:resilience4j-spring-boot3:2.2.0'
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
    implementation 'org.springframework.cloud:spring-cloud-starter-loadbalancer'

    testImplementation 'org.wiremock:wiremock-standalone:3.9.2'

    testImplementation project(':ftgo-order-service-contracts')
    testImplementation 'org.springframework.cloud:spring-cloud-contract-verifier'
    testImplementation 'io.rest-assured:rest-assured'
}
```

Also add the plugin version to the root `plugins {}` block:

```groovy
// build.gradle (root) — add to the existing plugins block
id 'org.springframework.cloud.contract' version '4.3.0' apply false
```

Confirm `4.3.0` is the release aligned with Spring Cloud `2025.0.3`
("Northfields" train) before running — if the build fails to resolve, check
`https://start.spring.io` or the Spring Cloud Contract release notes for
the version paired with `2025.0.3` and use that instead; the rest of this
plan's contract/test code does not depend on the exact plugin patch
version.

- [ ] **Step 5: Write the generated-test base class**

```java
// ftgo-order-service/src/contractTest/java/com/sanjay/ftgo/order/contracttest/HttpBase.java
package com.sanjay.ftgo.order.contracttest;

import com.sanjay.ftgo.order.api.OrderController;
import com.sanjay.ftgo.order.api.OrderResponse;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderStatus;
import io.restassured.module.mockmvc.RestAssuredMockMvc;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class HttpBase {

    @BeforeEach
    public void setup() {
        OrderRepository orderRepository = mock(OrderRepository.class);
        Order order = new Order(1223232L, 1L, 1L,
                List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(1223232L)).thenReturn(Optional.of(order));

        OrderController controller = new OrderController(null, null, null, null, orderRepository);
        RestAssuredMockMvc.standaloneSetup(controller);
    }
}
```

`OrderController`'s constructor takes `orderService`, `orderTransitions`,
`cancellationSagaTrigger`, `revisionSagaTrigger`, `orderRepository` (see
`OrderController.java` above) — only `orderRepository` is exercised by the
`GET /{id}` endpoint under test, so the other four are `null`. If the
project's `Order` constructor signature differs from
`new Order(id, consumerId, restaurantId, lineItems, status)`, check
`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`
for the real constructor and adjust — this listing follows the same
constructor shape already used in `ftgo-order-service/src/test/java/.../domain/CancelOrderSagaOrchestratorTest.java`'s
`cancelPendingOrder()` fixture from the Ch.9 work.

- [ ] **Step 6: Generate and run the provider-side test**

Run: `./gradlew :ftgo-order-service:contractTest`
Expected: PASS. This task generates and runs a test class named after the
contract file (e.g. `OrderShouldReturnOrderByIdTest`) under
`build/generated-test-sources` — do not hand-write this test class
yourself, it's code-generated from Step 3's Groovy contract each build.

- [ ] **Step 7: Commit**

```bash
git add settings.gradle ftgo-order-service-contracts ftgo-order-service/build.gradle \
  ftgo-order-service/src/contractTest build.gradle
git commit -m "test: add REST consumer-driven contract for GET /orders/{id}"
```

---

### Task 3: REST contract — consumer-side test in `ftgo-mobile-gateway`

**Files:**
- Modify: `ftgo-mobile-gateway/build.gradle`
- Create: `ftgo-mobile-gateway/src/test/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsHandlerContractTest.java`

**Interfaces:**
- Consumes: `OrderDetailsHandler.fetchOrderDetails(Long orderId)` returning
  `Mono<OrderDetails>` (`ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsHandler.java`,
  existing, unchanged), `BackendClients` record (same package, existing,
  unchanged), the `com.sanjay.ftgo:ftgo-order-service-contracts` artifact
  published by Task 2.
- Produces: nothing consumed by later tasks — this is a leaf test.

- [ ] **Step 1: Add Stub Runner dependency**

```groovy
// ftgo-mobile-gateway/build.gradle — add to dependencies {}
testImplementation 'org.springframework.cloud:spring-cloud-starter-contract-stub-runner'
testImplementation project(path: ':ftgo-order-service-contracts')
```

- [ ] **Step 2: Write the consumer-side test**

```java
// ftgo-mobile-gateway/src/test/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsHandlerContractTest.java
package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.web.reactive.function.client.WebClient;

import static org.junit.jupiter.api.Assertions.assertEquals;

@AutoConfigureStubRunner(
        ids = "com.sanjay.ftgo:ftgo-order-service-contracts:+:stubs:8085",
        stubsMode = StubRunnerProperties.StubsMode.LOCAL)
class OrderDetailsHandlerContractTest {

    private OrderDetailsHandler handler;

    @BeforeEach
    void setUp() {
        WebClient orderServiceClient = WebClient.create("http://localhost:8085");
        BackendClients clients = new BackendClients(orderServiceClient, null, null, null);
        handler = new OrderDetailsHandler(clients, throwableClass -> {
            throw new UnsupportedOperationException("circuit breaker not exercised by this contract test");
        });
    }

    @Test
    void fetchOrderDetailsParsesOrderServiceSectionPerContract() {
        // Exercises only the orderService section of the composed OrderDetails; the other
        // three backends (kitchen/accounting/delivery) have no client configured above and are
        // out of scope for this contract.
    }
}
```

`OrderDetailsHandler`'s constructor takes `(BackendClients clients,
ReactiveCircuitBreakerFactory circuitBreakerFactory)` — see
`OrderDetailsHandler.java` above. `ReactiveCircuitBreakerFactory` is a
functional-shaped dependency; if it's not a plain lambda-compatible
interface in this project's actual signature, replace the anonymous lambda
above with `org.springframework.cloud.client.circuitbreaker.NoOpReactiveCircuitBreakerFactory`
(a real, no-dependency implementation Spring Cloud already provides) —
confirm which fits by checking
`ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsHandler.java`'s
import for `ReactiveCircuitBreakerFactory` before finalizing this file.

The test body above is a scaffold showing correct wiring (stub runner
config, client construction) — replace the empty test method with a real
assertion once wiring is confirmed working:

```java
    @Test
    void fetchOrderDetailsParsesOrderServiceSectionPerContract() {
        WebClient orderServiceClient = WebClient.create("http://localhost:8085");
        WebClient unusedClient = WebClient.create("http://localhost:1"); // unreachable, unused by this contract
        BackendClients clients = new BackendClients(orderServiceClient, unusedClient, unusedClient, unusedClient);
        // (rebuild handler here with clients if constructed differently in @BeforeEach)

        // The contract's example order id is 1223232 (Task 2, Step 3) — verifying the section
        // for that id came back Found with the contract's exact status confirms this consumer
        // correctly parses what the real provider (per the shared contract) actually returns.
    }
```

Given the four-way `Mono.zip` in `fetchOrderDetails` (see
`OrderDetailsHandler.java` above) requires all four backend calls to
complete, and only `orderService` has a live stub here, finalize this test
by either (a) pointing the other three `WebClient`s at WireMock instances
configured to return `404` (so `SectionResult.NotFound` short-circuits
those sections without failing the `zip`), or (b) calling a smaller,
directly-testable method if one exists on `OrderDetailsHandler` for a
single section — check the actual class for either option and pick
whichever keeps this test scoped to only the order-service contract
without asserting anything about the other three sections.

- [ ] **Step 3: Run the test**

Run: `./gradlew :ftgo-mobile-gateway:test --tests OrderDetailsHandlerContractTest`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add ftgo-mobile-gateway/build.gradle ftgo-mobile-gateway/src/test/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsHandlerContractTest.java
git commit -m "test: add consumer-side REST contract test for OrderDetailsHandler"
```

---

### Task 4: Pub/sub contract — `OrderCreated` provider-side test (order-service)

**Files:**
- Modify: `ftgo-order-service-contracts/src/main/resources/contracts/order/orderCreated.groovy` (create)
- Modify: `ftgo-order-service/build.gradle`
- Create: `ftgo-order-service/src/contractTest/java/com/sanjay/ftgo/order/contracttest/MessagingBase.java`

**Interfaces:**
- Consumes: `OrderDomainEventPublisher.publishOrderCreated(Order order,
  String eventId)` (`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderDomainEventPublisher.java`,
  existing, unchanged), `OutboxPublisher.publishPendingEvents()`
  (`ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxPublisher.java`,
  existing, unchanged — this is the class that actually calls
  `kafkaTemplate.send`, since events go through the transactional outbox
  rather than being published directly), `KafkaContractTestSupport` (Task 1).
- Produces: nothing consumed by later tasks.

This project's outbox pattern means "publish an event" is two steps in
production: `OrderDomainEventPublisher` writes a row, and the separately
scheduled `OutboxPublisher.publishPendingEvents()` relays it to Kafka. The
contract's trigger method must do both, so the resulting Kafka message is
exactly what production would eventually send — not a shortcut that
bypasses the outbox.

- [ ] **Step 1: Write the pub/sub contract**

```groovy
// ftgo-order-service-contracts/src/main/resources/contracts/order/orderCreated.groovy
package contracts.order

import org.springframework.cloud.contract.spec.Contract

Contract.make {
    label 'orderCreated'
    input {
        triggeredBy('orderCreated()')
    }
    outputMessage {
        sentTo('order.events')
        body(
                eventId: $(consumer(regex('[0-9a-f-]{36}')), producer('11111111-1111-1111-1111-111111111111')),
                eventType: 'OrderCreated',
                orderId: 1223232,
                consumerId: 1,
                restaurantId: 1,
                lineItems: [
                        [menuItemId: 10, quantity: 2]
                ]
        )
    }
}
```

This mirrors `OrderEvent`'s exact field names (see `OrderEvent.java` in
`ftgo-order-history-service`, used as the wire shape both sides serialize
to/from — confirm `ftgo-order-service`'s own `OrderEvent` record, produced
by `OrderDomainEventPublisher`, has the identical field set before
finalizing this contract body).

- [ ] **Step 2: Add contract-test dependencies to `ftgo-order-service/build.gradle`**

```groovy
// add to the existing dependencies {} block from Task 2
testImplementation testFixtures(project(':ftgo-common'))
```

- [ ] **Step 3: Write the messaging base class**

```java
// ftgo-order-service/src/contractTest/java/com/sanjay/ftgo/order/contracttest/MessagingBase.java
package com.sanjay.ftgo.order.contracttest;

import com.sanjay.ftgo.common.contracttest.KafkaContractTestSupport;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.OutboxPublisher;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderDomainEventPublisher;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderStatus;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier;
import org.springframework.context.annotation.Import;

import java.util.List;

@SpringBootTest
@AutoConfigureMessageVerifier
@Import(KafkaContractTestSupport.class)
public abstract class MessagingBase {

    @Autowired
    private OrderDomainEventPublisher orderDomainEventPublisher;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    protected void orderCreated() {
        Order order = new Order(1223232L, 1L, 1L,
                List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        orderDomainEventPublisher.publishOrderCreated(order, "11111111-1111-1111-1111-111111111111");
        outboxPublisher.publishPendingEvents();
    }
}
```

If `@SpringBootTest` without `classes = ...` fails to find a bootable
configuration (order-service's real `@SpringBootApplication` pulls in
production Kafka/Eureka/DB config unsuited for this isolated test), narrow
it to `@SpringBootTest(classes = {OrderDomainEventPublisher.class,
OutboxPublisher.class, /* the JPA repository config actually needed */})`
or an equivalent minimal `@Configuration` — check how
`ftgo-order-service`'s own existing `@DataJpaTest`-style tests
(`OrderRepositoryTest`) scope their Spring context and follow the same
narrowing approach here, since this test needs the same JPA/outbox slice
plus this task's Kafka bridge, not the full application.

- [ ] **Step 4: Run the generated test**

Run: `./gradlew :ftgo-order-service:contractTest`
Expected: PASS (in addition to Task 2's already-passing REST contract test
in the same run).

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service-contracts/src/main/resources/contracts/order/orderCreated.groovy \
  ftgo-order-service/build.gradle ftgo-order-service/src/contractTest/java/com/sanjay/ftgo/order/contracttest/MessagingBase.java
git commit -m "test: add pub/sub consumer-driven contract for OrderCreated event"
```

---

### Task 5: Pub/sub contract — consumer-side test in `ftgo-order-history-service`

**Files:**
- Modify: `ftgo-order-history-service/build.gradle`
- Create: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListenerContractTest.java`

**Interfaces:**
- Consumes: `OrderEventListener.onMessage(String payload)` (`ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListener.java`,
  existing, unchanged), `OrderViewService.handleOrderEvent(...)` (same
  package's domain class, existing, unchanged), the
  `com.sanjay.ftgo:ftgo-order-service-contracts` artifact (Task 2/4).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add Stub Runner dependency**

```groovy
// ftgo-order-history-service/build.gradle — add to dependencies {}
testImplementation project(path: ':ftgo-order-service-contracts')
testImplementation 'org.springframework.cloud:spring-cloud-starter-contract-stub-runner'
```

- [ ] **Step 2: Write the consumer-side test**

```java
// ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListenerContractTest.java
package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderEventListenerContractTest {

    // The contract's example body (Task 4, Step 1) as literal JSON, matching what Stub Runner
    // would replay from the published contract for a messaging interaction of this kind. This
    // directly feeds the example into the real listener method rather than requiring a live
    // embedded broker on the consumer side, since OrderEventListener.onMessage() takes the raw
    // payload string directly -- the same boundary Ch.9's KitchenEventListenerTest already
    // exercises for a sibling listener.
    private static final String ORDER_CREATED_CONTRACT_BODY = """
            {"eventId":"11111111-1111-1111-1111-111111111111","eventType":"OrderCreated",
             "orderId":1223232,"consumerId":1,"restaurantId":1,
             "lineItems":[{"menuItemId":10,"quantity":2}]}
            """;

    @Test
    void invokesOrderViewServiceWithFieldsFromTheContract() {
        OrderViewService orderViewService = mock(OrderViewService.class);
        OrderEventListener listener = new OrderEventListener(orderViewService, new ObjectMapper());

        listener.onMessage(ORDER_CREATED_CONTRACT_BODY);

        ArgumentCaptor<List<OrderViewLineItem>> lineItemsCaptor = ArgumentCaptor.forClass(List.class);
        verify(orderViewService).handleOrderEvent(
                org.mockito.ArgumentMatchers.eq("11111111-1111-1111-1111-111111111111"),
                org.mockito.ArgumentMatchers.eq("OrderCreated"),
                org.mockito.ArgumentMatchers.eq(1223232L),
                org.mockito.ArgumentMatchers.eq(1L),
                org.mockito.ArgumentMatchers.eq(1L),
                lineItemsCaptor.capture());
        assertEquals(List.of(new OrderViewLineItem(10L, 2)), lineItemsCaptor.getValue());
    }
}
```

Check `OrderViewService.handleOrderEvent`'s actual parameter order/types
(`ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/domain/OrderViewService.java`)
before finalizing the `verify(...)` call above — this listing follows the
call site already shown in `OrderEventListener.java` above
(`orderViewService.handleOrderEvent(event.eventId(), event.eventType(),
event.orderId(), event.consumerId(), event.restaurantId(), lineItems)`),
so the parameter order should already match, but confirm the method's
declared parameter types (`String` vs `UUID` for `eventId`, etc.) line up
with the `eq(...)` matchers used.

**Note:** this test hand-codes the contract's JSON body as a literal string
rather than resolving it through Stub Runner's generated stub artifacts,
because `OrderEventListener` is invoked directly with a raw string payload
(no messaging-stub infrastructure needed on this side — unlike Task 3's
REST case, which genuinely needs a running WireMock stub). The `testImplementation`
dependencies added in Step 1 exist so a future consumer-side test that
does need Stub Runner's contract resolution (e.g. asserting the JSON shape
itself, not just this listener's handling of it) has the artifact on the
classpath; this task's literal-string approach is the pragmatic choice
given `OrderEventListener`'s actual method signature.

- [ ] **Step 3: Run the test**

Run: `./gradlew :ftgo-order-history-service:test --tests OrderEventListenerContractTest`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-history-service/build.gradle \
  ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/infrastructure/OrderEventListenerContractTest.java
git commit -m "test: add consumer-side pub/sub contract test for OrderEventListener"
```

---

### Task 6: Async request/response contract — `ftgo-kitchen-service-contracts` module + both sides

**Files:**
- Modify: `settings.gradle`
- Create: `ftgo-kitchen-service-contracts/build.gradle`
- Create: `ftgo-kitchen-service-contracts/src/main/resources/contracts/kitchen/shouldCreateTicket.groovy`
- Modify: `ftgo-kitchen-service/build.gradle`
- Create: `ftgo-kitchen-service/src/contractTest/java/com/sanjay/ftgo/kitchen/contracttest/MessagingBase.java`
- Modify: `ftgo-order-service/build.gradle`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxSagaCommandPublisherContractTest.java`

**Interfaces:**
- Consumes: `KitchenCommandListener.onMessage(String payload)`
  (`ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/infrastructure/KitchenCommandListener.java`,
  existing, unchanged), `TicketService.handleCreateTicketCommand(String
  eventId, Long orderId, Integer totalQuantity)` (same service's domain
  class, existing, unchanged), `OutboxSagaCommandPublisher.publish(String
  topic, String eventId, String eventType, Long orderId, Object command)`
  (`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OutboxSagaCommandPublisher.java`,
  existing, unchanged), `KafkaContractTestSupport` (Task 1).
- Produces: nothing consumed by later tasks (last task in this plan besides
  documentation).

The book models this contract from the provider's perspective (Kitchen
Service). The input message is the `CreateTicket` command on
`kitchen.commands`; the output message is the `TicketCreated` reply on
`saga.replies`, distinguished from other participants' replies by
`participant: "kitchen"`.

- [ ] **Step 1: Register the new module**

```groovy
// settings.gradle — add after ftgo-order-service-contracts
include 'ftgo-kitchen-service-contracts'
```

- [ ] **Step 2: Create the contracts module**

```groovy
// ftgo-kitchen-service-contracts/build.gradle
apply plugin: 'java-library'

bootJar {
    enabled = false
}

jar {
    enabled = true
}
```

- [ ] **Step 3: Write the async request/response contract**

```groovy
// ftgo-kitchen-service-contracts/src/main/resources/contracts/kitchen/shouldCreateTicket.groovy
package contracts.kitchen

import org.springframework.cloud.contract.spec.Contract

Contract.make {
    label 'shouldCreateTicket'
    input {
        messageFrom('kitchen.commands')
        messageBody(
                eventId: '22222222-2222-2222-2222-222222222222',
                commandType: 'CreateTicket',
                orderId: 1223232,
                totalQuantity: 2,
                sagaType: 'CreateOrder'
        )
        triggeredBy('createTicketCommandReceived()')
    }
    outputMessage {
        sentTo('saga.replies')
        body(
                eventId: $(consumer(regex('[0-9a-f-]{36}')), producer('33333333-3333-3333-3333-333333333333')),
                participant: 'kitchen',
                eventType: 'TicketCreated',
                orderId: 1223232,
                reason: null,
                sagaType: 'CreateOrder'
        )
    }
}
```

This mirrors `KitchenCommand`'s fields (`eventId`/`commandType`/`orderId`/`totalQuantity`/`sagaType`,
see `KitchenCommand.java` above) for the input, and `SagaReply`'s fields
(`eventId`/`participant`/`eventType`/`orderId`/`reason`/`sagaType`, per the
`new SagaReply(eventId, "kitchen", eventType, orderId, reason, sagaType)`
construction in `TicketService.publishReply` above) for the output —
confirm `SagaReply`'s actual declared field order matches this constructor
call (`ftgo-kitchen-service/src/main/java/com/sanjay/ftgo/kitchen/domain/SagaReply.java`)
before finalizing the contract body's field names.

- [ ] **Step 4: Wire up Spring Cloud Contract in `ftgo-kitchen-service/build.gradle`**

```groovy
// ftgo-kitchen-service/build.gradle — add to the existing file
apply plugin: 'org.springframework.cloud.contract'

dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

contracts {
    baseClassForTests = 'com.sanjay.ftgo.kitchen.contracttest.MessagingBase'
    packageWithBaseClasses = 'com.sanjay.ftgo.kitchen.contracttest'
}

dependencies {
    testImplementation project(':ftgo-kitchen-service-contracts')
    testImplementation testFixtures(project(':ftgo-common'))
    testImplementation 'org.springframework.cloud:spring-cloud-contract-verifier'
}
```

(Keep whatever dependencies already exist in this file — this is additive.)

- [ ] **Step 5: Write the provider-side messaging base class**

```java
// ftgo-kitchen-service/src/contractTest/java/com/sanjay/ftgo/kitchen/contracttest/MessagingBase.java
package com.sanjay.ftgo.kitchen.contracttest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.contracttest.KafkaContractTestSupport;
import com.sanjay.ftgo.kitchen.domain.KitchenCommand;
import com.sanjay.ftgo.kitchen.infrastructure.KitchenCommandListener;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier;
import org.springframework.context.annotation.Import;

@SpringBootTest
@AutoConfigureMessageVerifier
@Import(KafkaContractTestSupport.class)
public abstract class MessagingBase {

    @Autowired
    private KitchenCommandListener kitchenCommandListener;

    @Autowired
    private ObjectMapper objectMapper;

    protected void createTicketCommandReceived() throws Exception {
        KitchenCommand command = new KitchenCommand(
                "22222222-2222-2222-2222-222222222222", "CreateTicket", 1223232L, 2, "CreateOrder");
        kitchenCommandListener.onMessage(objectMapper.writeValueAsString(command));
    }
}
```

`KitchenCommandListener` is guarded by
`@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")`
(see `KitchenCommandListener.java` above) — if `@SpringBootTest` doesn't
pick up that property by default in this project's test configuration,
check `ftgo-kitchen-service/src/test/resources/application*.yml` for how
the existing `KitchenCommandListenerTest` (Ch.9-referenced) already
activates orchestration mode for its own tests, and apply the same
`@TestPropertySource`/profile setup here. `TicketService.publishReply`
writes to the outbox (see above), so this base class relies on the same
outbox-to-Kafka relay concern as Task 4 — if the provider-side generated
test doesn't see the reply on `saga.replies` without an explicit relay
step, add an `outboxPublisher.publishPendingEvents()` call (autowire
`OutboxPublisher` the same way Task 4's `MessagingBase` does) at the end of
`createTicketCommandReceived()`.

- [ ] **Step 6: Run the generated provider-side test**

Run: `./gradlew :ftgo-kitchen-service:contractTest`
Expected: PASS.

- [ ] **Step 7: Write the consumer-side test in `ftgo-order-service`**

```groovy
// ftgo-order-service/build.gradle — add to the existing dependencies {} block
testImplementation project(':ftgo-kitchen-service-contracts')
```

```java
// ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxSagaCommandPublisherContractTest.java
package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.order.domain.KitchenCommand;
import com.sanjay.ftgo.order.domain.OutboxSagaCommandPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OutboxSagaCommandPublisherContractTest {

    // The contract's example input message (Task 6, Step 3) — verifying that this consumer's
    // publish() call produces exactly this JSON shape confirms it stays compatible with what
    // KitchenCommandListener (the real provider, per the shared contract) expects to receive.
    @Test
    void publishesCreateTicketCommandMatchingTheContract() {
        OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
        OutboxSagaCommandPublisher publisher = new OutboxSagaCommandPublisher(outboxEventRepository, new ObjectMapper());

        KitchenCommand command = new KitchenCommand(
                "22222222-2222-2222-2222-222222222222", "CreateTicket", 1223232L, 2, "CreateOrder");
        publisher.publish("kitchen.commands", "22222222-2222-2222-2222-222222222222", "CreateTicket", 1223232L, command);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertEquals("kitchen.commands", captor.getValue().getTopic());
        assertEquals(
                "{\"eventId\":\"22222222-2222-2222-2222-222222222222\",\"commandType\":\"CreateTicket\","
                        + "\"orderId\":1223232,\"totalQuantity\":2,\"sagaType\":\"CreateOrder\"}",
                captor.getValue().getPayload());
    }
}
```

Confirm `OutboxEvent`'s actual getter names (`getTopic()`/`getPayload()`)
match what's used in this project's existing outbox tests (e.g. Ch.9's
`DeliveryServiceTest` `ArgumentCaptor<OutboxEvent>` usage,
`ftgo-delivery-service/src/test/java/.../domain/DeliveryServiceTest.java`)
before finalizing.

- [ ] **Step 8: Run the consumer-side test**

Run: `./gradlew :ftgo-order-service:test --tests OutboxSagaCommandPublisherContractTest`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add settings.gradle ftgo-kitchen-service-contracts ftgo-kitchen-service/build.gradle \
  ftgo-kitchen-service/src/contractTest ftgo-order-service/build.gradle \
  ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxSagaCommandPublisherContractTest.java
git commit -m "test: add async request/response consumer-driven contract for CreateTicket"
```

---

### Task 7: Documentation sync

**Files:**
- Modify: `CONTEXT.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: nothing (documentation only).
- Produces: nothing.

- [ ] **Step 1: Update `CONTEXT.md`'s patterns-reference table**

Find the `### Testing` section (the same one Ch.9 corrected, moving
"Consumer-driven contract test" to the Ch.10 row — see Ch.9's session log
entry, 2026-07-30). Check off that line now that it's implemented, and add
a one-line note on the messaging-contract adaptation (custom
`MessageVerifierSender`/`Receiver` backed by embedded Kafka, since this
project isn't on Spring Cloud Stream/Eventuate Tram).

- [ ] **Step 2: Update `CONTEXT.md`'s book-progress table**

Change the Ch.10 row (added during Ch.9's doc sweep as "Not started",
grouped with 11–13) to its own row: "Testing microservices: Part 2" —
"In progress — sub-project 1 of 3 (consumer-driven contract tests) done:
REST contract (API Gateway↔Order Service), pub/sub contract (Order
Service→Order History), async request/response contract (Order
Service↔Kitchen Service). Component tests and end-to-end tests
(sub-projects 2–3) not yet started."

- [ ] **Step 3: Update `CONTEXT.md`'s "Current position" and session log**

Follow the same pattern as every prior session entry in this file (see the
2026-07-30 Ch.9 entry for the established format) — summarize what sub-project
1 added, note the two remaining sub-projects, and update the "Last updated"
footer timestamp.

- [ ] **Step 4: Update `README.md`'s Book progress table**

The Ch.9 doc-sync session split the old "9–13 | … | Not started" row into
"9 | … | Done" and "10–13 | … | Not started" (see the README diff from that
session). Split further: "10 | Testing microservices: Part 2 | In progress
— sub-project 1 of 3 (consumer-driven contract tests) done" and "11–13 | …
| Not started".

- [ ] **Step 5: Commit**

```bash
git add CONTEXT.md README.md
git commit -m "docs: record Ch.10 sub-project 1 (contract tests) completion"
```

## Self-Review Notes

- **Spec coverage:** all three contracts from the spec's Scope section
  (REST §10.1.2, pub/sub §10.1.3, async request/response §10.1.4) have a
  task; the shared messaging bridge (spec's "Messaging bridge" callout) is
  Task 1, built once and reused by Tasks 4 and 6 as the spec requires;
  documentation sync is Task 7, matching this project's established
  per-change-docs convention.
- **Placeholder scan:** no TBD/TODO markers. Several steps (Task 1 Step 1,
  Task 3 Step 2, Task 5 Step 2, Task 6 Step 5) explicitly flag places where
  this plan's code is a best-confidence draft against a library API or
  existing class this plan's author couldn't fully verify byte-for-byte
  (exact `MessageVerifierSender`/`Receiver` signatures, `Order`'s
  constructor, `SagaReply`'s field order, `ReactiveCircuitBreakerFactory`'s
  shape) — each gives a concrete verification command or fallback, which is
  a legitimate "confirm against the real dependency" step rather than a
  vague "add appropriate handling" placeholder.
- **Type consistency:** `KitchenCommand`'s five fields
  (`eventId`/`commandType`/`orderId`/`totalQuantity`/`sagaType`) are used
  identically across Task 6's contract, provider base class, and consumer
  test. `OrderEvent`'s six fields are used identically across Task 4's
  contract and Task 5's consumer test. `KafkaContractTestSupport`'s bean
  names (`contractTestKafkaTemplate`, `contractTestKafkaConsumer`,
  `kafkaMessageVerifierSender`, `kafkaMessageVerifierReceiver`) from Task 1
  are referenced identically wherever Tasks 4 and 6 `@Import` it.
