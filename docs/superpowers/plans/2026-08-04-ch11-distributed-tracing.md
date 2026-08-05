# Ch.11 §11.3.3 Distributed Tracing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Instrument all 9 FTGO services (7 business services + 2 gateways) with Micrometer Tracing + the OpenTelemetry bridge, exporting spans via OTLP to a new Grafana Tempo backend, so a single external request produces one trace spanning every HTTP and Kafka hop it touches, verified by a new Cucumber e2e scenario that queries Tempo's API.

**Architecture:** Tracing dependencies and `management.tracing`/`management.otlp` config are added centrally the same way `spring-boot-starter-actuator` already is (root `build.gradle`'s `actuatorModules` list + shared `application.yml` `management:` block per service). HTTP/JDBC spans come from Spring Boot autoconfiguration for free. Kafka spans come from Boot 3.5's built-in Micrometer Observation instrumentation, activated via `spring.kafka.template.observation-enabled` / `spring.kafka.listener.observation-enabled` properties — except `ftgo-order-history-service`'s hand-built `KafkaListenerContainerFactory` bean, which needs one explicit line to opt in. Gateway reactive context propagation relies on Spring Boot's `ContextPropagationAutoConfiguration` (auto-enables `Hooks.enableAutomaticContextPropagation()` once `io.micrometer:context-propagation` is on the classpath) — verified, not hand-coded. A new `tempo` compose service and Grafana datasource complete the backend; a new Cucumber scenario proves the whole chain end to end.

**Tech Stack:** Spring Boot 3.5.16, Micrometer Tracing (`micrometer-tracing-bridge-otel`), OpenTelemetry OTLP exporter (`opentelemetry-exporter-otlp`), Grafana Tempo (`grafana/tempo:2.6.1`), Grafana 11.3.1 (already present), Gradle multi-project build, Cucumber/JUnit e2e suite (`ftgo-end-to-end-test`).

## Global Constraints

- Spring Boot version is `3.5.16` (root `build.gradle:2`) — do not assume a different minor version's defaults.
- `management.tracing.sampling.probability: 1.0` (100% sampling) in every instrumented service — this is a learning project, not production traffic; guarantees the e2e scenario's trace is always exported.
- `management.otlp.tracing.endpoint: http://tempo:4318/v1/traces` — OTLP/HTTP, not gRPC.
- The `tempo` compose service must NOT be on any business service's `depends_on: condition: service_healthy` critical path — same rule already followed for `prometheus`/`grafana`. Business services get `depends_on: tempo: condition: service_started` (best-effort).
- Docker builds in this sandbox must be done sequentially per-service (`docker compose build <service>` one at a time) — parallel/full builds OOM the ~7.75GiB sandbox. Bring the stack up manually, then run e2e tests with `./gradlew :ftgo-end-to-end-test:e2eTest -x composeUp` against the already-running stack (Gradle's own `docker-compose` plugin causes duplicate-rebuild OOM otherwise).
- No manual span code — only Kafka observation-enable properties/config and gateway context-propagation verification count as "explicit config," per the spec.
- Per-change documentation (not a full chapter sweep — §11.3 stays open after this) lands in the same commits as the code it describes, per this project's `CLAUDE.md`.

---

### Task 1: Tracing dependencies and OTLP config for all 9 services

**Files:**
- Modify: `build.gradle:69-77` (root)
- Modify: `ftgo-order-service/src/main/resources/application.yml:75-82` (management block)
- Modify: `ftgo-kitchen-service/src/main/resources/application.yml` (management block)
- Modify: `ftgo-consumer-service/src/main/resources/application.yml` (management block)
- Modify: `ftgo-restaurant-service/src/main/resources/application.yml` (management block)
- Modify: `ftgo-accounting-service/src/main/resources/application.yml` (management block)
- Modify: `ftgo-delivery-service/src/main/resources/application.yml` (management block)
- Modify: `ftgo-order-history-service/src/main/resources/application.yml` (management block)
- Modify: `ftgo-mobile-gateway/src/main/resources/application.yml` (management block)
- Modify: `ftgo-public-gateway/src/main/resources/application.yml` (management block)
- Test: `ftgo-order-service/src/test/java/.../OrderServiceApplicationTests.java` (or existing Spring context test — used to verify the context still loads with tracing beans)

**Interfaces:**
- Consumes: nothing from earlier tasks (first task).
- Produces: every one of the 9 modules now has `io.micrometer:micrometer-tracing-bridge-otel` and `io.opentelemetry:opentelemetry-exporter-otlp` on the classpath, and `management.tracing.sampling.probability` / `management.otlp.tracing.endpoint` set. Later tasks (Kafka, gateway propagation, Tempo compose service) depend on these being present.

- [ ] **Step 1: Add tracing dependencies to the root `build.gradle`'s `actuatorModules` block**

Edit `build.gradle` lines 73-77 from:
```groovy
configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        implementation 'io.micrometer:micrometer-registry-prometheus'
    }
}
```
to:
```groovy
configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        implementation 'io.micrometer:micrometer-registry-prometheus'
        implementation 'io.micrometer:micrometer-tracing-bridge-otel'
        implementation 'io.opentelemetry:opentelemetry-exporter-otlp'
    }
}
```
The `actuatorModules` list already names all 9 target services/gateways — no change needed there.

- [ ] **Step 2: Add tracing config to each of the 9 services' `management:` block**

Every service's `application.yml` has this identical block (confirmed via grep across all 9 files; `ftgo-order-service/src/main/resources/application.yml:75-82` shown as the template):
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, prometheus
  endpoint:
    health:
      show-details: always
```
Change it to (adding two new keys, same indentation, keeping the existing `endpoints`/`endpoint` keys untouched):
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, prometheus
  endpoint:
    health:
      show-details: always
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: http://tempo:4318/v1/traces
```
Apply this identical edit to all 9 files:
- `ftgo-order-service/src/main/resources/application.yml`
- `ftgo-kitchen-service/src/main/resources/application.yml`
- `ftgo-consumer-service/src/main/resources/application.yml`
- `ftgo-restaurant-service/src/main/resources/application.yml`
- `ftgo-accounting-service/src/main/resources/application.yml`
- `ftgo-delivery-service/src/main/resources/application.yml`
- `ftgo-order-history-service/src/main/resources/application.yml`
- `ftgo-mobile-gateway/src/main/resources/application.yml`
- `ftgo-public-gateway/src/main/resources/application.yml`

(If a given file's `management:` block differs slightly in what other keys it has, preserve those keys — only add the `tracing:` and `otlp:` subkeys shown above.)

- [ ] **Step 3: Verify the build compiles and existing Spring context tests still pass**

Run: `./gradlew :ftgo-order-service:test :ftgo-kitchen-service:test :ftgo-consumer-service:test :ftgo-restaurant-service:test :ftgo-accounting-service:test :ftgo-delivery-service:test :ftgo-order-history-service:test :ftgo-mobile-gateway:test :ftgo-public-gateway:test`

Expected: all existing tests pass (this confirms the new tracing beans don't break application context startup — Spring Boot autoconfigures `ObservationAutoConfiguration`/`OpenTelemetryAutoConfiguration` beans automatically once the dependencies are present, no explicit bean wiring needed).

- [ ] **Step 4: Commit**

```bash
git add build.gradle ftgo-order-service/src/main/resources/application.yml ftgo-kitchen-service/src/main/resources/application.yml ftgo-consumer-service/src/main/resources/application.yml ftgo-restaurant-service/src/main/resources/application.yml ftgo-accounting-service/src/main/resources/application.yml ftgo-delivery-service/src/main/resources/application.yml ftgo-order-history-service/src/main/resources/application.yml ftgo-mobile-gateway/src/main/resources/application.yml ftgo-public-gateway/src/main/resources/application.yml
git commit -m "feat: add Micrometer Tracing + OTLP export to all 9 services"
```

---

### Task 2: Tempo compose service and Grafana datasource

**Files:**
- Create: `tempo/tempo.yaml`
- Modify: `compose.yml` (add `tempo` service block; add `depends_on: tempo: condition: service_started` to the 9 business/gateway service blocks)
- Create: `grafana/provisioning/datasources/tempo.yml`

**Interfaces:**
- Consumes: nothing new from Task 1 directly, but the `tempo` service this task creates is the OTLP receiver that Task 1's `management.otlp.tracing.endpoint: http://tempo:4318/v1/traces` config in every service points at.
- Produces: a running `tempo` container reachable at `http://tempo:4318` (OTLP ingest, from other containers) and `http://localhost:3200` (query API, from the host — used by Task 5's e2e test).

- [ ] **Step 1: Create the Tempo config file**

Create `tempo/tempo.yaml`:
```yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        http:
        grpc:

storage:
  trace:
    backend: local
    local:
      path: /var/tempo/traces
    wal:
      path: /var/tempo/wal

compactor:
  compaction:
    block_retention: 24h
```

- [ ] **Step 2: Add the `tempo` service block to `compose.yml`**

Insert this block immediately before the existing `grafana:` service block (which currently follows `prometheus:` per `compose.yml:350-376`):
```yaml
  tempo:
    image: grafana/tempo:2.6.1
    command: ["-config.file=/etc/tempo.yaml"]
    volumes:
      - ./tempo/tempo.yaml:/etc/tempo.yaml:ro
    ports:
      - "3200:3200"
      - "4318:4318"
```

- [ ] **Step 3: Add `tempo` as a best-effort dependency on all 9 business/gateway services**

For each of the 9 service blocks in `compose.yml` (`order-service`, `kitchen-service`, `consumer-service`, `restaurant-service`, `accounting-service`, `delivery-service`, `order-history-service`, `mobile-gateway`, `public-gateway`), add `tempo: condition: service_started` to its existing `depends_on:` map (do not touch any existing `condition: service_healthy` entries — only add the new line alongside them). E.g. if `order-service`'s current `depends_on:` looks like:
```yaml
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_healthy
```
change it to:
```yaml
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_healthy
      tempo:
        condition: service_started
```
Apply the same `tempo: condition: service_started` addition to the other 8 services' `depends_on:` maps.

- [ ] **Step 4: Add the Tempo datasource provisioning file**

Create `grafana/provisioning/datasources/tempo.yml` (mirroring the existing `grafana/provisioning/datasources/prometheus.yml`):
```yaml
apiVersion: 1

datasources:
  - name: Tempo
    type: tempo
    access: proxy
    url: http://tempo:3200
    isDefault: false
    editable: false
```

- [ ] **Step 5: Verify Tempo starts and accepts OTLP**

Run:
```bash
docker compose build order-service
docker compose up -d tempo order-service mysql kafka zookeeper
sleep 15
curl -s http://localhost:3200/status/version
docker compose down
```
Expected: `curl` returns a JSON/text response with a Tempo version string (not connection-refused), confirming the query API on port 3200 is reachable from the host.

- [ ] **Step 6: Commit**

```bash
git add tempo/tempo.yaml compose.yml grafana/provisioning/datasources/tempo.yml
git commit -m "feat: add Grafana Tempo compose service and datasource"
```

---

### Task 3: Kafka span propagation

**Files:**
- Modify: `ftgo-order-service/src/main/resources/application.yml` (add `spring.kafka.template.observation-enabled: true`)
- Modify: `ftgo-kitchen-service/src/main/resources/application.yml` (add `spring.kafka.template.observation-enabled: true` and `spring.kafka.listener.observation-enabled: true`)
- Modify: `ftgo-accounting-service/src/main/resources/application.yml` (same as kitchen)
- Modify: `ftgo-delivery-service/src/main/resources/application.yml` (same as kitchen)
- Modify: `ftgo-order-history-service/src/main/resources/application.yml` (same as kitchen)
- Modify: `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/config/KafkaConsumerConfig.java`
- Test: `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/config/KafkaConsumerConfigTest.java`

**Interfaces:**
- Consumes: the `spring.kafka.*` block already present in each service's `application.yml` (e.g. `ftgo-order-service/src/main/resources/application.yml:11-17`, `bootstrap-servers`/`consumer.group-id`/etc.) — this task adds sibling keys, doesn't restructure it.
- Produces: `KafkaConsumerConfig.kafkaListenerContainerFactory(ConsumerFactory<Object, Object>)` bean (unchanged signature) now returns a factory with observation enabled — later tasks/tests can rely on `factory.getContainerProperties().isObservationEnabled() == true`.

- [ ] **Step 1: Enable Kafka producer observation for `ftgo-order-service`**

`ftgo-order-service` only produces to Kafka (via `SagaCommandRequestPublisher`'s autoconfigured `KafkaTemplate<String, String>` — no listener). Add to its `spring.kafka:` block in `application.yml` (right after the existing `consumer:` subkeys, e.g. after line 17 `auto-offset-reset: earliest`):
```yaml
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
    template:
      observation-enabled: true
```
(Only add the `template.observation-enabled` key — do not add `listener.observation-enabled` here since order-service has no `@KafkaListener`.)

- [ ] **Step 2: Enable both producer and consumer observation for `ftgo-kitchen-service`, `ftgo-accounting-service`, `ftgo-delivery-service`**

Each of these three services both produces and consumes domain events via autoconfigured Kafka beans (no custom `KafkaConsumerConfig`-style bean). Add to each one's `spring.kafka:` block:
```yaml
    template:
      observation-enabled: true
    listener:
      observation-enabled: true
```
(Preserve each file's existing `bootstrap-servers`/`consumer:` subkeys — only add `template:` and `listener:` as new sibling keys under `kafka:`.)

- [ ] **Step 3: Enable producer observation for `ftgo-order-history-service`'s `spring.kafka` block**

`ftgo-order-history-service` only consumes (no publish path), but add the `listener.observation-enabled: true` property alongside the existing `spring.kafka:` config for consistency and to document intent, even though Step 4 below is what actually flips it on for the manually-built factory:
```yaml
    listener:
      observation-enabled: true
```

- [ ] **Step 4: Write a failing test asserting the custom listener factory has observation enabled**

Create `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/config/KafkaConsumerConfigTest.java`:
```java
package com.sanjay.ftgo.orderhistory.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigTest {

    @Test
    void listenerContainerFactoryHasObservationEnabled() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "test-group");
        ConsumerFactory<Object, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        KafkaConsumerConfig config = new KafkaConsumerConfig();
        ConcurrentKafkaListenerContainerFactory<?> factory =
                (ConcurrentKafkaListenerContainerFactory<?>) config.kafkaListenerContainerFactory(consumerFactory);

        assertThat(factory.getContainerProperties().isObservationEnabled()).isTrue();
    }
}
```

- [ ] **Step 2 (test): Run it to verify it fails**

Run: `./gradlew :ftgo-order-history-service:test --tests "com.sanjay.ftgo.orderhistory.config.KafkaConsumerConfigTest"`
Expected: FAIL — `isObservationEnabled()` returns `false` (the current bean never calls the setter).

- [ ] **Step 5: Enable observation on the manually-built factory**

Edit `ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/config/KafkaConsumerConfig.java`, in the `kafkaListenerContainerFactory` method, add one line after `factory.setConsumerFactory(consumerFactory);`:
```java
    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Hand-built factory bypasses Boot's spring.kafka.listener.observation-enabled property,
        // which only applies to the autoconfigured factory this bean replaces - must opt in explicitly.
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(200L, 3)));
        return factory;
    }
```

- [ ] **Step 6: Run the test again to verify it passes**

Run: `./gradlew :ftgo-order-history-service:test --tests "com.sanjay.ftgo.orderhistory.config.KafkaConsumerConfigTest"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/resources/application.yml ftgo-kitchen-service/src/main/resources/application.yml ftgo-accounting-service/src/main/resources/application.yml ftgo-delivery-service/src/main/resources/application.yml ftgo-order-history-service/src/main/resources/application.yml ftgo-order-history-service/src/main/java/com/sanjay/ftgo/orderhistory/config/KafkaConsumerConfig.java ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/config/KafkaConsumerConfigTest.java
git commit -m "feat: enable Kafka producer/consumer tracing observation"
```

---

### Task 4: Verify gateway reactive context propagation

**Files:**
- Modify: `ftgo-mobile-gateway/src/test/java/**` (new test, exact file created in Step 1 below)
- Modify: `ftgo-public-gateway/src/test/java/**` (new test, exact file created in Step 1 below)

**Interfaces:**
- Consumes: `io.micrometer:context-propagation`, which is a transitive dependency of `io.micrometer:micrometer-tracing-bridge-otel` (added to both gateway modules in Task 1) — no new dependency declaration needed.
- Produces: confirmation (via test + manual curl) that trace context survives `RequestLoggingFilter`/`JwtValidationFilter` in `ftgo-gateway-common`; no new bean or interface for other tasks to consume — this task is verification-only. If verification fails, Step 4 below is the fallback code change.

- [ ] **Step 1: Confirm `context-propagation` is on the classpath for both gateways**

Run: `./gradlew :ftgo-mobile-gateway:dependencies :ftgo-public-gateway:dependencies --configuration runtimeClasspath | grep context-propagation`

Expected: `io.micrometer:context-propagation` appears in both modules' dependency trees (pulled in transitively via `micrometer-tracing-bridge-otel`).

- [ ] **Step 2: Write a test verifying `Hooks.isEnableAutomaticContextPropagation()` is true when the gateway context starts**

Create `ftgo-public-gateway/src/test/java/com/sanjay/ftgo/publicgateway/ContextPropagationTest.java`:
```java
package com.sanjay.ftgo.publicgateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Hooks;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ContextPropagationTest {

    @Test
    void automaticContextPropagationIsEnabled() {
        assertThat(Hooks.isEnableAutomaticContextPropagation()).isTrue();
    }
}
```

- [ ] **Step 3: Run the test**

Run: `./gradlew :ftgo-public-gateway:test --tests "com.sanjay.ftgo.publicgateway.ContextPropagationTest"`

Expected: PASS, because Spring Boot 3.5.16's `org.springframework.boot.autoconfigure.reactor.ContextPropagationAutoConfiguration` calls `Hooks.enableAutomaticContextPropagation()` automatically once `io.micrometer:context-propagation` and `reactor-core` are both present — both are already satisfied (reactor-core via `spring-cloud-starter-gateway`, context-propagation via Task 1's tracing dependency).

If the test instead FAILS (propagation not auto-enabled in this environment), add a fallback bean to `ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/GatewayCommonAutoConfiguration.java`:
```java
    @Bean
    public InitializingBean enableReactorContextPropagation() {
        return () -> reactor.core.publisher.Hooks.enableAutomaticContextPropagation();
    }
```
(add the matching `org.springframework.beans.factory.InitializingBean` import), then re-run Step 3 until it passes.

- [ ] **Step 4: Repeat Steps 2-3 for `ftgo-mobile-gateway`**

Create the analogous `ftgo-mobile-gateway/src/test/java/com/sanjay/ftgo/mobilegateway/ContextPropagationTest.java` (same body, package `com.sanjay.ftgo.mobilegateway`), and run:
`./gradlew :ftgo-mobile-gateway:test --tests "com.sanjay.ftgo.mobilegateway.ContextPropagationTest"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ftgo-public-gateway/src/test/java/com/sanjay/ftgo/publicgateway/ContextPropagationTest.java ftgo-mobile-gateway/src/test/java/com/sanjay/ftgo/mobilegateway/ContextPropagationTest.java
git commit -m "test: verify reactor automatic context propagation is enabled in both gateways"
```
(If Step 3's fallback bean was needed, also `git add ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/GatewayCommonAutoConfiguration.java` to the same commit.)

---

### Task 5: End-to-end Cucumber scenario verifying a multi-service trace

**Files:**
- Modify: `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`
- Modify: `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/PlaceReviseCancelOrderStepDefinitions.java`

**Interfaces:**
- Consumes: the existing `Given a restaurant "..." with a menu item "..." priced at ...`, `And an active consumer "..."`, `When the consumer places an order for 1 of the menu item at the restaurant`, `Then the order is eventually approved` steps already implemented in `PlaceReviseCancelOrderStepDefinitions.java`; the running `tempo` container's query API on `http://localhost:3200` (from Task 2).
- Produces: nothing consumed by later tasks (last functional task) — Task 6 is docs-only.

- [ ] **Step 1: Add the new scenario to the feature file**

Add to `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`, after the existing "Placing and approving an order increments the order-service Prometheus counters" scenario:
```gherkin
  Scenario: Placing and approving an order produces a single trace spanning multiple services
    Given a restaurant "Ajanta Tracing E2E" with a menu item "Lamb Biryani" priced at 14.00
    And an active consumer "Tracing E2E Consumer"
    When the consumer places an order for 1 of the menu item at the restaurant
    Then the order is eventually approved
    And Tempo eventually has a trace for "ftgo-public-gateway" spanning at least 2 distinct services
```

- [ ] **Step 2: Add the step definition**

Add to `PlaceReviseCancelOrderStepDefinitions.java`, alongside the existing helper methods (near `counterEventuallyAtLeastOne`). First add these imports at the top of the file if not already present:
```java
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
```
Then add the step method and its two helpers:
```java
    @Then("Tempo eventually has a trace for {string} spanning at least 2 distinct services")
    public void tempoEventuallyHasMultiServiceTrace(String serviceName) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        long startNanos = Instant.now().minus(Duration.ofMinutes(2)).getEpochSecond();
        Set<String> distinctServices = Set.of();
        while (Instant.now().isBefore(deadline)) {
            String traceId = findRecentTraceId(serviceName, startNanos);
            if (traceId != null) {
                distinctServices = fetchTraceServiceNames(traceId);
                if (distinctServices.size() >= 2) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("Expected a Tempo trace for " + serviceName
                + " spanning >= 2 services, last saw: " + distinctServices);
    }

    private String findRecentTraceId(String serviceName, long startEpochSeconds) throws Exception {
        long endEpochSeconds = Instant.now().getEpochSecond();
        String query = URLEncoder.encode(
                "{resource.service.name=\"" + serviceName + "\"}", StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:3200/api/search?q=" + query
                        + "&start=" + startEpochSeconds + "&end=" + endEpochSeconds))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            return null;
        }
        JsonNode root = objectMapper.readTree(response.body());
        JsonNode traces = root.path("traces");
        if (!traces.isArray() || traces.isEmpty()) {
            return null;
        }
        return traces.get(0).path("traceID").asText(null);
    }

    private Set<String> fetchTraceServiceNames(String traceId) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:3200/api/traces/" + traceId))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            return Set.of();
        }
        JsonNode root = objectMapper.readTree(response.body());
        Set<String> serviceNames = new HashSet<>();
        for (JsonNode resourceSpan : root.path("batches")) {
            for (JsonNode attribute : resourceSpan.path("resource").path("attributes")) {
                if ("service.name".equals(attribute.path("key").asText())) {
                    serviceNames.add(attribute.path("value").path("stringValue").asText());
                }
            }
        }
        return serviceNames;
    }
```
(This uses `objectMapper`/`httpClient` fields already present in the class, per the existing `counterEventuallyAtLeastOne` method.)

- [ ] **Step 3: Bring up the full stack manually and confirm builds are current**

Run (sequential builds to avoid sandbox OOM, per Global Constraints):
```bash
docker compose build --pull=never service-registry auth-server restaurant-service consumer-service order-service kitchen-service accounting-service delivery-service order-history-service mobile-gateway public-gateway
docker compose up -d
sleep 60
```

- [ ] **Step 4: Run the e2e suite against the running stack**

Run: `./gradlew :ftgo-end-to-end-test:e2eTest -x composeUp --tests "*PlaceReviseCancelOrder*"`

Expected: all scenarios in `PlaceReviseCancelOrder.feature` pass, including the new "produces a single trace spanning multiple services" scenario. If the new scenario fails because Tempo's actual JSON response field names differ from `batches`/`resource.attributes` (OTLP JSON schema can vary slightly by Tempo version), inspect the live response directly:
```bash
curl -s "http://localhost:3200/api/search?q=%7Bresource.service.name%3D%22ftgo-public-gateway%22%7D&start=$(date -d '2 minutes ago' +%s)&end=$(date +%s)" | head -c 2000
```
and adjust the JSON field paths in `fetchTraceServiceNames`/`findRecentTraceId` to match the actual shape observed, then re-run Step 4.

- [ ] **Step 5: Tear down the stack**

Run: `docker compose down`

- [ ] **Step 6: Commit**

```bash
git add ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/PlaceReviseCancelOrderStepDefinitions.java
git commit -m "test: add e2e scenario verifying a multi-service trace via Tempo API"
```

---

### Task 6: Documentation sweep

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Modify: `ftgo-order-service/README.md`, `ftgo-kitchen-service/README.md`, `ftgo-consumer-service/README.md`, `ftgo-restaurant-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-delivery-service/README.md`, `ftgo-order-history-service/README.md`, `ftgo-mobile-gateway/README.md`, `ftgo-public-gateway/README.md`
- Modify: `README.md` (root)
- Modify: `CONTEXT.md`

**Interfaces:**
- Consumes: the final state of Tasks 1-5 (dependency list, config keys, Tempo compose service, Kafka observation properties, the new e2e scenario) — this task only describes what already exists, no new interfaces.
- Produces: nothing consumed by other tasks (last task in the plan).

- [ ] **Step 1: Add a Distributed tracing section to `docs/ARCHITECTURE.md`**

Add a new `## Distributed tracing (Ch.11, §11.3.3)` section (placed alongside the existing observability sections, e.g. after the application-metrics section) covering: Micrometer Tracing + OTel bridge replacing Sleuth/Zipkin; OTLP export to Grafana Tempo at `http://tempo:4318/v1/traces`; 100% sampling rationale; automatic HTTP/JDBC span instrumentation via Spring Boot autoconfiguration; Kafka span propagation via `spring.kafka.template.observation-enabled`/`spring.kafka.listener.observation-enabled` (and the one-line opt-in needed for `ftgo-order-history-service`'s custom listener factory); gateway reactive context propagation via Spring Boot's `ContextPropagationAutoConfiguration`; the e2e verification approach (Tempo search API polling, mirroring the Prometheus-counter pattern from the metrics sub-project).

- [ ] **Step 2: Add a "Tracing" subsection to each of the 9 service/gateway READMEs**

For each of the 7 business-service READMEs and the 2 gateway READMEs, add a short "### Tracing" subsection (near any existing "Observability"/"Metrics" section) stating: traces exported via OTLP to Tempo, 100% sampled, viewable in Grafana's Tempo datasource; and, only for `ftgo-order-history-service`'s README, a note that its custom Kafka listener factory explicitly opts into observation since it bypasses Boot's autoconfigured listener factory.

- [ ] **Step 3: Update root `README.md`**

Add "Grafana Tempo" to the tech stack list (alongside the existing Prometheus/Grafana entries from the metrics sub-project) and update the Book progress table's Ch.11 row/notes to reflect distributed tracing (§11.3.3) as done, noting §11.3 overall remains open (log aggregation, exception tracking, audit logging still unscheduled).

- [ ] **Step 4: Update `CONTEXT.md`**

Update "Current position" to reflect distributed tracing complete; update the "Services to build"/progress table entry for Ch.11; update the "Patterns reference" checklist to mark Distributed Tracing done; add a session log entry dated with today's date describing this sub-project (Tempo backend choice, full 9-service+2-gateway scope, Kafka observation-property discovery, Cucumber verification approach).

- [ ] **Step 5: Commit**

```bash
git add docs/ARCHITECTURE.md ftgo-order-service/README.md ftgo-kitchen-service/README.md ftgo-consumer-service/README.md ftgo-restaurant-service/README.md ftgo-accounting-service/README.md ftgo-delivery-service/README.md ftgo-order-history-service/README.md ftgo-mobile-gateway/README.md ftgo-public-gateway/README.md README.md CONTEXT.md
git commit -m "docs: document Ch.11 §11.3.3 distributed tracing"
```
