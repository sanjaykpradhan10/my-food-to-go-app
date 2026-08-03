# Ch.11 Sub-Project 1: Health Checks (§11.3.1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give all 9 running FTGO services (7 business services + 2 gateways) a real `/actuator/health` endpoint backed by Spring Boot Actuator's auto-configured DataSource/Kafka/Eureka indicators, and wire Docker Compose to use it for startup ordering, per the book's Health check API pattern (§11.3.1).

**Architecture:** Add `spring-boot-starter-actuator` to the 9 target services via a new module-name allowlist in the root `build.gradle` (mirroring the existing `reactiveModules` pattern), expose `management.endpoints.web.exposure.include: health` and `management.endpoint.health.show-details: always` in each service's `application.yml`, then add Docker healthcheck blocks to `compose.yml` and upgrade real inter-service `depends_on` entries to `condition: service_healthy`. Verify with a lightweight per-service slice test (Task 1) and a full-stack Cucumber scenario in the existing `ftgo-end-to-end-test` module (Task 3) that asserts real `UP` status across every service once the whole stack is actually running.

**Tech Stack:** Spring Boot Actuator (auto-configured DataSource/Kafka/discovery-client indicators — no custom indicator code), Docker Compose healthchecks, Cucumber (existing `ftgo-end-to-end-test` module).

## Global Constraints

- Only these 9 services get Actuator: `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-restaurant-service`, `ftgo-accounting-service`, `ftgo-delivery-service`, `ftgo-order-history-service`, `ftgo-mobile-gateway`, `ftgo-public-gateway`. `ftgo-service-registry` is explicitly out of scope.
- Use only Spring Boot Actuator's auto-configured indicators (DataSource, Kafka, Eureka discovery-client). No hand-written `HealthIndicator` classes.
- `management.endpoint.health.show-details: always` (safe: no external/untrusted clients hit these ports in this project; showing indicator detail is the point of the exercise).
- Verified this session: `eclipse-temurin:21-jre` (the runtime base image for every service Dockerfile) already has both `curl` and `wget` installed — **no Dockerfile changes are needed** for the Compose healthchecks.
- Service ports (host == container in every case): order-service `8082`, kitchen-service `8083`, consumer-service `8081`, restaurant-service `8085`, accounting-service `8084`, delivery-service `8086`, order-history-service `8088`, mobile-gateway `8090`, public-gateway `8091`.
- This is sub-project 1 of Ch.11 (not the chapter's completion) — only the per-change documentation rule applies (README.md files + CONTEXT.md), not a full `docs/ARCHITECTURE.md` sweep.

---

### Task 1: Add Actuator to all 9 services + a representative slice test per stack

**Files:**
- Modify: `build.gradle:41-55` (add a new `actuatorModules` list + `configure(...)` block, following the existing `reactiveModules` pattern right below it)
- Modify: `ftgo-order-service/src/main/resources/application.yml`, `ftgo-kitchen-service/src/main/resources/application.yml`, `ftgo-consumer-service/src/main/resources/application.yml`, `ftgo-restaurant-service/src/main/resources/application.yml`, `ftgo-accounting-service/src/main/resources/application.yml`, `ftgo-delivery-service/src/main/resources/application.yml`, `ftgo-order-history-service/src/main/resources/application.yml`, `ftgo-mobile-gateway/src/main/resources/application.yml`, `ftgo-public-gateway/src/main/resources/application.yml` (add a `management:` block to each)
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/HealthEndpointTest.java` (new — servlet stack, representative of the 7 DB-backed services)
- Test: `ftgo-public-gateway/src/test/java/com/sanjay/ftgo/publicgateway/HealthEndpointTest.java` (new — reactive stack, representative of the 2 gateways)

**Interfaces:**
- Produces: `/actuator/health` on every one of the 9 services, returning JSON with a top-level `status` field and (since `show-details: always`) a `components` object. Later tasks (2, 3) consume this endpoint by URL only — no Java interface is produced.

- [ ] **Step 1: Add the `actuatorModules` block to the root build**

In `build.gradle`, immediately after the existing `reactiveModules` block (after line 55, the closing `}` of the `configure(subprojects.findAll { !reactiveModules.contains(it.name) })` block), add:

```gradle
// The 9 services actually run as standalone processes/containers in compose.yml and benefit
// from a real health endpoint (Ch.11, §11.3.1 Health check API pattern). Library modules
// (ftgo-common, ftgo-gateway-common), contract-stub modules, ftgo-service-registry (the Eureka
// server itself — out of scope for this pattern), and ftgo-end-to-end-test (a test-only module,
// never runs standalone) are deliberately excluded.
def actuatorModules = ['ftgo-order-service', 'ftgo-kitchen-service', 'ftgo-consumer-service',
                        'ftgo-restaurant-service', 'ftgo-accounting-service', 'ftgo-delivery-service',
                        'ftgo-order-history-service', 'ftgo-mobile-gateway', 'ftgo-public-gateway']

configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
    }
}
```

- [ ] **Step 2: Add the `management` block to each of the 9 services' `application.yml`**

Add this block to each file (placement doesn't matter relative to existing top-level keys — append at the end of the file):

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      show-details: always
```

Apply this to exactly these 9 files:
- `ftgo-order-service/src/main/resources/application.yml`
- `ftgo-kitchen-service/src/main/resources/application.yml`
- `ftgo-consumer-service/src/main/resources/application.yml`
- `ftgo-restaurant-service/src/main/resources/application.yml`
- `ftgo-accounting-service/src/main/resources/application.yml`
- `ftgo-delivery-service/src/main/resources/application.yml`
- `ftgo-order-history-service/src/main/resources/application.yml`
- `ftgo-mobile-gateway/src/main/resources/application.yml`
- `ftgo-public-gateway/src/main/resources/application.yml`

- [ ] **Step 3: Write the servlet-stack slice test (order-service)**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/HealthEndpointTest.java`:

```java
package com.sanjay.ftgo.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class HealthEndpointTest {

    @Autowired
    private MockMvc mockMvc;

    // order-service's src/test/resources/application.yml swaps in an H2 datasource (real
    // connection, so the "db" indicator reports UP) but leaves Kafka pointed at
    // localhost:9092, which nothing is listening on in a plain unit test — asserting the
    // overall aggregate `status` would be flaky (it goes DOWN because of the unreachable
    // Kafka indicator). Assert on the "db" component specifically instead; the Docker-based
    // end-to-end scenario (Task 3) is what proves the real, live "UP" aggregate.
    @Test
    void healthEndpointReportsDatabaseComponentUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").exists())
                .andExpect(jsonPath("$.components.db.status").value("UP"));
    }
}
```

- [ ] **Step 4: Run the order-service test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.HealthEndpointTest"`
Expected: `BUILD SUCCESSFUL`, 1 test passed.

- [ ] **Step 5: Write the reactive-stack slice test (public-gateway)**

Create `ftgo-public-gateway/src/test/java/com/sanjay/ftgo/publicgateway/HealthEndpointTest.java`:

```java
package com.sanjay.ftgo.publicgateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@AutoConfigureWebTestClient
class HealthEndpointTest {

    @Autowired
    private WebTestClient webTestClient;

    // The "test" profile (src/test/resources/application-test.yml) disables the Eureka client
    // entirely, so there's no discoveryComposite indicator to assert on here — this only proves
    // the endpoint is wired up and responding on the reactive stack. The Docker-based end-to-end
    // scenario (Task 3) is what proves the real "UP" aggregate with Eureka actually registered.
    @Test
    void healthEndpointRespondsOk() {
        webTestClient.get().uri("/actuator/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").exists();
    }
}
```

- [ ] **Step 6: Run the public-gateway test to verify it passes**

Run: `./gradlew :ftgo-public-gateway:test --tests "com.sanjay.ftgo.publicgateway.HealthEndpointTest"`
Expected: `BUILD SUCCESSFUL`, 1 test passed.

- [ ] **Step 7: Run the full fast test suite to check for regressions**

Run: `./gradlew test -x :ftgo-end-to-end-test:e2eTest`
Expected: `BUILD SUCCESSFUL` (the Docker-dependent `ftgo-end-to-end-test` module's `e2eTest` task is excluded, matching this repo's established convention — see `docs/ARCHITECTURE.md`'s End-to-end testing section).

- [ ] **Step 8: Commit**

```bash
git add build.gradle \
  ftgo-order-service/src/main/resources/application.yml ftgo-order-service/src/test/java/com/sanjay/ftgo/order/HealthEndpointTest.java \
  ftgo-kitchen-service/src/main/resources/application.yml \
  ftgo-consumer-service/src/main/resources/application.yml \
  ftgo-restaurant-service/src/main/resources/application.yml \
  ftgo-accounting-service/src/main/resources/application.yml \
  ftgo-delivery-service/src/main/resources/application.yml \
  ftgo-order-history-service/src/main/resources/application.yml \
  ftgo-mobile-gateway/src/main/resources/application.yml \
  ftgo-public-gateway/src/main/resources/application.yml ftgo-public-gateway/src/test/java/com/sanjay/ftgo/publicgateway/HealthEndpointTest.java
git commit -m "feat: add Actuator health endpoint to all 9 FTGO services"
```

---

### Task 2: Wire Docker Compose healthchecks and startup ordering

**Files:**
- Modify: `compose.yml`

**Interfaces:**
- Consumes: `/actuator/health` on each of the 9 services (Task 1) at the ports listed in Global Constraints.
- Produces: `service_healthy` Docker Compose condition for each of the 9 services, consumable by Task 3's full-stack `e2eTest` run (which brings up this same `compose.yml` unmodified).

- [ ] **Step 1: Add a `healthcheck` block to each of the 9 services**

In `compose.yml`, add this block (with the service's own container-internal port) to each of the 9 service definitions, following the same style already used for `mysql` (lines 15-19) and `kafka-connect` (lines 71-75):

```yaml
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:<port>/actuator/health"]
      interval: 10s
      timeout: 5s
      retries: 10
```

Ports per service: `restaurant-service` → `8085`, `order-service` → `8082`, `kitchen-service` → `8083`, `consumer-service` → `8081`, `accounting-service` → `8084`, `delivery-service` → `8086`, `order-history-service` → `8088`, `mobile-gateway` → `8090`, `public-gateway` → `8091`.

- [ ] **Step 2: Upgrade real inter-service `depends_on` entries to `condition: service_healthy`**

Make these exact changes in `compose.yml`:

`order-service`'s `depends_on` (currently lines 116-124) — `restaurant-service` changes from `condition: service_started` to `condition: service_healthy` (order-service calls restaurant-service synchronously at order-creation time):

```yaml
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      service-registry:
        condition: service_started
      restaurant-service:
        condition: service_healthy
```

`mobile-gateway`'s `depends_on` (currently lines 230-235, list form with no conditions) changes to:

```yaml
    depends_on:
      service-registry:
        condition: service_started
      order-service:
        condition: service_healthy
      kitchen-service:
        condition: service_healthy
      accounting-service:
        condition: service_healthy
      delivery-service:
        condition: service_healthy
```

`public-gateway`'s `depends_on` (currently lines 245-252, list form with no conditions) changes to:

```yaml
    depends_on:
      service-registry:
        condition: service_started
      order-service:
        condition: service_healthy
      kitchen-service:
        condition: service_healthy
      accounting-service:
        condition: service_healthy
      delivery-service:
        condition: service_healthy
      order-history-service:
        condition: service_healthy
      restaurant-service:
        condition: service_healthy
```

Leave every other `depends_on` entry as-is: `kitchen-service`, `consumer-service`, `accounting-service`, `delivery-service`, and `order-history-service` have no dependency on another *business* service (only on `mysql`/`kafka`/`service-registry`), so their `depends_on` blocks are unchanged. `restaurant-service`'s own `depends_on` (on `mysql` and `service-registry`) is also unchanged — it has no dependency on another business service either.

- [ ] **Step 3: Validate the Compose file parses**

Run: `docker compose -f compose.yml config > /dev/null`
Expected: exits `0` with no output (a YAML/schema error would print to stderr and exit non-zero).

- [ ] **Step 4: Commit**

```bash
git add compose.yml
git commit -m "feat: wire Docker Compose healthchecks for all 9 FTGO services"
```

---

### Task 3: Automated full-stack health verification (Cucumber)

**Files:**
- Create: `ftgo-end-to-end-test/src/test/resources/features/AllServicesReportHealthy.feature`
- Create: `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/HealthCheckStepDefinitions.java`

**Interfaces:**
- Consumes: `/actuator/health` on each of the 9 services (Task 1 + Task 2), reached directly on their host-mapped ports (not through a gateway — this checks infrastructure state, mirroring `PlaceReviseCancelOrderStepDefinitions`'s direct-port calls for restaurant/consumer fixture setup).
- The `EndToEndTestRunner` class (`ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/EndToEndTestRunner.java`) already does `@SelectClasspathResource("features")`, so the new `.feature` file is picked up automatically — no runner changes needed.

- [ ] **Step 1: Write the feature file**

Create `ftgo-end-to-end-test/src/test/resources/features/AllServicesReportHealthy.feature`:

```gherkin
Feature: All services report healthy (end-to-end)

  Scenario: Every FTGO service's health endpoint reports UP
    Then every service's health endpoint eventually reports UP
```

- [ ] **Step 2: Write the step definitions**

Create `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/HealthCheckStepDefinitions.java`:

```java
package com.sanjay.ftgo.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.en.Then;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class HealthCheckStepDefinitions {

    // Every DB-backed business service except consumer-service; each must report a "db"
    // component UP, in addition to the "discoveryComposite" (Eureka) component. There is no
    // "kafka" component to assert on: Spring Boot's actuator-autoconfigure no longer ships a
    // Kafka health contributor (verified absent from spring-boot-actuator-autoconfigure 3.5.16
    // -- only KafkaMetricsAutoConfiguration remains under actuate.autoconfigure.kafka), and none
    // of the 9 services registers a custom one, so live health JSON never has a "kafka" key.
    private static final List<Map.Entry<String, Integer>> DB_BACKED_SERVICES = List.of(
            Map.entry("order-service", 8082),
            Map.entry("kitchen-service", 8083),
            Map.entry("restaurant-service", 8085),
            Map.entry("accounting-service", 8084),
            Map.entry("delivery-service", 8086),
            Map.entry("order-history-service", 8088)
    );

    // consumer-service is DB-backed but, unlike the other 6, has no eureka-client dependency at
    // all (pre-existing, unrelated to Ch.11) -- it never registers with Eureka, so it has no
    // "discoveryComposite" component either.
    private static final Map.Entry<String, Integer> CONSUMER_SERVICE = Map.entry("consumer-service", 8081);

    // Gateways: no DB/Kafka of their own, but still register with Eureka.
    private static final List<Map.Entry<String, Integer>> GATEWAY_SERVICES = List.of(
            Map.entry("mobile-gateway", 8090),
            Map.entry("public-gateway", 8091)
    );

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Then("every service's health endpoint eventually reports UP")
    public void everyServicesHealthEndpointEventuallyReportsUp() throws Exception {
        for (Map.Entry<String, Integer> service : DB_BACKED_SERVICES) {
            JsonNode health = fetchHealthWithRetry(service.getKey(), service.getValue());
            assertEquals("UP", health.get("status").asText(), service.getKey() + " overall status");
            assertEquals("UP", health.get("components").get("db").get("status").asText(), service.getKey() + " db component");
            assertEquals("UP", health.get("components").get("discoveryComposite").get("status").asText(), service.getKey() + " discoveryComposite component");
        }
        {
            JsonNode health = fetchHealthWithRetry(CONSUMER_SERVICE.getKey(), CONSUMER_SERVICE.getValue());
            assertEquals("UP", health.get("status").asText(), CONSUMER_SERVICE.getKey() + " overall status");
            assertEquals("UP", health.get("components").get("db").get("status").asText(), CONSUMER_SERVICE.getKey() + " db component");
        }
        for (Map.Entry<String, Integer> service : GATEWAY_SERVICES) {
            JsonNode health = fetchHealthWithRetry(service.getKey(), service.getValue());
            assertEquals("UP", health.get("status").asText(), service.getKey() + " overall status");
            assertEquals("UP", health.get("components").get("discoveryComposite").get("status").asText(), service.getKey() + " discoveryComposite component");
        }
    }

    // Mirrors PlaceReviseCancelOrderStepDefinitions's retry-with-backoff pattern: services may
    // still be finishing Eureka self-registration or their first successful DB/Kafka connection
    // check immediately after container startup.
    private JsonNode fetchHealthWithRetry(String serviceName, int port) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/actuator/health"))
                        .GET()
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                JsonNode body = objectMapper.readTree(response.body());
                if (response.statusCode() == 200 && "UP".equals(body.path("status").asText())) {
                    return body;
                }
                lastFailure = new IllegalStateException(serviceName + " returned " + response.statusCode() + ": " + response.body());
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(2000);
        }
        fail("Health check for " + serviceName + " on port " + port + " did not report UP within 60s", lastFailure);
        return null;
    }
}
```

- [ ] **Step 3: Run the full-stack scenario**

Run: `COMPOSE_PARALLEL_LIMIT=1 ./gradlew :ftgo-end-to-end-test:e2eTest`

If this OOMs (this sandbox's Docker daemon has ~7.75GiB total and has hit this before — see `docs/ARCHITECTURE.md`'s End-to-end testing section for the established workaround): pre-build images sequentially first with `COMPOSE_PARALLEL_LIMIT=1 docker compose -p ftgo-e2e-test -f compose.yml build`, clean up stale containers/cache if a prior failed attempt left any (`docker compose -p ftgo-e2e-test -f compose.yml down -v`, `docker container prune -f`, `docker builder prune -f --filter until=1h`), then retry the Gradle command.

Expected: `BUILD SUCCESSFUL`, both `AllServicesReportHealthy` and the pre-existing `PlaceReviseCancelOrder` scenarios report as passed (2 scenarios total) — the latter re-running here is this task's regression check for Task 2's `depends_on` changes, per this plan's stated regression risk.

- [ ] **Step 4: Commit**

```bash
git add ftgo-end-to-end-test/src/test/resources/features/AllServicesReportHealthy.feature ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/HealthCheckStepDefinitions.java
git commit -m "test: add full-stack health check scenario to ftgo-end-to-end-test"
```

---

### Task 4: Documentation

**Files:**
- Modify: `ftgo-order-service/README.md`, `ftgo-kitchen-service/README.md`, `ftgo-consumer-service/README.md`, `ftgo-restaurant-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-delivery-service/README.md`, `ftgo-order-history-service/README.md`, `ftgo-mobile-gateway/README.md`, `ftgo-public-gateway/README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/ARCHITECTURE.md`

**Interfaces:** None — documentation only, no code.

- [ ] **Step 1: Add a "Health check" section to each of the 7 DB-backed services' README**

Add this section (adjust only for `ftgo-consumer-service`, which has no `discoveryComposite` component — see the note below):

```markdown
## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only (no custom
`HealthIndicator` code). Reports:
- `db` — MySQL reachability via the service's `DataSource`.
- `discoveryComposite` — Eureka registration status.

There is no `kafka` component: Spring Boot's actuator-autoconfigure no longer ships a Kafka
health contributor as of this project's Spring Boot version (3.5.16) — verified directly against
the built jars (`spring-boot-actuator-autoconfigure` retains only `KafkaMetricsAutoConfiguration`
under `actuate.autoconfigure.kafka`; `spring-kafka` ships no health-indicator class either) — and
adding a custom one is out of scope for this sub-project.

`management.endpoint.health.show-details: always` — safe here since these ports aren't exposed
to untrusted clients in this project; full component detail is the point of exercising this
pattern. Verified against the real, running stack by `ftgo-end-to-end-test`'s
`AllServicesReportHealthy.feature`.
```

For `ftgo-consumer-service/README.md` specifically, drop the `discoveryComposite` bullet and add
instead: "`ftgo-consumer-service` has no `eureka-client` dependency (pre-existing, unrelated to
Ch.11), so it never registers with Eureka and has no `discoveryComposite` component — only `db`."

Insert this section into each of these 7 files: `ftgo-order-service/README.md`, `ftgo-kitchen-service/README.md`, `ftgo-consumer-service/README.md`, `ftgo-restaurant-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-delivery-service/README.md`, `ftgo-order-history-service/README.md`. Place it directly after each file's existing `## API` section (or after the equivalent top-level section documenting the service's endpoints, if the heading name differs slightly) — check each file's existing structure with `grep -n "^## " <file>` before inserting, since not every one of these 7 READMEs necessarily has an identically named section immediately before where this belongs.

- [ ] **Step 2: Add a "Health check" section to each gateway's README**

Add this section to `ftgo-mobile-gateway/README.md` and `ftgo-public-gateway/README.md`:

```markdown
## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only. Reports
`discoveryComposite` (Eureka registration status) — no `db`/`kafka` components, since gateways
have neither of their own. `management.endpoint.health.show-details: always` for the same reason
given in the business services' READMEs. Verified against the real, running stack by
`ftgo-end-to-end-test`'s `AllServicesReportHealthy.feature`.
```

- [ ] **Step 3: Add a short note to `docs/ARCHITECTURE.md`**

Add a new top-level section near the end of `docs/ARCHITECTURE.md` (check its existing heading structure with `grep -n "^## " docs/ARCHITECTURE.md` first and place this after the last pattern/sub-project section, before any closing "Testing"/"Appendix"-style section if one exists):

```markdown
## Health check API (Ch.11, §11.3.1)

Every business service (7) and both gateways (2) expose `GET /actuator/health` via Spring Boot
Actuator's auto-configured indicators — no custom `HealthIndicator` code. `ftgo-service-registry`
is excluded (it's the Eureka server, not a business service).

- **DB-backed services** (order, kitchen, restaurant, accounting, delivery, order-history):
  `db` (DataSource reachability), `discoveryComposite` (Eureka registration). No `kafka`
  component — Spring Boot 3.5.16's actuator-autoconfigure ships no Kafka health contributor
  (verified against the built jars; only `KafkaMetricsAutoConfiguration` remains), and a custom
  one is out of scope for this sub-project.
- **`ftgo-consumer-service`**: DB-backed like the other 6, but has no `eureka-client` dependency
  at all (pre-existing, unrelated to Ch.11) — reports `db` only, no `discoveryComposite`.
- **Gateways** (mobile, public): `discoveryComposite` only — no DB or Kafka of their own.

`compose.yml` adds a `healthcheck` block per service (`curl -f
http://localhost:<port>/actuator/health`) and upgrades `depends_on` to `condition:
service_healthy` for real inter-service dependencies (order-service → restaurant-service; both
gateways → the business services they route to), so the stack won't route traffic to a service
before it's actually ready. Verified end-to-end by `ftgo-end-to-end-test`'s
`AllServicesReportHealthy.feature`.

A full dedicated section with sequence diagrams, matching this file's other patterns, is deferred
to Ch.11's eventual chapter-completion documentation sweep — this is sub-project 1 of an
unscheduled number of Ch.11 sub-projects.
```

- [ ] **Step 4: Update `CONTEXT.md`**

Three edits:

1. In the "Book progress" table, change the Ch.11 row (currently `| 11 | Developing production-ready services | Not started | — | |`) to:
   ```
   | 11 | Developing production-ready services | In progress | Medium | Sub-project 1 of N (health checks, §11.3.1) done: all 9 services (7 business services + both gateways) get a real `/actuator/health` endpoint via Spring Boot Actuator's auto-configured DataSource/Kafka/Eureka indicators (no custom `HealthIndicator` code); `ftgo-service-registry` excluded (it's the Eureka server, not a business service). `compose.yml` gained healthchecks on all 9 services and `depends_on: condition: service_healthy` on the real inter-service dependencies (order-service→restaurant-service; both gateways→the business services they route to). Verified against the real stack by a new `AllServicesReportHealthy.feature` scenario in `ftgo-end-to-end-test`. Further sub-projects (security §11.1, configurability §11.2, remaining observability patterns §11.3.2–11.3.6, microservice chassis §11.4) not yet scheduled. |
   ```

2. In the "Patterns reference" checklist section, change `- [ ] Health check API (Ch. 11)` to `- [x] Health check API (Ch. 11, §11.3.1) — all 9 services' \`/actuator/health\`, auto-configured indicators only`.

3. In the "Current position" section, add a new bullet (matching the existing style of the Ch.10 entry immediately above it) summarizing this sub-project — mention the 9 services, the 3 auto-configured indicators, `ftgo-service-registry`'s exclusion, and that further Ch.11 sub-projects remain unscheduled. Also add a corresponding session log entry at the end of the session log section, dated with today's date, noting the worktree/branch/PR once opened (leave a note "PR pending" if writing this before the PR exists — the controller fills in the real number once opened, following this project's established convention from Ch.9/Ch.10 sub-projects).

- [ ] **Step 5: Commit**

```bash
git add ftgo-order-service/README.md ftgo-kitchen-service/README.md ftgo-consumer-service/README.md \
  ftgo-restaurant-service/README.md ftgo-accounting-service/README.md ftgo-delivery-service/README.md \
  ftgo-order-history-service/README.md ftgo-mobile-gateway/README.md ftgo-public-gateway/README.md \
  CONTEXT.md docs/ARCHITECTURE.md
git commit -m "docs: document Ch.11 sub-project 1 (health checks) in service READMEs, ARCHITECTURE.md, and CONTEXT.md"
```
