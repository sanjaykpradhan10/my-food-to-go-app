# Ch.11 §11.2: Externalized Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pull-based Spring Cloud Config Server (git-backed against this repo) for configuration values shared across services, alongside the existing push-based (compose env var) layer for per-environment/secret values, and demonstrate live config refresh via `/actuator/refresh` on a property this project's own code controls end-to-end.

**Architecture:** A new `ftgo-config-server` module serves properties from a new `config-repo/` directory in this repo (git backend, `search-paths: config-repo`) on port 8888. All 9 services add `spring.config.import: optional:configserver:...` with `fail-fast: false`, so config-server values augment (and can override) each service's local `application.yml`, but a config-server outage never blocks startup. The 5 services that run `OutboxPublisher` (order, kitchen, accounting, delivery, consumer) additionally pull a per-service outbox/saga config file. `OutboxPublisher`'s polling interval becomes a `@RefreshScope` bean read through a `SchedulingConfigurer`-registered dynamic trigger, proven live-refreshable by a new Cucumber scenario.

**Tech Stack:** Spring Cloud Config Server/Client 2024.0.0 (this repo's existing Spring Cloud BOM version — see Global Constraints), Spring Cloud Context (`@RefreshScope`), Spring Boot Actuator `/actuator/refresh`, JGit (transitively, via Spring Cloud Config Server's git backend).

## Global Constraints

- Spring Cloud BOM version for every new dependency in this plan is **2024.0.0** — the version already pinned in the root `build.gradle`'s `dependencyManagement` block (`ftgo-common` separately pins `2025.0.3` for its own reasons; do not change that).
- Java 21, Spring Boot 3.5.16 (already pinned at the root) — do not override per-module.
- Config-server unavailability must never block a business service from starting: every config-consuming service uses `optional:configserver:...` in `spring.config.import` AND `spring.cloud.config.fail-fast: false` (belt-and-braces, per the approved spec).
- Compose env vars still win over config-server values, which still win over each service's local `application.yml` — do not remove or "deduplicate away" any existing local `application.yml` keys this plan touches; local values are the fallback, kept deliberately.
- `ftgo-config-server` is a leaf service (no `depends_on`), addressed by other services via a fixed hostname (`config-server` in Docker, `localhost` locally) — it is never registered with Eureka, matching `ftgo-authorization-server` and `ftgo-service-registry`'s own non-discovery-based addressing pattern already in this repo.
- The 5 services with an `OutboxPublisher` are: `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-accounting-service`, `ftgo-delivery-service`, `ftgo-consumer-service`. **Correction from the spec:** `ftgo-order-history-service` does NOT run `OutboxPublisher` (it only consumes Kafka events to build a read view — its `application.yml` has no `outbox:`/`saga:` block) — it does NOT get a per-service `config-repo/ftgo-order-history-service.yml` file, unlike what the spec's Architecture section implied. It still participates in the shared `config-repo/application.yml` wiring like every other service.
- **Correction from the spec:** the spec's Architecture section named "6 config-consuming services" for `spring.config.import` wiring (the 5 outbox services + order-history-service), but `ftgo-restaurant-service`, `ftgo-mobile-gateway`, and `ftgo-public-gateway` also duplicate the same shared values (`eureka.*`, JWK-set URI, `management.tracing.*`, `management.otlp.*`, `management.endpoints.web.exposure.include`) verified directly in their `application.yml` files. All 9 services (7 business services + 2 gateways) get `spring.config.import` wiring to the shared `config-repo/application.yml`; only the 5 outbox services additionally pull a per-service file.
- Gateways use a different JWK-set-URI property key (`gateway.jwt.jwk-set-uri`) than business services (`spring.security.oauth2.resourceserver.jwt.jwk-set-uri`) — the shared `config-repo/application.yml` sets both keys to the same value; each service ignores the key it doesn't use.

---

## Task 1: `ftgo-config-server` module + `config-repo/` content

**Files:**
- Create: `settings.gradle` (add one `include` line)
- Modify: `build.gradle:52` (add `ftgo-config-server` to `noDefaultWebJpaModules`)
- Create: `ftgo-config-server/build.gradle`
- Create: `ftgo-config-server/Dockerfile`
- Create: `ftgo-config-server/src/main/java/com/sanjay/ftgo/configserver/FtgoConfigServerApplication.java`
- Create: `ftgo-config-server/src/main/resources/application.yml`
- Create: `ftgo-config-server/src/test/resources/application-test.yml`
- Create: `ftgo-config-server/src/test/resources/config-repo-test/application.yml`
- Test: `ftgo-config-server/src/test/java/com/sanjay/ftgo/configserver/ConfigServerServesSharedPropertiesTest.java`
- Create: `config-repo/application.yml`
- Create: `config-repo/ftgo-order-service.yml`
- Create: `config-repo/ftgo-kitchen-service.yml`
- Create: `config-repo/ftgo-accounting-service.yml`
- Create: `config-repo/ftgo-delivery-service.yml`
- Create: `config-repo/ftgo-consumer-service.yml`

**Interfaces:**
- Produces: `config-repo/application.yml` keys consumed by Task 2 — `spring.kafka.bootstrap-servers`, `spring.security.oauth2.resourceserver.jwt.jwk-set-uri`, `gateway.jwt.jwk-set-uri`, `eureka.client.service-url.defaultZone`, `eureka.instance.prefer-ip-address`, `management.endpoints.web.exposure.include` (value `health, prometheus, refresh`), `management.endpoint.health.show-details`, `management.tracing.sampling.probability`, `management.otlp.tracing.endpoint`.
- Produces: `config-repo/ftgo-<service>.yml` (5 files) with keys `outbox.poll-fixed-delay-ms` (`2000`), `outbox.batch-size` (`20`), `saga.mode` (`choreography`) — consumed by Task 4's refresh demo (order-service's file specifically).
- Produces: config server reachable at `http://localhost:8888` locally / `http://config-server:8888` in Docker (Task 3 wires the Docker hostname).

- [ ] **Step 1: Register the module**

Add to `settings.gradle` (after the `ftgo-authorization-server` line):
```groovy
include 'ftgo-config-server'
```

- [ ] **Step 2: Exclude the module from the default web/JPA block**

In `build.gradle`, change:
```groovy
def noDefaultWebJpaModules = reactiveModules + ['ftgo-authorization-server']
```
to:
```groovy
def noDefaultWebJpaModules = reactiveModules + ['ftgo-authorization-server', 'ftgo-config-server']
```
(`ftgo-config-server` declares its own web starter via `spring-cloud-config-server` and has no database, same reasoning already documented above that line for `ftgo-authorization-server`.)

- [ ] **Step 3: Module build file**

Create `ftgo-config-server/build.gradle`:
```groovy
// spring-cloud-config-server pulls in spring-boot-starter-web transitively, but this module
// declares it directly for the same reason ftgo-authorization-server does: it's excluded from
// the root build.gradle's default web/JPA block (see noDefaultWebJpaModules), so nothing else
// supplies it.
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.cloud:spring-cloud-config-server'
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
}
```

- [ ] **Step 4: Dockerfile**

Create `ftgo-config-server/Dockerfile`:
```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-config-server:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-config-server/build/libs/*.jar app.jar
# The git backend (see application.yml) needs an actual git working copy to read config-repo/
# from — a plain "COPY config-repo/" would not be a git repository, and Spring Cloud Config
# Server's git backend requires one even for local/file:// URIs. Copying the whole build-stage
# workspace (which is a checkout of this repo, .git included) is the simplest way to give the
# runtime image something JGit can open.
COPY --from=build /workspace /config-source-repo
EXPOSE 8888
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 5: Application class**

Create `ftgo-config-server/src/main/java/com/sanjay/ftgo/configserver/FtgoConfigServerApplication.java`:
```java
package com.sanjay.ftgo.configserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

@SpringBootApplication
@EnableConfigServer
public class FtgoConfigServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(FtgoConfigServerApplication.class, args);
    }
}
```

- [ ] **Step 6: Application config**

Create `ftgo-config-server/src/main/resources/application.yml`:
```yaml
spring:
  application:
    name: ftgo-config-server
  cloud:
    config:
      server:
        git:
          # Gradle's bootRun working directory for a subproject is that subproject's own
          # directory, so "${user.dir}/.." resolves to the repo root when run locally
          # (./gradlew :ftgo-config-server:bootRun). The Docker image overrides this via
          # CONFIG_SERVER_GIT_URI (see compose.yml) to point at /config-source-repo, the full
          # workspace checkout copied into the runtime image (see Dockerfile).
          uri: ${CONFIG_SERVER_GIT_URI:file://${user.dir}/..}
          search-paths: config-repo
          clone-on-start: true

server:
  port: 8888
```

- [ ] **Step 7: config-repo shared file**

Create `config-repo/application.yml`:
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
  security:
    oauth2:
      resourceserver:
        jwt:
          jwk-set-uri: http://localhost:9000/oauth2/jwks

gateway:
  jwt:
    jwk-set-uri: http://localhost:9000/oauth2/jwks

eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true

management:
  endpoints:
    web:
      exposure:
        include: health, prometheus, refresh
  endpoint:
    health:
      show-details: always
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: http://localhost:4318/v1/traces
```

- [ ] **Step 8: config-repo per-service files**

Create `config-repo/ftgo-order-service.yml`, `config-repo/ftgo-kitchen-service.yml`, `config-repo/ftgo-accounting-service.yml`, `config-repo/ftgo-delivery-service.yml`, `config-repo/ftgo-consumer-service.yml` — identical content in each:
```yaml
outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20

saga:
  mode: choreography
```

- [ ] **Step 9: Write the test-fixture config (native backend, no git needed)**

Testing against the real git backend would require the test to run from inside a git checkout with a `config-repo/` directory two levels up — coupling the test to the CI checkout layout. Instead, test the server's serving behavior with Spring Cloud Config Server's `native` profile, which reads plain files with no git plumbing.

Create `ftgo-config-server/src/test/resources/application-test.yml`:
```yaml
spring:
  cloud:
    config:
      server:
        native:
          search-locations: classpath:/config-repo-test/
    profiles:
      active: native
```

Create `ftgo-config-server/src/test/resources/config-repo-test/application.yml`:
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, prometheus, refresh
outbox:
  poll-fixed-delay-ms: 2000
```

- [ ] **Step 10: Write the failing test**

Create `ftgo-config-server/src/test/java/com/sanjay/ftgo/configserver/ConfigServerServesSharedPropertiesTest.java`:
```java
package com.sanjay.ftgo.configserver;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class ConfigServerServesSharedPropertiesTest {

    @LocalServerPort
    private int port;

    private final TestRestTemplate restTemplate = new TestRestTemplate();

    @Test
    void servesSharedPropertiesForAnyApplicationName() {
        String body = restTemplate.getForObject(
                "http://localhost:" + port + "/ftgo-order-service/default", String.class);

        assertThat(body).contains("\"outbox.poll-fixed-delay-ms\":\"2000\"");
        assertThat(body).contains("health, prometheus, refresh");
    }
}
```

- [ ] **Step 11: Run test to verify it fails**

Run: `./gradlew :ftgo-config-server:test --tests ConfigServerServesSharedPropertiesTest`
Expected: FAIL — module/class do not exist yet before Steps 1-9 are done. (If run after Steps 1-9, this should already PASS — Steps 1-9 are the implementation; this step exists to confirm the test is meaningful, i.e. it fails if e.g. `search-locations` is misconfigured. If it's green on first run after Steps 1-9, temporarily rename the property key in `application-test.yml` to confirm the test fails on a real config mismatch, then revert.)

- [ ] **Step 12: Run test to verify it passes**

Run: `./gradlew :ftgo-config-server:test --tests ConfigServerServesSharedPropertiesTest`
Expected: PASS

- [ ] **Step 13: Commit**

```bash
git add settings.gradle build.gradle ftgo-config-server config-repo
git commit -m "feat: add ftgo-config-server module and config-repo shared configuration"
```

---

## Task 2: Wire `spring.config.import` into all 9 services

**Files:**
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Modify: `ftgo-kitchen-service/src/main/resources/application.yml`
- Modify: `ftgo-accounting-service/src/main/resources/application.yml`
- Modify: `ftgo-delivery-service/src/main/resources/application.yml`
- Modify: `ftgo-consumer-service/src/main/resources/application.yml`
- Modify: `ftgo-order-history-service/src/main/resources/application.yml`
- Modify: `ftgo-restaurant-service/src/main/resources/application.yml`
- Modify: `ftgo-mobile-gateway/src/main/resources/application.yml`
- Modify: `ftgo-public-gateway/src/main/resources/application.yml`

**Interfaces:**
- Consumes: `ftgo-config-server` from Task 1, reachable at `http://localhost:8888` in this task's local/test context (Task 3 adds the Docker-hostname override).
- Produces: every service now has `spring.config.import` pointed at the config server — Task 3's compose wiring and Task 5's e2e scenario depend on this being present on all 9.

- [ ] **Step 1: Add the import block to each service**

Add this block under the top-level `spring:` key in each of the 9 files listed above (as a sibling of `application:`, `datasource:`, etc. — for the two gateways, which have no `datasource:`/`jpa:` keys, add it as a sibling of `application:` and `cloud:`):
```yaml
  config:
    import: "optional:configserver:http://localhost:8888"
  cloud:
    config:
      fail-fast: false
```

For example, `ftgo-order-service/src/main/resources/application.yml`'s `spring:` block becomes:
```yaml
spring:
  application:
    name: ftgo-order-service
  config:
    import: "optional:configserver:http://localhost:8888"
  cloud:
    config:
      fail-fast: false
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_order
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    ...
```
(Only the `config:`/`cloud:` addition is new — every other key in each file stays exactly as it is today; this is the deliberate local-fallback duplication described in Global Constraints.)

Apply the identical `config:`/`cloud:` block (same two keys, same values) to the remaining 8 files.

- [ ] **Step 2: Verify each service still boots without a running config server**

None of these 9 services has a config server running in their own unit/context tests, and `optional:` + `fail-fast: false` means that must keep working. Run each service's existing context-loading test suite (do not write new tests — this step is a regression check that Step 1 didn't break startup):
```bash
./gradlew :ftgo-order-service:test :ftgo-kitchen-service:test :ftgo-accounting-service:test :ftgo-delivery-service:test :ftgo-consumer-service:test :ftgo-order-history-service:test :ftgo-restaurant-service:test :ftgo-mobile-gateway:test :ftgo-public-gateway:test
```
Expected: PASS on all 9 — this is the proof that `optional:configserver:...` correctly falls back to each service's local `application.yml` when no config server is reachable, exactly as Global Constraints requires.

- [ ] **Step 3: Commit**

```bash
git add ftgo-order-service/src/main/resources/application.yml \
        ftgo-kitchen-service/src/main/resources/application.yml \
        ftgo-accounting-service/src/main/resources/application.yml \
        ftgo-delivery-service/src/main/resources/application.yml \
        ftgo-consumer-service/src/main/resources/application.yml \
        ftgo-order-history-service/src/main/resources/application.yml \
        ftgo-restaurant-service/src/main/resources/application.yml \
        ftgo-mobile-gateway/src/main/resources/application.yml \
        ftgo-public-gateway/src/main/resources/application.yml
git commit -m "feat: wire all 9 services to optionally import config from ftgo-config-server"
```

---

## Task 3: `compose.yml` wiring

**Files:**
- Modify: `compose.yml`

**Interfaces:**
- Consumes: `ftgo-config-server`'s Dockerfile (Task 1) and every service's `spring.config.import` key (Task 2).
- Produces: a running `config-server` compose service other tasks' manual/e2e Docker verification (Task 5's manual step, Task 6's docs) can reference at `http://config-server:8888`.

- [ ] **Step 1: Add the `config-server` service block**

Add a new service block to `compose.yml` (alongside the other infra services like `authorization-server`/`service-registry` — place it near those):
```yaml
  config-server:
    build:
      context: .
      dockerfile: ftgo-config-server/Dockerfile
    ports:
      - "8888:8888"
```
(No `depends_on` — per Global Constraints, `ftgo-config-server` is a leaf service.)

- [ ] **Step 2: Point every consuming service at the Docker hostname, non-blocking**

For each of the 9 service blocks modified in Task 2 (`order-service`, `kitchen-service`, `accounting-service`, `delivery-service`, `consumer-service`, `order-history-service`, `restaurant-service`, `mobile-gateway`, `public-gateway`), add this line to that service's existing `environment:` block:
```yaml
      SPRING_CONFIG_IMPORT: "optional:configserver:http://config-server:8888"
```
And add this to that service's existing `depends_on:` block (create one with just this entry if the service currently has no `depends_on:` at all):
```yaml
      config-server:
        condition: service_started
```
(`service_started`, not `service_healthy` — per Global Constraints and the same rule already applied to `tempo`/`prometheus`/`grafana`: config-server must never gate a business service's startup on being fully ready.)

- [ ] **Step 3: Validate compose syntax**

Run: `docker compose -f compose.yml config --quiet`
Expected: no output, exit code 0 (confirms valid YAML/schema — this does not start containers).

- [ ] **Step 4: Commit**

```bash
git add compose.yml
git commit -m "feat: add config-server to compose.yml and wire all 9 services to it"
```

---

## Task 4: Live-refreshable outbox polling interval

**Files:**
- Modify: `ftgo-common/build.gradle`
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxProperties.java`
- Modify: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxPublisher.java`
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxSchedulingConfig.java`
- Modify: `ftgo-order-service/build.gradle`, `ftgo-kitchen-service/build.gradle`, `ftgo-accounting-service/build.gradle`, `ftgo-delivery-service/build.gradle`, `ftgo-consumer-service/build.gradle`
- Test: `ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxSchedulingConfigTest.java`

**Interfaces:**
- Consumes: `outbox.poll-fixed-delay-ms` from each of the 5 outbox services' config (local `application.yml` fallback from before this plan, or `config-repo/ftgo-<service>.yml` from Task 1 once the config server is live).
- Produces: `OutboxProperties` — a `@RefreshScope`/`@ConfigurationProperties(prefix = "outbox")` bean with `getPollFixedDelayMs()`/`setPollFixedDelayMs(long)`, consumed by `OutboxSchedulingConfig`. `OutboxPublisher.publishPendingEvents()`'s signature and behavior are unchanged (still `public void publishPendingEvents()`) — only its scheduling mechanism moves out of the class.
- Produces (Task 5 depends on this): after a `POST /actuator/refresh` call on an order-service instance whose config-repo file was edited, the next `publishPendingEvents()` invocation is scheduled using the new delay, without restart.

- [ ] **Step 1: Add Spring Cloud Context and Config Client to `ftgo-common`**

`@RefreshScope` lives in `spring-cloud-context`, which every consumer of `ftgo-common` needs on its classpath since `OutboxProperties`/`OutboxSchedulingConfig` live there. Add to `ftgo-common/build.gradle`'s `dependencies` block (alongside the existing `api 'org.springframework.kafka:spring-kafka'` line):
```groovy
    api 'org.springframework.cloud:spring-cloud-context'
```

- [ ] **Step 2: Add Spring Cloud Config Client to the 5 outbox services**

`/actuator/refresh` and `ContextRefresher` require `spring-cloud-starter-config` on the classpath of the process being refreshed. Add to each of `ftgo-order-service/build.gradle`, `ftgo-kitchen-service/build.gradle`, `ftgo-accounting-service/build.gradle`, `ftgo-delivery-service/build.gradle`, `ftgo-consumer-service/build.gradle` (creating the file with just this `dependencies` block if it doesn't already exist — check each file first, several of these services may already have a `build.gradle` with other entries to append to):
```groovy
    implementation 'org.springframework.cloud:spring-cloud-starter-config'
```

- [ ] **Step 3: Write the failing test for the properties bean**

Create `ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxSchedulingConfigTest.java`:
```java
package com.sanjay.ftgo.common.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TriggerContext;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxSchedulingConfigTest {

    @Test
    void nextExecutionUsesCurrentPropertyValue() {
        OutboxProperties properties = new OutboxProperties();
        properties.setPollFixedDelayMs(2000);
        OutboxSchedulingConfig.PollTrigger trigger = new OutboxSchedulingConfig.PollTrigger(properties);

        Instant lastCompletion = Instant.parse("2026-01-01T00:00:00Z");
        TriggerContext context = mock(TriggerContext.class);
        when(context.lastCompletion()).thenReturn(lastCompletion);

        Instant firstNext = trigger.nextExecution(context);
        assertThat(firstNext).isEqualTo(lastCompletion.plusMillis(2000));

        // Simulate a live refresh changing the bound property value.
        properties.setPollFixedDelayMs(500);
        Instant secondNext = trigger.nextExecution(context);
        assertThat(secondNext).isEqualTo(lastCompletion.plusMillis(500));
    }

    @Test
    void fallsBackToNowWhenNoPriorCompletion() {
        OutboxProperties properties = new OutboxProperties();
        properties.setPollFixedDelayMs(1000);
        OutboxSchedulingConfig.PollTrigger trigger = new OutboxSchedulingConfig.PollTrigger(properties);

        TriggerContext context = mock(TriggerContext.class);
        when(context.lastCompletion()).thenReturn(null);

        Instant before = Instant.now();
        Instant next = trigger.nextExecution(context);
        Instant after = Instant.now();

        assertThat(next).isBetween(before.plusMillis(1000), after.plusMillis(1000));
    }
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `./gradlew :ftgo-common:test --tests OutboxSchedulingConfigTest`
Expected: FAIL — `OutboxProperties` and `OutboxSchedulingConfig` do not exist yet.

- [ ] **Step 5: Implement `OutboxProperties`**

Create `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxProperties.java`:
```java
package com.sanjay.ftgo.common.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

// @RefreshScope wraps this bean in a proxy that gets torn down and re-bound to fresh property
// values on every POST /actuator/refresh, so OutboxSchedulingConfig always reads the current
// outbox.poll-fixed-delay-ms — including a value pulled from ftgo-config-server after a live
// config-repo edit, without restarting the process.
@Component
@RefreshScope
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    private long pollFixedDelayMs = 2000;

    public long getPollFixedDelayMs() {
        return pollFixedDelayMs;
    }

    public void setPollFixedDelayMs(long pollFixedDelayMs) {
        this.pollFixedDelayMs = pollFixedDelayMs;
    }
}
```

- [ ] **Step 6: Implement `OutboxSchedulingConfig`**

Create `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxSchedulingConfig.java`:
```java
package com.sanjay.ftgo.common.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.time.Instant;

// A plain @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms}") method resolves that
// placeholder once, at bean-creation time — it can never observe a later config-server refresh.
// Registering a Trigger here instead means Spring calls nextExecution(...) fresh before every
// run, so it can read the current value of the @RefreshScope-backed OutboxProperties bean each
// time, making the polling interval live-refreshable.
@Configuration
@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)
public class OutboxSchedulingConfig implements SchedulingConfigurer {

    private final OutboxPublisher outboxPublisher;
    private final OutboxProperties outboxProperties;

    public OutboxSchedulingConfig(OutboxPublisher outboxPublisher, OutboxProperties outboxProperties) {
        this.outboxPublisher = outboxPublisher;
        this.outboxProperties = outboxProperties;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(outboxPublisher::publishPendingEvents, new PollTrigger(outboxProperties));
    }

    static class PollTrigger implements Trigger {

        private final OutboxProperties outboxProperties;

        PollTrigger(OutboxProperties outboxProperties) {
            this.outboxProperties = outboxProperties;
        }

        @Override
        public Instant nextExecution(TriggerContext triggerContext) {
            Instant lastCompletion = triggerContext.lastCompletion();
            Instant base = (lastCompletion != null) ? lastCompletion : Instant.now();
            return base.plusMillis(outboxProperties.getPollFixedDelayMs());
        }
    }
}
```

- [ ] **Step 7: Remove the now-redundant `@Scheduled` annotation from `OutboxPublisher`**

In `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxPublisher.java`, change:
```java
    @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms:2000}")
    @Transactional
    public void publishPendingEvents() {
```
to:
```java
    @Transactional
    public void publishPendingEvents() {
```
Remove the now-unused `import org.springframework.scheduling.annotation.Scheduled;` line from the same file.

- [ ] **Step 8: Run test to verify it passes**

Run: `./gradlew :ftgo-common:test --tests OutboxSchedulingConfigTest`
Expected: PASS

- [ ] **Step 9: Run each outbox service's existing test suite**

`OutboxPublisher` no longer self-schedules — confirm each of the 5 services that rely on it still runs polling correctly end-to-end at the unit/integration level:
```bash
./gradlew :ftgo-order-service:test :ftgo-kitchen-service:test :ftgo-accounting-service:test :ftgo-delivery-service:test :ftgo-consumer-service:test
```
Expected: PASS on all 5 — any existing test that exercises outbox publishing (directly calling `publishPendingEvents()` or waiting for the `@Scheduled` cadence) should be unaffected, since `OutboxSchedulingConfig` still triggers the exact same method on the same default 2000ms cadence; only the trigger mechanism changed.

- [ ] **Step 10: Commit**

```bash
git add ftgo-common/build.gradle \
        ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxProperties.java \
        ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxPublisher.java \
        ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/OutboxSchedulingConfig.java \
        ftgo-common/src/test/java/com/sanjay/ftgo/common/outbox/OutboxSchedulingConfigTest.java \
        ftgo-order-service/build.gradle ftgo-kitchen-service/build.gradle \
        ftgo-accounting-service/build.gradle ftgo-delivery-service/build.gradle \
        ftgo-consumer-service/build.gradle
git commit -m "feat: make outbox polling interval live-refreshable via @RefreshScope"
```

---

## Task 5: End-to-end live-refresh Cucumber scenario

**Files:**
- Modify: `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature` (or wherever this project's existing outbox-timing-sensitive step definitions live — locate via the pattern used by the distributed-tracing/application-metrics scenarios; if a more fitting existing `.feature` file for outbox/order flows is found during implementation, add the scenario there instead and note the location in the commit message)
- Modify: `ftgo-end-to-end-test/src/test/java/.../PlaceReviseCancelOrderStepDefinitions.java` (exact package path: locate the file used for distributed tracing's `tempoEventuallyHasMultiServiceTrace` step, per the prior sub-project's summary — this plan assumes new step methods are added to that same file)

**Interfaces:**
- Consumes: `ftgo-config-server` (Task 1) running and reachable at `http://localhost:8888` (or the Docker-network address the e2e suite already uses for other services), `config-repo/ftgo-order-service.yml` (Task 1) as the file this scenario edits, order-service's `/actuator/refresh` endpoint (Task 4).
- Produces: none consumed by later tasks — this is the terminal verification task before documentation.

- [ ] **Step 1: Write the scenario**

Add to the existing `.feature` file used for order-placement scenarios:
```gherkin
  Scenario: Live config refresh changes the outbox polling interval without a restart
    Given the outbox poll interval for ftgo-order-service is set to 2000 milliseconds via the config repo
    When I place an order and measure the outbox publish delay
    Then the measured outbox publish delay is close to 2000 milliseconds
    When I set the outbox poll interval for ftgo-order-service to 300 milliseconds via the config repo
    And I refresh the configuration for ftgo-order-service
    And I place another order and measure the outbox publish delay
    Then the measured outbox publish delay is close to 300 milliseconds
```

- [ ] **Step 2: Implement the step definitions**

Add these methods to the step definitions class (imports assumed already present for `RestAssured`/`given()` style HTTP calls, matching this project's existing step-definition style for hitting actuator endpoints — adapt the exact HTTP client call to match whatever the file already uses for e.g. the Tempo API polling steps):
```java
private static final Path ORDER_SERVICE_CONFIG_FILE =
        Path.of(System.getProperty("user.dir"), "..", "config-repo", "ftgo-order-service.yml");

private long publishDelayMillis;

@Given("the outbox poll interval for ftgo-order-service is set to {int} milliseconds via the config repo")
public void setOutboxPollInterval(int millis) throws IOException {
    String content = String.format(
            "outbox:%n  poll-fixed-delay-ms: %d%n  batch-size: 20%n%nsaga:%n  mode: choreography%n",
            millis);
    Files.writeString(ORDER_SERVICE_CONFIG_FILE, content);
}

@When("I set the outbox poll interval for ftgo-order-service to {int} milliseconds via the config repo")
public void updateOutboxPollInterval(int millis) throws IOException {
    setOutboxPollInterval(millis);
}

@When("I refresh the configuration for ftgo-order-service")
public void refreshOrderServiceConfig() {
    given()
        .baseUri(orderServiceBaseUrl()) // reuse whatever base-URL helper this class already has for order-service calls
        .post("/actuator/refresh")
        .then()
        .statusCode(200);
}

@When("I place an order and measure the outbox publish delay")
public void placeOrderAndMeasureDelay() {
    long start = System.currentTimeMillis();
    placeOrder(); // reuse this project's existing order-placement step helper
    waitForOrderEventOnKafka(); // reuse this project's existing Kafka-event-wait helper used by saga scenarios
    publishDelayMillis = System.currentTimeMillis() - start;
}

@When("I place another order and measure the outbox publish delay")
public void placeAnotherOrderAndMeasureDelay() {
    placeOrderAndMeasureDelay();
}

@Then("the measured outbox publish delay is close to {int} milliseconds")
public void assertPublishDelayCloseTo(int expectedMillis) {
    // Generous tolerance: this measures wall-clock time across an HTTP call, a DB write, a
    // scheduled poll, and a Kafka round-trip, not just the poll interval in isolation.
    assertThat(publishDelayMillis).isBetween((long) expectedMillis, expectedMillis + 3000L);
}
```
Replace `placeOrder()`, `waitForOrderEventOnKafka()`, and `orderServiceBaseUrl()` with this project's actual existing helper method names found in the step definitions file — these three helpers already exist in some form (every prior order-placement scenario needs them); do not reimplement order placement or Kafka waiting from scratch.

- [ ] **Step 3: Bring the stack up and run the scenario**

```bash
docker compose up -d --build
./gradlew :ftgo-end-to-end-test:test --tests "*LiveConfigRefresh*"
```
Expected: PASS — the second measured delay is meaningfully smaller than the first, proving the refresh took effect without restarting `order-service`.

- [ ] **Step 4: Manual verification (per spec)**

With the stack still up: stop the `config-server` container (`docker compose stop config-server`), then restart `order-service` (`docker compose restart order-service`) and confirm via `docker compose logs order-service` that it starts successfully and logs a config-import warning rather than failing — proving the non-blocking fallback path. Restart `config-server` afterward (`docker compose start config-server`) to leave the stack in its normal state.

- [ ] **Step 5: Commit**

```bash
git add ftgo-end-to-end-test
git commit -m "test: add e2e scenario verifying live outbox-interval refresh via config server"
```

---

## Task 6: Documentation sweep

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Create: `ftgo-config-server/README.md`
- Modify: `ftgo-order-service/README.md`, `ftgo-kitchen-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-delivery-service/README.md`, `ftgo-consumer-service/README.md`, `ftgo-order-history-service/README.md`, `ftgo-restaurant-service/README.md`, `ftgo-mobile-gateway/README.md`, `ftgo-public-gateway/README.md`
- Modify: `README.md`
- Modify: `CONTEXT.md`

**Interfaces:** None — this task only touches documentation.

- [ ] **Step 1: `docs/ARCHITECTURE.md`**

Add an "Externalized configuration (Ch.11, §11.2)" section describing: the push/pull split (compose env vars > config-server > local `application.yml` fallback), the git-backed config server pointed at this repo's own `config-repo/`, the `optional:`/`fail-fast: false` non-blocking contract, and the live-refresh mechanism (`OutboxProperties`/`OutboxSchedulingConfig`/`/actuator/refresh`) with the exact property name (`outbox.poll-fixed-delay-ms`) that's refreshable and which ones are not (Kafka/Eureka/JWT settings — explain why, per the spec's Out of scope section).

- [ ] **Step 2: `ftgo-config-server/README.md`**

Create following this project's existing per-service README structure (see `ftgo-authorization-server/README.md` for the template of a small infra-service README): what it is, port 8888, the git backend and `search-paths: config-repo`, how to query it manually (`curl http://localhost:8888/ftgo-order-service/default`), and that it's a leaf service with no `depends_on`.

- [ ] **Step 3: Per-service "Configuration" subsections**

Add a short "Configuration" subsection to each of the 9 service READMEs listed above: which shared values come from `config-repo/application.yml`, which per-service values come from `config-repo/ftgo-<service>.yml` (only for the 5 outbox services — say so explicitly for the other 4), and the fallback behavior if the config server is unreachable. For `ftgo-order-service/README.md` specifically, additionally document the live-refresh demo (`outbox.poll-fixed-delay-ms` via `POST /actuator/refresh`).

- [ ] **Step 4: Root `README.md`**

Add Spring Cloud Config Server to the tech stack list. Update the "Book progress" table's Ch.11 row/sub-row for §11.2 to done, consistent with how §11.3.3/§11.3.4/§11.1 rows were updated in prior sub-projects.

- [ ] **Step 5: `CONTEXT.md`**

Update "Current position" with the same level of detail as the distributed-tracing entry (see the existing Ch.11 paragraph in `CONTEXT.md` for the expected depth/style) describing what was built. Change `- [ ] Externalized configuration (Ch. 11)` to `- [x] Externalized configuration (Ch. 11)` in the services/patterns checklist. Do not perform a full chapter-completion sweep — §11.3's remaining three patterns (log aggregation, exception tracking, audit logging) are still unscheduled, so Ch.11 stays **In progress** overall, per the spec's Documentation section.

- [ ] **Step 6: Commit**

```bash
git add docs/ARCHITECTURE.md ftgo-config-server/README.md \
        ftgo-order-service/README.md ftgo-kitchen-service/README.md \
        ftgo-accounting-service/README.md ftgo-delivery-service/README.md \
        ftgo-consumer-service/README.md ftgo-order-history-service/README.md \
        ftgo-restaurant-service/README.md ftgo-mobile-gateway/README.md \
        ftgo-public-gateway/README.md README.md CONTEXT.md
git commit -m "docs: document Ch.11 §11.2 externalized configuration"
```
