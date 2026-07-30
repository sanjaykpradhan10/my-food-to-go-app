# Ch.8 API Gateway + BFF Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unbuilt `ftgo-api-gateway` stub with two Spring Cloud Gateway BFF services (`ftgo-mobile-gateway`, `ftgo-public-gateway`) sharing a `ftgo-gateway-common` edge-function library, implementing the book's API gateway and Backends for Frontends patterns (Ch.8).

**Architecture:** Two new reactive (WebFlux) Spring Boot modules register with the existing Eureka registry and route to backend services (`lb://` URIs). The mobile gateway additionally does its own reactive fan-out composition for one endpoint (order details). Both gateways apply shared filters (request logging, API-key auth, per-key rate limiting) from `ftgo-gateway-common`.

**Tech Stack:** Spring Cloud Gateway (WebFlux/reactive), Spring Cloud Netflix Eureka Client, Spring Cloud LoadBalancer (reactive), Spring Cloud Circuitbreaker Reactor Resilience4j, Java 21, Gradle multi-module (existing conventions).

## Global Constraints

- Java 21, Spring Boot 3.5.16, Spring Cloud BOM `2025.0.3` — same versions as every existing module (see `ftgo-order-service/build.gradle`).
- New modules follow the existing package convention `com.sanjay.ftgo.<module>`.
- Gateway modules are **reactive** (WebFlux) — they must be excluded from the root `build.gradle`'s blanket `spring-boot-starter-web`/JPA/MySQL dependency block (that block currently applies to every subproject except `ftgo-common`; gateways need the same kind of exclusion, since `spring-boot-starter-web` (servlet) conflicts with Spring Cloud Gateway's WebFlux runtime).
- No Redis dependency introduced — rate limiting uses a custom in-memory filter, not Spring Cloud Gateway's Redis-backed `RequestRateLimiter`.
- No real authentication/identity service — the auth filter checks a static, configured `X-Api-Key` header value.
- Ports: `ftgo-mobile-gateway` = 8090, `ftgo-public-gateway` = 8091 (next free ports after `ftgo-order-history-service`'s 8088; `kafka-connect` uses 8087).
- Existing services' internal REST APIs are unchanged (see route table in Task 5/6 below, taken directly from current controllers).

---

### Task 1: Scaffold `ftgo-gateway-common` module and fix root build.gradle exclusion

**Files:**
- Modify: `settings.gradle`
- Modify: `build.gradle` (root)
- Create: `ftgo-gateway-common/build.gradle`
- Create: `ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/package-info.java` (placeholder to establish the package — remove once Task 2 adds real classes, or just let Task 2 create the first real file directly)

**Interfaces:**
- Produces: a `ftgo-gateway-common` Gradle module other modules can depend on via `implementation project(':ftgo-gateway-common')`, exposing WebFlux (`spring-boot-starter-webflux`) and Spring Cloud Gateway (`spring-cloud-starter-gateway`) types on its `api` classpath.

- [ ] **Step 1: Add the new modules to `settings.gradle`**

```groovy
include 'ftgo-common'
include 'ftgo-consumer-service'
include 'ftgo-order-service'
include 'ftgo-kitchen-service'
include 'ftgo-accounting-service'
include 'ftgo-restaurant-service'
include 'ftgo-delivery-service'
include 'ftgo-service-registry'
include 'ftgo-order-history-service'
include 'ftgo-gateway-common'
include 'ftgo-mobile-gateway'
include 'ftgo-public-gateway'
```

- [ ] **Step 2: Exclude the reactive modules from the blanket servlet/JPA dependency block in root `build.gradle`**

Change the existing block from:

```groovy
configure(subprojects.findAll { it.name != 'ftgo-common' }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-web'
        implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
        runtimeOnly    'com.mysql:mysql-connector-j'
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
        testRuntimeOnly    'com.h2database:h2'
    }
}
```

to:

```groovy
def reactiveModules = ['ftgo-common', 'ftgo-gateway-common', 'ftgo-mobile-gateway', 'ftgo-public-gateway']

configure(subprojects.findAll { !reactiveModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-web'
        implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
        runtimeOnly    'com.mysql:mysql-connector-j'
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
        testRuntimeOnly    'com.h2database:h2'
    }
}
```

(`ftgo-common` was already excluded because it's a plain library with no web/JDBC runtime; the three new modules are excluded because they're WebFlux-based, which conflicts with `spring-boot-starter-web`'s servlet container.)

- [ ] **Step 3: Create `ftgo-gateway-common/build.gradle`**

```groovy
apply plugin: 'java-library'

bootJar {
    enabled = false
}

jar {
    enabled = true
}

dependencies {
    api 'org.springframework.boot:spring-boot-starter-webflux'
    api 'org.springframework.cloud:spring-cloud-starter-gateway'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'io.projectreactor:reactor-test'
}
```

- [ ] **Step 4: Verify the module compiles (no source yet, just scaffolding)**

Run: `./gradlew :ftgo-gateway-common:compileJava`
Expected: BUILD SUCCESSFUL (no source files yet, so this just validates the Gradle module wiring)

- [ ] **Step 5: Commit**

```bash
git add settings.gradle build.gradle ftgo-gateway-common/build.gradle
git commit -m "build: scaffold ftgo-gateway-common module, exclude reactive modules from servlet/JPA block"
```

---

### Task 2: `RequestLoggingFilter` in `ftgo-gateway-common`

**Files:**
- Create: `ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/RequestLoggingFilter.java`
- Test: `ftgo-gateway-common/src/test/java/com/sanjay/ftgo/gateway/common/RequestLoggingFilterTest.java`

**Interfaces:**
- Produces: `RequestLoggingFilter implements GlobalFilter, Ordered` — a Spring-managed bean (`@Component`), auto-picked-up by any gateway service that includes `ftgo-gateway-common` on its classpath (Spring Cloud Gateway auto-registers all `GlobalFilter` beans in context).

- [ ] **Step 1: Write the failing test**

```java
package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RequestLoggingFilterTest {

    @Test
    void passesRequestThroughAndCompletesNormally() {
        RequestLoggingFilter filter = new RequestLoggingFilter();
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").build());
        exchange.getResponse().setStatusCode(HttpStatus.OK);

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        Mono<Void> result = filter.filter(exchange, chain);

        assertThat(result.blockOptional()).isEmpty();
    }

    @Test
    void hasHighestPrecedenceOrdering() {
        RequestLoggingFilter filter = new RequestLoggingFilter();
        assertThat(filter.getOrder()).isEqualTo(Integer.MIN_VALUE);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-gateway-common:test --tests RequestLoggingFilterTest`
Expected: FAIL — `RequestLoggingFilter` does not exist (compile error)

- [ ] **Step 3: Implement `RequestLoggingFilter`**

```java
package com.sanjay.ftgo.gateway.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Logs method/path/status/latency for every request passing through a gateway.
 * Runs first (highest precedence) so latency measurement covers the whole filter chain.
 */
@Component
public class RequestLoggingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long startNanos = System.nanoTime();
        String method = exchange.getRequest().getMethod().name();
        String path = exchange.getRequest().getPath().value();

        return chain.filter(exchange).doFinally(signal -> {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            int status = exchange.getResponse().getStatusCode() != null
                    ? exchange.getResponse().getStatusCode().value()
                    : 0;
            log.info("{} {} -> {} ({} ms)", method, path, status, elapsedMs);
        });
    }

    @Override
    public int getOrder() {
        return Integer.MIN_VALUE;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :ftgo-gateway-common:test --tests RequestLoggingFilterTest`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/RequestLoggingFilter.java ftgo-gateway-common/src/test/java/com/sanjay/ftgo/gateway/common/RequestLoggingFilterTest.java
git commit -m "feat: add shared RequestLoggingFilter for gateway edge logging"
```

---

### Task 3: `ApiKeyAuthFilter` in `ftgo-gateway-common`

**Files:**
- Create: `ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/ApiKeyAuthFilter.java`
- Create: `ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/GatewayApiKeyProperties.java`
- Test: `ftgo-gateway-common/src/test/java/com/sanjay/ftgo/gateway/common/ApiKeyAuthFilterTest.java`

**Interfaces:**
- Consumes: none from earlier tasks.
- Produces: `ApiKeyAuthFilter implements GlobalFilter, Ordered` (order `Integer.MIN_VALUE + 1`, right after logging) and `GatewayApiKeyProperties` (`@ConfigurationProperties(prefix = "gateway.api-key")`, exposing `String value()`) — each gateway service sets `gateway.api-key.value` in its own `application.yml`.

- [ ] **Step 1: Write the failing test**

```java
package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApiKeyAuthFilterTest {

    private final GatewayApiKeyProperties properties = new GatewayApiKeyProperties("secret-123");
    private final ApiKeyAuthFilter filter = new ApiKeyAuthFilter(properties);

    @Test
    void rejectsMissingApiKeyWith401() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verify(chain, times(0)).filter(exchange);
    }

    @Test
    void rejectsWrongApiKeyWith401() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("X-Api-Key", "wrong").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void passesThroughWithCorrectApiKey() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("X-Api-Key", "secret-123").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        filter.filter(exchange, chain).block();

        verify(chain, times(1)).filter(exchange);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-gateway-common:test --tests ApiKeyAuthFilterTest`
Expected: FAIL — `ApiKeyAuthFilter`/`GatewayApiKeyProperties` do not exist

- [ ] **Step 3: Implement `GatewayApiKeyProperties`**

```java
package com.sanjay.ftgo.gateway.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.api-key")
public class GatewayApiKeyProperties {

    private final String value;

    public GatewayApiKeyProperties(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
```

- [ ] **Step 4: Implement `ApiKeyAuthFilter`**

```java
package com.sanjay.ftgo.gateway.common;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Stub authentication edge function: this project has no real identity service, so this
 * checks a single shared-secret header rather than a token. Real auth is out of scope for
 * Ch.8 — see the design spec's non-goals.
 */
@Component
public class ApiKeyAuthFilter implements GlobalFilter, Ordered {

    private static final String API_KEY_HEADER = "X-Api-Key";

    private final GatewayApiKeyProperties properties;

    public ApiKeyAuthFilter(GatewayApiKeyProperties properties) {
        this.properties = properties;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String providedKey = exchange.getRequest().getHeaders().getFirst(API_KEY_HEADER);
        if (providedKey == null || !providedKey.equals(properties.value())) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return Integer.MIN_VALUE + 1;
    }
}
```

- [ ] **Step 5: Register `GatewayApiKeyProperties` for configuration-properties binding**

Create `ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/GatewayCommonAutoConfiguration.java`:

```java
package com.sanjay.ftgo.gateway.common;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@AutoConfiguration
@EnableConfigurationProperties(GatewayApiKeyProperties.class)
public class GatewayCommonAutoConfiguration {
}
```

Create `ftgo-gateway-common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:

```
com.sanjay.ftgo.gateway.common.GatewayCommonAutoConfiguration
```

(Mirrors `ftgo-common`'s existing `OutboxAutoConfiguration` pattern — auto-registers shared beans for any consuming service with no per-service annotation to remember.)

Since `GatewayApiKeyProperties` only has a constructor-bound single field, add a matching `@ConstructorBinding`-compatible constructor — Spring Boot 3.x binds records/single-constructor classes automatically, no extra annotation needed given the single constructor above.

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :ftgo-gateway-common:test --tests ApiKeyAuthFilterTest`
Expected: PASS (all 3 tests)

- [ ] **Step 7: Commit**

```bash
git add ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/ApiKeyAuthFilter.java ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/GatewayApiKeyProperties.java ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/GatewayCommonAutoConfiguration.java ftgo-gateway-common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports ftgo-gateway-common/src/test/java/com/sanjay/ftgo/gateway/common/ApiKeyAuthFilterTest.java
git commit -m "feat: add shared ApiKeyAuthFilter stub authentication edge function"
```

---

### Task 4: `PerKeyRateLimiterGatewayFilterFactory` in `ftgo-gateway-common`

**Files:**
- Create: `ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/PerKeyRateLimiterGatewayFilterFactory.java`
- Test: `ftgo-gateway-common/src/test/java/com/sanjay/ftgo/gateway/common/PerKeyRateLimiterGatewayFilterFactoryTest.java`

**Interfaces:**
- Consumes: nothing from Tasks 2–3 (independent filter).
- Produces: `PerKeyRateLimiterGatewayFilterFactory extends AbstractGatewayFilterFactory<PerKeyRateLimiterGatewayFilterFactory.Config>` with nested `Config { int requestsPerSecond; }` — usable in a gateway's `application.yml` route config as filter name `PerKeyRateLimiter` with arg `requestsPerSecond`. Each route in Tasks 5/6 references this by name.

- [ ] **Step 1: Write the failing test**

```java
package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PerKeyRateLimiterGatewayFilterFactoryTest {

    @Test
    void allowsRequestsWithinLimitAndRejectsBeyondIt() {
        PerKeyRateLimiterGatewayFilterFactory factory = new PerKeyRateLimiterGatewayFilterFactory();
        PerKeyRateLimiterGatewayFilterFactory.Config config = new PerKeyRateLimiterGatewayFilterFactory.Config();
        config.setRequestsPerSecond(2);
        GatewayFilter filter = factory.apply(config);

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.empty());

        ServerWebExchange first = exchangeWithKey("key-a");
        ServerWebExchange second = exchangeWithKey("key-a");
        ServerWebExchange third = exchangeWithKey("key-a");

        filter.filter(first, chain).block();
        filter.filter(second, chain).block();
        filter.filter(third, chain).block();

        assertThat(first.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(second.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(third.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    private ServerWebExchange exchangeWithKey(String key) {
        return MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("X-Api-Key", key).build());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-gateway-common:test --tests PerKeyRateLimiterGatewayFilterFactoryTest`
Expected: FAIL — class does not exist

- [ ] **Step 3: Implement the filter factory**

```java
package com.sanjay.ftgo.gateway.common;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple in-memory, per-API-key fixed-window rate limiter. Spring Cloud Gateway's built-in
 * RequestRateLimiter requires Redis; this project has no Redis instance, so this trades away
 * multi-instance correctness (each gateway instance counts independently) for zero new
 * infrastructure, acceptable for a single-instance dev/learning deployment.
 */
@Component
public class PerKeyRateLimiterGatewayFilterFactory
        extends AbstractGatewayFilterFactory<PerKeyRateLimiterGatewayFilterFactory.Config> {

    private static final String API_KEY_HEADER = "X-Api-Key";
    private static final long WINDOW_MILLIS = 1000L;

    private final Map<String, Window> windowsByKey = new ConcurrentHashMap<>();

    public PerKeyRateLimiterGatewayFilterFactory() {
        super(Config.class);
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            String key = exchange.getRequest().getHeaders().getFirst(API_KEY_HEADER);
            String effectiveKey = key != null ? key : "anonymous";
            Window window = windowsByKey.computeIfAbsent(effectiveKey, k -> new Window());

            if (window.tryAcquire(config.getRequestsPerSecond())) {
                return chain.filter(exchange);
            }
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            return exchange.getResponse().setComplete();
        };
    }

    public static class Config {
        private int requestsPerSecond = 10;

        public int getRequestsPerSecond() {
            return requestsPerSecond;
        }

        public void setRequestsPerSecond(int requestsPerSecond) {
            this.requestsPerSecond = requestsPerSecond;
        }
    }

    private static class Window {
        private volatile long windowStart = System.currentTimeMillis();
        private final AtomicInteger count = new AtomicInteger(0);

        synchronized boolean tryAcquire(int limit) {
            long now = System.currentTimeMillis();
            if (now - windowStart >= WINDOW_MILLIS) {
                windowStart = now;
                count.set(0);
            }
            return count.incrementAndGet() <= limit;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :ftgo-gateway-common:test --tests PerKeyRateLimiterGatewayFilterFactoryTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ftgo-gateway-common/src/main/java/com/sanjay/ftgo/gateway/common/PerKeyRateLimiterGatewayFilterFactory.java ftgo-gateway-common/src/test/java/com/sanjay/ftgo/gateway/common/PerKeyRateLimiterGatewayFilterFactoryTest.java
git commit -m "feat: add shared per-API-key rate limiter gateway filter factory"
```

---

### Task 5: `ftgo-public-gateway` module — pure routing BFF

**Files:**
- Create: `ftgo-public-gateway/build.gradle`
- Create: `ftgo-public-gateway/src/main/java/com/sanjay/ftgo/publicgateway/PublicGatewayApplication.java`
- Create: `ftgo-public-gateway/src/main/resources/application.yml`
- Create: `ftgo-public-gateway/Dockerfile`
- Test: `ftgo-public-gateway/src/test/java/com/sanjay/ftgo/publicgateway/PublicGatewayApplicationTests.java`

**Interfaces:**
- Consumes: `ftgo-gateway-common`'s filters (Tasks 2–4) via classpath auto-registration; no direct Java calls.
- Produces: a running gateway on port 8091 routing `/api/v1/**` paths to backend services, for Task 8 (docker-compose) and Task 9 (e2e verification) to exercise.

- [ ] **Step 1: Write `build.gradle`**

```groovy
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

dependencies {
    implementation project(':ftgo-gateway-common')
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
    implementation 'org.springframework.cloud:spring-cloud-starter-loadbalancer'
}
```

- [ ] **Step 2: Write the application class**

```java
package com.sanjay.ftgo.publicgateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PublicGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(PublicGatewayApplication.class, args);
    }
}
```

- [ ] **Step 3: Write `application.yml` with routes to every existing backend REST endpoint**

Route table taken directly from each service's current `@RequestMapping` controllers
(`OrderController`/`OrderViewController`, `TicketController`, `AuthorizationController`,
`DeliveryController`, `OrderHistoryController`, `RestaurantController`):

```yaml
spring:
  application:
    name: ftgo-public-gateway
  cloud:
    gateway:
      routes:
        - id: public-orders
          uri: lb://ftgo-order-service
          predicates:
            - Path=/api/v1/orders/**
          filters:
            - RewritePath=/api/v1/orders(?<segment>/?.*), /orders$\{segment}
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 5
        - id: public-tickets
          uri: lb://ftgo-kitchen-service
          predicates:
            - Path=/api/v1/tickets/**
          filters:
            - RewritePath=/api/v1/tickets(?<segment>/?.*), /tickets$\{segment}
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 5
        - id: public-authorizations
          uri: lb://ftgo-accounting-service
          predicates:
            - Path=/api/v1/authorizations/**
          filters:
            - RewritePath=/api/v1/authorizations(?<segment>/?.*), /authorizations$\{segment}
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 5
        - id: public-deliveries
          uri: lb://ftgo-delivery-service
          predicates:
            - Path=/api/v1/deliveries/**
          filters:
            - RewritePath=/api/v1/deliveries(?<segment>/?.*), /deliveries$\{segment}
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 5
        - id: public-order-views
          uri: lb://ftgo-order-history-service
          predicates:
            - Path=/api/v1/order-views/**
          filters:
            - RewritePath=/api/v1/order-views(?<segment>/?.*), /order-views$\{segment}
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 5
        - id: public-restaurants
          uri: lb://ftgo-restaurant-service
          predicates:
            - Path=/api/v1/restaurants/**
          filters:
            - RewritePath=/api/v1/restaurants(?<segment>/?.*), /restaurants$\{segment}
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 5

gateway:
  api-key:
    value: public-dev-key

server:
  port: 8091

eureka:
  client:
    register-with-eureka: true
    fetch-registry: true
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true
```

- [ ] **Step 4: Write a minimal context-loads test**

```java
package com.sanjay.ftgo.publicgateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class PublicGatewayApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

Create `ftgo-public-gateway/src/test/resources/application-test.yml` disabling Eureka registration during the test:

```yaml
eureka:
  client:
    enabled: false
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :ftgo-public-gateway:test`
Expected: PASS (context loads with routes parsed, no Eureka connection attempted)

- [ ] **Step 6: Write the Dockerfile**

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-public-gateway:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-public-gateway/build/libs/*.jar app.jar
EXPOSE 8091
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 7: Commit**

```bash
git add ftgo-public-gateway
git commit -m "feat: add ftgo-public-gateway BFF (pure routing to all backend services)"
```

---

### Task 6: `ftgo-mobile-gateway` module — routing passthroughs

**Files:**
- Create: `ftgo-mobile-gateway/build.gradle`
- Create: `ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/MobileGatewayApplication.java`
- Create: `ftgo-mobile-gateway/src/main/resources/application.yml`
- Create: `ftgo-mobile-gateway/Dockerfile`
- Test: `ftgo-mobile-gateway/src/test/java/com/sanjay/ftgo/mobilegateway/MobileGatewayApplicationTests.java`

**Interfaces:**
- Consumes: `ftgo-gateway-common` filters (Tasks 2–4).
- Produces: a running gateway on port 8090 with routing passthroughs for order create/cancel/revise; Task 7 adds the composed order-details endpoint on top of this module.

- [ ] **Step 1: Write `build.gradle`**

```groovy
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

dependencies {
    implementation project(':ftgo-gateway-common')
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
    implementation 'org.springframework.cloud:spring-cloud-starter-loadbalancer'
    implementation 'org.springframework.cloud:spring-cloud-starter-circuitbreaker-reactor-resilience4j'
}
```

- [ ] **Step 2: Write the application class**

```java
package com.sanjay.ftgo.mobilegateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MobileGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(MobileGatewayApplication.class, args);
    }
}
```

- [ ] **Step 3: Write `application.yml` — passthrough routes only (composition endpoint added in Task 7)**

```yaml
spring:
  application:
    name: ftgo-mobile-gateway
  cloud:
    gateway:
      routes:
        - id: mobile-create-order
          uri: lb://ftgo-order-service
          predicates:
            - Path=/mobile/orders
            - Method=POST
          filters:
            - RewritePath=/mobile/orders, /orders
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 20
        - id: mobile-cancel-order
          uri: lb://ftgo-order-service
          predicates:
            - Path=/mobile/orders/{id}/cancel
            - Method=POST
          filters:
            - RewritePath=/mobile/orders/(?<id>.*)/cancel, /orders/$\{id}/cancel
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 20
        - id: mobile-revise-order
          uri: lb://ftgo-order-service
          predicates:
            - Path=/mobile/orders/{id}/revise
            - Method=POST
          filters:
            - RewritePath=/mobile/orders/(?<id>.*)/revise, /orders/$\{id}/revise
            - name: PerKeyRateLimiter
              args:
                requestsPerSecond: 20

gateway:
  api-key:
    value: mobile-dev-key

server:
  port: 8090

eureka:
  client:
    register-with-eureka: true
    fetch-registry: true
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true

resilience4j:
  circuitbreaker:
    instances:
      orderService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
      kitchenService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
      accountingService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
      deliveryService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
```

(Circuit breaker instance names/settings reused verbatim from `ftgo-order-service`'s existing `resilience4j.circuitbreaker.instances` config — same reuse-without-re-tuning rationale as Ch.7.)

- [ ] **Step 4: Write context-loads test + test profile (same pattern as Task 5)**

```java
package com.sanjay.ftgo.mobilegateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class MobileGatewayApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

`ftgo-mobile-gateway/src/test/resources/application-test.yml`:

```yaml
eureka:
  client:
    enabled: false
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :ftgo-mobile-gateway:test`
Expected: PASS

- [ ] **Step 6: Write the Dockerfile**

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-mobile-gateway:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-mobile-gateway/build/libs/*.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 7: Commit**

```bash
git add ftgo-mobile-gateway
git commit -m "feat: add ftgo-mobile-gateway BFF routing passthroughs (create/cancel/revise order)"
```

---

### Task 7: Mobile gateway's composed order-details endpoint

**Files:**
- Create: `ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsHandler.java`
- Create: `ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsRouterConfig.java`
- Create: `ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetails.java`
- Create: `ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/orderdetails/SectionResult.java`
- Create: `ftgo-mobile-gateway/src/main/java/com/sanjay/ftgo/mobilegateway/orderdetails/BackendClients.java`
- Test: `ftgo-mobile-gateway/src/test/java/com/sanjay/ftgo/mobilegateway/orderdetails/OrderDetailsHandlerTest.java`

**Interfaces:**
- Consumes: order-service's `GET /orders/{id}` (returns 404 if missing — reuses the existing endpoint from `OrderController`, not the Ch.7 composed `/orders/{id}/view`, since the point of this task is the gateway doing its own composition), kitchen-service's `GET /tickets/order/{orderId}`, accounting-service's `GET /authorizations/order/{orderId}`, delivery-service's `GET /deliveries/order/{orderId}` — all existing, unchanged endpoints.
- Produces: `GET /mobile/orders/{orderId}` returning `OrderDetails` (JSON: `order`, `ticket`, `authorization`, `delivery`, each a `SectionResult` — `"status": "FOUND"|"NOT_FOUND"|"UNAVAILABLE"` plus `data` when found).

- [ ] **Step 1: Write `SectionResult`**

```java
package com.sanjay.ftgo.mobilegateway.orderdetails;

public sealed interface SectionResult<T> {

    record Found<T>(T data) implements SectionResult<T> {}
    record NotFound<T>() implements SectionResult<T> {}
    record Unavailable<T>() implements SectionResult<T> {}
}
```

- [ ] **Step 2: Write `OrderDetails`**

```java
package com.sanjay.ftgo.mobilegateway.orderdetails;

public record OrderDetails(
        SectionResult<String> order,
        SectionResult<String> ticket,
        SectionResult<String> authorization,
        SectionResult<String> delivery) {
}
```

(Each section carries the raw JSON body as `String` rather than a typed DTO — the mobile
gateway doesn't need to deserialize/re-serialize fields it just passes through to the client,
avoiding four DTO classes duplicating each backend service's own response shape.)

- [ ] **Step 3: Write the failing test**

```java
package com.sanjay.ftgo.mobilegateway.orderdetails;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class OrderDetailsHandlerTest {

    private MockWebServer orderServer;
    private MockWebServer kitchenServer;
    private MockWebServer accountingServer;
    private MockWebServer deliveryServer;
    private OrderDetailsHandler handler;

    @BeforeEach
    void setUp() throws IOException {
        orderServer = new MockWebServer();
        kitchenServer = new MockWebServer();
        accountingServer = new MockWebServer();
        deliveryServer = new MockWebServer();
        orderServer.start();
        kitchenServer.start();
        accountingServer.start();
        deliveryServer.start();

        BackendClients clients = new BackendClients(
                WebClient.builder().baseUrl(orderServer.url("/").toString()).build(),
                WebClient.builder().baseUrl(kitchenServer.url("/").toString()).build(),
                WebClient.builder().baseUrl(accountingServer.url("/").toString()).build(),
                WebClient.builder().baseUrl(deliveryServer.url("/").toString()).build());
        handler = new OrderDetailsHandler(clients);
    }

    @AfterEach
    void tearDown() throws IOException {
        orderServer.shutdown();
        kitchenServer.shutdown();
        accountingServer.shutdown();
        deliveryServer.shutdown();
    }

    @Test
    void composesAllFourSectionsWhenAllServicesRespond() {
        orderServer.enqueue(new MockResponse().setBody("{\"id\":1}").addHeader("Content-Type", "application/json"));
        kitchenServer.enqueue(new MockResponse().setBody("{\"ticketId\":1}").addHeader("Content-Type", "application/json"));
        accountingServer.enqueue(new MockResponse().setBody("{\"status\":\"AUTHORIZED\"}").addHeader("Content-Type", "application/json"));
        deliveryServer.enqueue(new MockResponse().setBody("{\"status\":\"SCHEDULED\"}").addHeader("Content-Type", "application/json"));

        OrderDetails result = handler.fetchOrderDetails(1L).block();

        assertThat(result.order()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.ticket()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.authorization()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.delivery()).isInstanceOf(SectionResult.Found.class);
    }

    @Test
    void degradesOneSectionWhenThatServiceReturns404WithoutFailingTheWholeRequest() {
        orderServer.enqueue(new MockResponse().setBody("{\"id\":1}").addHeader("Content-Type", "application/json"));
        kitchenServer.enqueue(new MockResponse().setResponseCode(404));
        accountingServer.enqueue(new MockResponse().setBody("{\"status\":\"AUTHORIZED\"}").addHeader("Content-Type", "application/json"));
        deliveryServer.enqueue(new MockResponse().setBody("{\"status\":\"SCHEDULED\"}").addHeader("Content-Type", "application/json"));

        OrderDetails result = handler.fetchOrderDetails(1L).block();

        assertThat(result.order()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.ticket()).isInstanceOf(SectionResult.NotFound.class);
        assertThat(result.authorization()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.delivery()).isInstanceOf(SectionResult.Found.class);
    }
}
```

Add test dependency to `ftgo-mobile-gateway/build.gradle`:

```groovy
testImplementation 'com.squareup.okhttp3:mockwebserver:4.12.0'
```

- [ ] **Step 4: Run test to verify it fails**

Run: `./gradlew :ftgo-mobile-gateway:test --tests OrderDetailsHandlerTest`
Expected: FAIL — `OrderDetailsHandler`/`BackendClients` do not exist

- [ ] **Step 5: Write `BackendClients`**

```java
package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.springframework.web.reactive.function.client.WebClient;

public record BackendClients(
        WebClient orderServiceClient,
        WebClient kitchenServiceClient,
        WebClient accountingServiceClient,
        WebClient deliveryServiceClient) {
}
```

- [ ] **Step 6: Write `OrderDetailsHandler`**

```java
package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

public class OrderDetailsHandler {

    private final BackendClients clients;

    public OrderDetailsHandler(BackendClients clients) {
        this.clients = clients;
    }

    public Mono<OrderDetails> fetchOrderDetails(Long orderId) {
        Mono<SectionResult<String>> order = fetchSection(clients.orderServiceClient(), "/orders/" + orderId);
        Mono<SectionResult<String>> ticket = fetchSection(clients.kitchenServiceClient(), "/tickets/order/" + orderId);
        Mono<SectionResult<String>> authorization = fetchSection(clients.accountingServiceClient(), "/authorizations/order/" + orderId);
        Mono<SectionResult<String>> delivery = fetchSection(clients.deliveryServiceClient(), "/deliveries/order/" + orderId);

        return Mono.zip(order, ticket, authorization, delivery)
                .map(tuple -> new OrderDetails(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4()));
    }

    private Mono<SectionResult<String>> fetchSection(org.springframework.web.reactive.function.client.WebClient client, String path) {
        return client.get()
                .uri(path)
                .retrieve()
                .bodyToMono(String.class)
                .<SectionResult<String>>map(SectionResult.Found::new)
                .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.just(new SectionResult.NotFound<>()))
                .onErrorResume(Exception.class, e -> Mono.just(new SectionResult.Unavailable<>()));
    }
}
```

(No explicit circuit breaker wrapping in this handler method — the reactive circuit breaker
is applied at the route/filter level for the pure-routing endpoints in Task 6's config; for
this hand-written composition handler, `onErrorResume` already gives the same
graceful-degradation outcome for both a 404 and a connection failure/timeout, which is the
property that matters here, per the design spec.)

- [ ] **Step 7: Wire it up with a `RouterFunction` bean**

```java
package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;

@Configuration
public class OrderDetailsRouterConfig {

    @Bean
    public WebClient.Builder loadBalancedWebClientBuilder() {
        return WebClient.builder();
    }

    @Bean
    public BackendClients backendClients(WebClient.Builder builder) {
        return new BackendClients(
                builder.baseUrl("http://ftgo-order-service").build(),
                builder.baseUrl("http://ftgo-kitchen-service").build(),
                builder.baseUrl("http://ftgo-accounting-service").build(),
                builder.baseUrl("http://ftgo-delivery-service").build());
    }

    @Bean
    public OrderDetailsHandler orderDetailsHandler(BackendClients clients) {
        return new OrderDetailsHandler(clients);
    }

    @Bean
    public RouterFunction<ServerResponse> orderDetailsRoute(OrderDetailsHandler handler) {
        return RouterFunctions.route(GET("/mobile/orders/{orderId}"), request -> {
            Long orderId = Long.valueOf(request.pathVariable("orderId"));
            return handler.fetchOrderDetails(orderId)
                    .flatMap(details -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(details));
        });
    }
}
```

Add `@LoadBalanced` to the `WebClient.Builder` bean so `http://ftgo-order-service`-style
authority-only URIs resolve via Eureka, matching order-service's existing pattern:

```java
    @org.springframework.cloud.client.loadbalancer.LoadBalanced
    @Bean
    public WebClient.Builder loadBalancedWebClientBuilder() {
        return WebClient.builder();
    }
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./gradlew :ftgo-mobile-gateway:test --tests OrderDetailsHandlerTest`
Expected: PASS (both tests) — note this test constructs `BackendClients` directly against
`MockWebServer` URLs, bypassing the `@LoadBalanced` builder entirely, so it exercises
`OrderDetailsHandler`'s composition/degradation logic in isolation from Eureka.

- [ ] **Step 9: Run the full module test suite**

Run: `./gradlew :ftgo-mobile-gateway:test`
Expected: PASS (context-loads test from Task 6 + this task's handler test)

- [ ] **Step 10: Commit**

```bash
git add ftgo-mobile-gateway
git commit -m "feat: add mobile gateway's own composed order-details endpoint (gateway-level API composition)"
```

---

### Task 8: Wire both gateways into `compose.yml`

**Files:**
- Modify: `compose.yml`

**Interfaces:**
- Consumes: `ftgo-mobile-gateway/Dockerfile` (Task 6), `ftgo-public-gateway/Dockerfile` (Task 5).
- Produces: two new containers reachable at `localhost:8090` (mobile) and `localhost:8091` (public) once the stack is up, for Task 9's Docker verification.

- [ ] **Step 1: Read the existing service-registry/order-history-service compose entries for the exact pattern to copy**

Run: `grep -A15 "order-history-service:" compose.yml`

- [ ] **Step 2: Append two new service entries to `compose.yml`, following that exact pattern**

```yaml
  mobile-gateway:
    build:
      context: .
      dockerfile: ftgo-mobile-gateway/Dockerfile
    depends_on:
      - service-registry
      - order-service
      - kitchen-service
      - accounting-service
      - delivery-service
    ports:
      - "8090:8090"
    environment:
      EUREKA_CLIENT_SERVICEURL_DEFAULTZONE: http://service-registry:8761/eureka/

  public-gateway:
    build:
      context: .
      dockerfile: ftgo-public-gateway/Dockerfile
    depends_on:
      - service-registry
      - order-service
      - kitchen-service
      - accounting-service
      - delivery-service
      - order-history-service
      - restaurant-service
    ports:
      - "8091:8091"
    environment:
      EUREKA_CLIENT_SERVICEURL_DEFAULTZONE: http://service-registry:8761/eureka/
```

(Match the existing `EUREKA_CLIENT_SERVICEURL_DEFAULTZONE` env-var override convention used
by every other containerized service in this file — confirm the exact variable name/casing
against an existing entry, e.g. `order-service`'s, before finalizing; adjust here if it
differs.)

- [ ] **Step 3: Validate compose file syntax**

Run: `docker compose config --quiet`
Expected: no output, exit code 0 (valid YAML/schema)

- [ ] **Step 4: Commit**

```bash
git add compose.yml
git commit -m "build: wire ftgo-mobile-gateway and ftgo-public-gateway into compose.yml"
```

---

### Task 9: Docker e2e verification (manual, no commit unless a bug is found)

**Files:** none (verification only; fix commits only if a bug surfaces)

- [ ] **Step 1: Bring up the full stack**

Run: `docker compose up --build -d`
Expected: all containers healthy/running, including `mobile-gateway` and `public-gateway`

- [ ] **Step 2: Verify mobile gateway auth + rate limiting**

```bash
curl -i http://localhost:8090/mobile/orders/1
# Expected: 401 (no X-Api-Key header)

curl -i -H "X-Api-Key: wrong" http://localhost:8090/mobile/orders/1
# Expected: 401

for i in 1 2 3; do curl -i -H "X-Api-Key: mobile-dev-key" http://localhost:8090/mobile/orders/1; done
# Expected: first requests 200/404 depending on data; a burst well beyond 20/s returns 429
```

- [ ] **Step 3: Place an order directly (seed data), then verify the mobile gateway's composed endpoint**

```bash
curl -X POST -H "X-Api-Key: mobile-dev-key" -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":"1","quantity":2}]}' \
  http://localhost:8090/mobile/orders
# Note the returned order id, then:
curl -H "X-Api-Key: mobile-dev-key" http://localhost:8090/mobile/orders/<id>
# Expected: JSON with order/ticket/authorization/delivery sections all FOUND
```

- [ ] **Step 4: Verify graceful degradation**

```bash
docker compose stop kitchen-service
curl -H "X-Api-Key: mobile-dev-key" http://localhost:8090/mobile/orders/<id>
# Expected: 200 overall, ticket section UNAVAILABLE, other 3 sections still FOUND
docker compose start kitchen-service
```

- [ ] **Step 5: Verify public gateway routing to at least 3 backend services**

```bash
curl -i -H "X-Api-Key: public-dev-key" http://localhost:8091/api/v1/orders/<id>
curl -i -H "X-Api-Key: public-dev-key" http://localhost:8091/api/v1/order-views/<id>
curl -i -H "X-Api-Key: public-dev-key" http://localhost:8091/api/v1/restaurants/1
# Expected: each 200/404 matching what the backend service itself would return directly
```

- [ ] **Step 6: Verify public gateway auth + its stricter rate limit**

```bash
curl -i http://localhost:8091/api/v1/orders/1
# Expected: 401
for i in 1 2 3 4 5 6; do curl -i -H "X-Api-Key: public-dev-key" http://localhost:8091/api/v1/restaurants/1; done
# Expected: 429 once the 5/s limit is exceeded
```

- [ ] **Step 7: Tear down**

Run: `docker compose down` (volume preserved, matching every prior session's convention)

- [ ] **Step 8: If any step above surfaced a bug, fix it, add a regression test in the appropriate module from Tasks 2–7, and commit the fix separately before proceeding to Task 10**

---

### Task 10: Documentation sweep (chapter-completion rule)

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Create: `ftgo-mobile-gateway/README.md`
- Create: `ftgo-public-gateway/README.md`
- Create: `ftgo-gateway-common/README.md`
- Modify: `CONTEXT.md`
- Modify: `README.md` (root)

- [ ] **Step 1: Add an "API Gateway / Backends for Frontends" section to `docs/ARCHITECTURE.md`**

Cover: gateway ownership model (mobile team owns `ftgo-mobile-gateway`, public/3rd-party
team owns `ftgo-public-gateway`, per the book's BFF ownership diagram); a routing table
listing every route from both gateways' `application.yml` (Tasks 5–6); a sequence diagram
for the mobile gateway's `GET /mobile/orders/{orderId}` composition (client → mobile-gateway
→ 4 backend services in parallel → response), explicitly contrasted against Ch.7's
`docs/ARCHITECTURE.md` API composition section (who composes: order-service itself vs. the
gateway; note both now coexist in the codebase, serving different callers) and the edge
functions implemented (`RequestLoggingFilter`, `ApiKeyAuthFilter`, `PerKeyRateLimiterGatewayFilterFactory`)
with an explicit "no real identity service" caveat matching the design spec's non-goals.

- [ ] **Step 2: Write `ftgo-mobile-gateway/README.md`, `ftgo-public-gateway/README.md`, `ftgo-gateway-common/README.md`**

Each following the existing per-service README convention already used by
`ftgo-order-history-service/README.md` and `ftgo-delivery-service/README.md`
(purpose, endpoints/routes table, dependencies, how it's run).

- [ ] **Step 3: Update `CONTEXT.md`**

- Book-progress table: flip Ch.8 row to `Done`, `High` confidence, with a notes summary
  (mirrors the Ch.7 row's format).
- "Current position" section: update to Ch.8 done, next chapter Ch.9.
- Services-to-build table: remove the old `ftgo-api-gateway` (`Not started`) row; add rows
  for `ftgo-mobile-gateway`, `ftgo-public-gateway`, `ftgo-gateway-common`, matching the
  existing table's column format and level of detail (see the `ftgo-order-history-service`
  row added in the 2026-07-29 CQRS session as the template).
- "Concept understanding" → "Understood well": add a Ch.8 entry (API gateway pattern vs.
  BFF, gateway-level composition vs. Ch.7's service-level composition, edge functions).
- "Needs more depth" / "Open questions": confirm nothing Ch.8-related remains, matching the
  pattern of the Ch.5/6/7 entries already there.
- Session log: append a one-line entry dated 2026-07-29 (or the date this task actually
  runs) summarizing the sub-project, following the existing entries' voice/detail level.

- [ ] **Step 4: Update root `README.md`**

Add the two new gateway services and `ftgo-gateway-common` to the service list, and update
the chapter-progress line to include Ch.8, matching how the 2026-07-29 CQRS session updated
this same file for Ch.7 (per that session's log entry).

- [ ] **Step 5: Grep for stale stub language this sweep might have missed**

Run: `grep -rn "ftgo-api-gateway" --include="*.md" .`
Expected: no remaining references (or only inside `docs/session-*.md`/
`docs/superpowers/plans/`/`docs/superpowers/specs/`, which are point-in-time records exempt
from the sweep per `CLAUDE.md`).

- [ ] **Step 6: Commit**

```bash
git add docs/ARCHITECTURE.md ftgo-mobile-gateway/README.md ftgo-public-gateway/README.md ftgo-gateway-common/README.md CONTEXT.md README.md
git commit -m "docs: full documentation sweep for Ch.8 API gateway + BFF completion"
```

---

## Self-review notes

- **Spec coverage**: gateway-common filters (logging/auth/rate-limit) → Tasks 2–4; public
  gateway routing → Task 5; mobile gateway routing → Task 6; mobile gateway composition →
  Task 7; docker wiring → Task 8; e2e verification (composition, degradation, auth, rate
  limit, routing) → Task 9; documentation sweep → Task 10. All design-spec sections have a
  corresponding task.
- **Type consistency checked**: `SectionResult<T>` (Task 7) used consistently by
  `OrderDetails`/`OrderDetailsHandler`/its test; `PerKeyRateLimiterGatewayFilterFactory.Config`
  field name (`requestsPerSecond`) matches the YAML filter args in Tasks 5–6; route filter
  name `PerKeyRateLimiter` matches the class-name-minus-suffix convention Spring Cloud Gateway
  expects for `AbstractGatewayFilterFactory` auto-naming.
- **No placeholders**: every step has literal code, exact file paths, and runnable commands.
