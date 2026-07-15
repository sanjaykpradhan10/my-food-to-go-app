# Ch.3 Client-Side Service Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace order-service's hardcoded `restaurant-service.base-url` with real client-side service discovery: a new Eureka registry service, restaurant-service registering itself on startup, and order-service resolving it at call time via a `@LoadBalanced RestClient` instead of a fixed URL.

**Architecture:** A new `ftgo-service-registry` module runs a standalone Eureka server. `restaurant-service` becomes a Eureka client and registers under its `spring.application.name`. `order-service` becomes a Eureka client too, but calls `http://ftgo-restaurant-service` (a logical service name) through a `@LoadBalanced RestClient` — Spring Cloud LoadBalancer resolves that name to a real instance via the registry before each request. `RestaurantServiceProxy` (the circuit-breaker-wrapped class) does not change; only how its `RestClient` bean is built changes.

**Tech Stack:** Spring Boot 3.5.3, Spring Cloud 2025.0.0 (Netflix Eureka Server/Client, Spring Cloud LoadBalancer), WireMock (existing test infra, unchanged).

## Global Constraints

- Java 21, Spring Boot 3.5.3 (root `build.gradle`) — do not change
- Spring Cloud BOM version: `2025.0.0`, imported via `dependencyManagement { imports { mavenBom ... } }` in each module that needs Spring Cloud starters (root `build.gradle`'s shared `subprojects` block is not touched — Spring Cloud deps are added per-module)
- The root `build.gradle`'s `subprojects` block unconditionally adds `spring-boot-starter-data-jpa` + `mysql-connector-j` + H2 test runtime to every module, including the new `ftgo-service-registry`, which needs neither. `FtgoServiceRegistryApplication` MUST exclude `DataSourceAutoConfiguration` and `HibernateJpaAutoConfiguration` or `bootRun`/tests will fail trying to configure a DataSource that was never intended to exist
- Eureka self-preservation mode must be disabled on the registry (`eureka.server.enable-self-preservation: false`) — with only 1-2 registered clients in this dev setup, self-preservation would keep evicted instances marked "up" indefinitely, and Task 5's manual verification depends on eviction actually being observable
- `eureka.instance.prefer-ip-address: true` on every Eureka client (restaurant-service, order-service) — without it, Eureka advertises container hostnames that aren't reliably resolvable from other containers (same class of problem Kafka's dual-listener fix addressed)
- order-service does not register itself with Eureka (`eureka.client.register-with-eureka: false`) — it only discovers, it isn't a discovery target in this pass
- Tests never talk to a live Eureka server — `eureka.client.enabled: false` plus Spring Cloud LoadBalancer's static `spring.cloud.discovery.client.simple.instances.*` config in test `application.yml` files
- `RestaurantServiceProxy.java` and `RestaurantServiceProxyTest.java` are not modified by this plan — only `RestClientConfig.java` (how the `RestClient` bean is built) and config files change

Full context: `docs/superpowers/specs/2026-07-15-ch3-service-discovery-design.md`

---

### Task 1: scaffold `ftgo-service-registry` (Eureka server)

**Files:**
- Modify: `settings.gradle`
- Create: `ftgo-service-registry/build.gradle`
- Create: `ftgo-service-registry/src/main/java/com/sanjay/ftgo/registry/FtgoServiceRegistryApplication.java`
- Create: `ftgo-service-registry/src/main/resources/application.yml`
- Create: `ftgo-service-registry/src/test/java/com/sanjay/ftgo/registry/FtgoServiceRegistryApplicationTests.java`
- Create: `ftgo-service-registry/Dockerfile`

**Interfaces:**
- Produces: a Eureka server reachable at `http://localhost:8761` (`/eureka/apps` REST API, and a browser dashboard) — consumed by Task 2 (restaurant-service registers) and Task 3 (order-service discovers)

- [ ] **Step 1: Register the module in the multi-project build**

Modify `settings.gradle`:

```groovy
rootProject.name = 'my-food-to-go-app'

include 'ftgo-consumer-service'
include 'ftgo-order-service'
include 'ftgo-kitchen-service'
include 'ftgo-accounting-service'
include 'ftgo-restaurant-service'
include 'ftgo-delivery-service'
include 'ftgo-service-registry'
```

- [ ] **Step 2: Create the module's build file**

Create `ftgo-service-registry/build.gradle`:

```groovy
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.0'
    }
}

dependencies {
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-server'
}
```

- [ ] **Step 3: Create the application class**

Create `ftgo-service-registry/src/main/java/com/sanjay/ftgo/registry/FtgoServiceRegistryApplication.java`:

```java
package com.sanjay.ftgo.registry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
@EnableEurekaServer
public class FtgoServiceRegistryApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoServiceRegistryApplication.class, args);
    }
}
```

The `exclude` is required: the root `build.gradle`'s shared `subprojects` block puts `spring-boot-starter-data-jpa` and `mysql-connector-j` on every module's classpath, including this one, and this module has no database at all. Without excluding these two autoconfiguration classes, Spring Boot will try to build a `DataSource` at startup and fail with "Failed to determine a suitable driver class."

- [ ] **Step 4: Configure the registry**

Create `ftgo-service-registry/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: ftgo-service-registry

server:
  port: 8761

eureka:
  client:
    register-with-eureka: false
    fetch-registry: false
  server:
    enable-self-preservation: false
```

`register-with-eureka`/`fetch-registry` are `false` because this instance IS the registry — it doesn't need to register with itself or fetch a registry from itself. `enable-self-preservation: false` ensures instance eviction actually happens promptly in this small dev setup (see Global Constraints).

- [ ] **Step 5: Write the smoke test**

Create `ftgo-service-registry/src/test/java/com/sanjay/ftgo/registry/FtgoServiceRegistryApplicationTests.java`:

```java
package com.sanjay.ftgo.registry;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class FtgoServiceRegistryApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

- [ ] **Step 6: Run the test**

Run: `./gradlew :ftgo-service-registry:test`
Expected: PASS — context loads without attempting any DataSource configuration.

- [ ] **Step 7: Manually verify the registry actually starts and serves its API**

Run: `./gradlew :ftgo-service-registry:bootRun` (in one terminal), then in another:
```bash
curl -s http://localhost:8761/eureka/apps
```
Expected: an XML response with an empty `<applications>` element (no services registered yet — that's expected, nothing has registered as a client at this point). Stop `bootRun` (Ctrl-C) once confirmed.

- [ ] **Step 8: Create the Dockerfile**

Create `ftgo-service-registry/Dockerfile`:

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-service-registry:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-service-registry/build/libs/*.jar app.jar
EXPOSE 8761
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 9: Commit**

```bash
git add settings.gradle \
        ftgo-service-registry/build.gradle \
        ftgo-service-registry/src/main/java/com/sanjay/ftgo/registry/FtgoServiceRegistryApplication.java \
        ftgo-service-registry/src/main/resources/application.yml \
        ftgo-service-registry/src/test/java/com/sanjay/ftgo/registry/FtgoServiceRegistryApplicationTests.java \
        ftgo-service-registry/Dockerfile
git commit -m "feat(service-registry): scaffold Eureka server module"
```

---

### Task 2: restaurant-service registers with the registry

**Files:**
- Modify: `ftgo-restaurant-service/build.gradle`
- Modify: `ftgo-restaurant-service/src/main/resources/application.yml`
- Modify: `ftgo-restaurant-service/src/test/resources/application.yml`

**Interfaces:**
- Consumes: the Eureka server from Task 1 (`http://localhost:8761/eureka/`)
- Produces: restaurant-service registered under service ID `FTGO-RESTAURANT-SERVICE` (Eureka uppercases application names by convention) — consumed by Task 3 (order-service discovers it)

No production code changes are needed — registration is automatic once the client dependency and config are present.

- [ ] **Step 1: Add the Eureka client dependency**

Replace `ftgo-restaurant-service/build.gradle`:

```groovy
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.0'
    }
}

dependencies {
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
}
```

- [ ] **Step 2: Configure registration**

Replace `ftgo-restaurant-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: ftgo-restaurant-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_restaurant
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update

server:
  port: 8085

eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true
```

- [ ] **Step 3: Disable Eureka in the test profile**

Replace `ftgo-restaurant-service/src/test/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MySQL
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: create-drop
    database-platform: org.hibernate.dialect.H2Dialect

eureka:
  client:
    enabled: false
```

Without this, `@SpringBootTest` tests (`FtgoRestaurantServiceApplicationTests`, `RestaurantControllerTest`) would try to register with a real Eureka server that isn't running during test runs.

- [ ] **Step 4: Run the full restaurant-service test suite**

Run: `./gradlew :ftgo-restaurant-service:test`
Expected: PASS — `FtgoRestaurantServiceApplicationTests`, `RestaurantControllerTest`, `RestaurantRepositoryTest` all unaffected by this change.

- [ ] **Step 5: Manually verify registration**

Start the registry from Task 1: `./gradlew :ftgo-service-registry:bootRun` (separate terminal), wait for it to be up, then start restaurant-service: `./gradlew :ftgo-restaurant-service:bootRun` (another terminal). Wait ~30 seconds (Eureka's default registration/heartbeat interval), then:
```bash
curl -s http://localhost:8761/eureka/apps/FTGO-RESTAURANT-SERVICE
```
Expected: an XML response describing one registered instance with `<status>UP</status>`. Stop both processes (Ctrl-C) once confirmed.

- [ ] **Step 6: Commit**

```bash
git add ftgo-restaurant-service/build.gradle \
        ftgo-restaurant-service/src/main/resources/application.yml \
        ftgo-restaurant-service/src/test/resources/application.yml
git commit -m "feat(restaurant-service): register with Eureka service registry"
```

---

### Task 3: order-service discovers restaurant-service

**Files:**
- Modify: `ftgo-order-service/build.gradle`
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Modify: `ftgo-order-service/src/test/resources/application.yml`

**Interfaces:**
- Consumes: the Eureka server from Task 1 (production), the WireMock server on port 8089 (test, via Spring Cloud LoadBalancer's `SimpleDiscoveryClient` instead of Eureka)
- Produces: `RestClient` bean `restaurantServiceRestClient` — same bean name as before, still consumed by `RestaurantServiceProxy` (unchanged) — now resolving `http://ftgo-restaurant-service` via discovery instead of a fixed base URL

**No changes to `RestaurantServiceProxy.java` or `RestaurantServiceProxyTest.java`** — both are already written against `RestClient`'s abstract `baseUrl`-relative calls (`restClient.get().uri("/restaurants/{id}", restaurantId)`), which works identically whether the underlying `RestClient` was built with a static base URL or a load-balanced one.

- [ ] **Step 1: Add Eureka client + LoadBalancer dependencies**

Replace `ftgo-order-service/build.gradle`:

```groovy
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.0'
    }
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-aop'
    implementation 'io.github.resilience4j:resilience4j-spring-boot3:2.2.0'
    implementation 'org.springframework.kafka:spring-kafka'
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
    implementation 'org.springframework.cloud:spring-cloud-starter-loadbalancer'

    testImplementation 'org.wiremock:wiremock-standalone:3.9.2'
}
```

- [ ] **Step 2: Replace `RestClientConfig` to build a load-balanced `RestClient`**

Replace `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    @Bean
    @LoadBalanced
    public RestClient.Builder loadBalancedRestClientBuilder() {
        return RestClient.builder();
    }

    @Bean
    public RestClient restaurantServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofSeconds(2))
                .withReadTimeout(Duration.ofSeconds(2));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

        return loadBalancedRestClientBuilder
                .baseUrl("http://ftgo-restaurant-service")
                .requestFactory(requestFactory)
                .build();
    }
}
```

`http://ftgo-restaurant-service` is a logical name, not a real host — it matches restaurant-service's `spring.application.name` (Eureka resolves it case-insensitively). The `@LoadBalanced RestClient.Builder` bean gets an interceptor that rewrites this logical host to a real `host:port` pulled from the registry (or, in tests, from the static `SimpleDiscoveryClient` config) before each request goes out. The 2s connect/read timeouts are preserved exactly as before.

- [ ] **Step 3: Update production config — remove the static base URL, add discovery config**

Replace `ftgo-order-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: ftgo-order-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_order
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092

server:
  port: 8082

outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20

eureka:
  client:
    register-with-eureka: false
    service-url:
      defaultZone: http://localhost:8761/eureka/
  instance:
    prefer-ip-address: true

resilience4j:
  circuitbreaker:
    instances:
      restaurantService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.sanjay.ftgo.order.domain.RestaurantNotFoundException
```

Note `restaurant-service.base-url` is gone (replaced by the hardcoded discovery URL in `RestClientConfig`), and `eureka.client.register-with-eureka: false` — order-service discovers, but isn't registered as a discovery target itself in this pass.

- [ ] **Step 4: Update test config — static discovery client instead of Eureka**

Replace `ftgo-order-service/src/test/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MySQL
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: create-drop
    database-platform: org.hibernate.dialect.H2Dialect
  kafka:
    bootstrap-servers: localhost:9092
  cloud:
    discovery:
      client:
        simple:
          instances:
            ftgo-restaurant-service[0]:
              uri: http://localhost:8089

eureka:
  client:
    enabled: false

resilience4j:
  circuitbreaker:
    instances:
      restaurantService:
        sliding-window-size: 4
        failure-rate-threshold: 50
        wait-duration-in-open-state: 2s
        permitted-number-of-calls-in-half-open-state: 2
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.sanjay.ftgo.order.domain.RestaurantNotFoundException
```

`eureka.client.enabled: false` disables the real Eureka client's autoconfiguration during tests, which allows Spring Cloud LoadBalancer's `SimpleDiscoveryClient` (backed by the static `spring.cloud.discovery.client.simple.instances.*` config) to become the active `DiscoveryClient` instead. `ftgo-restaurant-service` matches restaurant-service's `spring.application.name` (matched case-insensitively) and resolves to WireMock's fixed port — the same port `RestaurantServiceProxyTest` already stands WireMock up on.

- [ ] **Step 5: Run `RestaurantServiceProxyTest` to confirm the discovery path works**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.RestaurantServiceProxyTest"`
Expected: PASS, all 3 tests (`returnsRestaurantInfoOnSuccess`, `throwsRestaurantNotFoundOn404`, `tripsCircuitBreakerAfterRepeatedFailures`) — with zero code changes to this test file, it now exercises the discovery-resolved call path instead of a hardcoded URL.

- [ ] **Step 6: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — `OrderServiceTest` and `OutboxPublisherTest` construct their collaborators directly (no Spring context), so they're unaffected. `OrderControllerTest` mocks `OrderService`, also unaffected. `FtgoOrderServiceApplicationTests` boots the full context including the new Eureka client + LoadBalancer beans; with `eureka.client.enabled: false` in the test profile, no live registry connection is attempted.

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/build.gradle \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/resources/application.yml
git commit -m "feat(order-service): discover restaurant-service via Eureka instead of a hardcoded URL"
```

---

### Task 4: docker-compose wiring

**Files:**
- Modify: `compose.yml`

- [ ] **Step 1: Replace `compose.yml`**

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
      - mysql-data:/var/lib/mysql
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
    ports:
      - "2181:2181"

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

  service-registry:
    build:
      context: .
      dockerfile: ftgo-service-registry/Dockerfile
    ports:
      - "8761:8761"

  restaurant-service:
    build:
      context: .
      dockerfile: ftgo-restaurant-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      service-registry:
        condition: service_started
    ports:
      - "8085:8085"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_restaurant
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/

  order-service:
    build:
      context: .
      dockerfile: ftgo-order-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      service-registry:
        condition: service_started
      restaurant-service:
        condition: service_started
    ports:
      - "8082:8082"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/

  kitchen-service:
    build:
      context: .
      dockerfile: ftgo-kitchen-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
    ports:
      - "8083:8083"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_kitchen
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092

volumes:
  mysql-data:
```

Note `order-service`'s `RESTAURANT_SERVICE_BASE_URL` env var is gone — discovery replaces it. `eureka.instance.prefer-ip-address: true` (set in each service's `application.yml` in Tasks 2-3) means no per-environment host override is needed here the way Kafka needed dual listeners.

- [ ] **Step 2: Rebuild and start the stack**

Run: `docker compose down && docker compose up -d --build`
Expected: all 7 containers (`mysql`, `zookeeper`, `kafka`, `service-registry`, `restaurant-service`, `order-service`, `kitchen-service`) reach `Up` state. Building `service-registry`'s image for the first time may take a couple of minutes (Gradle build inside Docker, same as every other service).

- [ ] **Step 3: Commit**

```bash
git add compose.yml
git commit -m "feat: wire service-registry into docker compose, switch order-service to Eureka discovery"
```

---

### Task 5: manual end-to-end verification

Not a code change — confirms discovery actually works across containers, and that the circuit breaker still degrades gracefully when the discovered instance disappears.

- [ ] **Step 1: Start the stack fresh**

```bash
docker compose down
docker compose up -d --build
sleep 30
docker compose ps
```
Expected: all 7 containers `Up`. The 30s sleep allows time for restaurant-service to register and order-service to fetch the registry (Eureka's default registration/fetch intervals are ~30s).

- [ ] **Step 2: Confirm restaurant-service is registered**

```bash
curl -s http://localhost:8761/eureka/apps/FTGO-RESTAURANT-SERVICE
```
Expected: XML showing one instance, `<status>UP</status>`.

- [ ] **Step 3: Confirm order-service resolves it via discovery**

```bash
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 1}]}' | jq
```
Expected: `201`-shaped response — proving the call was routed through discovery, since `RESTAURANT_SERVICE_BASE_URL` no longer exists anywhere in this stack.

- [ ] **Step 4: Verify eviction and graceful degradation**

```bash
docker compose stop restaurant-service
```
Wait ~30-45 seconds (Eureka's default heartbeat timeout is 90s by default, but `enable-self-preservation: false` allows eviction to proceed once the instance is confirmed unreachable rather than being protected). Poll:
```bash
curl -s http://localhost:8761/eureka/apps/FTGO-RESTAURANT-SERVICE
```
until the instance is gone (404 or empty `<application>`), then:
```bash
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 1}]}'
```
Expected: `503` (via the existing `RestaurantServiceUnavailableException` → circuit breaker fallback path) — no instance to resolve, or the resolved instance's connection fails, and the circuit breaker's fallback method converts it to a graceful 503 exactly as it did with the static-URL setup.

- [ ] **Step 5: Verify recovery without restarting order-service**

```bash
docker compose start restaurant-service
sleep 30
curl -s http://localhost:8761/eureka/apps/FTGO-RESTAURANT-SERVICE
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 1}]}' | jq
```
Expected: registry shows the instance `UP` again, and the order succeeds (`201`) — order-service never restarted, proving discovery is dynamic, not resolved once at startup.

- [ ] **Step 6: Update `CONTEXT.md`**

Mark "Client-side discovery / Server-side discovery (Ch. 3)" as done in the Communication patterns checklist, add `ftgo-service-registry` to the services table, update the `ftgo-order-service` row's notes to mention discovery-based restaurant-service resolution, and add a session-log line following the existing format (see the two 2026-07-15 entries already in the file for style).
