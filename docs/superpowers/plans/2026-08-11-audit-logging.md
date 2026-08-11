# Ch.11 §11.3.6 Audit Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record who did what to which business object across the 9 FTGO services, via a shared Spring AOP aspect that publishes audit events to Kafka, consumed and stored by a new `ftgo-audit-log-service` with an ADMIN-only query API.

**Architecture:** A single `AuditLoggingAspect` class lives in `ftgo-common` and is picked up automatically (Spring Boot auto-configuration, no per-service wiring) by every service that already depends on `ftgo-common`. It wraps every `@PostMapping` method that is also `@PreAuthorize`-gated, extracts the caller's JWT subject, the HTTP action, and the first `@PathVariable` (if any) as the business object id, and publishes a JSON `AuditLogEntryEvent` to the Kafka topic `audit-log` using the existing shared `eventKafkaTemplate` bean (already auto-configured by `ftgo-common`'s `OutboxAutoConfiguration` — no new producer config needed). `ftgo-audit-log-service` is a new service, structurally a sibling of `ftgo-order-history-service` (Kafka consumer → JPA entity → query REST endpoint), with its own MySQL database `ftgo_audit_log`.

**Tech Stack:** Spring Boot 3.5.16 / Java 21, Spring AOP (`spring-boot-starter-aop`), Spring Kafka, Spring Data JPA, MySQL 8.4, Spring Security OAuth2 resource server (JWT).

## Global Constraints

- Java 21, Spring Boot 3.5.16, Spring Cloud 2025.0.3 BOM (services)/2024.0.0 BOM (root subprojects default) — match whichever BOM the sibling service (`ftgo-order-history-service`) uses, since `ftgo-audit-log-service` is structured identically.
- No new infrastructure containers: reuse the existing `mysql` and `kafka` compose services.
- Audit publishing is fire-and-forget and must never fail or block the request it's auditing — any Kafka publish exception is caught and logged locally in the aspect, never propagated.
- Only mutating (`@PostMapping`) + `@PreAuthorize`-gated controller methods are audited in this pass; `@GetMapping` endpoints are explicitly out of scope.
- The audit-log query endpoint (`GET /audit-log`) is `@PreAuthorize("hasRole('ADMIN')")` only.
- `ftgo-audit-log-service` joins the root `build.gradle`'s `actuatorModules` list (actuator, Micrometer tracing, Prometheus, JSON logging, GlitchTip/Sentry) and the `glitchtip-provisioner`/`sentry-dsn` wiring, matching every other business service's observability baseline.
- This is the last unstarted Ch.11 sub-project: the final task in this plan is the full chapter-completion documentation sweep (`docs/ARCHITECTURE.md` full sections w/ diagrams for every saga/pattern, every touched `ftgo-*-service/README.md` gets full parity, `CONTEXT.md`'s "Concept understanding" section updated), not just the per-change doc sync.

---

### Task 1: `AuditLoggingAspect` in `ftgo-common`

**Files:**
- Modify: `ftgo-common/build.gradle` (add `spring-boot-starter-aop` as an `api` dependency)
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/audit/AuditLogEntryEvent.java`
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/audit/AuditLoggingAspect.java`
- Create: `ftgo-common/src/main/java/com/sanjay/ftgo/common/audit/AuditLoggingAutoConfiguration.java`
- Modify: `ftgo-common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` (register the new auto-configuration; read this file first — it already lists `OutboxAutoConfiguration`, follow the same one-class-per-line format)
- Modify: `ftgo-order-service/build.gradle:49` (delete the now-redundant explicit `implementation 'org.springframework.boot:spring-boot-starter-aop'` line — it comes transitively via `ftgo-common`'s new `api` dependency, same pattern already used for `spring-kafka`)
- Test: `ftgo-common/src/test/java/com/sanjay/ftgo/common/audit/AuditLoggingAspectTest.java`

**Interfaces:**
- Produces: `AuditLogEntryEvent` (record) — fields `String userId, List<String> roles, String action, String entityType, String entityId, String outcome, String failureReason, String serviceName, Instant timestamp`. This is the exact JSON shape `ftgo-audit-log-service`'s Kafka consumer (Task 2) deserializes.
- Produces: Kafka topic name constant `"audit-log"`, used both here and in Task 2's `@KafkaListener`.
- Consumes: `KafkaTemplate<String, String> eventKafkaTemplate` bean (already defined in `ftgo-common/src/main/java/com/sanjay/ftgo/common/outbox/KafkaProducerConfig.java`, auto-registered via `OutboxAutoConfiguration`'s component scan of `com.sanjay.ftgo.common.outbox` — the aspect's package is `com.sanjay.ftgo.common.audit`, a sibling, so it needs its own auto-configuration class rather than relying on that existing scan).

- [ ] **Step 1: Add the AOP starter to `ftgo-common`**

Read `ftgo-common/build.gradle` first (it's short). Add this line to the `dependencies { }` block, next to the existing `api 'org.springframework.kafka:spring-kafka'` line, with a one-line comment matching that file's existing comment style:

```groovy
    // AuditLoggingAspect (com.sanjay.ftgo.common.audit) needs Spring AOP proxying enabled on
    // every consuming service's classpath — api, not implementation, so it propagates the same
    // way spring-kafka does above (see that dependency's comment).
    api 'org.springframework.boot:spring-boot-starter-aop'
```

- [ ] **Step 2: Remove the now-redundant AOP dependency from `ftgo-order-service`**

In `ftgo-order-service/build.gradle`, delete line 49 (`implementation 'org.springframework.boot:spring-boot-starter-aop'`) — it's now supplied transitively via `ftgo-common`. Leave the `resilience4j-spring-boot3` line untouched; that dependency is what actually needed AOP here originally, and it still needs it, just now transitively.

- [ ] **Step 3: Write `AuditLogEntryEvent`**

```java
package com.sanjay.ftgo.common.audit;

import java.time.Instant;
import java.util.List;

public record AuditLogEntryEvent(
        String userId,
        List<String> roles,
        String action,
        String entityType,
        String entityId,
        String outcome,
        String failureReason,
        String serviceName,
        Instant timestamp) {

    public static final String SUCCESS = "SUCCESS";
    public static final String FAILURE = "FAILURE";
}
```

- [ ] **Step 4: Write the failing test for the aspect**

This test spins up a minimal Spring context with AOP enabled, a fake `@RestController` carrying the exact annotation shape production controllers use, and a `KafkaTemplate` stubbed to capture what gets sent — proving the aspect intercepts, extracts the right fields, and never blocks/rethrows differently than the original method.

```java
package com.sanjay.ftgo.common.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = AuditLoggingAspectTest.TestConfig.class)
class AuditLoggingAspectTest {

    @Autowired
    private FakeOrderController fakeOrderController;

    @Autowired
    private AtomicReference<String> capturedTopic;

    @Autowired
    private AtomicReference<String> capturedPayload;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void publishesAuditEventOnSuccess() throws Exception {
        Jwt jwt = mock(Jwt.class);
        when(jwt.getSubject()).thenReturn("42");
        when(jwt.getClaimAsStringList("roles")).thenReturn(List.of("CONSUMER"));

        fakeOrderController.cancel(7L, jwt);

        assertThat(capturedTopic.get()).isEqualTo("audit-log");
        AuditLogEntryEvent event = objectMapper.readValue(capturedPayload.get(), AuditLogEntryEvent.class);
        assertThat(event.userId()).isEqualTo("42");
        assertThat(event.roles()).containsExactly("CONSUMER");
        assertThat(event.entityType()).isEqualTo("Order");
        assertThat(event.entityId()).isEqualTo("7");
        assertThat(event.outcome()).isEqualTo(AuditLogEntryEvent.SUCCESS);
        assertThat(event.action()).contains("cancel");
        assertThat(event.serviceName()).isEqualTo("test-service");
    }

    @Test
    void publishesFailureEventAndRethrowsWhenMethodThrows() throws Exception {
        Jwt jwt = mock(Jwt.class);
        when(jwt.getSubject()).thenReturn("99");
        when(jwt.getClaimAsStringList("roles")).thenReturn(List.of("ADMIN"));

        assertThatThrownBy(() -> fakeOrderController.explode(1L, jwt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");

        AuditLogEntryEvent event = objectMapper.readValue(capturedPayload.get(), AuditLogEntryEvent.class);
        assertThat(event.outcome()).isEqualTo(AuditLogEntryEvent.FAILURE);
        assertThat(event.failureReason()).contains("IllegalStateException");
    }

    @RestController
    @RequestMapping("/orders")
    static class FakeOrderController {

        @PreAuthorize("hasAnyRole('CONSUMER', 'ADMIN')")
        @PostMapping("/{id}/cancel")
        public String cancel(@PathVariable Long id, Jwt jwt) {
            return "cancelled";
        }

        @PreAuthorize("hasRole('ADMIN')")
        @PostMapping("/{id}/explode")
        public String explode(@PathVariable Long id, Jwt jwt) {
            throw new IllegalStateException("boom");
        }
    }

    @Configuration
    @EnableAspectJAutoProxy
    static class TestConfig {

        @Bean
        AtomicReference<String> capturedTopic() {
            return new AtomicReference<>();
        }

        @Bean
        AtomicReference<String> capturedPayload() {
            return new AtomicReference<>();
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules();
        }

        @Bean
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> eventKafkaTemplate(AtomicReference<String> capturedTopic,
                                                           AtomicReference<String> capturedPayload) {
            KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
            when(template.send(anyString(), anyString())).thenAnswer(invocation -> {
                capturedTopic.set(invocation.getArgument(0));
                capturedPayload.set(invocation.getArgument(1));
                return CompletableFuture.completedFuture(mock(SendResult.class));
            });
            return template;
        }

        @Bean
        AuditLoggingAspect auditLoggingAspect(KafkaTemplate<String, String> eventKafkaTemplate,
                                               ObjectMapper objectMapper) {
            return new AuditLoggingAspect(eventKafkaTemplate, objectMapper, "test-service");
        }

        @Bean
        FakeOrderController fakeOrderController() {
            return new FakeOrderController();
        }
    }
}
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `./gradlew :ftgo-common:test --tests AuditLoggingAspectTest`
Expected: FAIL — `AuditLoggingAspect` does not exist yet (compile error).

- [ ] **Step 6: Write `AuditLoggingAspect`**

```java
package com.sanjay.ftgo.common.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

import java.lang.reflect.Parameter;
import java.time.Instant;
import java.util.List;

// Applies to every @PostMapping + @PreAuthorize-gated @RestController method across all 9
// services, via ftgo-common's shared classpath - see AuditLoggingAutoConfiguration for how this
// bean gets registered without each service needing to declare it. Read-only (@GetMapping)
// endpoints are deliberately not matched: "who did what to which business object" is fully
// answered by mutating actions alone (Ch.11 §11.3.6 design doc), and auditing every read would
// add volume with no demonstrated need.
@Aspect
@Component
public class AuditLoggingAspect {

    private static final Logger log = LoggerFactory.getLogger(AuditLoggingAspect.class);

    private final KafkaTemplate<String, String> eventKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String serviceName;

    public AuditLoggingAspect(KafkaTemplate<String, String> eventKafkaTemplate,
                               ObjectMapper objectMapper,
                               @Value("${spring.application.name}") String serviceName) {
        this.eventKafkaTemplate = eventKafkaTemplate;
        this.objectMapper = objectMapper;
        this.serviceName = serviceName;
    }

    @Around("@annotation(org.springframework.web.bind.annotation.PostMapping) "
            + "&& @annotation(org.springframework.security.access.prepost.PreAuthorize)")
    public Object auditMutatingCall(ProceedingJoinPoint joinPoint) throws Throwable {
        Jwt jwt = findJwtArgument(joinPoint);
        String action = describeAction(joinPoint);
        String entityType = describeEntityType(joinPoint);
        String entityId = describeEntityId(joinPoint);

        try {
            Object result = joinPoint.proceed();
            publish(jwt, action, entityType, entityId, AuditLogEntryEvent.SUCCESS, null);
            return result;
        } catch (Throwable t) {
            publish(jwt, action, entityType, entityId, AuditLogEntryEvent.FAILURE, t.getClass().getSimpleName());
            throw t;
        }
    }

    private Jwt findJwtArgument(ProceedingJoinPoint joinPoint) {
        for (Object arg : joinPoint.getArgs()) {
            if (arg instanceof Jwt jwt) {
                return jwt;
            }
        }
        return null;
    }

    private String describeAction(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        return "POST " + signature.getDeclaringType().getSimpleName() + "." + signature.getName();
    }

    private String describeEntityType(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String simpleName = signature.getDeclaringType().getSimpleName();
        return simpleName.endsWith("Controller")
                ? simpleName.substring(0, simpleName.length() - "Controller".length())
                : simpleName;
    }

    // Uses the first @PathVariable-annotated parameter, in declared order, as the business
    // object id - every mutating endpoint in this codebase that has one uses it as the sole
    // path variable identifying the object being acted on (e.g. OrderController.cancel(Long
    // id, ...)). Endpoints with none (e.g. createOrder, POST /consumers) still record the
    // actor/action with a null entityId; that's still useful for support/compliance even
    // without a pre-existing id.
    private String describeEntityId(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Parameter[] parameters = signature.getMethod().getParameters();
        Object[] args = joinPoint.getArgs();
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].isAnnotationPresent(org.springframework.web.bind.annotation.PathVariable.class)) {
                return args[i] == null ? null : String.valueOf(args[i]);
            }
        }
        return null;
    }

    private void publish(Jwt jwt, String action, String entityType, String entityId, String outcome,
                          String failureReason) {
        try {
            List<String> roles = jwt == null ? List.of() : jwt.getClaimAsStringList("roles");
            String userId = jwt == null ? null : jwt.getSubject();
            AuditLogEntryEvent event = new AuditLogEntryEvent(userId, roles == null ? List.of() : roles, action,
                    entityType, entityId, outcome, failureReason, serviceName, Instant.now());
            String payload = objectMapper.writeValueAsString(event);
            eventKafkaTemplate.send("audit-log", payload);
        } catch (Exception e) {
            // Best-effort: audit logging must never fail or block the request it's auditing.
            log.warn("Failed to publish audit log event for action {}", action, e);
        }
    }
}
```

- [ ] **Step 7: Write `AuditLoggingAutoConfiguration`**

```java
package com.sanjay.ftgo.common.audit;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

// Registers AuditLoggingAspect for any service that depends on ftgo-common, the same
// auto-configuration pattern OutboxAutoConfiguration uses for OutboxPublisher - no per-service
// @ComponentScan or @EnableAspectJAutoProxy needed.
@AutoConfiguration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = "com.sanjay.ftgo.common.audit")
public class AuditLoggingAutoConfiguration {
}
```

- [ ] **Step 8: Register the auto-configuration**

Open `ftgo-common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`, and add this line (keep the existing `OutboxAutoConfiguration` line untouched):

```
com.sanjay.ftgo.common.audit.AuditLoggingAutoConfiguration
```

- [ ] **Step 9: Run the test to verify it passes**

Run: `./gradlew :ftgo-common:test --tests AuditLoggingAspectTest`
Expected: PASS (2 tests)

- [ ] **Step 10: Run the full `ftgo-common` test suite and `ftgo-order-service` compile to check for regressions**

Run: `./gradlew :ftgo-common:test :ftgo-order-service:compileJava :ftgo-order-service:compileTestJava`
Expected: PASS — confirms the Step 2 dependency removal didn't break `resilience4j-spring-boot3`'s AOP usage in `ftgo-order-service`.

- [ ] **Step 11: Commit**

```bash
git add ftgo-common/build.gradle ftgo-common/src/main/java/com/sanjay/ftgo/common/audit/ \
        ftgo-common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports \
        ftgo-common/src/test/java/com/sanjay/ftgo/common/audit/ ftgo-order-service/build.gradle
git commit -m "feat: add shared AuditLoggingAspect for mutating controller calls"
```

---

### Task 2: `ftgo-audit-log-service` scaffolding, domain, and Kafka consumer

**Files:**
- Modify: `settings.gradle` (add `include 'ftgo-audit-log-service'`)
- Create: `ftgo-audit-log-service/build.gradle`
- Create: `ftgo-audit-log-service/Dockerfile`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/FtgoAuditLogServiceApplication.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/config/PersistenceConfig.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/config/KafkaConsumerConfig.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/security/SecurityConfig.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/security/RolesClaimJwtAuthenticationConverter.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/domain/AuditLogEntry.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/domain/AuditLogEntryRepository.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/infrastructure/AuditLogEventListener.java`
- Create: `ftgo-audit-log-service/src/main/resources/application.yml`
- Create: `ftgo-audit-log-service/src/test/resources/application.yml`
- Test: `ftgo-audit-log-service/src/test/java/com/sanjay/ftgo/auditlog/infrastructure/AuditLogEventListenerTest.java`
- Test: `ftgo-audit-log-service/src/test/java/com/sanjay/ftgo/auditlog/FtgoAuditLogServiceApplicationTests.java`

**Interfaces:**
- Consumes: `AuditLogEntryEvent` JSON shape from Task 1 (topic `audit-log`).
- Produces: `AuditLogEntry` JPA entity — fields `Long id (identity), String userId, String roles (comma-joined), String action, String entityType, String entityId, String outcome, String failureReason, String serviceName, Instant timestamp`. Task 3's controller and DTO depend on this exact field set and `AuditLogEntryRepository`'s query methods.
- Produces: `AuditLogEntryRepository extends JpaRepository<AuditLogEntry, Long>` with query methods `findByUserId`, `findByEntityTypeAndEntityId`, `findByTimestampBetween` (Task 3 composes these).

- [ ] **Step 1: Register the module**

In `settings.gradle`, add this line after `include 'ftgo-order-history-service'`:

```groovy
include 'ftgo-audit-log-service'
```

- [ ] **Step 2: Write `build.gradle`**

Identical shape to `ftgo-order-history-service/build.gradle` (read it first if you haven't) — this service needs the same Spring Cloud BOM override, Eureka client, OAuth2 resource server, and config-client dependencies:

```groovy
dependencyManagement {
    imports {
        mavenBom 'org.springframework.cloud:spring-cloud-dependencies:2025.0.3'
    }
}

dependencies {
    // spring-kafka comes transitively via ftgo-common's `api` dependency
    implementation project(':ftgo-common')
    implementation 'org.springframework.cloud:spring-cloud-starter-netflix-eureka-client'
    implementation 'org.springframework.boot:spring-boot-starter-oauth2-resource-server'
    implementation 'org.springframework.boot:spring-boot-starter-security'
    implementation 'org.springframework.cloud:spring-cloud-starter-config'
}
```

- [ ] **Step 3: Write the application class**

```java
package com.sanjay.ftgo.auditlog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FtgoAuditLogServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoAuditLogServiceApplication.class, args);
    }
}
```

- [ ] **Step 4: Write `PersistenceConfig`**

```java
package com.sanjay.ftgo.auditlog.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoAuditLogServiceApplication - see ftgo-order-history-service's
// PersistenceConfig for why @EntityScan/@EnableJpaRepositories must not sit directly on the
// @SpringBootApplication class (breaks @WebMvcTest slice filtering).
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.auditlog.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.auditlog.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
```

- [ ] **Step 5: Write `KafkaConsumerConfig`**

This service has only one `@KafkaListener` (no concurrent-write races on the same row like `order-history-service` has), so it can rely on Boot's default listener container factory — no custom bean needed. Write this file as a placeholder-free no-op documentation of that decision, matching this codebase's convention of writing down *why* something is absent rather than leaving silent gaps:

```java
package com.sanjay.ftgo.auditlog.config;

// Deliberately empty: unlike ftgo-order-history-service (4 listeners racing to update the same
// order_views row, needing a custom retrying container factory - see that service's
// KafkaConsumerConfig), this service has a single @KafkaListener performing plain inserts with
// no shared-row contention, so Boot's default listener container factory is sufficient.
public class KafkaConsumerConfig {
}
```

- [ ] **Step 6: Write `SecurityConfig` and `RolesClaimJwtAuthenticationConverter`**

Identical to `ftgo-order-history-service`'s versions (same package-relative shape, just under `com.sanjay.ftgo.auditlog.security`):

```java
package com.sanjay.ftgo.auditlog.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, RolesClaimJwtAuthenticationConverter converter)
            throws Exception {
        http.authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/actuator/health", "/actuator/prometheus").permitAll()
                        .anyRequest().authenticated())
                .csrf(csrf -> csrf.disable())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(converter)));
        return http.build();
    }

    @Bean
    public RolesClaimJwtAuthenticationConverter rolesClaimJwtAuthenticationConverter() {
        return new RolesClaimJwtAuthenticationConverter();
    }
}
```

```java
package com.sanjay.ftgo.auditlog.security;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.util.Collection;
import java.util.List;

public class RolesClaimJwtAuthenticationConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        List<String> roles = jwt.getClaimAsStringList("roles");
        Collection<GrantedAuthority> authorities = roles == null ? List.of()
                : roles.stream().<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role)).toList();
        return new JwtAuthenticationToken(jwt, authorities);
    }
}
```

- [ ] **Step 7: Write `AuditLogEntry`**

```java
package com.sanjay.ftgo.auditlog.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "audit_log_entries")
public class AuditLogEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id")
    private String userId;

    // Stored as a comma-joined string rather than an @ElementCollection table: this field is
    // write-once (set at insert time from the Kafka event, never updated) and only ever read
    // back whole for display, so a normalized child table would be pure overhead - unlike
    // OrderView's line items in ftgo-order-history-service, which are genuinely relational.
    @Column(name = "roles")
    private String roles;

    @Column(name = "action", nullable = false)
    private String action;

    @Column(name = "entity_type")
    private String entityType;

    @Column(name = "entity_id")
    private String entityId;

    @Column(name = "outcome", nullable = false)
    private String outcome;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "service_name", nullable = false)
    private String serviceName;

    @Column(name = "occurred_at", nullable = false)
    private Instant timestamp;

    protected AuditLogEntry() {
    }

    public AuditLogEntry(String userId, String roles, String action, String entityType, String entityId,
                          String outcome, String failureReason, String serviceName, Instant timestamp) {
        this.userId = userId;
        this.roles = roles;
        this.action = action;
        this.entityType = entityType;
        this.entityId = entityId;
        this.outcome = outcome;
        this.failureReason = failureReason;
        this.serviceName = serviceName;
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public String getRoles() {
        return roles;
    }

    public String getAction() {
        return action;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getOutcome() {
        return outcome;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
}
```

- [ ] **Step 8: Write `AuditLogEntryRepository`**

```java
package com.sanjay.ftgo.auditlog.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface AuditLogEntryRepository extends JpaRepository<AuditLogEntry, Long> {

    List<AuditLogEntry> findByUserIdOrderByTimestampDesc(String userId);

    List<AuditLogEntry> findByEntityTypeAndEntityIdOrderByTimestampDesc(String entityType, String entityId);

    List<AuditLogEntry> findByTimestampBetweenOrderByTimestampDesc(Instant from, Instant to);

    List<AuditLogEntry> findAllByOrderByTimestampDesc();
}
```

- [ ] **Step 9: Write the failing test for the Kafka listener**

```java
package com.sanjay.ftgo.auditlog.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import com.sanjay.ftgo.common.audit.AuditLogEntryEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AuditLogEventListenerTest {

    @Mock
    private AuditLogEntryRepository repository;

    @Test
    void persistsAuditLogEntryFromEvent() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        AuditLogEventListener listener = new AuditLogEventListener(repository, objectMapper);

        AuditLogEntryEvent event = new AuditLogEntryEvent("42", List.of("CONSUMER", "ADMIN"),
                "POST OrderController.cancel", "Order", "7", AuditLogEntryEvent.SUCCESS, null,
                "ftgo-order-service", Instant.parse("2026-08-11T10:00:00Z"));
        String payload = objectMapper.writeValueAsString(event);

        listener.onMessage(payload);

        ArgumentCaptor<AuditLogEntry> captor = ArgumentCaptor.forClass(AuditLogEntry.class);
        verify(repository).save(captor.capture());
        AuditLogEntry saved = captor.getValue();
        assertThat(saved.getUserId()).isEqualTo("42");
        assertThat(saved.getRoles()).isEqualTo("CONSUMER,ADMIN");
        assertThat(saved.getAction()).isEqualTo("POST OrderController.cancel");
        assertThat(saved.getEntityType()).isEqualTo("Order");
        assertThat(saved.getEntityId()).isEqualTo("7");
        assertThat(saved.getOutcome()).isEqualTo("SUCCESS");
        assertThat(saved.getServiceName()).isEqualTo("ftgo-order-service");
    }

    @Test
    void skipsMalformedPayloadWithoutThrowing() {
        AuditLogEventListener listener = new AuditLogEventListener(repository, new ObjectMapper());

        listener.onMessage("not valid json");

        // No exception, no save - see onMessage's catch block.
    }
}
```

- [ ] **Step 10: Run the test to verify it fails**

Run: `./gradlew :ftgo-audit-log-service:test --tests AuditLogEventListenerTest`
Expected: FAIL — `AuditLogEventListener` does not exist yet.

- [ ] **Step 11: Write `AuditLogEventListener`**

```java
package com.sanjay.ftgo.auditlog.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import com.sanjay.ftgo.common.audit.AuditLogEntryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuditLogEventListener {

    private static final Logger log = LoggerFactory.getLogger(AuditLogEventListener.class);

    private final AuditLogEntryRepository repository;
    private final ObjectMapper objectMapper;

    public AuditLogEventListener(AuditLogEntryRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "audit-log", groupId = "audit-log-service")
    public void onMessage(String payload) {
        AuditLogEntryEvent event;
        try {
            event = objectMapper.readValue(payload, AuditLogEntryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed audit log event: {}", payload, e);
            return;
        }
        String roles = event.roles() == null || event.roles().isEmpty() ? null : String.join(",", event.roles());
        AuditLogEntry entry = new AuditLogEntry(event.userId(), roles, event.action(), event.entityType(),
                event.entityId(), event.outcome(), event.failureReason(), event.serviceName(), event.timestamp());
        repository.save(entry);
    }
}
```

- [ ] **Step 12: Run the test to verify it passes**

Run: `./gradlew :ftgo-audit-log-service:test --tests AuditLogEventListenerTest`
Expected: PASS (2 tests)

- [ ] **Step 13: Write `application.yml` (main and test)**

`ftgo-audit-log-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: ftgo-audit-log-service
  cloud:
    config:
      fail-fast: false
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_audit_log
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: audit-log-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
    listener:
      observation-enabled: true
  security:
    oauth2:
      resourceserver:
        jwt:
          jwk-set-uri: http://localhost:9000/oauth2/jwks

server:
  port: 8089

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
        include: health, prometheus
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

`ftgo-audit-log-service/src/test/resources/application.yml` — copy `ftgo-order-history-service/src/test/resources/application.yml` verbatim, but read that file first (it overrides `spring.datasource.url` to an in-memory H2 URL and disables Eureka registration/config-import for tests) and substitute `ftgo_audit_log`/`audit-log-service` wherever it names `ftgo_order_history`/`order-history-service`.

- [ ] **Step 14: Write the application context smoke test**

Copy `ftgo-order-history-service/src/test/java/com/sanjay/ftgo/orderhistory/FtgoOrderHistoryServiceApplicationTests.java` verbatim into `ftgo-audit-log-service/src/test/java/com/sanjay/ftgo/auditlog/FtgoAuditLogServiceApplicationTests.java`, updating only the package declaration and the `@SpringBootTest(classes = ...)` reference to `FtgoAuditLogServiceApplication`.

- [ ] **Step 15: Write `Dockerfile`**

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-audit-log-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-audit-log-service/build/libs/*.jar app.jar
EXPOSE 8089
ENTRYPOINT ["sh", "-c", "[ -f /shared/dsn.env ] && export $(grep '^SENTRY_DSN=' /shared/dsn.env) ; exec java -jar app.jar"]
```

- [ ] **Step 16: Run the full new-module test suite**

Run: `./gradlew :ftgo-audit-log-service:test`
Expected: PASS (all tests in the module)

- [ ] **Step 17: Commit**

```bash
git add settings.gradle ftgo-audit-log-service/
git commit -m "feat: add ftgo-audit-log-service Kafka consumer and domain model"
```

---

### Task 3: Audit log query API

**Files:**
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/api/AuditLogController.java`
- Create: `ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/api/AuditLogEntryResponse.java`
- Test: `ftgo-audit-log-service/src/test/java/com/sanjay/ftgo/auditlog/api/AuditLogControllerTest.java`

**Interfaces:**
- Consumes: `AuditLogEntry`, `AuditLogEntryRepository` from Task 2.
- Produces: `GET /audit-log?userId=&entityType=&entityId=&from=&to=` — used by Task 4's e2e Cucumber scenario.

- [ ] **Step 1: Write `AuditLogEntryResponse`**

```java
package com.sanjay.ftgo.auditlog.api;

import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;

import java.time.Instant;

public record AuditLogEntryResponse(
        Long id,
        String userId,
        String roles,
        String action,
        String entityType,
        String entityId,
        String outcome,
        String failureReason,
        String serviceName,
        Instant timestamp) {

    public static AuditLogEntryResponse from(AuditLogEntry entry) {
        return new AuditLogEntryResponse(entry.getId(), entry.getUserId(), entry.getRoles(), entry.getAction(),
                entry.getEntityType(), entry.getEntityId(), entry.getOutcome(), entry.getFailureReason(),
                entry.getServiceName(), entry.getTimestamp());
    }
}
```

- [ ] **Step 2: Write the failing test for the controller**

```java
package com.sanjay.ftgo.auditlog.api;

import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import com.sanjay.ftgo.auditlog.security.SecurityConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = AuditLogController.class)
@org.springframework.context.annotation.Import(SecurityConfig.class)
class AuditLogControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AuditLogEntryRepository repository;

    @Test
    @WithMockUser(roles = "ADMIN")
    void returnsEntriesForUserId() throws Exception {
        AuditLogEntry entry = new AuditLogEntry("42", "CONSUMER", "POST OrderController.cancel", "Order", "7",
                "SUCCESS", null, "ftgo-order-service", Instant.parse("2026-08-11T10:00:00Z"));
        when(repository.findByUserIdOrderByTimestampDesc("42")).thenReturn(List.of(entry));

        mockMvc.perform(get("/audit-log").param("userId", "42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].userId").value("42"))
                .andExpect(jsonPath("$[0].action").value("POST OrderController.cancel"));
    }

    @Test
    @WithMockUser(roles = "CONSUMER")
    void forbidsNonAdmin() throws Exception {
        mockMvc.perform(get("/audit-log"))
                .andExpect(status().isForbidden());
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `./gradlew :ftgo-audit-log-service:test --tests AuditLogControllerTest`
Expected: FAIL — `AuditLogController` does not exist yet.

- [ ] **Step 4: Write `AuditLogController`**

```java
package com.sanjay.ftgo.auditlog.api;

import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/audit-log")
public class AuditLogController {

    private final AuditLogEntryRepository repository;

    public AuditLogController(AuditLogEntryRepository repository) {
        this.repository = repository;
    }

    @PreAuthorize("hasRole('ADMIN')")
    @GetMapping
    public ResponseEntity<List<AuditLogEntryResponse>> query(
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String entityType,
            @RequestParam(required = false) String entityId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to) {

        List<AuditLogEntry> entries;
        if (userId != null) {
            entries = repository.findByUserIdOrderByTimestampDesc(userId);
        } else if (entityType != null && entityId != null) {
            entries = repository.findByEntityTypeAndEntityIdOrderByTimestampDesc(entityType, entityId);
        } else if (from != null && to != null) {
            entries = repository.findByTimestampBetweenOrderByTimestampDesc(from, to);
        } else {
            entries = repository.findAllByOrderByTimestampDesc();
        }

        return ResponseEntity.ok(entries.stream().map(AuditLogEntryResponse::from).toList());
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :ftgo-audit-log-service:test --tests AuditLogControllerTest`
Expected: PASS (2 tests)

- [ ] **Step 6: Run the full module test suite**

Run: `./gradlew :ftgo-audit-log-service:test`
Expected: PASS (all tests)

- [ ] **Step 7: Commit**

```bash
git add ftgo-audit-log-service/src/main/java/com/sanjay/ftgo/auditlog/api/ \
        ftgo-audit-log-service/src/test/java/com/sanjay/ftgo/auditlog/api/
git commit -m "feat: add ADMIN-only audit log query endpoint"
```

---

### Task 4: Infrastructure wiring — MySQL database, compose, actuatorModules, config-repo, GlitchTip

**Files:**
- Modify: `infrastructure/mysql/init.sql`
- Modify: `build.gradle:69-71` (the `actuatorModules` list)
- Modify: `compose.yml` (new `audit-log-service` container; add `audit-log-service` to `order-service`'s... no — add the new container in the same alphabetical/logical position as `order-history-service`, right after it)
- Create: `config-repo/ftgo-audit-log-service.yml` (empty placeholder is NOT acceptable per "No Placeholders" — see Step 4 below for why this file is intentionally omitted instead)

**Interfaces:**
- Consumes: `ftgo-audit-log-service` image built from Task 2's `Dockerfile`.
- Produces: a running `audit-log-service` container reachable at `http://audit-log-service:8089` from other compose services, and `http://localhost:8089` from the host — used by Task 5's e2e test.

- [ ] **Step 1: Add the `ftgo_audit_log` database**

In `infrastructure/mysql/init.sql`, add after the `ftgo_order_history` database/grant lines (keep the existing `debezium` user block at the end untouched):

```sql
CREATE DATABASE IF NOT EXISTS ftgo_audit_log;
```

and add the matching grant line next to the other `GRANT ALL PRIVILEGES` lines:

```sql
GRANT ALL PRIVILEGES ON ftgo_audit_log.*  TO 'ftgo'@'%';
```

- [ ] **Step 2: Add `ftgo-audit-log-service` to `actuatorModules`**

In root `build.gradle`, find the `actuatorModules` list (currently ends with `'ftgo-mobile-gateway', 'ftgo-public-gateway'`) and add `'ftgo-audit-log-service'` to it, e.g.:

```groovy
def actuatorModules = ['ftgo-order-service', 'ftgo-kitchen-service', 'ftgo-consumer-service',
                        'ftgo-restaurant-service', 'ftgo-accounting-service', 'ftgo-delivery-service',
                        'ftgo-order-history-service', 'ftgo-audit-log-service', 'ftgo-mobile-gateway',
                        'ftgo-public-gateway']
```

This gives the new service actuator/Prometheus/tracing/JSON-logging/Sentry-GlitchTip dependencies automatically, matching every sibling service — no other file needs a per-dependency change for those concerns.

- [ ] **Step 3: Add the compose service**

In `compose.yml`, insert a new `audit-log-service:` entry immediately after the `order-history-service:` block (before `mobile-gateway:`), matching that block's shape exactly but with this service's own values:

```yaml
  audit-log-service:
    build:
      context: .
      dockerfile: ftgo-audit-log-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      service-registry:
        condition: service_started
      authorization-server:
        condition: service_healthy
      tempo:
        condition: service_started
      config-server:
        condition: service_started
      glitchtip-provisioner:
        condition: service_completed_successfully
    ports:
      - "8089:8089"
    volumes:
      - sentry-dsn:/shared:ro
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_audit_log
      SPRING_CONFIG_IMPORT: "optional:configserver:http://config-server:8888"
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      MANAGEMENT_OTLP_TRACING_ENDPOINT: http://tempo:4318/v1/traces
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/
      SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWKSETURI: http://authorization-server:9000/oauth2/jwks
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8089/actuator/health"]
      interval: 10s
      timeout: 5s
      retries: 10
```

- [ ] **Step 4: No new config-repo file needed**

`ftgo-order-history-service.yml` doesn't exist in `config-repo/` either (only services with a per-service override beyond `application.yml`'s shared defaults get their own file — check `ls config-repo/` to confirm `ftgo-order-history-service.yml` is absent). `ftgo-audit-log-service` has no per-service overrides beyond what `config-repo/application.yml`'s shared defaults already provide (Kafka bootstrap, JWT JWK URI, Eureka, management/tracing/Sentry), so it needs no new config-repo file — it picks up `application.yml`'s shared defaults the same way `order-history-service` does. This step intentionally makes no file change; it exists to document why, so a reviewer doesn't flag the absence as a gap.

- [ ] **Step 5: Bring up the stack and verify the new service registers and passes health checks**

Run (from repo root): `docker compose up -d --build mysql kafka zookeeper service-registry authorization-server config-server tempo glitchtip-db glitchtip-redis glitchtip glitchtip-worker glitchtip-provisioner audit-log-service`

Wait for `audit-log-service` to report healthy: `docker compose ps audit-log-service` — `STATUS` column shows `healthy`.

Expected: `curl -f http://localhost:8089/actuator/health` (from the host) returns `{"status":"UP", ...}`.

- [ ] **Step 6: Commit**

```bash
git add infrastructure/mysql/init.sql build.gradle compose.yml
git commit -m "feat: wire ftgo-audit-log-service into compose, MySQL, and actuatorModules"
```

---

### Task 5: End-to-end verification scenario

**Files:**
- Modify: `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`
- Create: `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/AuditLoggingStepDefinitions.java`

**Interfaces:**
- Consumes: `GET http://localhost:8089/audit-log?entityType=Order&entityId=<id>` from Task 3, the existing consumer/order placement step definitions already used by `PlaceReviseCancelOrder.feature`'s first scenario.

- [ ] **Step 1: Read the existing step definitions for the patterns to follow**

Read `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/ExceptionTrackingStepDefinitions.java` in full — it's the most recent addition and shows this module's conventions for polling an external HTTP API with a timeout/retry loop, obtaining an ADMIN bearer token, and asserting on a JSON response. Also read whichever step-definitions class implements `"the consumer places an order for {int} of the menu item at the restaurant"` and `"the order is eventually approved"` (used by `PlaceReviseCancelOrder.feature`'s first scenario) to find the shared `World`/context object that already holds the placed order's id and the consumer's id — this task's new step definitions need to read the order id from that same shared context, not re-place an order.

- [ ] **Step 2: Add the new scenario**

Append to `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`, after the existing "Triggering an uncaught exception reports it to GlitchTip" scenario:

```gherkin
  Scenario: Placing an order records an audit log entry
    Given a restaurant "Ajanta Audit E2E" with a menu item "Tandoori Chicken" priced at 15.00
    And an active consumer "Audit E2E Consumer"
    When the consumer places an order for 1 of the menu item at the restaurant
    Then the order is eventually approved
    And the audit log eventually has an entry for the placed order with action containing "createOrder"
```

- [ ] **Step 3: Write the step definition**

Follow `ExceptionTrackingStepDefinitions.java`'s exact polling-loop and HTTP-client conventions (read it first — this step reuses its `RestTemplate`/`HttpHeaders` setup and its retry-with-timeout helper method rather than reintroducing a new one). The shape:

```java
package com.sanjay.ftgo.e2e;

import io.cucumber.java.en.Then;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class AuditLoggingStepDefinitions {

    private final World world;
    private final RestTemplate restTemplate = new RestTemplate();

    public AuditLoggingStepDefinitions(World world) {
        this.world = world;
    }

    @Then("the audit log eventually has an entry for the placed order with action containing {string}")
    public void theAuditLogEventuallyHasAnEntry(String actionFragment) {
        Long orderId = world.getLastPlacedOrderId();
        String adminToken = world.getAdminBearerToken();

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(adminToken);
        HttpEntity<Void> request = new HttpEntity<>(headers);

        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        String url = "http://localhost:8089/audit-log?entityType=Order&entityId=" + orderId;
        boolean found = false;
        String lastBody = null;
        while (Instant.now().isBefore(deadline) && !found) {
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, request, String.class);
            lastBody = response.getBody();
            if (lastBody != null && lastBody.contains(actionFragment)) {
                found = true;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        assertThat(found)
                .withFailMessage("No audit log entry containing '%s' found for order %d within 30s. Last response: %s",
                        actionFragment, orderId, lastBody)
                .isTrue();
    }
}
```

**Note for the implementer:** `World.getLastPlacedOrderId()` and `World.getAdminBearerToken()` are placeholder method names — replace them with whatever the shared context class in this module actually exposes (found in Step 1's read). Do not invent new fields on that shared context without first checking whether an equivalent accessor already exists, since every other scenario in this feature file already tracks the placed order id and an admin token.

- [ ] **Step 4: Run the new scenario against the running stack**

Ensure the full stack from Task 4 Step 5 is up, plus `restaurant-service`, `order-service`, `kitchen-service`, `accounting-service`, `mobile-gateway`, `public-gateway`, `authorization-server` (whatever the existing scenarios in this feature file already require running).

Run: `./gradlew :ftgo-end-to-end-test:test --tests "*PlaceReviseCancelOrder*"` (or the project's existing Cucumber runner class/tag for this feature file — check `ftgo-end-to-end-test/src/test/java` for the `@RunWith`/`@Suite` entry point used by the other scenarios).

Expected: PASS — all scenarios in `PlaceReviseCancelOrder.feature`, including the new one.

- [ ] **Step 5: Commit**

```bash
git add ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature \
        ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/AuditLoggingStepDefinitions.java
git commit -m "test: add e2e scenario verifying audit log entries for placed orders"
```

---

### Task 6: Full documentation sweep (Ch.11 completion)

This is the last unstarted Ch.11 sub-project — completing it flips Ch.11 to Done, triggering this repo's `CLAUDE.md` full chapter-completion sweep, not just the per-change doc sync used by every prior task in this plan.

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: every `ftgo-*-service/README.md` that exists in this repo (check `find . -maxdepth 2 -name README.md -not -path "./node_modules/*"` for the current list)

**Interfaces:** none (documentation only).

- [ ] **Step 1: Per-change doc sync for audit logging itself**

- `README.md`: add `ftgo-audit-log-service` to the service list/status table, add it to the tech-stack/observability row if one exists, add port `8089` to the running-locally port list, flip the "Book progress" table's Ch.11 §11.3.6 row (and the overall Ch.11 row) to Done.
- `CONTEXT.md`: update "Current position" to reflect Ch.11 complete; add `ftgo-audit-log-service` to the "Services to build" table as Done; check the "Patterns reference" checklist's Audit logging item; add a session-log entry describing this sub-project (mirror the existing entries' format — what was built, key decisions, PR number once known).
- `docs/ARCHITECTURE.md`: add a new "Audit logging (Ch.11, §11.3.6)" section, matching the depth of the existing "Log aggregation"/"Distributed tracing"/"Exception tracking" sections — architecture description, the `AuditLoggingAspect` interception mechanism, the Kafka `audit-log` topic, the `ftgo-audit-log-service` CQRS-style consumer/query shape, and its query API.

- [ ] **Step 2: Full chapter-completion sweep — `docs/ARCHITECTURE.md`**

Read the whole file first. For every saga/pattern documented there that lacks the full treatment already given to the earliest sagas (sequence diagrams for happy path + every compensation case — check which existing sections already have this, e.g. the Order Cancellation/Revision sagas, versus which were added later and might be thinner), bring it up to the same depth. This is the deferred sweep this repo's `CLAUDE.md` requires once Ch.11 flips to Done — grep for saga/pattern names across `*.md` (excluding `docs/session-*.md`, `docs/superpowers/plans/`, `docs/superpowers/specs/`, which stay as point-in-time records) to find every place that might need updating.

- [ ] **Step 3: Full chapter-completion sweep — per-service READMEs**

For every `ftgo-*-service/README.md` that exists, verify full API/events/domain-model parity with the current code — not just a status-label bump. Specifically check: does its API section list every current `@PostMapping`/`@GetMapping` endpoint including this plan's new `AuditLogController`? Does its events section (if any) mention that its mutating endpoints now emit audit events via `AuditLoggingAspect`? Create `ftgo-audit-log-service/README.md` from scratch if this repo's convention is one README per service directory (check whether `ftgo-order-history-service/README.md` exists as the template to follow).

- [ ] **Step 4: Full chapter-completion sweep — `CONTEXT.md`'s "Concept understanding" section**

Read the current "Understood well" / "Needs more depth" / "Open questions" lists. Move any item that names audit logging, AOP, or a Ch.11 observability pattern that's now fully implemented out of "Needs more depth"/"Open questions" and into "Understood well" — these describe current understanding, not a historical log, so stale entries here are a real inaccuracy, not just an omission.

- [ ] **Step 5: Commit**

```bash
git add README.md CONTEXT.md docs/ARCHITECTURE.md ftgo-*-service/README.md
git commit -m "docs: full Ch.11 completion sweep — architecture, service READMEs, concept understanding"
```
