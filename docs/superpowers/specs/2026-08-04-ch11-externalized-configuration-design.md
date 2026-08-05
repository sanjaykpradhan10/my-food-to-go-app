# Ch.11 §11.2: Externalized configuration — Design Spec

**Status:** Approved
**Book reference:** *Microservices Patterns* §11.2 "Externalized configuration" (pp. 356–361)

## Context

Health checks (§11.3.1), security (§11.1 sub-projects 1–2), application metrics (§11.3.4), and distributed tracing (§11.3.3) are done and merged. This sub-project covers Ch.11's §11.2 pattern. §11.3's remaining patterns (log aggregation, exception tracking, audit logging) and §11.4 (microservice chassis) stay unscheduled after this — each gets its own spec/plan/implementation cycle per this project's established decomposition convention.

All 9 services (7 business services + 2 gateways) currently use a purely push-based configuration model: values live in each service's `application.yml`, with `compose.yml` supplying per-environment overrides via Spring's relaxed-binding env var names (`SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE`, `SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWKSETURI`, etc.). This works but means several values that are byte-for-byte identical across every service's `application.yml` (Kafka bootstrap servers, JWK-set URI, Eureka defaultZone, actuator exposure list, tracing sampling probability, OTLP endpoint, outbox polling defaults, saga mode) are duplicated 6-9 times, with no single place to change them and no mechanism to change a running service's behavior without restart.

## Goal

Add a pull-based layer (Spring Cloud Config Server) for values that are shared across services, while keeping the existing push-based layer (compose env vars) for genuinely per-environment/secret values. Demonstrate that the pull-based layer supports live refresh (`POST /actuator/refresh`) without restarting the consuming service, using a property this project's own code controls end-to-end.

## Architecture

**New module: `ftgo-config-server`** — a minimal Spring Boot app depending on `spring-cloud-config-server` (`@EnableConfigServer`), port `8888`. Not registered with Eureka — like `ftgo-authorization-server` and the `service-registry` itself, other services address it by a fixed compose hostname (`config-server:8888` in Docker, `localhost:8888` locally), so it doesn't need discovery to be discovered.

**Backend: git, pointed at this same repository.** The config server's `spring.cloud.config.server.git.uri` points at this repo (local clone path in dev, the repo's remote URL in Docker/CI) with `search-paths: config-repo`. No second repository or credential set to manage — configuration lives in the same place the code that consumes it lives, versioned by the same commits.

**New `config-repo/` directory** at the repo root, containing:
- `config-repo/application.yml` — properties shared by every service that talks to Kafka/Eureka/the JWT issuer, or exports actuator/tracing endpoints: `spring.kafka.bootstrap-servers`, `spring.security.oauth2.resourceserver.jwt.jwk-set-uri`, `eureka.client.service-url.defaultZone`, `eureka.instance.prefer-ip-address`, `management.endpoints.web.exposure.include`, `management.endpoint.health.show-details`, `management.tracing.sampling.probability`, `management.otlp.tracing.endpoint`.
- `config-repo/ftgo-order-service.yml`, `config-repo/ftgo-kitchen-service.yml`, `config-repo/ftgo-accounting-service.yml`, `config-repo/ftgo-delivery-service.yml`, `config-repo/ftgo-consumer-service.yml`, `config-repo/ftgo-order-history-service.yml` — one file per Kafka-producing/consuming service, each holding that service's `outbox.poll-fixed-delay-ms`, `outbox.batch-size`, and `saga.mode` (values are identical today; still modeled per-service because Spring Cloud Config keys service-specific files by `spring.application.name`, and `ftgo-order-service.yml` is where the refresh demo's property lives).

Spring Cloud Config's property-source precedence places config-server values *below* environment variables and command-line args but *above* the consuming service's own local `application.yml`. Combined with `compose.yml`'s existing env var overrides, this gives a three-tier precedence: compose env vars (per-environment/secrets) > config-server (shared cross-service defaults) > local `application.yml` (fallback when the config server is unreachable).

**Consumer wiring:** each of the 6 config-consuming services adds:
```yaml
spring:
  config:
    import: "optional:configserver:http://localhost:8888"
  cloud:
    config:
      fail-fast: false
```
(`optional:` prefix and `fail-fast: false` both express the same non-blocking intent belt-and-braces — if the config server is down or unreachable at startup, the service logs a warning and proceeds with its local `application.yml` values rather than refusing to start.) `compose.yml` overrides `SPRING_CONFIG_IMPORT` to `optional:configserver:http://config-server:8888` for the Docker network, following the same hostname-override convention already used for `tempo`, `service-registry`, and `authorization-server`.

`compose.yml` gains a `config-server` service block: builds/runs `ftgo-config-server`, `depends_on` nothing (it's a leaf — needs no other service up), and every config-consuming service's `depends_on` gets `config-server: condition: service_started` (best-effort, non-blocking — same rule already applied to `tempo`/`prometheus`/`grafana`: an infrastructure service other than the database/broker itself must never gate a business service's startup).

## Live refresh demonstration

`ftgo-order-service`'s `OutboxPublisher` currently reads `outbox.poll-fixed-delay-ms` into a static `@Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms}")` — Spring resolves that SpEL-style placeholder once at bean-creation time, so it cannot pick up a config-server value change after startup no matter what refresh mechanism is added downstream. This sub-project changes the polling mechanism from a fixed-delay `@Scheduled` method to a `ScheduledTaskRegistrar`-based dynamic trigger (registered in a `@Configuration` class implementing `SchedulingConfigurer`), where the trigger's next-execution-time computation reads the current value of an `@RefreshScope`-annotated `@ConfigurationProperties(prefix = "outbox")` bean on every invocation. This makes the polling interval a live-refreshable value without touching `OutboxPublisher`'s own publish logic.

**Demonstrating it works:** with the stack up, call `GET /actuator/env/outbox.poll-fixed-delay-ms` on `ftgo-order-service` to see the current value sourced from the config server; edit `config-repo/ftgo-order-service.yml`'s value and commit; call `POST /actuator/refresh` on `ftgo-order-service`; call the same `GET /actuator/env` endpoint again and confirm the new value is now in effect — no restart. `management.endpoints.web.exposure.include` (already shared-config-driven) needs `refresh` added to its value list for this to work.

## Verification

**New Cucumber scenario** in `ftgo-end-to-end-test`: place an order through the flow that triggers outbox publishing, record the observed time between the outbox row being written and the corresponding Kafka message appearing (reusing whatever polling/timing assertions the existing saga step definitions already use to wait for Kafka events), then have a step definition commit a change to `config-repo/ftgo-order-service.yml`'s `outbox.poll-fixed-delay-ms` value directly on the branch the config server's git backend already tracks (the e2e test's own checkout of this repo — no second repo or profile needed), call order-service's `POST /actuator/refresh`, place a second order, and assert the newly observed delay reflects the changed interval within a tolerance window. Mirrors the existing pattern of driving a real flow and asserting against an observability/config backend's own observable effect, used by the Prometheus-counter and Tempo-trace verification scenarios in prior sub-projects.

**Manual Docker verification:** bring up the full stack including `config-server`, confirm all 6 config-consuming services report `UP` health despite the config server being available (proving the shared values resolved correctly), then stop `config-server` and restart one business service to confirm it still starts successfully using its local `application.yml` fallback (proving the non-blocking/optional behavior).

## Documentation

Per-change docs land in the same commit: `docs/ARCHITECTURE.md` gains an "Externalized configuration (Ch.11, §11.2)" section describing the push/pull split and the refresh mechanism; new `ftgo-config-server/README.md`; each of the 6 config-consuming services' READMEs gain a short "Configuration" subsection pointing at the shared config-repo files plus documenting the `fail-fast: false` fallback behavior; root `README.md`'s tech stack row and Book progress table; `CONTEXT.md`'s Current position and the `- [ ] Externalized configuration (Ch. 11)` checklist item moves to done. No full chapter-completion sweep — §11.3's remaining three patterns are still unscheduled, so Ch.11 as a whole stays **In progress**.

## Out of scope (deferred)

- Spring Cloud Bus (broadcast-refresh-to-all-instances via a message broker) — this sub-project's refresh demo targets one service, called directly; fanning a single `/actuator/refresh` call out to every instance of every service is a separate, larger pattern not needed to demonstrate the core externalized-configuration idea.
- Encrypting secrets in the config repository (Spring Cloud Config's `{cipher}`/`/encrypt`, `/decrypt` endpoints) — no real secrets move into `config-repo/`; database credentials and other per-environment secrets stay exactly where they are today, in `compose.yml` env vars.
- Config server high availability / clustering — a single instance is sufficient for this learning project, matching how `service-registry` and `authorization-server` are already deployed as singletons.
- Refreshing Kafka, Eureka, or JWT-related properties live — those are consumed by Spring Boot's own autoconfiguration, which does not support `@RefreshScope` rebinding for connection-level beans (a live-refresh attempt there would require restarting the underlying client, not just re-reading a property); the refresh demo is scoped to `outbox.poll-fixed-delay-ms`, a value this project's own code controls end-to-end.
