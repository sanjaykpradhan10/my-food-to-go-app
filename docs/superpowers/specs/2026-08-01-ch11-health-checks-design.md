# Ch.11 sub-project 1: Health checks (§11.3.1) — Design

**Chapter:** 11 — Developing production-ready services
**Sub-project:** 1 of N (first slice of Ch.11; further sub-projects — configurability §11.2, distributed tracing §11.3.3, metrics §11.3.4, etc. — will be brainstormed separately)

## Goal

Give every business service and gateway a real `/actuator/health` endpoint backed by Spring Boot Actuator's auto-configured indicators (DataSource, Kafka, Eureka discovery), per the book's Health check API pattern (§11.3.1), and wire Docker Compose to use it for startup ordering.

## Scope

**In scope — the 9 services:**
- `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-restaurant-service`, `ftgo-accounting-service`, `ftgo-delivery-service`, `ftgo-order-history-service` (DB + Kafka + Eureka)
- `ftgo-mobile-gateway`, `ftgo-public-gateway` (Eureka only, reactive stack)

**Out of scope:**
- `ftgo-service-registry` (the Eureka server itself) — not a business service, excluded per explicit decision during brainstorming.
- Custom/hand-written health indicators (e.g. checking a specific downstream REST call) — auto-configured indicators only, matching what the book's §11.3.1 example shows.
- Other Ch.11 areas (security §11.1, configurability §11.2, distributed tracing/metrics/exception tracking/audit logging §11.3.2–11.3.6, microservice chassis §11.4) — each is its own future sub-project.

## Architecture

Config-only change; no new Java classes for the health mechanism itself:

1. Add `spring-boot-starter-actuator` as a dependency for all 9 services. Since the root `build.gradle`'s `subprojects {}` block already applies common config uniformly, add the Actuator dependency there (both the servlet-stack `reactiveModules`-excluded services and the WebFlux-based gateways use the same artifact — Actuator auto-detects the reactive vs. servlet stack).
2. Each of the 9 services' `application.yml` gains:
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
   `show-details: always` is safe here — this is a learning project with no external/untrusted clients hitting these ports directly, and seeing indicator detail (which component failed) is the entire point of exercising this pattern.
3. No indicator-specific code: the DataSource indicator activates automatically wherever a `DataSource` bean exists (the 7 non-gateway services); the Eureka discovery-client indicator activates automatically for 8 of the 9 services since they already register with Eureka (`eureka.client.register-with-eureka: true`) — the exception is `ftgo-consumer-service`, which has no `eureka-client` dependency at all (a pre-existing architectural fact, unrelated to Ch.11) and so never gets a `discoveryComposite` component. **No Kafka indicator exists to activate**: verified directly against the built jars that `spring-boot-actuator-autoconfigure` 3.5.16 ships no Kafka health contributor (only `KafkaMetricsAutoConfiguration` remains under `actuate.autoconfigure.kafka`) and `spring-kafka` 3.3.16 ships no health-indicator class either — Spring Boot dropped the auto-configured Kafka health indicator in the 3.x line. Per this spec's "no custom `HealthIndicator` code" constraint, no `kafka` component is asserted anywhere in this sub-project.

## Docker Compose wiring

- Add a `healthcheck` block to each of the 9 services' entries in `compose.yml`, matching the existing pattern used for `mysql` and `kafka-connect`:
  ```yaml
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:<service-port>/actuator/health"]
    interval: 10s
    timeout: 5s
    retries: 10
  ```
- Update `depends_on` entries elsewhere in `compose.yml` that reference these services to `condition: service_healthy` where a real dependency exists (e.g. gateways depending on the services they route to). Entries with no direct dependency relationship stay as-is.
- **Dockerfile change required:** each service's Dockerfile builds `FROM eclipse-temurin:21-jre` for its runtime stage, which does not include `curl` by default. Add `RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*` to each of the 9 Dockerfiles' final stage. (`ftgo-service-registry`'s Dockerfile is untouched — out of scope.)

## Automated verification

Extend the existing `ftgo-end-to-end-test` module (already brings up the full `compose.yml` stack via the `com.avast.gradle.docker-compose` plugin and `e2eTest` task) with a new Cucumber scenario, `AllServicesReportHealthy.feature`:

- After the stack is up, call `GET /actuator/health` directly against each of the 9 services' own host-mapped ports — not through a gateway, since this checks infrastructure state, not a user-facing journey.
- Reuse the existing retry-with-backoff HTTP helper from `PlaceReviseCancelOrderStepDefinitions` (services may still be settling immediately after container startup).
- Assertions per service:
  - HTTP 200, top-level `status == "UP"`.
  - For the 7 DB-backed services: `components.db.status == "UP"`. No `kafka` component is asserted (see Architecture §3 — no such indicator exists in this Spring Boot version).
  - For all services except `ftgo-consumer-service` (8 of 9): `components.discoveryComposite.status == "UP"` (Eureka). `ftgo-consumer-service` has no Eureka registration, so only its `db` component and overall `status` are asserted.

This scenario runs in the same `e2eTest` Gradle task/compose stack as `PlaceReviseCancelOrder.feature` — no new Docker Compose file or Gradle module.

## Manual verification (during implementation, not part of the automated suite)

- `docker compose -f compose.yml ps` after `up` shows all 9 services (plus `mysql`, `kafka-connect`) as `healthy`.
- Spot-check the JSON body of `/actuator/health` on one DB-backed service and one gateway to confirm the expected components appear.

## Regression risk

Changing `depends_on` conditions in `compose.yml` changes startup ordering/timing for the whole stack. The existing `PlaceReviseCancelOrder.feature` scenario (already in `ftgo-end-to-end-test`) is re-run as part of this sub-project's own verification to catch any startup-ordering regression the `depends_on` changes might introduce.

## Documentation

Per this project's chapter-completion doc-sync convention: since this is the first Ch.11 sub-project (not the chapter's completion), only the per-change rule applies — no full sweep yet. This change touches:
- `CONTEXT.md` — "Current position" / session log entry for this sub-project; Ch.11 row in the book-progress table moves from "Not started" to "In progress"; "Patterns reference" checklist gains `Health check API (Ch.11, §11.3.1)`.
- Each of the 9 services' `README.md` — note the new `/actuator/health` endpoint and what it reports.
- `docs/ARCHITECTURE.md` — a short note on the Health check API pattern is reasonable but a full dedicated section (with diagrams) is deferred to Ch.11's eventual full-chapter close-out sweep, consistent with how Ch.10's per-sub-project commits kept doc depth proportional and only did the full sweep at chapter completion.
