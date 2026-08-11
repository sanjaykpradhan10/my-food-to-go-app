# Ch.11 §11.3.5: Exception Tracking — Design

## Pattern

[Exception tracking](http://microservices.io/patterns/observability/audit-logging.html) (Richardson, *Microservices Patterns*, §11.3.5): services report exceptions to a central exception-tracking service via a client library, which de-duplicates exceptions, generates alerts, and manages their resolution — instead of the traditional approach of scanning multi-line log entries for exceptions, which has no de-duplication and no resolution tracking.

## Tool: GlitchTip (self-hosted, Sentry-protocol-compatible)

The book names Sentry.io as the canonical open-source, self-hostable exception-tracking service (its other named example, Honeybadger, is cloud-only). Real self-hosted Sentry (v9+) was considered and rejected: it requires ClickHouse + Kafka + Redis + Postgres for its event store — roughly 10-15 containers — wildly out of proportion with every other Ch.11 observability sub-project's footprint (ELK added 4 containers, Tempo added 1, Prometheus/Grafana added 2).

GlitchTip implements the same DSN-based ingestion protocol as Sentry, so the official `sentry-spring-boot-starter` client library works against it unmodified, while its own footprint is 3 containers (app + Postgres + Redis for its Celery task queue) — proportional to prior sub-projects.

## Architecture

New containers added to `compose.yml`, following the same pattern already used for `tempo`/`prometheus`/`grafana`/the ELK stack:

- **`glitchtip`** — the GlitchTip app container, exposing its web UI and DSN ingestion endpoint.
- **`glitchtip-db`** — `postgres:16`, GlitchTip's data store.
- **`glitchtip-redis`** — `redis:7`, backing GlitchTip's Celery task queue (alert processing, digest emails).

Each of the 9 services (7 business services + 2 gateways) is configured with a per-project DSN pointing at the `glitchtip` compose service, obtained by pre-provisioning a GlitchTip organization/project at container startup (mirroring the Kibana index-pattern and Grafana datasource auto-provisioning already used in this project) so no manual click-through setup is needed to get a working demo.

## Per-service instrumentation

- Add `io.sentry:sentry-spring-boot-starter` to the root `build.gradle`'s shared `actuatorModules` block (same block that already carries the metrics/tracing/logging dependencies), so all 9 services pick it up uniformly.
- Add `sentry.dsn` (and `sentry.environment: local`) to each service's `application.yml` / the shared config-server defaults.
- **No `@ExceptionHandler` or `@ControllerAdvice` code changes.** `sentry-spring-boot-starter` auto-registers a Spring `HandlerExceptionResolver` that captures any exception reaching Spring's default error handling — i.e. anything *not* already caught by an existing local `@ExceptionHandler` (`OrderController`, `TicketController`, `DeliveryController`, and similar). Those existing handlers already map expected domain exceptions (`OrderNotFoundException`, `UnsupportedStateTransitionException`, `RestaurantServiceUnavailableException`, etc.) to 4xx responses and are left untouched — they represent expected business outcomes, not bugs, and routing them to GlitchTip would just add noise. Only genuinely unhandled exceptions (bugs, producing 500s today) get captured.
- `traceId`/`spanId` from the existing Micrometer Tracing MDC context are attached to captured events (Sentry's SDK reads active trace context automatically when `micrometer-tracing-bridge-otel` is present), so a GlitchTip issue can be cross-referenced with the corresponding Tempo trace and Kibana log lines — completing the correlation triangle started by the log-aggregation and tracing sub-projects.

## Testing / acceptance

New Cucumber scenario in the e2e test module: send a request crafted to hit a genuinely unhandled code path in one service (not one of the domain exceptions already covered by that service's `@ExceptionHandler`), assert the response is a 500, then poll GlitchTip's issues API until a matching issue appears, asserting on the captured exception class and service name. Mirrors the polling-verification pattern already used for the metrics (Prometheus) and tracing (Tempo) sub-projects. The specific endpoint/input used to trigger the unhandled exception is an implementation-planning detail, to be identified during SDD task breakdown.

## Documentation

Per this repo's `CLAUDE.md` per-change doc-sync rule: update `README.md` (tech-stack/observability row, running-locally port list), `CONTEXT.md` (Current position, Patterns reference, session log), and add an "Exception tracking (Ch.11, §11.3.5)" section to `docs/ARCHITECTURE.md`, matching the depth of the existing "Log aggregation" and "Distributed tracing" sections there. This sub-project alone does not flip Ch.11/§11.3 to Done — §11.3.6 (audit logging) remains unstarted, so the full chapter-completion sweep stays deferred until that ships too.
