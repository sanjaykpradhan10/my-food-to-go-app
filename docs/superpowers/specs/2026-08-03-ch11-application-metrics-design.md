# Ch.11 §11.3.4: Application metrics — Design Spec

**Status:** Approved
**Book reference:** *Microservices Patterns* §11.3.4 "Applying the Application metrics pattern" (pp. 373–376)

## Context

Ch.11 sub-project 1 (health checks, §11.3.1) and the security sub-project (§11.1) are done and merged. This is the next §11.3 observability pattern: Application metrics. §11.3 has five remaining independent patterns (log aggregation, distributed tracing, application metrics, exception tracking, audit logging) — each gets its own sub-project, its own spec/plan/implementation cycle, per this project's established decomposition convention. This spec covers **application metrics only**.

All 9 services (7 business services + 2 gateways) already depend on `spring-boot-starter-actuator` (added centrally in root `build.gradle`'s `actuatorModules` list for the health-check sub-project) and already expose `GET /actuator/health`.

## Goal

Every service exposes Prometheus-scrapeable metrics (`GET /actuator/prometheus`): free auto-configured JVM/HTTP metrics for all 9, plus hand-instrumented business counters on the 7 business services mirroring the book's own `OrderService` worked example. A new `prometheus` container scrapes all 9 services; a new `grafana` container visualizes them on one dashboard; a small set of Prometheus alerting rules covers the highest-value failure signals.

## Architecture

**Metrics library:** `micrometer-registry-prometheus` added to the same `actuatorModules` list in root `build.gradle` (all 9 services). No code changes needed for the free metrics — Spring Boot autoconfigures `/actuator/prometheus` once the registry is on the classpath and `management.endpoints.web.exposure.include` includes `prometheus`.

**Custom business counters:** each of the 7 business services' domain/application-service layer gets a `MeterRegistry`-based counter increment at each domain-significant state transition, mirroring the book's `meterRegistry.counter("placed_orders").increment()` pattern exactly (`Counter` API, not `Timer`/`Gauge` — matches the book's own choice and this project's existing style of favoring the book's literal mechanism over YAGNI shortcuts, for the learning value).

| Service | Counters | Where incremented |
|---|---|---|
| `ftgo-order-service` | `orders_placed`, `orders_approved`, `orders_rejected`, `orders_cancelled` | `OrderService`, at the same points `Order`'s `approve()`/`reject()`/`cancel()` are called |
| `ftgo-kitchen-service` | `tickets_accepted`, `tickets_preparing`, `tickets_ready_for_pickup`, `tickets_picked_up`, `tickets_cancelled` | `TicketService`, at the same points `Ticket`'s `accept()`/`preparing()`/`readyForPickup()`/`pickedUp()`/`cancel()` are called |
| `ftgo-accounting-service` | `authorizations_approved`, `authorizations_declined`, `authorizations_reversed` | `AccountingService`, at `Authorization`'s create-approved/create-declined/`reverse()` |
| `ftgo-delivery-service` | `deliveries_scheduled`, `deliveries_picked_up`, `deliveries_delivered`, `deliveries_cancelled` | `DeliveryService`, at `Delivery`'s create-scheduled/`pickedUp()`/`delivered()`/`cancel()` |
| `ftgo-restaurant-service` | `restaurants_created` | restaurant creation endpoint's service method |
| `ftgo-consumer-service` | `consumers_created` | consumer creation endpoint's service method |
| `ftgo-order-history-service` | `order_views_updated` | the `@KafkaListener` method(s) that upsert `OrderView` rows |

Each counter is a plain no-tag `Counter` (matching the book's example — no per-service dimension needed since Prometheus already labels every scraped metric with the target `job`/`instance`).

**compose.yml additions:**
- `prometheus` service: official `prom/prometheus` image, mounts a new `prometheus/prometheus.yml` (scrape config: one static target per business service + gateway, `metrics_path: /actuator/prometheus`, `scrape_interval: 5s`) and `prometheus/alert_rules.yml`. Exposed on host port `9090`.
- `grafana` service: official `grafana/grafana` image, mounts a provisioned datasource (Prometheus, auto-added, no manual UI setup) and one provisioned dashboard JSON. Exposed on host port `3000`. `depends_on: prometheus`.
- Neither container is on the `depends_on: condition: service_healthy` critical path of any business service — they're observers, not dependencies (a business service must never fail to start because Prometheus/Grafana aren't ready).

**Alert rules** (`prometheus/alert_rules.yml`, Prometheus's own native rule format — no Alertmanager, no notification channel; rules fire and are visible in Prometheus's own `/alerts` UI page only, since there's no real on-call for a learning project):
1. `ServiceDown` — `up == 0` for 30s on any scrape target.
2. `HighOrderRejectionRate` — `rate(orders_rejected_total[5m]) / rate(orders_placed_total[5m]) > 0.5` for 2m.
3. `HighAuthorizationDeclineRate` — `rate(authorizations_declined_total[5m]) / (rate(authorizations_approved_total[5m]) + rate(authorizations_declined_total[5m])) > 0.5` for 2m.

**Grafana dashboard:** one dashboard, provisioned as JSON, with panels for: per-service `up` status, JVM heap usage (all services), HTTP request rate/latency (all services), and the business counters from the table above (one panel per service, showing its counters as rate-per-minute time series).

## Verification

**New Cucumber scenario** in `ftgo-end-to-end-test` (extends the existing OAuth2-authenticated e2e suite): places an order through the full stack (reusing existing step definitions for auth + order placement), then polls `order-service`'s `GET /actuator/prometheus` directly (same direct-port-access pattern the health-check scenario uses) asserting `orders_placed_total` and `orders_approved_total` (or `orders_rejected_total`, depending on the scenario's credit-card fixture) both read back ≥ 1 after the flow completes.

**Manual Docker verification:** bring up the full stack, confirm in Prometheus's UI that all 9 scrape targets show `up`, confirm the 3 alert rules load without syntax errors (visible on Prometheus's `/alerts` page, all in `inactive` state under normal operation), and confirm the Grafana dashboard renders with live data after driving a few orders through.

## Documentation

Per-change docs land in the same commit: `docs/ARCHITECTURE.md` gains an "Application metrics (Ch.11, §11.3.4)" section; each of the 7 business-service READMEs gains a short "Metrics" subsection listing its custom counters; root `README.md`'s tech stack and Book progress table; `CONTEXT.md`'s Current position, Services to build table (if applicable), and Patterns reference. No full chapter-completion sweep — §11.3 as a whole isn't Done yet (log aggregation, distributed tracing, exception tracking, audit logging remain unscheduled).

## Out of scope (deferred)

- Alertmanager / real notification delivery (no on-call for a learning project).
- Per-request/per-endpoint custom timers beyond what Micrometer's HTTP auto-instrumentation already provides for free.
- Dimensional/tagged counters (e.g. `orders_total{status=...}` as a single tagged counter instead of 4 separate counters) — the book's own example uses separate counters, and this project follows that literal mechanism for the learning value.
- The other four §11.3 patterns (log aggregation, distributed tracing, exception tracking, audit logging) — separate future sub-projects.
