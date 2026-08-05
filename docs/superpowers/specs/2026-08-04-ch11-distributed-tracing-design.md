# Ch.11 §11.3.3: Distributed tracing — Design Spec

**Status:** Approved
**Book reference:** *Microservices Patterns* §11.3.3 "Using the Distributed tracing pattern" (pp. 370–373)

## Context

Ch.11 sub-project 1 (health checks, §11.3.1), the security sub-project (§11.1 sub-projects 1–2), and application metrics (§11.3.4) are done and merged. This is the next §11.3 observability pattern: Distributed tracing. §11.3 has four remaining independent patterns after this one (log aggregation, exception tracking, audit logging) — each gets its own sub-project, its own spec/plan/implementation cycle, per this project's established decomposition convention. This spec covers **distributed tracing only**.

The book pairs this pattern with Spring Cloud Sleuth + Zipkin. Sleuth is superseded by Micrometer Tracing in current Spring Boot; this project uses Micrometer Tracing with the OpenTelemetry bridge, exporting to Grafana Tempo (chosen over Zipkin so traces live in the same Grafana instance already provisioned for metrics — see the application-metrics sub-project).

All 9 services (7 business services + 2 gateways) already depend on `spring-boot-starter-actuator`. No tracing library is present anywhere in the codebase today — this is a clean-slate addition.

## Goal

Every service is instrumented so that a single external request produces one trace spanning every service it touches — HTTP hops (API composition, gateway → service, service → service) and Kafka hops (the choreography sagas) alike — visible in Grafana via a provisioned Tempo datasource. No manual span code anywhere: Spring Boot autoconfiguration handles HTTP client/server and JDBC spans once the tracing dependencies are on the classpath; only Kafka producer/consumer propagation and the gateways' reactive context propagation need explicit configuration.

## Architecture

**Tracing libraries** added to the same centrally-managed dependency list pattern used for actuator/metrics in root `build.gradle` (all 9 services):
- `io.micrometer:micrometer-tracing-bridge-otel`
- `io.opentelemetry:opentelemetry-exporter-otlp`
- `management.tracing.sampling.probability: 1.0` in each service's `application.yml` — 100% sampling. This is a learning project driven by manually-triggered e2e requests, not production traffic under cost pressure, and 100% sampling guarantees the e2e verification scenario's trace is always exported (no flaky "didn't get sampled" failures).
- `management.otlp.tracing.endpoint: http://tempo:4318/v1/traces` — OTLP/HTTP exporter pointed at the new `tempo` compose service.

**Automatic instrumentation** (no code changes): once the above dependencies are present, Spring Boot autoconfigures spans for inbound HTTP requests (every service's REST endpoints), outbound HTTP requests (order-service's `RestTemplate`/`WebClient` calls to restaurant/kitchen/accounting/delivery-service), and JDBC calls (all DB-backed services) — this mirrors how the metrics sub-project got JVM/HTTP metrics for free from Micrometer's autoconfiguration, with custom code needed only where the framework can't infer intent.

**Kafka propagation** (explicit config needed): Spring Boot does not auto-instrument Kafka producer/consumer headers the way it does HTTP. The choreography sagas (order/kitchen/accounting/delivery-service publish and consume domain events via Kafka) need a small `ProducerFactory`/`ConsumerFactory` interceptor configuration per Kafka-producing/consuming service (order, kitchen, accounting, delivery, order-history) using Micrometer's Kafka instrumentation (`KafkaTemplate` interceptor + listener container factory customizer), so a saga's Kafka hops appear as spans in the same trace as the HTTP request that triggered them, not as a disconnected trace.

**Gateway reactive propagation** (explicit config needed): both gateways (`ftgo-mobile-gateway`, `ftgo-public-gateway`) are Spring Cloud Gateway / WebFlux, which is reactive — trace context does not automatically survive across a Reactor `Flux`/`Mono` chain the way it does across a servlet thread. Micrometer's `context-propagation` library (`io.micrometer:context-propagation`, plus `Hooks.enableAutomaticContextPropagation()` or the equivalent Boot autoconfiguration flag) is required so trace context established at the start of a request is still present inside the existing `RequestLoggingFilter`/`JwtValidationFilter` `GlobalFilter`s and the downstream proxied call — this is the one piece of this sub-project that's genuinely code, not just dependency + YAML.

**compose.yml additions:**
- `tempo` service: official `grafana/tempo` image, mounts a minimal `tempo/tempo.yaml` config (local-disk block storage — no object-store backend needed at this scale, OTLP receiver enabled on gRPC `4317` and HTTP `4318`). Exposed on host port `3200` (Tempo's query API, used by the e2e verification step) plus `4318` (OTLP HTTP ingest, used by all 9 services).
- Grafana gains a second provisioned datasource (Tempo, alongside the existing Prometheus one from the metrics sub-project) — no manual UI setup, same provisioning-file pattern already in place at `grafana/provisioning/datasources/`.
- `tempo` is not on the `depends_on: condition: service_healthy` critical path of any business service — same rule as `prometheus`/`grafana`: an observability backend must never block a business service from starting. Business services get `depends_on: tempo: condition: service_started` (best-effort — a trace export failing early during Tempo startup just drops that span, same as Prometheus scrapes failing before it's up).

## Verification

**New Cucumber scenario** in `ftgo-end-to-end-test`: place an order through the public gateway (reusing existing place-order step definitions), then poll Tempo's search API (`GET /api/search?tags=service.name=ftgo-public-gateway&start=...&end=...`, narrow time window around the request) for a trace produced in that window, fetch that trace by ID, and assert its span list includes spans from at least two distinct `service.name` values (e.g. `ftgo-public-gateway` and `ftgo-order-service`) — proving the fan-out was actually captured as one linked trace, not just that each service independently emitted spans. Mirrors the Prometheus-counter verification pattern from the metrics sub-project (poll an observability backend's own API after driving a real flow, assert on the recorded data).

**Manual Docker verification:** bring up the full stack, drive a few orders through the public gateway, open Grafana's Tempo view (or Tempo's own minimal UI) and confirm traces show multiple spans across services with plausible parent/child timing, and confirm a Kafka-hop span (e.g. order-service → kitchen-service via the choreography saga) appears within the same trace as the HTTP request that triggered it.

## Documentation

Per-change docs land in the same commit: `docs/ARCHITECTURE.md` gains a "Distributed tracing (Ch.11, §11.3.3)" section describing the trace/span model and the Kafka + reactive propagation specifics; each of the 7 business-service READMEs plus both gateway READMEs gain a short "Tracing" subsection (mostly pointing at the shared config, since there's no per-service custom code outside Kafka producers/consumers); root `README.md`'s tech stack row and Book progress table; `CONTEXT.md`'s Current position and Patterns reference. No full chapter-completion sweep — §11.3 as a whole still won't be Done after this (exception tracking and audit logging remain unscheduled).

## Out of scope (deferred)

- Correlating trace IDs into log lines (that's log aggregation's territory, §11.3.2, not scheduled yet — the book explicitly treats this as a side effect of *combining* tracing with log aggregation, not tracing alone).
- Sampling-rate tuning for production traffic volumes (no real production traffic here).
- Any alerting on trace data (latency-based alerts, error-rate-from-traces) — alerting in this project is scoped to Prometheus metrics only, per the application-metrics sub-project's existing alert rules.
- Zipkin as an alternative backend — Tempo was the explicit choice this session to consolidate on one Grafana instance for both metrics and traces.
