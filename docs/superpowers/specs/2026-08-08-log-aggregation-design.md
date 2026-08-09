# Ch.11 §11.3.2: Log Aggregation — Design

## Pattern

[Log aggregation](http://microservices.io/patterns/observability/application-logging.html) (Richardson, *Microservices Patterns*, §11.3.2): aggregate the logs of all service instances into a centralized logging server that supports searching and alerting, so a request that spans multiple services (e.g. `getOrderDetails()` via API composition across the API gateway, Order Service, and Kitchen Service) can be debugged from one place instead of hunting through each service's own log file.

## Stack: ELK (Elasticsearch, Logstash, Kibana)

Chosen over Grafana Loki + Promtail (the alternative considered, which would have folded into the existing Grafana instance used for metrics/tracing) in favor of the book's own named stack.

## Architecture

New containers added to `compose.yml`, following the same pattern already used for `tempo`/`prometheus`/`grafana`:

- **Elasticsearch** — log store, one index per day: `ftgo-logs-%{+YYYY.MM.dd}`.
- **Logstash** — `beats` input (port 5044) → `json` filter (parses the JSON log line Filebeat forwards as a string field into top-level fields) → `elasticsearch` output.
- **Kibana** (port 5601) — UI, with a provisioned index pattern (`ftgo-logs-*`) imported at startup via Kibana's saved-objects import API, mirroring how `grafana/provisioning/` files provision Grafana's datasources/dashboards today. No manual setup needed to get a working "Discover" view.
- **Filebeat** — sidecar container using Docker's `autodiscover` module to tail all 9 services' `json-file` stdout logs (mounts `/var/lib/docker/containers` and `/var/run/docker.sock` read-only) and forward to Logstash's beats input. Each event is tagged with the source container's service name.

## Per-service logging change

- Add `net.logstash.logback:logstash-logback-encoder` to the root `build.gradle`'s shared `subprojects` block (alongside the existing `io.micrometer:micrometer-tracing-bridge-otel` dependency), so all 9 services pick it up uniformly.
- Add a `logback-spring.xml` configuring a console appender with `LogstashEncoder`, applied to all 9 services. This automatically:
  - Serializes MDC contents into the JSON output — `traceId`/`spanId` (already populated via `micrometer-tracing-bridge-otel` from the distributed-tracing work) and `service` (from `spring.application.name`) land in every log line for free, giving trace↔log correlation with zero new application code.
  - Serializes exceptions into a `stack_trace` field, so multi-line stack traces stay as one JSON event — no Logstash multiline codec needed.
- No change to log statements or `logging.level` config — this is a config-only, zero-business-logic diff.

## Testing / acceptance

This is an infrastructure/observability change, not application behavior — no new Cucumber e2e scenario. Acceptance is manual: start the full stack, place an order, and confirm a log line carrying that request's `traceId` is searchable in Kibana and matches the corresponding trace in Grafana/Tempo.

## Documentation

Per this repo's `CLAUDE.md` per-change doc-sync rule: update `README.md` (service/stack list), `CONTEXT.md` (Services/patterns tables, session log), and add a short "Log aggregation" section to `docs/ARCHITECTURE.md` (no dedicated observability doc file exists yet — this is the first observability pattern to get its own section there, matching the depth given to the saga sections already in that file).
