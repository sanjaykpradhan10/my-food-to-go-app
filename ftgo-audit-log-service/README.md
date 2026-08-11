# ftgo-audit-log-service

**Port:** 8089
**Bounded context:** None — an activity ledger, not a business domain (no aggregate, no write-side business logic)

## Role

This service implements the book's **audit logging** pattern (Ch.11, §11.3.6): a durable,
queryable record of *who did what to which business object*, kept separate from the services that
produced the activity. It is a pure Kafka consumer over the `audit-log` topic plus one read-only,
`ADMIN`-gated query endpoint — the same minimal shape as `ftgo-order-history-service`, and for
the same reason (nothing upstream should have to know it exists, or slow down because of it).

The events it consumes are produced by `ftgo-common`'s `AuditLoggingAspect`, an `@Around` advice
that intercepts every `@PostMapping` + `@PreAuthorize`-annotated controller method in the services
that put `ftgo-common` on their classpath. Neither this service nor the aspect appears anywhere in
any business service's own code — see `docs/ARCHITECTURE.md`'s "Audit logging (Ch.11, §11.3.6)"
section for the full mechanism and the endpoint-by-endpoint list of what is audited.

Consequently this service **makes no synchronous call to anything**; the services it audits never
call it. It does register with Eureka (unlike `ftgo-order-history-service`), which is incidental
rather than needed — nothing discovers it today.

## API

**`GET /audit-log`** — **Auth:** `ADMIN` (bearer JWT issued by `ftgo-authorization-server`, Ch.11
§11.1; validated by this service as an OAuth2 resource server). An audit log records who did what,
so read access to it is itself privileged — no other role can reach this endpoint. Always returns
`200` with a JSON array, newest first; an empty array rather than `404` when nothing matches.

Optional query parameters, **mutually exclusive and applied in a fixed precedence order** — the
first matching branch wins, later parameters are ignored:

| Precedence | Parameters | Repository method |
|---|---|---|
| 1 | `userId` | `findByUserIdOrderByTimestampDesc` |
| 2 | `entityType` **and** `entityId` (both required) | `findByEntityTypeAndEntityIdOrderByTimestampDesc` |
| 3 | `from` **and** `to` (both required, ISO-8601 `Instant`) | `findByTimestampBetweenOrderByTimestampDesc` |
| 4 | none of the above | `findAllByOrderByTimestampDesc` |

So `?userId=alice&entityType=Order` filters by `userId` only. A combinatorial filter would need a
`Specification`/Criteria query rather than derived repository methods — deliberately out of scope
for this learning project.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8089/audit-log?entityType=Order&entityId=42"
```

```json
[
  {
    "id": 17,
    "userId": "alice",
    "roles": "CONSUMER",
    "action": "POST OrderController.cancel",
    "entityType": "Order",
    "entityId": "42",
    "outcome": "SUCCESS",
    "failureReason": null,
    "serviceName": "ftgo-order-service",
    "timestamp": "2026-08-11T09:14:22.113Z"
  }
]
```

There is no `POST`/`PUT`/`DELETE`. This service never originates a write and never mutates a row —
an audit ledger is append-only by definition, and the only writer is the Kafka listener.

## Events consumed

| Topic | Listener | Consumer group | Payload |
|---|---|---|---|
| `audit-log` | `AuditLogEventListener` | `audit-log-service` | `AuditLogEntryEvent` (JSON) |

`AuditLogEntryEvent` lives in `ftgo-common` (`com.sanjay.ftgo.common.audit`) and is **shared**
between producer and consumer rather than copy-pasted per side. That breaks this codebase's usual
convention of per-consumer wire-format records — justified here because the producer is itself in
`ftgo-common` (the aspect), so there is exactly one producer type and one consumer type, both of
which would be redefining the same record from the same module.

```java
record AuditLogEntryEvent(String userId, List<String> roles, String action, String entityType,
                          String entityId, String outcome, String failureReason,
                          String serviceName, Instant timestamp)
```

`outcome` is one of the record's two constants, `SUCCESS` / `FAILURE`; `failureReason` is the
exception's `getSimpleName()` on failure, null otherwise. A malformed payload is logged at WARN
and skipped, never rethrown — one bad message must not stop the consumer group making progress,
the same policy every other consumer in this codebase uses.

**This service publishes nothing.** `ftgo-common`'s `OutboxPublisher` auto-configuration is on the
classpath (as in every service depending on `ftgo-common`) and its tables are scanned, but no code
here ever writes an `OutboxEvent` row, so the poller has nothing to poll.

## Domain model

`AuditLogEntry` (`@Entity`, table `audit_log_entries`, surrogate `@GeneratedValue` `id`) is **not**
a DDD aggregate: no invariants, no guarded state transitions, no domain events. Every row is
inserted once and never read-modify-written.

| Field | Column | Notes |
|---|---|---|
| `id` | `id` | identity-generated surrogate key |
| `userId` | `user_id` | JWT `sub` of the actor; nullable only for endpoints where a JWT genuinely can't be resolved |
| `roles` | `roles` | the JWT's role list, comma-joined into one column |
| `action` | `action` | `"POST <ControllerSimpleName>.<methodName>"`, not null |
| `entityType` | `entity_type` | controller simple name minus the `Controller` suffix |
| `entityId` | `entity_id` | first `@PathVariable` argument; null on creation endpoints |
| `outcome` | `outcome` | `SUCCESS` / `FAILURE`, not null |
| `failureReason` | `failure_reason` | exception simple name on failure |
| `serviceName` | `service_name` | producing service's `spring.application.name`, not null |
| `timestamp` | `occurred_at` | set by the aspect at publish time, not null |

`roles` is a comma-joined string rather than an `@ElementCollection` child table on purpose: it is
write-once and only ever read back whole for display, so normalizing it would be pure overhead —
unlike `ftgo-order-history-service`'s `OrderView` line items, which are genuinely relational.

`userId` is nullable in the schema because the aspect discovers the actor by scanning the
intercepted method's arguments for a `Jwt`, which is only found when the controller method
declares an `@AuthenticationPrincipal Jwt` parameter. Every audited endpoint across
`ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-delivery-service`, and `ftgo-consumer-service`
now declares one for this purpose, so in practice `userId` is populated on every entry these
services produce. `POST /restaurants` remains entirely unaudited (see below) rather than
null-`userId` — `ftgo-restaurant-service` has no dependency on `ftgo-common`, so the aspect is
never woven into its controllers at all.

## Idempotency & reliability

This service does **not** use the `processed_events` dedup ledger every other consumer in this
codebase does. That is deliberate: `audit_log_entries` is an append-only ledger of *attempts*, so a
Kafka redelivery producing a second identical row is a duplicate observation, not a corrupted
state — nothing downstream reads a computed aggregate off this table. Deduping would require the
producer to mint and carry an event id, which `AuditLogEntryEvent` does not have.

The weaker guarantee is on the producing side: the aspect publishes **best-effort**, outside any
transaction, wrapped in a try/catch that logs and swallows. Audit logging must never fail or block
the request it is auditing, so a Kafka outage loses audit records while the business transaction
still commits. Routing audit events through the transactional outbox would close that gap and is
the correct upgrade for a real compliance requirement; see `docs/ARCHITECTURE.md` for why it was
left out here.

`KafkaConsumerConfig` is deliberately empty. Unlike `ftgo-order-history-service` — whose four
listeners race to update the same `order_views` row and need a hand-built retrying container
factory — this service has one listener doing plain inserts with no shared-row contention, so
Boot's default listener container factory is sufficient.

## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only. Reports `db`
(MySQL reachability) and `discoveryComposite` (Eureka registration). No `kafka` component —
Spring Boot 3.5.16 ships no Kafka health contributor. `compose.yml` gives this service the same
`curl -f http://localhost:8089/actuator/health` healthcheck block as every other service.

## Metrics (Ch.11, §11.3.4)

`GET /actuator/prometheus` — Micrometer `PrometheusMeterRegistry`, scraped every 5s by the
`prometheus` compose service. JVM/HTTP metrics only; this service defines **no custom business
counter** (the row count in `audit_log_entries` is itself the volume signal, and the useful
counters — orders placed, tickets accepted — already exist on the services being audited).

## Tracing (Ch.11, §11.3.3)

Traces exported via OTLP/HTTP to Grafana Tempo (`http://tempo:4318/v1/traces`), 100% sampled.
HTTP and JDBC spans come free from Boot's autoconfiguration; `spring.kafka.listener.observation-enabled: true`
adds the consumer-side span, so an audited `POST` and the resulting audit-log insert appear in one
trace. This service uses Boot's default listener factory, so no explicit
`setObservationEnabled(true)` call is needed (contrast `ftgo-order-history-service`).

Note that the audit *record* itself carries no `traceId` field — it is keyed by actor and business
object, the axis an auditor searches on, not by request.

## Configuration (Ch.11, §11.2)

Three tiers: **Spring Cloud Config Server** (`config-repo/application.yml` shared defaults, no
per-service override file) > local `application.yml` fallback. Non-blocking startup contract
(`spring.cloud.config.fail-fast: false`) — an unreachable config server leaves this service running
on local defaults. Every property here requires a full restart to change; nothing is
`@RefreshScope`d (this service has no outbox poller, the one live-refreshable property in the
project).

## Exception tracking (Ch.11, §11.3.5)

`sentry-spring-boot-starter-jakarta`, DSN injected as `SENTRY_DSN` from the shared `sentry-dsn`
Docker volume by the Dockerfile's entrypoint wrapper, same as every other service. Captures only
exceptions that reach Spring's default error handling.

## Running standalone

```bash
./gradlew :ftgo-audit-log-service:test
```

Needs the full docker-compose stack (MySQL, Kafka, the authorization server for a token, and at
least one business service producing audited calls) to exercise live — see the root
[README](../README.md) for `docker compose up`.

Key environment variables (see `application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `SPRING_DATASOURCE_URL` | `jdbc:mysql://localhost:3306/ftgo_audit_log` | MySQL connection |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWKSETURI` | `http://localhost:9000/oauth2/jwks` | JWT validation |
| `SERVER_PORT` | `8089` | HTTP port |

No `SAGA_MODE` and no `PERSISTENCE_MODE` — this service is not a saga participant in either style
and has no aggregate to persist.

Verified live end-to-end by `ftgo-end-to-end-test`'s "Placing an order records an audit log entry"
scenario (`PlaceReviseCancelOrder.feature`), which places an order through the real containerized
stack and polls `GET /audit-log` until an `entityType=Order` entry with a `createOrder` action
appears.
