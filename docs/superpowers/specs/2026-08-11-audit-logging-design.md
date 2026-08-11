# Ch.11 §11.3.6: Audit Logging — Design

## Pattern

[Audit logging](http://microservices.io/patterns/observability/audit-logging.html) (Richardson, *Microservices Patterns*, §11.3.6): record each user's actions — identity of the user, the action performed, and the business object(s) acted upon — to help customer support, ensure compliance, and detect suspicious behavior.

The book names three implementation options: sprinkling audit calls directly into business logic (simple, error-prone to maintain), AOP (reliable interception but advice only sees method name/args, not business meaning), and event sourcing (audit log falls out of the event store for free, but only covers writes and only where event sourcing is already used).

This is the last unstarted Ch.11 sub-project — completing it flips Ch.11 to Done in `CONTEXT.md`'s progress table, triggering this repo's `CLAUDE.md` chapter-completion documentation sweep (full `docs/ARCHITECTURE.md` sections with diagrams, full parity for every touched service README, `CONTEXT.md` concept-understanding cleanup) in addition to the normal per-change doc sync.

## Chosen approach: Spring AOP + a dedicated audit-log service

Only `ftgo-order-service` uses event sourcing in this codebase; the other 8 services don't, so event sourcing alone can't give uniform coverage. This codebase has no existing `@Aspect`/AOP infrastructure (confirmed: no `@Aspect` usage anywhere, no `spring-boot-starter-aop` dependency), but Spring AOP is the natural fit here because it can be added **once**, in the shared `actuatorModules` block of the root `build.gradle` (the same block that already carries the Sentry, Micrometer, and actuator starters), and uniformly instruments all 9 services without per-service code changes to business logic.

Captured audit events are shipped over Kafka to a new **`ftgo-audit-log-service`**, which mirrors the existing CQRS shape of `ftgo-order-history-service`: consume domain-relevant events, persist to an owned MySQL database (`ftgo_audit_log`, alongside the existing per-service databases on the shared `mysql` container), expose a query API.

## Components

### 1. `AuditLoggingAspect` (added to `ftgo-common`, applied via the shared `actuatorModules` build.gradle block)

- `@Around` advice on `@RestController` methods that are also state-changing (POST/PUT/DELETE-mapped) and `@PreAuthorize`-gated — the existing convention in every controller in this codebase (`OrderController.createOrder`/`cancel`/`revise`, `TicketController`, `DeliveryController`, `RestaurantController`, `ConsumerController`, `AuthorizationController`, etc.).
- Read (`@GetMapping`) endpoints are explicitly **out of scope** for this pass: capturing "who did what to which business object" is satisfied by mutating actions alone, and blanket GET capture would add audit-log volume with no demonstrated need. Can be added later by annotating specific sensitive reads if a need arises.
- Extracts:
  - **who** — `jwt.getSubject()` pulled from the method's `@AuthenticationPrincipal Jwt` parameter (never from request-body fields, matching the existing `consumerId`-from-JWT convention in `OrderController.createOrder`).
  - **action** — the HTTP method + the Spring-resolved request mapping path (e.g. `POST /orders/{id}/cancel`).
  - **business object** — the method's `@PathVariable` value(s), and the simple class name of the controller's `@RequestMapping` base path as a stand-in for entity type (e.g. `/orders` → `Order`). This is generic enough to need no per-endpoint configuration; it degrades gracefully (entityId is `null`) for endpoints like `createOrder` that don't yet have a path-variable id at call time — those still record the actor and action, useful for support/compliance even without a pre-existing id.
  - **outcome** — `SUCCESS` if the wrapped method returns normally, `FAILURE` (with the exception's simple class name) if it throws. The advice re-throws whatever it caught unchanged — audit logging must never suppress or alter application behavior.
- Publishes an `AuditLogEntryEvent` to a Kafka topic named `audit-log`. This is fire-and-forget: a `KafkaTemplate.send(...)` call with no blocking `.get()`, and any publish failure is caught and logged locally, never propagated to the caller. Audit logging is best-effort observability, not a transactional guarantee — consistent with this project's existing outbox/Kafka reliability posture, and it must never turn an otherwise-successful business operation into a failed request.

`AuditLogEntryEvent` fields: `userId` (String, JWT subject), `roles` (List\<String\>, from the JWT `roles` claim, for support context), `action` (String, `"METHOD /path"`), `entityType` (String, nullable), `entityId` (String, nullable), `outcome` (`SUCCESS`/`FAILURE`), `failureReason` (String, nullable — only set on `FAILURE`), `serviceName` (String — `spring.application.name`, so entries are traceable to their origin service), `timestamp` (Instant).

### 2. `ftgo-audit-log-service` (new service)

- Kafka consumer on the `audit-log` topic, one handler that persists each `AuditLogEntryEvent` as a row in an `audit_log_entry` table (own MySQL schema `ftgo_audit_log`, following the one-database-per-service convention already used by every other service on the shared `mysql` container).
- `GET /audit-log` query endpoint, `@PreAuthorize("hasRole('ADMIN')")` only (no consumer/self-service access — audit logs are a support/compliance/security tool, not a user-facing feature). Supports optional filter query params: `userId`, `entityType`, `entityId`, `from`/`to` (ISO-8601 timestamps), each combinable, all optional (no filters = most recent N entries, paginated).
- Registers with Eureka, gets a config-server entry, and joins the `actuatorModules` list in `build.gradle` (health checks, metrics, tracing, logging — the same baseline every other business service has), and gets a `sentry.dsn` entry alongside the other 8 services (uniform observability coverage, per this project's existing "all 9/10 services get X" convention for every Ch.11 sub-project so far).

### 3. `compose.yml`

- New `audit-log-service` container, same shape as `order-history-service`'s entry (depends on `mysql`, `kafka`, `service-registry`, `authorization-server`, `tempo`, `config-server`, `glitchtip-provisioner`; own `SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_audit_log`; own port).
- No new infrastructure containers needed — reuses the existing `mysql` and `kafka` containers.

## Testing / acceptance

New Cucumber scenario in the e2e test module: place an order (already-established step definitions), then poll `ftgo-audit-log-service`'s `/audit-log?entityType=Order&entityId=<id>` endpoint until an entry appears, asserting `action` contains `POST /orders` and `userId` matches the consumer's JWT subject — mirrors the polling-verification pattern already used for the GlitchTip, Prometheus, and Tempo scenarios.

## Documentation

Because this sub-project completes Ch.11, this is the trigger for the full chapter-completion sweep required by this repo's `CLAUDE.md`:
- `docs/ARCHITECTURE.md`: new "Audit logging" section (matching the depth of "Log aggregation"/"Distributed tracing"/"Exception tracking"), **plus** the deferred full sweep — every saga/pattern section gets sequence diagrams for happy path + every compensation case, matching the depth already given to the earliest sagas.
- Every `ftgo-*-service/README.md` touched by Ch.11's work gets full API/events/domain-model parity, not just a status-label update.
- `CONTEXT.md`: "Current position", "Services to build" (add `ftgo-audit-log-service`), "Patterns reference" checklist (mark Audit logging done, flip Ch.11 to Done), session log entry, and the "Concept understanding" section — move completed items out of "Needs more depth"/"Open questions" now that Ch.11 is fully closed out.
- `README.md`: service list/status, tech stack, "Book progress" table (Ch.11 → Done), running-locally port list (new `audit-log-service` port).
