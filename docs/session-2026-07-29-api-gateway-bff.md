# Session — 2026-07-29 (Ch.8 — API gateway / Backends for Frontends)

**Tool:** Claude Code
**Duration:** Single session — full brainstorm → spec → plan → task-based implementation cycle (10 tasks), including live Docker e2e verification and a full documentation sweep.
**Repo:** https://github.com/sanjaykpradhan10/my-food-to-go-app
**Branch:** `worktree-api-gateway-bff`
**Spec:** `docs/superpowers/specs/2026-07-29-api-gateway-bff-design.md`
**Plan:** `docs/superpowers/plans/2026-07-29-api-gateway-bff-plan.md`

## Sub-project scope

Chapter 8 (external API patterns) implements both of the book's named patterns — API gateway and Backends for Frontends — as one combined sub-project rather than two separate ones, since BFF is really "one API-gateway-shaped edge service per client type" and the two aren't meaningfully separable in this codebase. Three new Gradle modules:

- **`ftgo-gateway-common`** — a shared WebFlux library (not a runnable service) holding the cross-cutting edge functions both gateways need: `RequestLoggingFilter` (method/path/status/latency logging), `ApiKeyAuthFilter` (a stub shared-secret `X-Api-Key` check — no real identity service, matching the design spec's non-goals), and `PerKeyRateLimiterGatewayFilterFactory` (an in-memory, per-API-key fixed-window rate limiter, chosen over Spring Cloud Gateway's Redis-backed built-in since this project has no Redis).
- **`ftgo-public-gateway`** (port 8091) — pure Spring Cloud Gateway routing, no hand-written composition code, fronting order/kitchen/accounting/delivery/order-history/restaurant-service under `/api/v1/...`. API key `public-dev-key`, 5 req/s per key.
- **`ftgo-mobile-gateway`** (port 8090) — three thin order-mutation routing passthroughs plus one hand-written composition endpoint, `GET /mobile/orders/{orderId}`, fanning out in parallel (`Mono.zip`) to order/kitchen/accounting/delivery-service, each behind its own `ReactiveCircuitBreaker`, resolving to a `SectionResult<T>` (`Found`/`NotFound`/`Unavailable`) per section — architecturally the same idea as Ch.7's API composition, but performed one layer further out, at the edge, and deliberately not delegating to Ch.7's own `GET /orders/{id}/view`. API key `mobile-dev-key`, 20 req/s per key.

## SDD execution flow

Ten tasks, executed in order:

1. **`ftgo-gateway-common` scaffold** — new Gradle module, `GatewayApiKeyProperties`, `GatewayCommonAutoConfiguration`.
2. **`RequestLoggingFilter`** — `GlobalFilter`, highest precedence (`Integer.MIN_VALUE`).
3. **`ApiKeyAuthFilter`** — `GlobalFilter`, `Integer.MIN_VALUE + 1`, validated against `GatewayApiKeyProperties`.
4. **`PerKeyRateLimiterGatewayFilterFactory`** — named `AbstractGatewayFilterFactory<Config>`, in-memory fixed-window counter.
5. **`ftgo-public-gateway`** — 6 declared routes, `RewritePath` + `PerKeyRateLimiter` filter pairs, all `lb://`-resolved via Eureka.
6. **`ftgo-mobile-gateway` routing** — 3 declared routes (create/cancel/revise order → order-service passthroughs).
7. **`ftgo-mobile-gateway` composition** — `GET /mobile/orders/{orderId}`, a hand-written `RouterFunction` (`OrderDetailsRouterConfig`/`OrderDetailsHandler`), `Mono.zip` fan-out with 4 `ReactiveCircuitBreaker`s and a reactive `SectionResult<T>` sealed interface.
8. **Docker wiring** — both gateways added to `compose.yml`, wired to `EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE` (note the exact casing — not `SERVICEURL`).
9. **Live Docker e2e verification** — the substantive part of this session; see below.
10. **Documentation sweep** (this task) — full chapter-completion sweep per `CLAUDE.md`'s rule.

## The 5 bugs found and fixed during Task 9's live e2e verification

None of these 5 were caught by the isolated unit/integration test suites written in Tasks 1–8 — every one required the full live-networked Eureka-discovery Docker Compose stack (all 12 services, real Eureka registration, real cross-service HTTP calls through the real gateway filter chain) to surface. This is the actual substance of the session's later half, and the clearest reminder yet in this project that discovery-dependent and filter-chain-dependent behavior genuinely cannot be verified by mocking the collaborator.

1. **`order-service` unreachable via Eureka.** `order-service`'s `application.yml` had carried `eureka.client.register-with-eureka: false` since Ch.1–7 (a config choice nobody had reason to revisit — nothing previously called order-service synchronously from outside its own JVM in a way that needed it discoverable). Ch.8's mobile gateway is the first caller that needs to *discover* order-service (rather than order-service discovering others, as in Ch.7). The mobile gateway's composed endpoint failed every "order" section lookup until this line was removed.

2. **`order-history-service` had no Eureka client at all.** No `spring-cloud-starter-netflix-eureka-client` dependency, no `eureka.client.*` config — a deliberate Ch.7 design choice (this service is Kafka-only, never called synchronously by anything, per its own README). Ch.8's public gateway needs to *route* to it, which requires discoverability that never existed. Fixed by adding the dependency and matching kitchen-service's existing config pattern, plus a per-module Spring Cloud BOM override (the root `build.gradle`'s BOM is stale at `spring-cloud-dependencies:2024.0.0`; this module needed its own `2025.0.3` override in its own `build.gradle`, the same convention `ftgo-order-service` and both new gateway modules already use).

3. **The mobile gateway's composed endpoint had zero auth or rate-limiting.** This is the RouterFunction-vs-Gateway-route filter-isolation finding described in `docs/ARCHITECTURE.md`: `GET /mobile/orders/{orderId}` is a hand-written WebFlux `RouterFunction`, dispatched via `RouterFunctionMapping`, which never passes through Spring Cloud Gateway's own `GlobalFilter` chain — so `ApiKeyAuthFilter` (and `RequestLoggingFilter`/`PerKeyRateLimiter`) silently never ran for this one endpoint, discovered only by manually calling it without an API key and getting a `200` instead of a `401`. Fixed by adding an explicit `.filter(...)` to `OrderDetailsRouterConfig` replicating `ApiKeyAuthFilter`'s exact check. Rate-limiting and request-logging on this endpoint remain a known, deliberately parked gap — not fixed in this branch.

4. **`order-service` had no `GET /orders/{id}` endpoint.** Only `POST` mappings existed on `OrderController` prior to this chapter — nothing had ever needed to read a single order back by ID via a plain REST call (Ch.7's `GET /orders/{id}/view` is a *different*, composed endpoint, not a raw order lookup). The mobile gateway's "order" composition section came back `Unavailable` on every call until a lightweight `GET /{id}` was added, returning just order-service's own raw data — deliberately not delegating to or duplicating `/{id}/view`'s Ch.7 composed-view responsibility.

5. **`SectionResult.NotFound` and `SectionResult.Unavailable` were JSON-indistinguishable.** Both are zero-field records in the mobile gateway's reactive `SectionResult<T>` sealed interface, so both serialized to identical `{}` — a client had no way to tell "this sub-resource doesn't exist yet" (e.g. no delivery scheduled) from "the backend is circuit-broken/unreachable," even though the whole point of the 3-state design (mirroring Ch.7's) is to keep those cases distinguishable. Fixed by adding a `@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "status")`/`@JsonSubTypes` discriminator (`FOUND`/`NOT_FOUND`/`UNAVAILABLE`) to the sealed interface.

All 5 fixes were independently code-reviewed and approved before this documentation sweep.

## Documentation sweep (this task)

Per `CLAUDE.md`'s chapter-completion rule: `docs/ARCHITECTURE.md` gained a new "API Gateway / Backends for Frontends" section (ownership model, routing tables for both gateways, the mobile-gateway composition sequence diagram, an explicit contrast table against Ch.7's API composition, and the RouterFunction-vs-Gateway-route filter-isolation callout); new `ftgo-gateway-common`/`ftgo-mobile-gateway`/`ftgo-public-gateway` READMEs following the existing per-service convention; `CONTEXT.md`'s book-progress table (Ch.8 flipped to Done), current position, services-to-build table, concept-understanding section, and session log; and the root `README.md`'s service list and book-progress line.

## Next actions

- [ ] Chapter 9 — Testing microservices: Part 1 — not started, no session yet.
- [ ] Still-deferred: the mobile gateway's composed endpoint has no rate-limiting or request-logging applied (RouterFunction filter-isolation gap, deliberately parked, not part of this chapter's scope).
- [ ] Still-deferred from earlier sessions: consider a Spring Boot 4.x migration now that 3.5.x is permanently frozen (no more OSS patches).

---

## Resuming in a new session

### In Claude Code
Open the project and say:
> "Read CONTEXT.md. Let's start Chapter 9 — testing microservices."

### In Claude Chat
Paste `CONTEXT.md`, then say:
> "I'm working through Microservices Patterns. Chapter 8 (external API patterns) is fully done — two BFF gateways (`ftgo-mobile-gateway`, `ftgo-public-gateway`) sharing a `ftgo-gateway-common` edge-function library, Docker-verified end to end, including 5 real bugs found and fixed during e2e verification (two Eureka-registration gaps, a missing order-service endpoint, a RouterFunction auth gap, and an ambiguous SectionResult JSON serialization). Ready to move to Chapter 9, testing microservices."
