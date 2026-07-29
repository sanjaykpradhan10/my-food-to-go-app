# Design — Ch.8 External API patterns: API Gateway + Backends for Frontends

**Date**: 2026-07-29
**Status**: Approved (pending write-up)
**Book chapter**: 8 — External API patterns (pages 253–291)

## Context

Chapters 1–7 built out the FTGO backend (9 services: consumer, order, kitchen,
accounting, restaurant, delivery, service-registry, order-history, plus
`ftgo-common`). No client has ever talked to these services except directly,
and the `ftgo-api-gateway` entry in `settings.gradle`/the services table is
still an unbuilt stub. Chapter 8 covers exactly this gap: how external clients
(mobile apps, browser JS, 3rd-party developers) should talk to a microservice
backend, via the **API gateway** pattern and its **Backends for Frontends
(BFF)** refinement (a separate gateway per client type, each owned by that
client's team).

This project has no real mobile app or browser SPA, so the "clients" are
conceptual — the design deliberately borrows the book's own FTGO worked
example (mobile client vs. public/3rd-party API) as the two BFFs to build,
since this project's existing services already map directly onto it.

## Goals

- Replace the `ftgo-api-gateway` stub with two real gateway services: a
  mobile BFF and a public BFF, on Spring Cloud Gateway (matching the book's
  own implementation in section 8.3).
- Demonstrate gateway-level API composition (the mobile gateway's own
  fan-out, independent of order-service's existing Ch.7 composed endpoint)
  alongside pure request routing (the public gateway).
- Demonstrate edge functions: request logging, rate limiting, and a stub
  authentication filter, shared via a common module.
- Full documentation sweep per this project's chapter-completion rule.

## Non-goals

- No real authentication/identity service — the auth filter checks a static
  shared-secret header, not real tokens/users.
- No GraphQL gateway (book section 8.3.3) — noted as a future extension.
- No changes to existing services' internal REST/Kafka APIs.
- No 3rd BFF (e.g. restaurant/admin) — mobile + public covers the pattern.

## Architecture

### New Gradle modules

- `ftgo-gateway-common` — shared edge-function filters, reused by both
  gateways (WebFlux `GlobalFilter` implementations, since Spring Cloud
  Gateway is reactive): `RequestLoggingFilter` (logs method/path/status/
  latency) and `ApiKeyAuthFilter` (checks an `X-Api-Key` header against a
  configured value; missing/wrong key → 401). Packaged as a plain library
  like `ftgo-common`, not a runnable service.
- `ftgo-mobile-gateway` (port 8090) — BFF for the consumer mobile client.
- `ftgo-public-gateway` (port 8091) — BFF for 3rd-party developers.

Both gateway services:
- Depend on `spring-cloud-starter-gateway` (WebFlux-based) and
  `spring-cloud-starter-netflix-eureka-client`, registering with the existing
  `ftgo-service-registry` and discovering backend services the same way
  order-service already does (`lb://` URIs / `@LoadBalanced`-equivalent
  reactive `WebClient.Builder` with `@LoadBalanced`).
- Depend on `ftgo-gateway-common` for shared filters.
- Rate limiting via a custom `GatewayFilterFactory` in
  `ftgo-gateway-common`, backed by a per-API-key in-memory token bucket.
  Spring Cloud Gateway's built-in `RequestRateLimiter` requires Redis;
  this project has no Redis instance, so a lightweight custom filter
  avoids adding a new infrastructure dependency for a stub edge function.

### Mobile gateway (`ftgo-mobile-gateway`, port 8090)

- `GET /mobile/orders/{orderId}` — hand-written reactive composition handler
  (`OrderDetailsHandler`, `RouterFunction`-style or `@RestController` with
  `Mono`), calling order-service, kitchen-service, delivery-service, and
  accounting-service concurrently via reactive `WebClient` calls combined
  with `Mono.zip(...)`, mirroring the book's Listing 8.1 example but done
  reactively rather than sequentially. Each call wrapped in a Resilience4j
  reactive circuit breaker (`reactor.circuitbreaker.operator`), returning a
  partial/degraded section on failure rather than failing the whole
  request — same graceful-degradation intent as Ch.7's `SectionResult`,
  reimplemented for the reactive stack since Spring Cloud Gateway can't
  reuse order-service's servlet-based `SectionResult` code directly.
- Pure routing passthroughs (Spring Cloud Gateway `RouteLocator` config,
  no custom code) for: `POST /mobile/orders` → order-service `POST /orders`,
  `POST /mobile/orders/{id}/cancel` → order-service, `POST
  /mobile/orders/{id}/revise` → order-service.
- Rate limiting: looser (e.g. 20 req/s per API key) — mobile client.
- Auth filter + logging filter applied globally.

### Public gateway (`ftgo-public-gateway`, port 8091)

- Pure request routing only, no composition — `RouteLocator` config
  mapping a versioned path prefix `/api/v1/...` to each backend service's
  existing REST endpoints (orders, tickets, deliveries, authorizations,
  restaurants, order-views). Third-party developers get the full,
  fine-grained API surface rather than a composed view.
- Rate limiting: stricter (e.g. 5 req/s per API key) — public-facing.
- Auth filter + logging filter applied globally.

## Data flow

```
Mobile client --> ftgo-mobile-gateway --> {order,kitchen,delivery,accounting}-service (composed)
                                       \-> order-service (routed passthrough)

3rd-party app --> ftgo-public-gateway --> any backend service (routed passthrough only)
```

Both gateways discover backend instances via Eureka, same as order-service's
existing `@LoadBalanced RestClient` pattern, adapted to Spring Cloud
Gateway's reactive `lb://` URI scheme.

## Error handling

- Composition failures in the mobile gateway degrade per-section (circuit
  breaker open or timeout → that section omitted/marked unavailable in the
  response), not a total 500 — consistent with Ch.7's precedent.
- Missing/invalid API key → 401 from the shared auth filter, before routing.
- Rate limit exceeded → 429 from the shared rate-limit filter.
- Routing to a service Eureka can't resolve → Spring Cloud Gateway's default
  503, unchanged (no custom handling needed — Ch.7 already established this
  is acceptable for a discovery failure).

## Testing

- `ftgo-gateway-common`: unit tests for `RequestLoggingFilter`,
  `ApiKeyAuthFilter`, and the custom rate-limiter filter (WebFlux
  `WebTestClient` against a minimal test route).
- `ftgo-mobile-gateway`: unit test for the composition handler (mocked
  `WebClient` responses via `MockWebServer` or stubbed `ExchangeFunction`),
  covering happy path and one degraded-section case.
- `ftgo-public-gateway`: minimal routing tests (config-driven, so mostly
  smoke-level).
- Docker e2e verification (both gateways, full stack): mobile gateway's
  composed order-details happy path and one degraded-section case (stop a
  backend container, confirm partial response); public gateway routing to
  at least 3 backend services; auth filter rejecting a bad/missing key on
  both gateways; rate limiting tripping under burst load on both gateways.

## Documentation sweep (chapter-completion rule)

- `docs/ARCHITECTURE.md` — new "API Gateway / BFF" section: gateway
  ownership model, routing table, composition sequence diagram for the
  mobile gateway's order-details endpoint, contrast table against Ch.7's
  service-level API composition (who composes, why two layers exist).
- New `ftgo-mobile-gateway/README.md`, `ftgo-public-gateway/README.md`,
  `ftgo-gateway-common/README.md` (or a shared "gateway" section if that
  reads better once written).
- `CONTEXT.md`: services-to-build table (replace the `ftgo-api-gateway`
  stub row with the two new gateway rows + `ftgo-gateway-common`), patterns
  reference, book-progress table (Ch.8 → Done), Concept understanding
  section (API gateway vs. BFF, why gateway-level composition differs from
  service-level composition), session log entry.
- Root `README.md` — service list, tech stack if Spring Cloud Gateway/
  reactive stack is newly introduced.

## Open questions resolved during brainstorming

- BFF split chosen: mobile vs. public (book's own FTGO example), not
  consumer vs. restaurant/admin.
- Gateway tech: Spring Cloud Gateway (reactive), not plain Spring MVC —
  matches book fidelity over intra-project consistency, an explicit
  trade-off accepted for this sub-project.
- Edge functions: logging + rate limiting + a stub auth filter (no real
  identity service).
- Composition ownership: the mobile gateway does its own fan-out
  composition rather than delegating to order-service's existing
  `/orders/{id}/view` — deliberately duplicates the *pattern* the book's
  Listing 8.1 shows, not the code, to see gateway-level composition
  hands-on.
