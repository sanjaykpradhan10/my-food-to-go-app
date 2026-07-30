# ftgo-mobile-gateway

**Port:** 8090
**Type:** Spring Cloud Gateway (reactive), one of two BFF-style gateways from Ch.8

## Role

The mobile-client-facing gateway from the book's Backends for Frontends pattern — owned, per the book's ownership diagram, by the mobile team, separately from the public/3rd-party team's own gateway (`ftgo-public-gateway`). Unlike the public gateway, this one is not purely routing: alongside three thin order-mutation passthroughs, it owns one hand-written composition endpoint (`GET /mobile/orders/{orderId}`) assembling a single mobile-shaped response from four backend services.

## Routes (declared Gateway routes)

Applied to all three: `PerKeyRateLimiter` at 20 req/s per API key (via `ftgo-gateway-common`).

| Route id | Path | Rewritten to | Backend |
|---|---|---|---|
| `mobile-create-order` | `POST /mobile/orders` | `POST /orders` | `ftgo-order-service` |
| `mobile-cancel-order` | `POST /mobile/orders/{id}/cancel` | `POST /orders/{id}/cancel` | `ftgo-order-service` |
| `mobile-revise-order` | `POST /mobile/orders/{id}/revise` | `POST /orders/{id}/revise` | `ftgo-order-service` |

## Composed endpoint: `GET /mobile/orders/{orderId}`

**Not a declared Gateway route** — implemented as a hand-written WebFlux `RouterFunction` (`OrderDetailsRouterConfig`/`OrderDetailsHandler`/`OrderDetailsHandler.fetchOrderDetails`), dispatched via Spring WebFlux's own `RouterFunctionMapping`. This distinction matters: Spring Cloud Gateway's `GlobalFilter`s only run for requests matched by a `RouteLocator`, so this endpoint is **not** covered by `RequestLoggingFilter` or `PerKeyRateLimiter` at all — see `docs/ARCHITECTURE.md`'s dedicated callout for the full explanation and the known follow-up gap this leaves (no logging, no rate limiting on this one endpoint).

Fans out in parallel (`Mono.zip`) to four backends, each call wrapped in its own `ReactiveCircuitBreaker` (2s timeout):

| Section | Backend call | Circuit breaker instance |
|---|---|---|
| `order` | `GET /orders/{orderId}` on order-service | `orderService` |
| `ticket` | `GET /tickets/order/{orderId}` on kitchen-service | `kitchenService` |
| `authorization` | `GET /authorizations/order/{orderId}` on accounting-service | `accountingService` |
| `delivery` | `GET /deliveries/order/{orderId}` on delivery-service | `deliveryService` |

Each section resolves to a `SectionResult<String>` (`Found`/`NotFound`/`Unavailable` — sealed interface with a `@JsonTypeInfo`/`@JsonSubTypes` `status` discriminator so the three states serialize to genuinely distinguishable JSON, not identical `{}` bodies for `NotFound` and `Unavailable`). A backend `404` maps to `NotFound`; any other failure (timeout past 2s, connection refused, open circuit) maps to `Unavailable`. The endpoint always returns `200` with whatever mix of the three the four calls produced.

**Auth on this endpoint**: since Gateway's own `ApiKeyAuthFilter` never runs here, `OrderDetailsRouterConfig` wraps the `RouterFunction` with its own inline `.filter(...)` replicating that filter's exact logic (checks `X-Api-Key` against the same `GatewayApiKeyProperties` bean the declared routes use) — `401` on missing/wrong key.

**Contrast with Ch.7's `GET /orders/{id}/view`** (order-service's own API-composition endpoint): this endpoint composes independently rather than delegating to it — see `docs/ARCHITECTURE.md`'s side-by-side comparison table for why (decoupling the mobile gateway's view from order-service's own view endpoint, different concurrency mechanisms, different `SectionResult` implementations for the servlet vs. reactive stacks).

## Edge functions

Applied via `ftgo-gateway-common`'s auto-configuration, but **only to the three declared routes above** — not to the composed endpoint (see callout above):

- **Request logging** (`RequestLoggingFilter`)
- **API-key auth** (`ApiKeyAuthFilter`) — this gateway's own key is `mobile-dev-key`, independent of the public gateway's key
- **Per-key rate limiting** (`PerKeyRateLimiter`) — 20 req/s per API key (looser than the public gateway's 5 req/s, since mobile clients are a more trusted, first-party caller)

## Dependencies

`spring-cloud-starter-gateway`, `spring-cloud-starter-netflix-eureka-client`, `spring-cloud-starter-loadbalancer` (`@LoadBalanced WebClient.Builder`, so the composed endpoint's backend calls resolve `http://ftgo-*-service`-style authority-only URIs via Eureka, matching order-service's existing Ch.7 composition pattern), `spring-cloud-starter-circuitbreaker-reactor-resilience4j`, `ftgo-gateway-common`.

## Running standalone

```bash
./gradlew :ftgo-mobile-gateway:test
```

Needs the full docker-compose stack (Eureka registry, order-service, kitchen-service, accounting-service, delivery-service all registered and running) to exercise live — see the root [README](../README.md) for `docker compose up`. Example live calls:

```bash
curl -X POST -H "X-Api-Key: mobile-dev-key" -H "Content-Type: application/json" \
  -d '{"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":101,"quantity":2}]}' \
  http://localhost:8090/mobile/orders

curl -H "X-Api-Key: mobile-dev-key" http://localhost:8090/mobile/orders/1
```

Key environment variables (see `application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE` | `http://localhost:8761/eureka/` | Eureka registry URL (note the exact casing — `SERVICE_URL`, not `SERVICEURL`) |
| `SERVER_PORT` | `8090` | HTTP port |

Resilience4j circuit breaker settings (`orderService`/`kitchenService`/`accountingService`/`deliveryService`, all identical): sliding window size 5, failure-rate threshold 50%, 5s wait-duration-in-open-state.
