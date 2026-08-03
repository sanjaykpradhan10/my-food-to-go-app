# ftgo-public-gateway

**Port:** 8091
**Type:** Spring Cloud Gateway (reactive), one of two BFF-style gateways from Ch.8

## Role

The public/3rd-party-facing gateway from the book's Backends for Frontends pattern — owned, per the book's ownership diagram, by whatever team is responsible for the external/partner-facing API, as a separate concern from the mobile team's own gateway (`ftgo-mobile-gateway`). This gateway is deliberately the simpler of the two: pure Spring Cloud Gateway routing (`RouteLocator`/YAML routes only), no hand-written composition endpoint of any kind — every route is a thin, uniform passthrough under `/api/v1/...` to exactly one backend service.

## Routes

All six routes rewrite `/api/v1/<resource>/**` to `/<resource>**` on the target service (resolved via Eureka, `lb://` scheme) and apply the shared `PerKeyRateLimiter` filter (from `ftgo-gateway-common`) at 5 requests/second per API key:

| Route id | Path | Rewritten to | Backend |
|---|---|---|---|
| `public-orders` | `/api/v1/orders/**` | `/orders**` | `ftgo-order-service` |
| `public-tickets` | `/api/v1/tickets/**` | `/tickets**` | `ftgo-kitchen-service` |
| `public-authorizations` | `/api/v1/authorizations/**` | `/authorizations**` | `ftgo-accounting-service` |
| `public-deliveries` | `/api/v1/deliveries/**` | `/deliveries**` | `ftgo-delivery-service` |
| `public-order-views` | `/api/v1/order-views/**` | `/order-views**` | `ftgo-order-history-service` |
| `public-restaurants` | `/api/v1/restaurants/**` | `/restaurants**` | `ftgo-restaurant-service` |

No route composes across services — every request this gateway handles is answered entirely by whichever single backend service its path routes to. Callers needing a composed multi-service view should use the mobile gateway's `GET /mobile/orders/{orderId}` instead (a deliberately different concern for a deliberately different caller — see `docs/ARCHITECTURE.md`'s BFF section).

## Edge functions

Applied via `ftgo-gateway-common`'s auto-configuration (see that module's own README for full detail):

- **Request logging** (`RequestLoggingFilter`) — every request, all six routes.
- **API-key auth** (`ApiKeyAuthFilter`) — this gateway's own key is `public-dev-key` (`gateway.api-key.value`), independent of the mobile gateway's key. Missing/wrong `X-Api-Key` header → `401`.
- **Per-key rate limiting** (`PerKeyRateLimiter`) — 5 req/s per API key, applied identically to all six routes (a public/3rd-party-facing gateway is deliberately throttled tighter than the mobile gateway's 20 req/s, since it's the surface exposed to less-trusted external callers).

## Dependencies

`spring-cloud-starter-gateway`, `spring-cloud-starter-netflix-eureka-client` (to resolve `lb://ftgo-*-service` URIs dynamically), `ftgo-gateway-common`.

## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only. Reports
`discoveryComposite` (Eureka registration status) — no `db`/`kafka` components, since gateways
have neither of their own. `management.endpoint.health.show-details: always` for the same reason
given in the business services' READMEs. Verified against the real, running stack by
`ftgo-end-to-end-test`'s `AllServicesReportHealthy.feature`.

## Running standalone

```bash
./gradlew :ftgo-public-gateway:test
```

Needs the full docker-compose stack (Eureka registry plus every routed-to backend service registered and running) to exercise live — see the root [README](../README.md) for `docker compose up`. Example live call:

```bash
curl -H "X-Api-Key: public-dev-key" http://localhost:8091/api/v1/restaurants/1
```

Key environment variables (see `application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE` | `http://localhost:8761/eureka/` | Eureka registry URL (note the exact casing — `SERVICE_URL`, not `SERVICEURL`) |
| `SERVER_PORT` | `8091` | HTTP port |

No `gateway.api-key.value` env override is wired in `compose.yml` — the key is fixed at `public-dev-key` via `application.yml` for this learning project, not sourced from a secret store.
