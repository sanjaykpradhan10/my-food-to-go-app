# Design: Client-Side Service Discovery (Ch. 3)

**Date**: 2026-07-15
**Status**: Approved

## Goal

Replace order-service's hardcoded `restaurant-service.base-url` config with
real client-side discovery via a Eureka registry: restaurant-service
registers itself on startup, order-service looks up healthy instances at
call time instead of relying on a fixed URL.

This closes the third of the four remaining Ch. 3 IPC patterns
(client-side discovery). Transaction log tailing (CDC) remains open — see
`CONTEXT.md` for tracking.

## Scope decisions

- **Why build this at all**: Docker Compose already gives DNS-based service
  resolution + load balancing across replicas for free, which is why
  order-service's hardcoded `restaurant-service:8085` URL already "works."
  This pass deliberately builds an explicit registry-based discovery
  pattern anyway, to learn the mechanics the book describes, independent
  of what the container platform would provide.
- **Client-side vs. server-side discovery**: client-side, via Netflix
  Eureka (the classic pairing for this book) — not a gateway/reverse
  proxy. Server-side discovery is deferred.
- **Registry**: a new module, `ftgo-service-registry`, running a
  standalone Eureka server.
- **Test strategy**: no live Eureka server in automated tests. Spring
  Cloud LoadBalancer's static "simple" discovery client is a drop-in
  swap for Eureka in the test profile, resolving the same logical service
  name to WireMock's fixed port. Production wires real Eureka; tests wire
  the simple discovery client. Existing `RestaurantServiceProxyTest` needs
  no code changes.
- **Docker Compose wiring is in scope for this pass** (not deferred) — the
  registry gets its own Dockerfile/compose entry now, so discovery is
  verified end-to-end across containers, not just on localhost. This
  mirrors the Kafka dual-listener lesson from the previous feature: an
  unverified container-networking assumption is exactly the kind of gap
  that silently breaks later.

## Architecture

```
┌───────────────────────┐         registers on startup      ┌────────────────────┐
│ ftgo-service-registry  │◀───────────────────────────────── │ restaurant-service │
│  (Eureka server,       │                                    └────────────────────┘
│   port 8761)           │
│                        │        looks up "ftgo-restaurant-service"
│                        │◀───────────────────────────────────┐
└───────────────────────┘                                    │
                                                    ┌────────────────────┐
                                                    │  order-service      │
                                                    │  RestaurantServiceProxy
                                                    │  (unchanged)        │
                                                    │  @LoadBalanced RestClient
                                                    │  → http://ftgo-restaurant-service
                                                    │  (resolved to a real host:port
                                                    │   by Spring Cloud LoadBalancer)
                                                    └────────────────────┘
```

`restaurant-service` becomes a Eureka client and registers itself under
its `spring.application.name` (`ftgo-restaurant-service`) on startup.
`order-service` also becomes a Eureka client, but instead of calling a
fixed URL, it builds its `RestClient` with `@LoadBalanced` and a base URL
of `http://ftgo-restaurant-service` — a *logical* service name, not a real
host. Spring Cloud LoadBalancer intercepts each call, asks the registry
which instances are currently registered under that name, and rewrites the
request to a real `host:port` before it goes out.

`RestaurantServiceProxy` — the class carrying the `@CircuitBreaker`
annotation — does not change. Only `RestClientConfig` (how the `RestClient`
bean is built) changes; the circuit breaker keeps wrapping the call exactly
as it does today, and if the registry has no instance to offer, that
failure flows into the same `findRestaurantFallback` path that already
handles `RestaurantServiceUnavailableException` — no new error-handling
code is needed.

This also removes the `RESTAURANT_SERVICE_BASE_URL` env-var override in
`compose.yml` introduced by the earlier Ch.3 REST work — discovery
replaces the need for it entirely.

## Module & dependency changes

### New module: `ftgo-service-registry`

- Added to `settings.gradle`, follows the same scaffold shape as other
  services (own `build.gradle`, `application.yml`, main class)
- Dependency: `org.springframework.cloud:spring-cloud-starter-netflix-eureka-server`,
  version managed via the Spring Cloud BOM `2025.0.0` (the release train
  confirmed compatible with Spring Boot 3.5.x)
- Main class gets `@EnableEurekaServer`
- `application.yml`: `server.port: 8761`; since this instance *is* the
  registry, it doesn't register with itself:
  `eureka.client.register-with-eureka: false`,
  `eureka.client.fetch-registry: false`

### `restaurant-service` (registers)

- Add dependency: `spring-cloud-starter-netflix-eureka-client` (same BOM)
- `application.yml` additions: `eureka.client.service-url.defaultZone:
  http://localhost:8761/eureka/` and `eureka.instance.prefer-ip-address:
  true` — without the latter, Eureka advertises the container's hostname,
  which isn't reliably reachable from other containers; advertising the
  container's actual IP instead is the standard fix (the same class of
  problem the Kafka dual-listener fix addressed for a different protocol)
- No code changes — registration is automatic once the client dependency +
  config are present

### `order-service` (discovers)

- Add dependencies: `spring-cloud-starter-netflix-eureka-client` +
  `spring-cloud-starter-loadbalancer` (same BOM)
- `RestClientConfig.java`: the `restaurantServiceRestClient` bean's
  `RestClient.Builder` gets `@LoadBalanced`, and `baseUrl` becomes the
  literal string `http://ftgo-restaurant-service` (the logical service ID)
  instead of the injected `restaurant-service.base-url` property. The
  existing 2s connect/read timeout settings are preserved.
- `application.yml`: remove `restaurant-service.base-url`; add the same
  `eureka.client.service-url.defaultZone` + `eureka.instance.prefer-ip-address:
  true` as restaurant-service (order-service must be a Eureka client to
  *look up* instances, even though nothing else registers it as a callable
  target)

### Test config (both services) — no real Eureka server in tests

- `spring.cloud.discovery.client.simple.instances.ftgo-restaurant-service[0].uri:
  http://localhost:8089` (order-service's test yml) — Spring Cloud
  LoadBalancer's static "simple" discovery client, a drop-in swap for
  Eureka that resolves the same logical name to WireMock's fixed port
- `eureka.client.enabled: false` in both test profiles, so the Eureka
  client starter doesn't try to actually register/fetch during tests
- `RestaurantServiceProxyTest` needs no code changes — it still calls
  `restaurantServiceProxy.findRestaurant(1L)` against WireMock on port
  8089, just resolved via the simple discovery client instead of a
  hardcoded base URL

## Docker Compose wiring

- New `service-registry` entry: build from `ftgo-service-registry/Dockerfile`
  (same multi-stage pattern as every other service), port `8761:8761`, no
  dependencies (it's the first thing other services need, but nothing
  blocks *it* from starting)
- `restaurant-service` gets `depends_on: service-registry: condition:
  service_started` and env var `EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE:
  http://service-registry:8761/eureka/`
- `order-service` gets the same `depends_on`/env var, and **loses** its
  `RESTAURANT_SERVICE_BASE_URL` env override — discovery replaces it
- `eureka.instance.prefer-ip-address: true` (set in `application.yml`, not
  compose) means both services advertise their real container IP
  regardless of environment, so no per-environment override is needed here
  the way Kafka needed dual listeners

## Testing & verification

- **Unit/integration tests**: `RestaurantServiceProxyTest` (existing,
  unchanged code) now runs against the simple discovery client instead of
  a static URL — proves the discovery-resolved call path works without
  needing a live registry in CI
- **New `ftgo-service-registry` module**: a minimal `contextLoads` smoke
  test (`@SpringBootTest`), matching how every other service started
  (restaurant-service and kitchen-service both began this way)
- **Manual e2e verification** via `docker compose up`:
  1. Confirm `service-registry` is reachable at `http://localhost:8761`
     and its dashboard/REST API (`GET /eureka/apps`) shows
     `FTGO-RESTAURANT-SERVICE` registered as `UP`
  2. `POST /orders` against order-service and confirm it still succeeds —
     proving the call was actually routed via discovery, not a leftover
     hardcoded URL
  3. Stop `restaurant-service` (`docker compose stop restaurant-service`),
     confirm the registry evicts it after its lease expires, and confirm
     order-service's circuit breaker still degrades gracefully (503)
     exactly as it did with the static-URL setup in the Ch.3 REST work
  4. Restart `restaurant-service`, confirm it re-registers and
     order-service resumes succeeding — without restarting order-service
     itself, proving discovery is dynamic

## Deferred (not in this pass)

- Server-side discovery / gateway pattern — the alternative approach, not
  needed alongside client-side discovery
- Multiple restaurant-service replicas / actual load balancing across >1
  instance — this pass proves discovery works with a single instance;
  scaling out is a natural follow-up but not required to demonstrate the
  pattern
- Transaction log tailing (CDC) — the other remaining Ch.3 pattern,
  tracked separately
