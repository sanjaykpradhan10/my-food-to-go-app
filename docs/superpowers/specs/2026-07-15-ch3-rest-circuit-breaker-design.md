# Ch. 3 IPC — Synchronous REST call with Circuit Breaker

**Date:** 2026-07-15
**Context:** Microservices Patterns (Chris Richardson), Chapter 3 — Interprocess communication. Reading complete (medium confidence); this is the "Implementing" step before moving to Ch. 4 (sagas).

## Goal

Implement the first IPC pattern from Ch. 3 in the FTGO app: a synchronous REST call from `order-service` to `restaurant-service`, protected by a circuit breaker. This anchors the chapter's RPI (Remote Procedure Invocation) pattern and the resilience concerns that motivate it, before layering on messaging/sagas in Ch. 4.

## Scope

In scope:
- Minimal `Restaurant` + `MenuItem` domain model in `restaurant-service`, persisted to MySQL via JPA, seeded with sample data.
- `GET /restaurants/{id}` endpoint on `restaurant-service`.
- Minimal in-memory `Order` domain object in `order-service` (no persistence — that's deferred to the saga/transactional work in Ch. 4).
- `POST /orders` endpoint on `order-service` that validates the restaurant and menu items exist by calling `restaurant-service` synchronously.
- Spring `RestClient` as the HTTP client, wrapped in a Resilience4j `@CircuitBreaker`.
- Distinct error handling for "not found" (business error) vs "circuit open / unreachable" (infrastructure error) vs "malformed request".
- Unit tests (fake port), HTTP-stub tests (WireMock/MockWebServer) including a forced circuit-open scenario, and JPA/controller tests on the restaurant-service side.
- Manual end-to-end verification via docker compose + curl, including stopping restaurant-service to observe the breaker trip.

Out of scope (deferred to later chapters):
- Order persistence (Ch. 4 sagas will introduce this properly alongside transactional consistency).
- Messaging/Kafka-based communication (separate Ch. 3 pattern, not this pass).
- Transactional outbox / transaction log tailing (separate Ch. 3 pattern, not this pass).
- Service discovery (single local instance per service; not needed yet).
- kitchen/delivery/accounting/consumer services (untouched in this pass).

## Architecture

- **restaurant-service**: owns `Restaurant` and `MenuItem` as its domain model, persisted to its own MySQL schema via JPA. Exposes a read endpoint for order-service to consult.
- **order-service**: owns a minimal `Order` domain object. Uses a hexagonal outbound port (`RestaurantServicePort`) with an adapter (`RestaurantServiceProxy`) that calls restaurant-service over HTTP. The circuit breaker lives in this adapter, not in the domain layer — the domain only sees a port interface and a small set of outcomes (found / not found / unavailable).

## Components

### restaurant-service
- `domain`: `Restaurant(id, name)`, `MenuItem(id, name, price)`
- `infrastructure`: JPA entities + repositories; seed data (1-2 restaurants with menu items) via `data.sql` or a `CommandLineRunner`
- `api` (inbound adapter): `RestaurantController` → `GET /restaurants/{id}` → `{id, name, menuItems:[{id,name,price}]}`, 404 if not found

### order-service
- `domain`: `Order(id, restaurantId, lineItems: List<OrderLineItem{menuItemId, quantity}>, status)` — in-memory only
- `domain` port: `RestaurantServicePort { findRestaurant(restaurantId): RestaurantInfo }`
- `infrastructure` (outbound adapter): `RestaurantServiceProxy implements RestaurantServicePort` — Spring `RestClient` call to `GET {restaurant-service}/restaurants/{id}`, annotated `@CircuitBreaker(name="restaurantService", fallbackMethod=...)`. Fallback distinguishes "service unavailable" from a normal 404.
- `api` (inbound adapter): `OrderController` → `POST /orders {restaurantId, lineItems}` → domain service validates restaurant + each menu item ID via the port → 201 with created `Order`, or 4xx/503 on failure

### Data flow
Client → `OrderController` → `OrderService.createOrder()` → `RestaurantServicePort.findRestaurant()` → `RestaurantServiceProxy` (RestClient + circuit breaker) → HTTP GET → restaurant-service → `RestaurantController` → JPA lookup → response bubbles back → order accepted/rejected.

## Error handling & config

- `404` — restaurant or menu item not found (business error, not a circuit-breaker concern)
- `503` — circuit open / restaurant-service unreachable (infrastructure error)
- `400` — malformed request
- Resilience4j config in `application.yml`: small sliding window (e.g. size 5), failure-rate threshold (e.g. 50%), short wait-duration-in-open-state (e.g. 5s) — tuned so the open state is reachable and observable in a local demo.
- `RestClient` connect/read timeouts set short enough that a hung restaurant-service doesn't hang order-service indefinitely — this is the concrete motivation for the pattern.

## Testing

- restaurant-service: `@WebMvcTest` for the controller, `@DataJpaTest` for the repository, using the existing H2 (MODE=MySQL) test setup.
- order-service: unit test against a fake `RestaurantServicePort` (no HTTP); a WireMock/MockWebServer-backed test exercising `RestaurantServiceProxy` against a real HTTP stub, including a scenario that forces the circuit breaker open.
- Manual end-to-end: `docker compose up` (MySQL) + both services running, `curl POST /orders` against a live restaurant-service, then stop restaurant-service and repeat to observe the circuit-breaker fallback.

## Decisions made during design

- REST client: Spring `RestClient` (built into Spring Boot 3.2+/6.1+, blocking by design) over OpenFeign (extra Spring Cloud dependency, more magic) or WebClient-blocking (wrong stack for a purely synchronous use case).
- Domain scope: minimal — Order stays in-memory for this pass; full persistence and cross-service consistency is Ch. 4's job (sagas).
- Circuit breaker included in this same pass rather than deferred, since Ch. 3 treats RPI and its resilience concerns as one unit.
