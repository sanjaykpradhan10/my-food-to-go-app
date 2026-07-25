# Design: API composition — `GET /orders/{id}/view` (Ch.7, sub-project 2 of 3)

**Date**: 2026-07-24
**Status**: Approved

## Goal

Implement the book's API composition pattern (Ch.7) as a real composite query on `order-service`: `GET /orders/{id}/view` assembles a single response from `Order` (local) plus `Restaurant`, `Ticket`, `Authorization`, and `Delivery` (all fetched synchronously from their owning services). This is the first read-side query pattern in this project — everything built so far has been command/event-driven. Sub-project 1 (Delivery aggregate + saga participation, merged in PR #16) is a direct prerequisite: without it, the composite view would have no delivery data to show.

**Deferred to sub-project 3**: CQRS read model — a separate future session.

## Scope decisions made during brainstorming

1. **Lives on `order-service`, not a dedicated composer service.** Matches the project's existing precedent — `order-service` already does a synchronous, circuit-breaker-wrapped REST call to `restaurant-service` (`RestaurantServiceProxy`) for order creation. This sub-project extends that exact pattern to 3 more downstream calls rather than introducing a new service ahead of Ch.8's API gateway.
2. **`kitchen-service`, `accounting-service`, and `delivery-service` all gain Eureka registration.** None of the three currently register with the service registry (only `restaurant-service` and `order-service` use Eureka today). This is a real, deliberate scope expansion — chosen over hardcoded/config-based URLs so the whole app's synchronous-call story stays architecturally consistent (every downstream `order-service` calls is discovered the same way), not because this sub-project needs discovery's dynamic-reconfiguration properties specifically.
3. **Each of `kitchen-service`, `accounting-service`, `delivery-service` gains its first (or first-ever, for accounting) read endpoint**: `GET /tickets/order/{orderId}`, `GET /authorizations/order/{orderId}`, `GET /deliveries/order/{orderId}` — 404 if no such aggregate exists yet for that order. Minimal, single-purpose additions exposing what each service's `findByOrderId` repository method already supports internally.
4. **Downstream "not found" is not an error.** A 404 from any of the 3 new endpoints (or from `restaurant-service`, though that's an existing, always-populated relationship) means "this section legitimately doesn't exist yet" — e.g. `Authorization` never gets created if the order was rejected before accounting ran, or `Ticket` doesn't exist for a not-yet-processed order. This is distinguished from a genuine downstream *outage* (circuit breaker open, timeout): the composite response's per-section result type carries three states — found, not-found, and unavailable — rather than collapsing "no data" and "can't reach the service" into one null.
5. **Each downstream call gets its own circuit breaker**, matching `RestaurantServiceProxy`'s existing Resilience4j configuration exactly (2s connect/read timeout, sliding-window-size 5, 50% failure threshold, 5s open-state wait, 3 permitted half-open calls). A slow/down `kitchen-service` degrades only the ticket section to `Unavailable`, not the whole request.
6. **The 4 downstream calls (restaurant, ticket, authorization, delivery) run in parallel**, via `CompletableFuture.supplyAsync(...)` on a shared `Executors.newVirtualThreadPerTaskExecutor()` bean. Chosen over sequential calls (lower latency for a 4-fan-out read) and over a manually-sized fixed thread pool (Java 21 virtual threads are a natural fit for this many short-lived, blocking I/O calls with no pool-size tuning decision to make or justify).
7. **`GET /orders/{id}/view` itself 404s only if the order doesn't exist.** Every other section (restaurant/ticket/authorization/delivery) independently degrades to `NotFound`/`Unavailable` inside a 200 response — the endpoint never fails outright just because one downstream section is missing or unreachable.

## New read endpoints

**`kitchen-service`**: `GET /tickets/order/{orderId}` → `TicketInfo(Long id, Long orderId, String status, ZonedDateTime readyBy)` or 404. New `TicketNotFoundException`-style 404 handling on `TicketController`, reusing `TicketRepository.findByOrderId` (already exists, used internally by `TicketService`).

**`accounting-service`**: `GET /authorizations/order/{orderId}` → `AuthorizationInfo(Long id, Long orderId, String status)` or 404. This is `accounting-service`'s first-ever REST controller (`api` package is currently empty) — a new `AuthorizationController`, reusing `AuthorizationRepository.findByOrderId` (already exists).

**`delivery-service`**: `GET /deliveries/order/{orderId}` → `DeliveryInfo(Long id, Long orderId, String status, Long courierId)` or 404. New endpoint added to the existing `DeliveryController`, reusing `DeliveryRepository.findByOrderId` (already exists — sub-project 1 deliberately deferred any `GET` endpoint here; this sub-project is where it lands).

All three follow the same pattern: `@GetMapping("/{resource}/order/{orderId}")`, `findByOrderId(orderId).map(this::toInfo).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build())` — no new exception types needed beyond what a simple `Optional`-to-404 mapping requires.

## Service discovery additions

`kitchen-service`, `accounting-service`, `delivery-service` each gain:
- The `spring-cloud-starter-netflix-eureka-client` dependency (already used by `restaurant-service`/`order-service`).
- `eureka.client.service-url.defaultZone` + `eureka.instance.prefer-ip-address: true` in `application.yml`, matching `restaurant-service`'s existing config exactly.
- Docker Compose: each service's container gains `EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE` and a `depends_on: service-registry: condition: service_started` entry, matching `restaurant-service`'s existing block.

Each of these 3 services *registers* with Eureka (so `order-service` can discover them) but does not itself need to *discover* anyone else — no new `RestClientConfig`/`@LoadBalanced` beans needed in kitchen/accounting/delivery-service themselves, only in `order-service`.

## `order-service` additions

**Three new ports/proxies**, each mirroring `RestaurantServicePort`/`RestaurantServiceProxy` exactly in structure (interface + `@Component` implementation + dedicated `RestClient` bean + named Resilience4j circuit breaker instance):

- `KitchenServicePort.findTicket(Long orderId): SectionResult<TicketInfo>` / `KitchenServiceProxy`
- `AccountingServicePort.findAuthorization(Long orderId): SectionResult<AuthorizationInfo>` / `AccountingServiceProxy`
- `DeliveryServicePort.findDelivery(Long orderId): SectionResult<DeliveryInfo>` / `DeliveryServiceProxy`

Unlike `RestaurantServicePort.findRestaurant` (which throws `RestaurantNotFoundException` on 404, since a missing restaurant during order *creation* is a real error), these three ports return a `SectionResult<T>` directly rather than throwing on 404 — a missing ticket/authorization/delivery during a *read* is an expected, common state (saga still in progress), not exceptional. Each proxy's `@CircuitBreaker` fallback method catches the 404 case (mapped to `NotFound<>()`) and any other throwable (mapped to `Unavailable<>(reason)`), so no exception ever escapes the proxy layer.

**`RestaurantServiceProxy` itself is NOT changed** — `findRestaurant` keeps its existing throw-on-404 contract for order creation's use of it. The composite query instead gets a new, separate `findRestaurantForView(Long restaurantId): SectionResult<RestaurantInfo>` method added to `RestaurantServicePort`/`RestaurantServiceProxy`, reusing the same underlying `RestClient` bean and circuit breaker instance, but with `SectionResult`-shaped (non-throwing) semantics — consistent with the other three ports.

**Response records** (new package `com.sanjay.ftgo.order.api.view`, or alongside existing `api` DTOs — implementer's call during planning):

```java
public sealed interface SectionResult<T> permits Found, NotFound, Unavailable {}
public record Found<T>(T data) implements SectionResult<T> {}
public record NotFound<T>() implements SectionResult<T> {}
public record Unavailable<T>(String reason) implements SectionResult<T> {}

public record TicketInfo(Long id, Long orderId, String status, ZonedDateTime readyBy) {}
public record AuthorizationInfo(Long id, Long orderId, String status) {}
public record DeliveryInfo(Long id, Long orderId, String status, Long courierId) {}
// RestaurantInfo already exists (order.domain), reused as-is

public record OrderSummary(Long id, String status, Long consumerId, Long restaurantId, List<LineItemView> lineItems) {
    public record LineItemView(Long menuItemId, int quantity) {}
}

public record OrderViewResponse(
        OrderSummary order,
        SectionResult<RestaurantInfo> restaurant,
        SectionResult<TicketInfo> ticket,
        SectionResult<AuthorizationInfo> authorization,
        SectionResult<DeliveryInfo> delivery) {}
```

**New `OrderViewController`** (or a new method on the existing `OrderController` — implementer's call during planning, leaning toward a separate controller to keep the write-side `OrderController` unchanged): `GET /orders/{id}/view`. Loads `Order` via the existing `OrderRepository`, 404s if not found (existing `OrderNotFoundException` pattern). Otherwise fires all 4 proxy calls via `CompletableFuture.supplyAsync(..., virtualThreadExecutor)`, `CompletableFuture.allOf(...).join()`s them, and assembles `OrderViewResponse`.

**New `VirtualThreadExecutorConfig`**: a single `@Bean ExecutorService orderViewExecutor()` returning `Executors.newVirtualThreadPerTaskExecutor()`, injected into `OrderViewController`.

## Data model

No schema changes anywhere — every new endpoint reads from an already-persisted aggregate via an already-existing repository method (`findByOrderId`). No new tables, no new columns.

## Testing

TDD, per this project's established convention:
- `TicketController`/`AuthorizationController`/`DeliveryController`: `@WebMvcTest`s for the 3 new `GET .../order/{orderId}` endpoints, found and 404 cases.
- `KitchenServiceProxy`/`AccountingServiceProxy`/`DeliveryServiceProxy`/`RestaurantServiceProxy.findRestaurantForView`: unit tests covering found (200), not-found (404 → `NotFound<>()`), and unavailable (circuit breaker open / timeout → `Unavailable<>(reason)`) — mirroring whatever test structure `RestaurantServiceProxy`'s existing test (if any) already uses for the throw-on-404 case, adapted for the new non-throwing contract.
- `OrderViewController`: unit tests with all 4 ports mocked, covering all-found, all-not-found (order created but nothing downstream has processed it yet), partial-unavailable (one port throws/circuit-breaks), and order-not-found (404) cases.
- Manual Docker e2e: call `GET /orders/{id}/view` at several points in an order's real lifecycle (immediately after creation before any saga step lands, mid-saga, full happy-path completion, after a decline/compensation) and verify each section's state matches the aggregate's real state at that moment; stop one downstream container (e.g. `docker compose stop kitchen-service`) mid-test and verify only the ticket section degrades to `Unavailable` while the rest of the response still returns normally.

## Docs

Per-change, landing in the same PR:
- `ftgo-order-service/README.md` — new `GET /orders/{id}/view` endpoint, response shape, the 3 new proxy dependencies.
- `ftgo-kitchen-service/README.md`, `ftgo-accounting-service/README.md`, `ftgo-delivery-service/README.md` — new read endpoint each, new Eureka registration.
- `docs/ARCHITECTURE.md` — new "API composition" section (this project's first query pattern), documenting the parallel-fan-out/circuit-breaker/`SectionResult` approach; service discovery diagram/table updated to include kitchen/accounting/delivery-service as registered instances.
- `CONTEXT.md` — services table updates, session log entry, "Querying" section of the patterns reference gets `[x] API composition (Ch.7)` checked off (CQRS remains unchecked until sub-project 3).

## Deferred (not in this pass)

- **Sub-project 3**: CQRS read model — a dedicated read-side service/table fed by Kafka events from all five services, a separate future session.
- Pagination, filtering, or any query beyond single-order-by-id — out of scope, not this sub-project's teaching point.
- Caching the composite response — the book's API composition pattern doesn't require it, and this project has no caching infrastructure yet.
