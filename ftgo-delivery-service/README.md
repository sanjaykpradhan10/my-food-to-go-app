# ftgo-delivery-service

**Port:** 8086
**Bounded context:** Delivery tracking (separate bounded context from Order — Ubiquitous Language: Delivery, not Order, per Ch.2 decomposition)

## Status: stub

This service is scaffolded (Spring Boot skeleton, package structure for hexagonal architecture — empty `api/`, `domain/`, `infrastructure/` packages) but has no real implementation yet. It's identified in the Ch.2 capability mapping as a distinct bounded context from Order, but hasn't been built out — no chapter so far has driven its implementation the way Ch.3/4 drove order/kitchen/consumer/accounting.

## Role (planned)

Once a relevant chapter requires it, this service will own delivery assignment and courier tracking — accepting a confirmed order for delivery, assigning a courier, and tracking pickup/drop-off status, as its own aggregate (`Delivery`) independent of `Order`.

## Running standalone

```bash
./gradlew :ftgo-delivery-service:test
```

Runs the generated `FtgoDeliveryServiceApplicationTests` context-load test — no real behavior to exercise yet.
