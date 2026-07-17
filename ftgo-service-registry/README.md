# ftgo-service-registry

**Port:** 8761
**Bounded context:** N/A — infrastructure service, not a business bounded context

## Role

A standalone Eureka server, enabling client-side service discovery (Ch. 3 pattern). **restaurant-service** registers itself here on startup (its `eureka.client` config has no `register-with-eureka` override, so it defaults to `true`). **order-service** only queries the registry — its config sets `register-with-eureka: false` — resolving `restaurant-service`'s address dynamically via a `@LoadBalanced RestClient` (Spring Cloud LoadBalancer) instead of a hardcoded URL. `fetch-registry`/`register-with-eureka` are both `false` on the registry itself, since it doesn't need to discover or register with anything.

## API

The standard Eureka dashboard and REST API (no custom application code) — viewable at http://localhost:8761 when running.

## Events

None.

## Domain model

None — this is pure infrastructure, no business entities.

## Running standalone

```bash
./gradlew :ftgo-service-registry:test
```

Has no database dependency (`DataSourceAutoConfiguration`/`HibernateJpaAutoConfiguration` are explicitly excluded), so it can run standalone without the rest of the Docker Compose stack.
