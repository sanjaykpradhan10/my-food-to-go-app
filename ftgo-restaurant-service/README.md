# ftgo-restaurant-service

**Port:** 8085
**Bounded context:** Restaurant/menu management

## Role

Owns the `Restaurant` and `MenuItem` aggregates — the catalog of restaurants and what they sell. It's a read-mostly service: order-service queries it synchronously to validate a restaurant and its menu items before creating an order. It has no write API of its own beyond startup seeding.

## API

`GET /restaurants/{id}`

Response:
```json
{
  "id": 1,
  "name": "Ajanta Indian Cuisine",
  "menuItems": [
    {"id": 1, "name": "Chicken Tikka Masala", "price": 14.99},
    {"id": 2, "name": "Garlic Naan", "price": 3.50}
  ]
}
```

Returns `404` (empty body) if no restaurant exists with that id.

`POST /restaurants`

Request:
```json
{
  "name": "Ajanta E2E",
  "menuItems": [
    {"name": "Chicken Vindaloo", "price": 12.00}
  ]
}
```

Response (`201`): same `RestaurantResponse` shape as `GET /restaurants/{id}` above, with server-generated ids (`GenerationType.IDENTITY`) for both the restaurant and its menu items. Added in Ch.10 sub-project 3 (end-to-end tests, §10.3) purely so the end-to-end test can create its own fixture data rather than depend on `DataSeeder`'s fixed seed ids — not exposed through either gateway (creating restaurants/menus isn't a public-facing operation in this application's design). `DataSeeder` is untouched and still separately seeds the two fixed restaurants below on every startup against an empty table.

## Health check (Ch.11, §11.3.1)

`GET /actuator/health` — Spring Boot Actuator, auto-configured indicators only (no custom
`HealthIndicator` code). Reports:
- `db` — MySQL reachability via the service's `DataSource`.
- `discoveryComposite` — Eureka registration status.

There is no `kafka` component: Spring Boot's actuator-autoconfigure no longer ships a Kafka
health contributor as of this project's Spring Boot version (3.5.16) — verified directly against
the built jars (`spring-boot-actuator-autoconfigure` retains only `KafkaMetricsAutoConfiguration`
under `actuate.autoconfigure.kafka`; `spring-kafka` ships no health-indicator class either) — and
adding a custom one is out of scope for this sub-project.

`management.endpoint.health.show-details: always` — safe here since these ports aren't exposed
to untrusted clients in this project; full component detail is the point of exercising this
pattern. Verified against the real, running stack by `ftgo-end-to-end-test`'s
`AllServicesReportHealthy.feature`.

## Events

None. This service doesn't produce or consume any Kafka events — it's reached only via synchronous REST (from order-service, via a circuit breaker).

## Service discovery

Registers itself with the Eureka registry (`ftgo-service-registry`) on startup, using `spring.application.name: ftgo-restaurant-service` as its registered identity. order-service discovers it dynamically through a `@LoadBalanced RestClient` resolved against the registry, rather than a hardcoded base URL. This matters because it's the Ch.3 client-side discovery pattern: the registry lookup happens fresh on every call, so instance changes (a restart, a scale-out) are picked up without restarting order-service.

## Domain model

`Restaurant` (id, name) has a `@OneToMany` to `MenuItem` (id, name, price), cascaded — deleting a restaurant deletes its menu items. Seed data (`DataSeeder`, runs once on an empty table) creates two restaurants:

| Restaurant | Menu items |
|---|---|
| Ajanta Indian Cuisine | Chicken Tikka Masala ($14.99), Garlic Naan ($3.50) |
| Pizza Palace | Margherita Pizza ($12.00), Pepperoni Pizza ($13.50) |

IDs are auto-increment, not fixed literals — if you're testing the saga manually via `curl`, always `GET /restaurants/1` first and read the actual `menuItems[].id` values rather than assuming `1`/`2`.

## Running standalone

```bash
./gradlew :ftgo-restaurant-service:test
```

Needs `docker compose up -d mysql service-registry` for a live run (`./gradlew :ftgo-restaurant-service:bootRun`) — tests use H2 in-memory and don't need Docker.
