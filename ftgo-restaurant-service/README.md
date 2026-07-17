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
