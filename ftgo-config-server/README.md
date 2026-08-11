# ftgo-config-server

**Port:** 8888
**Bounded context:** none — a cross-cutting infrastructure service (Ch.11, §11.2), not a business service.

## Role

A **Spring Cloud Config Server** backed by this repository's own `config-repo/` directory, serving externalized configuration to all 10 services (8 business services + 2 gateways) at startup and on-demand via REST. Centralized configuration management enables live refresh of a subset of properties (outbox polling interval) without restarting any service.

## Configuration sources

The config server uses `search-paths: config-repo` and reads from two property files:

- `config-repo/application.yml` — **shared defaults** consumed by all 10 services.
- `config-repo/ftgo-<service>.yml` — **per-service overrides** for the 5 outbox-publishing services (order, kitchen, accounting, delivery, consumer). The other 5 services (restaurant, order-history, audit-log, mobile-gateway, public-gateway) have no per-service file and use shared defaults only.

## Querying the config server

Fetch a service's resolved configuration (merged defaults + per-service overrides) via:

```bash
curl http://localhost:8888/ftgo-order-service/default
```

Response is a JSON object with a `propertySources` array, each entry carrying a `source` map of key-value pairs. The git backend's commit SHA is also included (`version`), useful for audit trails.

Example query for kitchen-service:
```bash
curl http://localhost:8888/ftgo-kitchen-service/default | jq '.propertySources[].source | keys[]'
```

## Non-blocking startup contract

Every service sets `spring.cloud.config.fail-fast: false`, so if the config server is unreachable at startup, the service still starts using its local `application.yml` values as fallback — silently, no error logged. This is the **"optional"** non-blocking contract: the service begins operation immediately and deduces configuration from local defaults, not waiting for remote config to arrive. In Docker Compose, this is enforced by `depends_on: config-server: condition: service_started` (non-blocking) rather than `condition: service_healthy` (blocking).

## Infrastructure dependencies

The config server is a **leaf service** — it has no `depends_on` of its own in `compose.yml` and does not depend on MySQL, Kafka, Eureka, or any other service. It only needs the git repository it's pointing at, which is this repository itself. As a result, it can start immediately and is available early in the boot sequence, minimizing the risk that a service starts before config-server is ready.

## Running standalone

```bash
./gradlew :ftgo-config-server:test
```

To run live, start the full stack (`docker compose up -d`) — every service that uses Spring Cloud Config's `optional:configserver` import (all 10) will try to reach this service at startup and use its values to override local `application.yml` defaults.
