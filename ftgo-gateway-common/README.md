# ftgo-gateway-common

**Type:** Shared library (WebFlux/Spring Cloud Gateway), not a runnable service — no `bootJar`, consumed via `implementation project(':ftgo-gateway-common')`

## Role

Both gateways introduced in Ch.8 (`ftgo-mobile-gateway`, `ftgo-public-gateway`) need the same three cross-cutting edge functions — request logging, API-key authentication, and per-key rate limiting. Rather than duplicate them, this module holds all three plus the Spring Boot auto-configuration that registers them, matching this codebase's existing convention of factoring shared infrastructure into its own library (see `ftgo-common`'s outbox/dedup extraction, Ch.3/Ch.4).

## Auto-configuration

`GatewayCommonAutoConfiguration` (registered via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`) `@Import`s the three `@Component`-annotated filters below and enables `GatewayApiKeyProperties` as a `@ConfigurationProperties` bean. A consuming gateway's `@SpringBootApplication` class does not component-scan this library's package, so relying on classpath component scanning (as some early services in this codebase did before their own auto-configuration was added) would silently fail to register these beans — auto-configuration avoids that class of bug from the start.

## Components

| Class | Type | Order | Purpose |
|---|---|---|---|
| `RequestLoggingFilter` | `GlobalFilter` | `Integer.MIN_VALUE` (first) | Logs `method path -> status (latency ms)` for every request. Times via `doFinally` around `chain.filter(exchange)` so the measured latency covers the whole downstream filter chain, not just this filter's own work. |
| `ApiKeyAuthFilter` | `GlobalFilter` | `Integer.MIN_VALUE + 1` (immediately after logging) | Validates the `X-Api-Key` request header against the single shared secret configured via `GatewayApiKeyProperties`. Missing or wrong key → `401`, request never reaches `chain.filter`. |
| `PerKeyRateLimiterGatewayFilterFactory` | Named `AbstractGatewayFilterFactory<Config>`, filter name `PerKeyRateLimiter` | Route-level (applied per-route in YAML, not global) | In-memory, per-API-key fixed 1-second window request counter. `Config.requestsPerSecond` sets the window's limit; exceeding it returns `429` without forwarding the request. |
| `GatewayApiKeyProperties` | `@ConfigurationProperties(prefix = "gateway.api-key")` | — | Binds `gateway.api-key.value` from each gateway's own `application.yml` — one key per gateway, not shared across gateways. |

## Why a hand-rolled rate limiter instead of Spring Cloud Gateway's built-in `RequestRateLimiter`

Spring Cloud Gateway ships a `RequestRateLimiter` filter, but it requires a Redis-backed `RateLimiter` implementation. This project has no Redis instance anywhere in its stack, so `PerKeyRateLimiterGatewayFilterFactory` trades away multi-instance correctness (each gateway instance counts independently — a key's true aggregate rate across N instances could exceed the configured limit) for zero new infrastructure. Acceptable for this project's single-instance dev/learning deployment; would need revisiting (Redis, or a distributed counter) for a real multi-instance production gateway.

## Dependencies

Depends on `spring-cloud-starter-gateway` (for `GlobalFilter`/`GatewayFilter`/`AbstractGatewayFilterFactory` types) and Spring Boot's `spring-boot-autoconfigure`. Exposes these as `api` (java-library plugin) so consuming gateway modules get the Gateway types transitively, the same pattern `ftgo-common` already uses for JPA/Kafka types.

## Running standalone

This module has no `main` class and cannot run on its own — it's a pure library.

```bash
./gradlew :ftgo-gateway-common:test
```

Tests are plain unit tests against `MockServerWebExchange`/`MockServerHttpRequest` (no Spring context needed for the filter-behavior tests) plus one `ApplicationContextRunner`-based test (`GatewayApiKeyPropertiesBindingTest`) proving the `@ConfigurationProperties` binding actually works from a property source, not just via direct construction.
