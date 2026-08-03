# ftgo-authorization-server

**Port:** 9000
**Bounded context:** none — a cross-cutting infrastructure service (Ch.11, §11.1), not a business service.

## Role

A Spring Authorization Server (`spring-security-oauth2-authorization-server`) issuing JWTs consumed by both gateways (`ftgo-mobile-gateway`, `ftgo-public-gateway`) and by the 7 business services acting as OAuth2 resource servers (order, kitchen, restaurant, accounting, delivery, consumer, order-history). Two grant types are supported, for two different kinds of caller:

- **Resource-owner password grant** (custom — this grant type is deprecated/removed from the OAuth2 spec's out-of-the-box support in Spring Authorization Server, so it's hand-implemented via `OAuth2ResourceOwnerPasswordAuthenticationConverter`/`Provider`/`Token`) — for end users (consumer/restaurant/courier/admin) authenticating with a username/password, matching how the book's mobile/web clients log in.
- **Client credentials grant** — for service-to-service calls where no end user is present. Currently registered for `ftgo-order-service` only, which uses it to attach a bearer token to its own outbound calls to restaurant/kitchen/accounting/delivery-service (`ServiceTokenClient`, Ch.11 §11.1 sub-project 2).

Every issued JWT carries a `roles` claim (a `List<String>`, e.g. `["CONSUMER"]`) added by a shared `jwtCustomizer`. For a password-grant token this is derived from the authenticated user's `ROLE_*` `GrantedAuthority`s; for a client-credentials token there's no user principal to derive it from, so the customizer branches on `AuthorizationGrantType.CLIENT_CREDENTIALS` and stamps a hardcoded `roles: ["SERVICE"]` instead. Every resource server's `RolesClaimJwtAuthenticationConverter` reads this same claim to build Spring Security authorities, so `@PreAuthorize("hasRole('SERVICE')")`/`hasAnyRole(..., 'SERVICE')` works identically to any other role.

## Registered clients

| `client_id` | Grant type(s) | Secret | Notes |
|---|---|---|---|
| `ftgo-gateway` | password, refresh_token | `gateway-secret` | Used by both gateways' `/oauth2/token` calls on behalf of an end user logging in. |
| `ftgo-order-service` | client_credentials | `order-service-secret` | `internal` scope; used only for order-service's own outbound service-to-service calls — no end user involved. |

Both clients authenticate via HTTP Basic (`CLIENT_SECRET_BASIC`). Access tokens have a 5-minute TTL for both grant types.

## Seeded users

Hardcoded in `FtgoUserDetailsService` — **no registration flow, no persistence**, every user shares the same encoded password (`"password"`):

| Username | Password | Role | Notes |
|---|---|---|---|
| `consumer1` | `password` | `CONSUMER` | id `1` — deliberately matches the auto-increment id `ftgo-consumer-service` assigns the first consumer created against a fresh database, so `ftgo-end-to-end-test`'s existing scenarios (which assume "an active consumer" is id `1`) keep working unchanged. |
| `consumer2` | `password` | `CONSUMER` | id `5` — a second consumer, used by the instance-based-ACL e2e scenarios to prove one consumer can't read another's order. |
| `restaurant1` | `password` | `RESTAURANT` | id `2`. |
| `courier1` | `password` | `COURIER` | id `3`. |
| `admin1` | `password` | `ADMIN` | id `4` — unrestricted access on every `@PreAuthorize`-guarded endpoint across every resource server. |

## API

**`POST /oauth2/token`** (password grant)

Request (`application/x-www-form-urlencoded`, `ftgo-gateway`'s Basic-auth credentials on the request):
```
grant_type=password&username=consumer1&password=password
```

Response (`200`):
```json
{"access_token": "<JWT>", "token_type": "Bearer", "expires_in": 300}
```

**`POST /oauth2/token`** (client credentials grant)

Request (`ftgo-order-service`'s Basic-auth credentials on the request):
```
grant_type=client_credentials&scope=internal
```

Response (`200`): same shape as above; the JWT's `roles` claim is `["SERVICE"]` regardless of any other claim.

**`GET /oauth2/jwks`**

Public JWK Set (RSA public key + key ID) used by every gateway and business service to validate a JWT's signature without contacting this server per-request (`spring.security.oauth2.resourceserver.jwt.jwk-set-uri`, cached per Spring's usual `JwtDecoder` behavior). Reachable without authentication — `permitAll()` on a dedicated `SecurityFilterChain` matched only to this path, since every caller needs it before it has any credential of its own.

## Key material

The RSA key pair backing `/oauth2/jwks` and every issued JWT's signature is generated fresh in memory on every application start (`KeyPairGenerator`, 2048-bit RSA, random key ID) — there is no persisted or externally-configured key. This means every restart invalidates every previously issued token and rotates the key ID; acceptable for this project's scope (a single, always-co-started authorization server per environment) but not how a production deployment would work.

## Caveat: dev/learning project only

This is a hand-rolled OAuth2 Authorization Server built to learn the pattern from *Microservices Patterns* Ch.11, §11.1 — it is **not** production-ready:

- Users are hardcoded (`FtgoUserDetailsService`) — there is no registration flow, no user store, no password reset.
- All 5 users share one password (`"password"`), stored only as a bcrypt hash in memory.
- The signing key is regenerated on every restart (see above) rather than persisted or rotated deliberately.
- Client secrets (`gateway-secret`, `order-service-secret`) are hardcoded in `AuthorizationServerConfig` rather than externalized to a secrets manager.
- `InMemoryOAuth2AuthorizationService`/`InMemoryRegisteredClientRepository` — nothing survives a restart, and this can't run as more than one instance.

## Running standalone

```bash
./gradlew :ftgo-authorization-server:test
```

To run live, start the full stack (`docker compose up -d`) — every gateway and resource server needs this service reachable at startup to fetch `/oauth2/jwks`.
