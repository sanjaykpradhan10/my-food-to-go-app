# Ch.11 sub-project 2: Security (§11.1) — Design Spec

## Context

Chapter 11, §11.1 ("Developing secure services") of *Microservices Patterns* describes how
authentication and authorization must change shape in a microservice architecture: an
in-memory session and thread-local security context (fine in a monolith) can't work across
service boundaries. The book's recommended design is:

1. The API gateway authenticates the client and obtains a transparent token (a JWT) from an
   OAuth 2.0 Authorization Server.
2. The gateway forwards that JWT as an `Authorization: Bearer` header to whichever services
   it invokes.
3. Each service — a Resource Server — independently validates the JWT's signature and reads
   the principal's identity and roles from its claims; there is no shared session store.

This sub-project follows directly from Ch.11 sub-project 1 (health checks, §11.3.1, merged as
PR #24). It builds on the existing FTGO order flow (Ch.8 API gateway/BFF, Ch.9/10 testing
sub-projects) but is a substantially larger diff: 7 business services + 2 gateways all gain new
security wiring, plus one new service.

**Current state (verified by direct codebase inspection, 2026-08-03):**
- Zero existing security infrastructure — `grep -rin "security\|oauth" --include=build.gradle`
  across the whole repo returns no matches.
- Both gateways currently perform only a shared-secret stub check: `ApiKeyAuthFilter` in
  `ftgo-gateway-common` validates a single static `X-Api-Key` header value per gateway
  (`public-dev-key` / `mobile-dev-key`). Its own Javadoc says: "stub authentication... real auth
  is out of scope for Ch.8." This sub-project replaces that stub.
- `ftgo-order-service`'s `Order.java` has a `private Long consumerId` field. `POST /orders`
  currently reads `consumerId` from the client-supplied request body; `GET /orders/{id}` takes
  only a path variable with no identity check at all — anyone can fetch any order today.
- The existing `ftgo-end-to-end-test` Cucumber suite (`PlaceReviseCancelOrder.feature`,
  `AllServicesReportHealthy.feature`) sends only the `X-Api-Key` header — no `Authorization`
  header anywhere.

## Architecture

### New module: `ftgo-authorization-server`

A minimal Spring Authorization Server (the actively maintained `spring-authorization-server`
library — the modern successor to what the book calls "Spring OAuth", which is unmaintained)
with:

- A small seeded, hardcoded user store (H2), one user per role needed by the test scenarios:
  `consumer1` (`CONSUMER`), `restaurant1` (`RESTAURANT`), `courier1` (`COURIER`), `admin1`
  (`ADMIN`). No registration flow — out of scope, per the book's own focus.
- **Custom Resource Owner Password Credentials Grant.** Spring Authorization Server
  intentionally ships only Authorization Code, Client Credentials, Refresh Token, and Device
  Code grants — Password Grant was dropped because OAuth 2.1 discourages it for third-party
  clients. The book's Figures 11.4/11.5 explicitly use Password Grant (`POST /oauth/token`
  with `userid`/`password`), because a gateway authenticating its own first-party clients is
  exactly the case Password Grant was designed for. This spec implements it as a custom
  `AuthenticationProvider` + `AuthenticationConverter` registered on the Authorization Server's
  token endpoint — a pattern Spring's own `spring-authorization-server` samples repo documents
  for this exact scenario. This is an explicit deviation from "batteries included" in favor of
  matching the book's design; it is not a workaround being smuggled in silently.
- **Refresh Token Grant**: supported out of the box by `spring-authorization-server` — no
  custom work needed, matching Figure 11.5's refresh flow.
- **JWT claims**:
  - `sub`: the user's ID as a string of a `Long` (e.g. `"1"`) — the seeded `consumer1` user's ID
    is chosen to match the `consumerId` already used by existing order-flow Cucumber scenarios,
    so the ACL check in Order Service (below) lines up with orders the e2e suite already
    creates.
  - `roles`: a JSON array of role strings, e.g. `["CONSUMER"]`.
  - Standard `exp`/`iat` claims with a short expiration (e.g. 5 minutes), consistent with the
    book's point that short-lived JWTs limit exposure from a leaked token.
- Exposes its JWK Set at the standard `/.well-known/jwks.json` endpoint so gateways and services
  can validate signatures without a shared secret.

### Gateways: `ftgo-public-gateway`, `ftgo-mobile-gateway`

- The existing `ApiKeyAuthFilter` global filter (in `ftgo-gateway-common`) is retired.
- A new JWT-validating global filter (also in `ftgo-gateway-common`, since both gateways
  already depend on it) replaces it: validates the incoming `Authorization: Bearer <jwt>`
  header's signature against the authorization server's JWK Set, rejects with 401 if missing/
  invalid, and forwards the same header unchanged to backend services on the proxied request.
  The gateway does not re-encode or re-issue the token — it passes through what it already
  validated.
- No role-based routing restrictions at the gateway layer for this sub-project (the book notes
  gateway-level authorization risks coupling the gateway to service internals) — all role
  enforcement happens in the services themselves.

### The 7 business services

Each of `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-restaurant-service`,
`ftgo-accounting-service`, `ftgo-delivery-service`, `ftgo-consumer-service`,
`ftgo-order-history-service` becomes an OAuth 2.0 Resource Server:

- Adds `spring-boot-starter-oauth2-resource-server`.
- Configures JWT validation against the authorization server's JWK Set endpoint
  (`spring.security.oauth2.resourceserver.jwt.jwk-set-uri`).
- Configures a custom `JwtAuthenticationConverter` (or `GrantedAuthoritiesMapper`) that reads
  the `roles` claim and maps each entry to a Spring Security `GrantedAuthority` (prefixed
  `ROLE_`, matching `@PreAuthorize("hasRole(...)")` conventions).
- Adds `@PreAuthorize` annotations to existing REST endpoints per the role mapping below.
  No new endpoints are added solely for this sub-project — only existing ones gain checks.

### Role-to-endpoint mapping

| Service | Endpoint(s) | Required role(s) |
|---|---|---|
| `ftgo-order-service` | `POST /orders`, `POST /orders/{id}/cancel`, `POST /orders/{id}/revise` | `CONSUMER` or `ADMIN` |
| `ftgo-order-service` | `GET /orders/{id}` | `CONSUMER` (ACL-checked, see below) or `ADMIN` (unrestricted) |
| `ftgo-kitchen-service` | existing write endpoints | `RESTAURANT` or `ADMIN` |
| `ftgo-restaurant-service` | existing write endpoints | `RESTAURANT` or `ADMIN` |
| `ftgo-delivery-service` | existing write endpoints | `COURIER` or `ADMIN` |
| `ftgo-accounting-service` | all endpoints | `ADMIN` only |
| `ftgo-order-history-service` | all endpoints | `ADMIN` only |
| `ftgo-consumer-service` | all endpoints | `ADMIN` only |

Services with only `ADMIN`-only endpoints today have no existing consumer/restaurant/courier
role split worth introducing artificially — narrowing further is future work, not this
sub-project's scope.

### Order Service ACL (instance-based authorization)

This is the one deviation from pure role-based checks, following the book's own
`getOrderDetails()` example directly:

- `GET /orders/{id}`: the controller derives the caller's identity from the JWT's `sub` claim
  (via `@AuthenticationPrincipal Jwt`, not the request), then:
  - If the caller has role `ADMIN`: return the order unconditionally.
  - If the caller has role `CONSUMER`: return the order only if the JWT's `sub` equals the
    order's `consumerId`; otherwise return 403 Forbidden.
  - Any other role (or no matching role): 403 Forbidden.
- `POST /orders`: the `consumerId` in the request is replaced with the JWT's `sub` claim value
  — no longer client-supplied. This closes the gap noted in the "Current state" section above.

## Testing

- Extend `ftgo-end-to-end-test`:
  - `PlaceReviseCancelOrderStepDefinitions` is updated to first perform the custom password
    grant against the authorization server (`consumer1`/its seeded password) to obtain a JWT,
    then send `Authorization: Bearer <jwt>` instead of `X-Api-Key` on all subsequent requests.
  - A new Cucumber scenario/feature (e.g. `OrderAccessControl.feature`) asserts:
    - A CONSUMER token can fetch its own order (200).
    - A CONSUMER token gets 403 fetching another consumer's order.
    - A request with no `Authorization` header gets 401 at the gateway.
    - A request with an expired/malformed JWT gets 401.
  - `AllServicesReportHealthy.feature` is unaffected (health endpoints stay unauthenticated —
    Actuator health checks are infrastructure-facing, not client-facing, consistent with common
    practice and this project's existing Ch.11 sub-project 1 scope).
- No new per-service unit/slice tests for `@PreAuthorize` wiring — consistent with this
  project's existing test depth (Ch.8–10 validated the order flow primarily at the e2e level);
  the e2e scenarios above are the primary verification for this sub-project too.

## Out of scope (explicitly deferred)

- User registration / self-service account creation — hardcoded seed users only.
- TLS between services — the book defers this to service meshes, covered (if at all) under
  §11.4.1 in a later sub-project.
- Auditing / audit logging — the book explicitly defers this to §11.3 (a separate, later
  sub-project on observability).
- Fine-grained ACLs on any service besides Order Service — the book's own example is
  Order Service–specific; extending it elsewhere is unfounded scope creep for this sub-project.
- Gateway-level, path-based authorization rules — the book notes this risks coupling the
  gateway to service internals; all enforcement stays in the services.

## Global Constraints (for the implementation plan)

- Roles: exactly `CONSUMER`, `RESTAURANT`, `COURIER`, `ADMIN` — no others.
- JWT claims: `sub` (user ID, string form of a `Long`), `roles` (array of role strings), plus
  standard `exp`/`iat`.
- JWT expiration: 5 minutes (short-lived, matching the book's stated rationale).
- The `ApiKeyAuthFilter` / `X-Api-Key` stub is retired, not kept alongside JWT auth.
- No new endpoints introduced solely for auth — only `@PreAuthorize` added to existing ones,
  except the Order Service ACL check which modifies existing endpoint behavior per the mapping
  above.
