# Ch.12 §12.4 sub-project B3c: Mesh traffic management — ServiceProfiles

## Context

B3a (Linkerd install + auto-mTLS) and B3b (linkerd-viz golden-metrics dashboard) are both done —
see `docs/ARCHITECTURE.md`'s Service mesh sections and `CONTEXT.md`'s Ch.12 progress-table entry.
B3c is the third and final §12.4 sub-project, closing out the service-mesh topic. Once B3c
completes, Ch.12 flips to Done in `CONTEXT.md`'s progress table, triggering the full
chapter-completion documentation sweep required by `CLAUDE.md`.

B3a's design doc scoped B3c as: "mesh-level traffic management — `ServiceProfiles`-based
retries/circuit breaking, contrasted with the existing Resilience4j-based application-level
resilience." The business services already use Resilience4j circuit breakers per downstream
dependency (`resilience4j.circuitbreaker.instances.<name>` in each service's `application.yml`,
applied via `@CircuitBreaker` annotations on proxy methods — see e.g.
`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxy.java`).
Adding mesh-level retries on top of that is a real correctness question, not just additional
config: two independent retry layers on the same call path risk compounding retries into a retry
storm, or silently duplicating a non-idempotent request.

## Goal

Add Linkerd `ServiceProfile` resources for `order-service`'s outbound calls to its four
downstream dependencies (`restaurant-service`, `kitchen-service`, `delivery-service`,
`accounting-service`), scoped to retrying only idempotent GET routes, as a mesh-layer complement
to (not a replacement for) the existing Resilience4j circuit breakers — and demonstrate, with
live fault-injection evidence, how the two layers behave together during an outage.

## Non-goals

- No Java code changes. Resilience4j configuration, `@CircuitBreaker` annotations, and fallback
  methods are untouched.
- No `ServiceProfile`s for POST/PUT routes (ticket creation, charge authorization, delivery
  scheduling) — retrying a non-idempotent request that already succeeded server-side but timed
  out on the response risks duplicate side effects. This is a correctness rule, not a scope
  simplification available to relax later without a explicit idempotency-key mechanism.
- No `ServiceProfile`s for services other than `order-service`'s four outbound calls. The other 9
  business services and 2 gateways are meshed (mTLS from B3a, golden metrics from B3b) but don't
  get retry policies in this sub-project — `order-service` has the richest existing
  Resilience4j setup and the most exercised call paths in the project's e2e suite and B2's k6
  verification, making it the best-instrumented comparison.

## Architecture

**Layering with Resilience4j.** The mesh retry sits below Resilience4j in the call stack, inside
`linkerd-proxy`, transparent to the JVM. A single transient failure (e.g. one dropped connection
during a rolling restart) gets retried and absorbed by the mesh before Resilience4j's
sliding-window failure-rate calculation ever sees it — from `order-service`'s Java code's
perspective, the call just succeeded, possibly a few milliseconds slower. A sustained outage
(the downstream is actually down, not just flaky) exhausts the mesh's retry budget, and the
resulting failures reach Resilience4j's circuit breaker, which trips open and invokes the
existing business-aware fallback (e.g. `RestaurantServiceProxy`'s `findRestaurantFallback`
mapping specific exceptions to domain results). This is intentional complementary layering:
the mesh absorbs transport-level noise, Resilience4j owns business-level failure semantics and
fallback behavior that `ServiceProfiles` has no way to express (it has no concept of a
`RestaurantNotFoundException`).

**Retry budget.** Each `ServiceProfile` uses a retry budget (`retryBudget`, Linkerd's own
retry-storm safeguard: a ratio, e.g. `retryRatio: 0.2` — retries capped at 20% of the underlying
request rate, plus a minimum floor `minRetriesPerSecond: 10`) rather than an unbounded per-request
retry count, so a sustained outage produces bounded extra load on an already-failing downstream
instead of amplifying it.

**Resource: one `ServiceProfile` per callee.** Four `ServiceProfile` custom resources, each named
`<service>.<namespace>.svc.cluster.local` (Linkerd's required naming convention, ties the profile
to the Service's DNS name) and owned by `order-service` as the caller — though a `ServiceProfile`
is technically attached to the destination Service, not the caller, so
`kitchen-service`'s profile would apply to *any* meshed caller of `kitchen-service`, not just
`order-service`. This is called out explicitly in the design because it's a scoping surprise:
this sub-project only has one caller of each of the four destinations in practice
(`order-service`), so the distinction doesn't currently matter, but a future service added as a
second caller of e.g. `restaurant-service` would inherit this profile's retry policy too.

**Route authoring — tap-derived, not hand-typed.** None of the four downstream services publish
an OpenAPI spec (checked: no springdoc/swagger dependency in any of their `build.gradle`), so
`linkerd profile --open-api` isn't available. Instead, `linkerd profile --tap deploy/<service> -n
ftgo --tap-duration 10s` observes real traffic (driven by a manual `curl` sweep or the existing
e2e suite exercising the service's actual routes during the tap window) and generates a
`ServiceProfile` skeleton with the routes it actually saw. The generated skeleton is then
hand-edited to add `isRetryable: true` and the retry budget only to GET routes, leaving generated
POST/PUT routes as `isRetryable: false` (the tap tool's default).

**Where they live.** A new `k8s/ftgo/templates/service-profiles.yaml` Helm template holding all
four `ServiceProfile` resources, following the existing per-concern-template convention (e.g.
`mysql-statefulset.yaml`, `namespace.yaml`) — versioned with the chart, applied through the same
`helm upgrade --install ftgo` cycle as everything else, not a one-off `kubectl apply`.

## Verification

For each of the four callees in turn:

1. Capture baseline: `linkerd viz routes deploy/order-service -n ftgo -o wide` with the callee
   healthy, confirming the GET route(s) show up with their `EFFECTIVE_SUCCESS`/`ACTUAL_SUCCESS`
   columns at parity (no retries needed when healthy).
2. Inject a fault: `kubectl scale deployment/<callee> -n ftgo --replicas=0`.
3. Drive traffic against `order-service`'s corresponding GET-based proxy method (e.g.
   `RestaurantServiceProxy.findRestaurant` via the mobile-gateway or a direct port-forward) and
   capture `linkerd viz routes` again — expect `ACTUAL_SUCCESS` (raw, pre-retry) to drop while
   `EFFECTIVE_SUCCESS` (post-retry, what the caller ultimately observed) stays higher for the
   retry window, then both converge to failure once the retry budget and Resilience4j's own
   failure threshold are exhausted and the circuit breaker trips (observable via the service's
   own logs / the fallback method's behavior).
4. Scale the callee back to 1, confirm recovery (`linkerd viz routes` back to baseline,
   Resilience4j's circuit breaker transitions back to closed after its configured
   `wait-duration-in-open-state`).
5. Repeat with the `ServiceProfile` for that callee temporarily removed (`kubectl delete
   serviceprofile`), showing the same fault without mesh-level retries — `ACTUAL_SUCCESS` and
   `EFFECTIVE_SUCCESS` drop together immediately, with recovery relying on Resilience4j's circuit
   breaker/fallback alone. This is the comparison evidence B3a's original note asked for.

Results (all four callees' before/after tables, `linkerd viz routes` output, and any relevant
Resilience4j/application-log excerpts) get captured in an evidence file at
`docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md` during
implementation, matching this project's existing evidence-doc convention (see B2's and B3a's
equivalents).

## Documentation

This completes Ch.12, so the chapter-completion sweep in `CLAUDE.md` applies in full:

- `docs/ARCHITECTURE.md` — new "Mesh traffic management — ServiceProfiles (§12.4, B3c)" section
  covering the `ServiceProfile` resources, the retry-budget mechanism, the tap-derived authoring
  approach, the callee-scoped-not-caller-scoped naming caveat, the layering diagram/comparison
  table against Resilience4j, and the captured evidence.
- `ftgo-order-service/README.md` (and any other service README the change touches) — brought to
  parity if the resilience/mesh section of its README references B3a/B3b without B3c.
- `README.md` — Ch.12 row's Book progress table, marking §12.4 (and therefore Ch.12) Done.
- `CONTEXT.md` — progress table, Current position, session log, and per the chapter-completion
  rule specifically: the "Concept understanding" section's `Understood well` /
  `Needs more depth` / `Open questions` buckets get reviewed and any Ch.12-service-mesh-related
  items still sitting in `Needs more depth`/`Open questions` get moved out if this work resolves
  them.
- Grep for `ServiceProfile`, `B3c`, and `mesh traffic management` across `*.md` (excluding
  `docs/session-*.md`, `docs/superpowers/plans/`, `docs/superpowers/specs/`) to catch anything the
  per-change rule missed, per `CLAUDE.md`.
