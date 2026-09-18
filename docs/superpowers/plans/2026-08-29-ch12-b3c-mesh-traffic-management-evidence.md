# Ch.12 B3c mesh traffic management — captured evidence

Date: 2026-09-18

## Cluster rebuild note

The local `kind` cluster from the 2026-08-29 session no longer existed at the start of this
session (fully torn down, not just degraded) and had to be recreated from scratch: `kind` cluster
+ local registry, all service images rebuilt and pushed, `helm install`, nginx-ingress, Gateway
API CRDs, Linkerd control plane + `linkerd viz`, then a full-namespace `kubectl rollout restart`
to inject `linkerd-proxy` sidecars into pods that had been created before Linkerd's
proxy-injector webhook existed.

During that rebuild, the same resource-contention pattern documented in the 2026-08-29 session
log recurred: Docker Desktop's container was pegged at ~957% CPU (of 1000% / 10 cores), and the
Linkerd control plane itself (`linkerd-destination`, `linkerd-identity`, `linkerd-proxy-injector`)
crash-looped for several minutes before stabilizing, which in turn caused every meshed pod's
`linkerd-proxy` sidecar to fail its own readiness probe (unable to resolve
`linkerd-dst-headless`/`linkerd-policy` in DNS while those pods had zero Service endpoints).
Recovery required, in order: scaling the full observability stack (Grafana, Prometheus, Logstash,
Kibana, GlitchTip x4, Tempo, Elasticsearch, Filebeat) and several non-B3c-critical app services
(kafka-connect, audit-log-service, order-history-service, consumer-service,
authorization-server, both gateways) to 0 replicas; waiting for the Linkerd control plane to
stabilize; then bringing `order-service` and its four downstream callees up **one at a time**
(rather than via a single `kubectl rollout restart` bringing up all five simultaneously) to avoid
five JVMs cold-starting under CPU contention at once. This is a stronger confirmation of the
2026-08-29 finding: this Mac's Docker Desktop allocation (10 CPU / 11.67GiB) has no headroom for
the full stack plus Linkerd's control plane and per-pod proxies running concurrently, and trimming
the observability stack to 0 replicas is the durable operating posture for this cluster, not a
one-time recovery step.

One side-effect worth noting for future sessions: `helm upgrade` (run to apply this sub-project's
`ServiceProfile`s) reset the imperative `kubectl scale --replicas=0` commands back to the chart's
templated `replicaCount` for every service, briefly reintroducing the same contention until the
non-essential services were scaled back down again. A `values.yaml`-level toggle to disable the
observability stack and gateways would avoid this fragility on the next Helm-driven change, but
implementing one is out of scope for B3c.

## ServiceProfile generation and application

The plan's tap-derived generation approach could not be used as written: the installed Linkerd CLI
(`edge-26.8.2`, matching the control plane version) has removed the `linkerd profile --tap` flag
entirely (not just changed its output shape — the flag doesn't exist in this version's `profile`
subcommand at all). `linkerd profile --template restaurant-service -n ftgo` was used instead (the
CLI's own documented fallback for generating a skeleton to hand-edit) to confirm the exact
`ServiceProfile` CRD shape this Linkerd version expects
(`spec.routes[].condition.{method,pathRegex}`, `spec.routes[].isRetryable`,
`spec.retryBudget.{retryRatio,minRetriesPerSecond,ttl}` — matching the plan's assumed shape
exactly). The four route paths themselves were taken directly from the plan's Global Constraints
section (already sourced from the `*Proxy.java` files during planning), not from live traffic
capture.

Four `ServiceProfile`s were hand-authored in `k8s/ftgo/templates/service-profiles.yaml`, each
marking its service's single GET route `isRetryable: true` with a `retryRatio: 0.2`/
`minRetriesPerSecond: 10`/`ttl: 10s` budget, and applied via
`helm upgrade ftgo ./k8s/ftgo --namespace ftgo`:

```
NAME                                        AGE
accounting-service.ftgo.svc.cluster.local   86s
delivery-service.ftgo.svc.cluster.local     86s
kitchen-service.ftgo.svc.cluster.local      87s
restaurant-service.ftgo.svc.cluster.local   87s
```

No field-manager ownership conflict occurred on this `helm upgrade` — consistent with the plan's
prediction, since this change only adds new `ServiceProfile` resources and doesn't touch any
Deployment's `image` field. (A field-manager conflict *did* occur later, during Task 2, on
`order-service`'s memory limit — see below.)

## Fault-injection verification

An order was seeded directly against `order-service` (`POST /orders`, `consumerId: 1,
restaurantId: 1, lineItems: [{menuItemId: 1, quantity: 2}]`, authenticated via a JWT obtained from
`authorization-server`'s resource-owner-password grant as `consumer1`/`password`), producing order
id `1`. `GET /orders/1/view` (the Ch.7 API-composition endpoint that fans out to all four
downstream services concurrently) returned all four sections populated, confirming the seed data
and route wiring were correct:

```
{"order":{"id":1,"status":"APPROVAL_PENDING",...},"restaurant":{"data":{...}},
 "ticket":{"data":{...}},"authorization":{},"delivery":{"data":{...}}}
```

### The central finding: mesh-level retries do not apply to order-service's real call path

Before any fault was injected, inspecting Prometheus's raw `response_total`/`route_response_total`
metrics (queried directly via `kubectl exec -n linkerd-viz deploy/prometheus -- wget -qO- ...`,
since `linkerd viz routes deploy/order-service --to svc/<name>` consistently showed only `[DEFAULT]`
with no data even after traffic was generated) revealed that order-service's outbound calls to all
four downstream services carry **no `dst_service` label and no `rt_route` label** — i.e. Linkerd
never classifies these calls against the `ServiceProfile`s at all, in either direction.

Root cause, confirmed against `ftgo-order-service/src/main/java/.../RestClientConfig.java`: all
four of order-service's `RestClient` beans are `@LoadBalanced` and target Eureka application names
(`http://ftgo-restaurant-service`, `http://ftgo-kitchen-service`, etc. — the `ftgo-` prefix is the
Eureka registration name from Ch.3, distinct from the plain Kubernetes Service name
`restaurant-service` etc.). Spring Cloud LoadBalancer resolves these via the `service-registry`
Eureka client to a specific **pod IP:port**, and dispatches the HTTP call directly to that address.
Linkerd's outbound `ServiceProfile`-based route classification and retry logic is keyed on the
request's `:authority`/`Host` header resolving to a Kubernetes Service's `<name>.<ns>.svc.cluster.local`
identity — a call addressed straight at a pod IP never goes through that lookup, so the
`ServiceProfile`s attached to `restaurant-service.ftgo.svc.cluster.local` etc. are structurally
unreachable from order-service's actual (Ch.3-established) call path, regardless of how correctly
they're authored.

This was verified empirically, not just architecturally, for two of the four callees:

**restaurant-service**: scaled to 0 (`kubectl wait --for=delete`), then 3× `GET /orders/1/view`
driven against `order-service`. All 3 returned `200` (Resilience4j's `findRestaurantFallback`
degrading that section, as designed) both **with** the `ServiceProfile` present and **with it
deleted** (`kubectl delete serviceprofile restaurant-service.ftgo.svc.cluster.local`) — identical
behavior either way, confirming the `ServiceProfile`'s presence made no observable difference.
`response_total{direction="outbound",deployment="order-service",dst_deployment="restaurant-service"}`
did not increment at all during the outage (the raw TCP connection to the now-gone pod IP fails
before any HTTP response is recorded), and `linkerd viz routes --to svc/restaurant-service` showed
only `[DEFAULT]` throughout, with the `ServiceProfile` in place or not.

**kitchen-service**: scaled to 0, 3× `GET /orders/1/view` driven, all 3 returned `200` (same
fallback pattern). `response_total{...rt_route!=""}` for order-service→kitchen-service traffic
returned zero results — no route ever got classified.

**delivery-service and accounting-service** were not independently fault-injected live (both
services were repeatedly crash-looping and recovering during this session under the same CPU
contention documented above, and driving two more full scale-to-0/restore cycles risked further
destabilizing an already-fragile cluster for a result the architecture already predicts with high
confidence). `RestClientConfig.java` shows `deliveryServiceRestClient` and
`accountingServiceRestClient` are built identically to the two tested proxies — same `@LoadBalanced`
annotation, same Eureka-name base URL pattern (`http://ftgo-delivery-service`,
`http://ftgo-accounting-service`) — so the same root cause applies to all four `ServiceProfile`s
uniformly, not just the two directly tested.

## Conclusion

The four `ServiceProfile`s are correctly authored per the plan's spec (retryable GET-only routes,
bounded retry budget) and are successfully applied to the cluster. However, **they have no observed
effect on order-service's actual traffic**, because order-service resolves all four downstream
dependencies via Eureka/Spring Cloud LoadBalancer client-side load balancing (a Ch.3 decision,
predating the mesh entirely) rather than by the Kubernetes Service DNS name Linkerd's outbound
route-retry mechanism requires. `/orders/{id}/view` continued returning `200` with a
degraded/`Unavailable` section throughout every fault, in every configuration tested (`ServiceProfile`
present or absent) — that resilience is coming entirely from Resilience4j's existing circuit
breakers and fallback methods (§3/§5), not from any mesh-layer retry, contradicting this plan's
original hypothesis that the two layers would visibly complement each other.

This is a genuine architectural finding rather than an implementation defect in this sub-project:
Linkerd's traffic-management features (`ServiceProfile` retries, and by the same mechanism,
traffic-splitting and the newer `HTTPRoute`-based policy) only govern traffic that Kubernetes'
Service abstraction actually carries. A service mesh and a client-side service-discovery library
(Eureka) are two competing implementations of the same concern (locating a healthy backend
instance), and layering one under the other without changing which one the application actually
calls through means the lower layer's traffic-shaping features go dark. Making order-service's
`ServiceProfile`s take effect would require switching `RestClientConfig` from Eureka-based
`@LoadBalanced` clients to plain Kubernetes Service-addressed calls (removing Ch.3's client-side
discovery for these four calls specifically) — a Java/architecture change explicitly out of scope
for B3c per the plan's Global Constraints ("No Java code changes"), and arguably a separate
learning topic in its own right (the book's own service-mesh chapter discussion of this exact
tension between platform-provided and library-provided traffic management).

## `order-service` memory-limit field-manager conflict

Separately from the ServiceProfile work: `order-service`'s app container was repeatedly `OOMKilled`
(exit 137) under this session's load while seeding the order and driving fault-injection traffic —
its `512Mi` memory limit (`k8s/ftgo/templates/app-service.yaml`) proved too tight for the JVM under
concurrent virtual-thread fan-out to four downstream calls plus Linkerd's own proxy overhead, on a
cluster already this resource-constrained. Worked around for the remainder of this session via
`kubectl patch deployment order-service --type=json ... resources/limits/memory -> 1Gi` (not
committed — a live-cluster-only workaround, since a chart-level memory bump is out of scope for
B3c). This patch subsequently caused the exact field-manager ownership conflict pattern already
documented in `CONTEXT.md` from 2026-08-29 (a `kubectl`-owned field blocking a later
`helm upgrade --reuse-values`) — `helm upgrade` failed once with `conflict ... .spec.template.spec.containers[name="order-service"].resources.limits.memory`, worked around by letting the
next `helm upgrade` (Task 2 Step 6) proceed once the specific conflicting call was retried; no
`--force-conflicts` was needed since the ServiceProfile-only resources in that particular apply
didn't touch the contested field. Not resolved as a permanent fix — same open item as the
pre-existing image-field conflict from 2026-08-29.
