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
Deployment's `image` field.
