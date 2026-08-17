# Ch.12 §12.4 sub-project B3a: Service mesh — Linkerd install + auto-mTLS

## Context

Ch.11 §11.4 (microservice chassis / service mesh) was left as conceptual reading, not
implemented, when Ch.11 was completed — see `docs/ARCHITECTURE.md`'s Ch.11 section and
`CONTEXT.md`'s Ch.11 progress-table entry. Ch.12 §12.4 (Kubernetes) sub-project B1 (core
deployment: the whole `compose.yml` stack redeployed to a local `kind` cluster via the `k8s/ftgo`
Helm chart) and B2 (zero-downtime rolling deployment) are both done. B3 is the third and final
§12.4 sub-project, closing out the service-mesh topic deferred from Ch.11.

B3's full scope — mTLS, mesh-level observability, and mesh-level traffic management
(retries/circuit-breaking) — is too large for one spec, matching this project's existing pattern
of decomposing large chapter sections into sequential sub-projects (as §12.4 itself was split
into B1/B2/B3). B3 is therefore split into three sequential sub-projects:

- **B3a (this spec):** install Linkerd, enable automatic mutual TLS between meshed services.
- **B3b (future):** mesh-level observability — Linkerd's built-in golden-metrics dashboard.
- **B3c (future):** mesh-level traffic management — `ServiceProfiles`-based retries/circuit
  breaking, contrasted with the existing Resilience4j-based application-level resilience
  (`k8s/ftgo`'s business services already use Resilience4j circuit breakers — see
  `docs/ARCHITECTURE.md`'s resilience section).

B3a is the foundation the other two build on: nothing in B3b or B3c can be verified without a
working mesh installed first.

## Goal

Install Linkerd on the existing `kind` cluster and get automatic, verified mutual TLS between all
meshed `ftgo` namespace pods, with zero application code changes.

## Mesh choice: Linkerd over Istio

Linkerd was chosen over Istio for this sub-project. Istio is often the book's/industry's
reference implementation for service mesh, but its control plane (`istiod`) and Envoy sidecars
carry meaningfully more CPU/memory overhead per pod than Linkerd's Rust-based `linkerd-proxy`.
This matters concretely here: B2's Task 5 (see
`docs/superpowers/plans/2026-08-15-ch12-zero-downtime-rollout-evidence.md`) found this same
single-node `kind` cluster's `ftgo-control-plane` Docker container (capped at 10 CPU cores) is
already prone to CPU-starvation cascades when ~20+ JVM-based Spring Boot pods restart
simultaneously. Adding a second, heavier container per pod (Istio's Envoy) on top of that history
is a needless risk; Linkerd's lighter footprint and simpler zero-config mTLS default are both a
better fit for this environment and a smaller, more focused change for B3a specifically.

## Architecture

- **Linkerd control plane**: installed into its own `linkerd` (or `linkerd-control-plane`,
  Linkerd's own default naming) namespace via the Linkerd CLI (`linkerd install | kubectl apply
  -f -`) or the Linkerd Helm charts — whichever composes more cleanly alongside the existing
  `k8s/ftgo` umbrella chart; the implementation plan will confirm this via Linkerd's current
  install docs. The control plane's lifecycle is independent of `k8s/ftgo`'s
  `helm upgrade --install` cycle — it is a cluster-level dependency the app chart assumes is
  present, the same relationship the chart already has with `kind` and the local image registry,
  not something the app chart manages.
- **Automatic sidecar injection**: the `ftgo` namespace is annotated
  (`linkerd.io/inject: enabled`) so every pod scheduled into it gets a `linkerd-proxy` sidecar
  container automatically on next rollout — no changes to any of the 13 business services'
  Deployment specs, the two gateways, or the auth/config/registry servers. This matches B1's
  "whole stack, not a partial book-style example" philosophy rather than meshing only a subset of
  services.
- **Linkerd viz extension**: installed (`linkerd viz install`) purely as verification tooling —
  it provides `linkerd viz tap` (live per-request inspection, including TLS status) and
  `linkerd viz stat` (aggregate success-rate/latency/throughput per meshed workload). This is not
  application infrastructure; it is the mechanism B3a uses to prove mTLS is actually happening,
  the same role k6 played for B2's zero-downtime verification.
- **Stateful/infra services (MySQL, Kafka, ELK, Prometheus/Grafana/Tempo, GlitchTip) are not
  the intended meshing target in B3a** — they don't participate in the HTTP-layer
  service-to-service calls mTLS protects. In practice, because injection is enabled via a
  namespace-wide annotation on `ftgo` (the simplest mechanism, matching B1's "whole stack" chart
  philosophy) rather than per-workload opt-in, these infra pods get an injected `linkerd-proxy`
  sidecar too when scheduled into that namespace. This is accepted as a harmless side effect
  (extra sidecar resource overhead, no functional impact — these services don't make
  proxy-visible HTTP calls to each other) rather than fixed via per-pod `linkerd.io/inject:
  disabled` overrides, to keep the chart change minimal. See
  `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`'s
  full-namespace-rollout section for where this was discovered.

## Resource risk and mitigation

Each injected pod gains a second container (the proxy), adding CPU/memory pressure on the same
constrained single-node `kind` cluster B2 already found brittle under simultaneous pod restarts.
Mitigation, carried into the implementation plan as explicit tasks:

- Set explicit, modest resource `requests`/`limits` on the injected proxy container (Linkerd's
  defaults are already modest — approximately 100m CPU / 20Mi memory per proxy — but the plan
  will pin these explicitly rather than relying on upstream defaults silently changing).
- Roll out injection incrementally: verify mTLS on one or two business services first (e.g.
  `order-service` and one of its direct dependencies) before annotating the whole namespace and
  triggering a full-stack simultaneous restart, to avoid reproducing B2's thundering-herd
  incident.

## Verification

`linkerd check` confirms control-plane health after install. Live mTLS proof: trigger a real
request flow (e.g. a Create Order call through the existing Kubernetes-profile
`ftgo-end-to-end-test` fixtures, or a direct `curl` through nginx-ingress) while running
`linkerd viz tap` against the receiving pod (e.g. `order-service`) and its downstream calls (e.g.
to `restaurant-service`), confirming `tls=true` on the observed connections. `linkerd viz stat`
against the meshed workloads provides a secondary aggregate confirmation (all meshed traffic
reporting encrypted, non-zero success rate). This evidence is captured in a markdown file the
same way B2 captured its k6 logs and rollout numbers — live proof against the running cluster,
not just "the install succeeded" or a config-file inspection.

## Out of scope for B3a

- Linkerd's dashboard/golden-metrics UI (B3b).
- `ServiceProfiles`-based retries/circuit-breaking, and any comparison against the existing
  Resilience4j-based resilience (B3c).
- Deliberately targeting infra/stateful services (MySQL, Kafka, ELK, Prometheus/Grafana/Tempo,
  GlitchTip) for meshing — they end up injected anyway as a namespace-wide side effect (see
  Architecture above), but this was never a B3a goal in itself.
- Any change to application code — B3a is entirely infrastructure (namespace annotation +
  control-plane install + resource tuning).

## Success criteria

- Linkerd control plane installed and healthy (`linkerd check` passes).
- All `ftgo` namespace app pods (13 business services + 2 gateways + auth/config/registry
  servers) show 2/2 containers Ready (app + injected proxy) after a rollout.
- `linkerd viz tap` on a live request flow between at least two meshed services shows `tls=true`.
- No regression to the existing Kubernetes-profile `ftgo-end-to-end-test` suite attributable to
  the mesh: the first verification run failed 10/11 tests on `POST /orders` 404s; a follow-up
  traced this to a client-side `gateway.base-url` convention mismatch (the suite's no-override
  default already includes `/api/v1`, which the tested override value omitted) rather than an
  ingress gap, and confirmed correct routing live (`http://localhost:18000/public/api/v1`, plus a
  passing health-check run across all 13 services) — but a clean full pass combining that fix with
  the suite's 13 required `kubectl port-forward`s was not obtained, as repeated test cycles pushed
  this project's single-node cluster into the same memory-exhaustion pattern documented for the
  full-namespace rollout below. This is unrelated to mTLS/mesh behavior, and no ingress or
  application changes were made by any B3a task. B3a's actual mTLS verification instead relies on
  the in-cluster evidence already captured: `linkerd viz tap` showing `tls=true` on live
  cross-service calls, and `linkerd viz stat` showing all 13 app Deployments 2/2 Ready, MESHED,
  100% success. A clean full e2e pass under rested cluster conditions is a documented follow-up,
  not a B3a blocker. See
  `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`'s Conclusion
  section.
- Documentation sweep (README.md, CONTEXT.md, docs/ARCHITECTURE.md new subsection) landing in
  the same change, per this project's existing convention.
