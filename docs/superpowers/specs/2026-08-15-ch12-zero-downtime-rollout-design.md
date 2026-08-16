# Design: Ch.12 §12.4 sub-project B2 — Zero-downtime rolling deployment

**Status:** Approved
**Date:** 2026-08-15
**Depends on:** Ch.12 §12.4 sub-project B1 (Kubernetes core deployment, `k8s/ftgo` Helm chart, merged PR #33)

## Goal

Demonstrate Kubernetes' rolling-update mechanism producing a genuinely
zero-downtime deploy of a real code change to `ftgo-order-service`, and
demonstrate rollback, on top of the existing `k8s/ftgo` Helm chart — per
book §12.4.4 (pp. 406–407).

This is a verification/demonstration sub-project, not new application
functionality. The interesting engineering content is: (a) the one real
chart gap it exposes (global-only image tagging) and (b) proving, with
captured evidence, that the chart's existing `readinessProbe`s actually
deliver a zero-downtime rollout rather than assuming they do.

## Background: what already exists

- `k8s/ftgo/templates/app-service.yaml` already sets a `readinessProbe`
  and `livenessProbe` (`/actuator/health`) on every business-service
  container. Kubernetes' Deployment controller will not terminate an old
  pod until its replacement passes `readinessProbe` — this is the
  mechanism that makes the rollout zero-downtime (book §12.4.4).
- No `strategy` is set on the Deployment, so it uses the default
  `RollingUpdate` with `maxSurge: 25%` / `maxUnavailable: 25%`. At
  `replicas: 1` these round to surge-1/unavailable-0 — i.e. Kubernetes
  already creates the new pod before removing the old one, without any
  chart change needed.
- The one real gap: `global.imageTag` (`values.yaml`) is a single value
  shared by every business service (`app-service.yaml` line 20:
  `"{{ $.Values.global.imageRegistry }}/ftgo-{{ .name }}:{{ $.Values.global.imageTag }}"`).
  There is currently no way to move one service to a new image version
  without moving all 8.

## Scope

### 1. Chart change — per-service image tag override

Add an optional `imageTag` field to `ftgo-order-service`'s entry in
`values.yaml`'s `businessServices` list. `app-service.yaml`'s image line
becomes:

```yaml
image: "{{ $.Values.global.imageRegistry }}/ftgo-{{ .name }}:{{ .imageTag | default $.Values.global.imageTag }}"
```

Every other business service is unaffected — an entry without `imageTag`
falls through to `global.imageTag` exactly as today.

### 2. Code change — observable version header

`ftgo-order-service` gains an `X-Service-Version` response header on its
endpoints, sourced from a Spring `@Value`-injected property
(`ftgo.service-version`), defaulted from the Gradle project version at
build time (already exposed via existing Spring Boot build-info wiring, or
a simple `@Value("${ftgo.service-version:unknown}")` backed by a Gradle
`processResources` filter — implementer's choice, whichever matches
existing conventions in this service). No business-logic change.

Bump the Gradle project version to `1.1.0` for the "new" build. The
previously-built image stays tagged `1.0.0` in the local kind registry
(rebuild and re-tag explicitly if needed — do not overwrite `1.0.0`).

### 3. k6 in-cluster verification Job

A new Helm-templated Job renders a k6 script that runs against
`http://order-service:8080` (in-cluster, bypassing ingress) for the
duration of a rollout: a tight loop of GET/POST requests against existing
order-service endpoints, asserting HTTP status on every response and
capturing the `X-Service-Version` response header per request, writing a
JSON summary to stdout (readable via `kubectl logs`).

This Job is **not** part of the normal `helm upgrade --install ftgo`
stack — it's a verification tool invoked manually
(`kubectl apply -f` a rendered template, or `helm template ... | kubectl apply -f -`
for just this one resource) immediately before triggering a rollout, and
deleted/re-created for each demo run.

Success criteria: **zero non-2xx responses** across the whole rollout
window, and the captured `X-Service-Version` trace shows a clean
transition from `1.0.0` → `1.1.0` with no gap.

### 4. Demo procedure (documented, not chart logic)

1. Build & push `ftgo-order-service:1.1.0` to the local kind registry
   (`1.0.0` must already exist from B1's deploy — rebuild/re-tag it first
   if the local registry was pruned).
2. Start the k6 Job.
3. `helm upgrade --install ftgo k8s/ftgo -n ftgo --set businessServices.order-service.imageTag=1.1.0`
   (exact `--set` path depends on the final values.yaml list structure —
   implementer resolves against the real schema).
4. Watch `kubectl rollout status deployment/order-service` and the k6
   output concurrently.
5. Capture k6's summary as evidence (zero dropped requests) — save to
   `docs/` alongside the ARCHITECTURE.md update, or inline in the report.
6. Demonstrate rollback: `kubectl rollout undo deployment/order-service`,
   re-run a short k6 burst to confirm `1.0.0` is back and still
   zero-downtime.

### 5. Documentation

`docs/ARCHITECTURE.md` gets a new subsection under the existing
Kubernetes deployment section (§12.4 B1) covering: rolling-update
mechanics (readinessProbe gating, the default surge-1/unavailable-0
behavior at replicas:1), the per-service `imageTag` override, the k6
verification Job, and a summary of the captured demo result (evidence
that the rollout was genuinely zero-downtime, plus the rollback
confirmation).

`README.md` / `CONTEXT.md` get their usual Ch.12 progress-table and
Current-position updates — this flips the B2 line from "not started" to
"done," but Ch.12 as a whole stays "In progress" until B3 also lands.

## Out of scope

- No autoscaling.
- No per-service `replicas` bump — single-replica zero-downtime is the
  whole point of this sub-project (it proves surge-before-terminate
  ordering, not high availability under load).
- No CI integration for the k6 Job — this is a one-off local
  demonstration, matching how the book presents §12.4.4.
- No change to any service other than `ftgo-order-service`.

## Testing / acceptance

- Existing `ftgo-order-service` unit tests still pass with the new
  version-header code path added.
- The k6 Job's captured summary (zero non-2xx, clean `1.0.0`→`1.1.0`
  version transition) is the acceptance evidence for the zero-downtime
  claim.
- Rollback re-verified with a second short k6 burst.
- Full monorepo `./gradlew test --continue` shows no new regressions
  beyond the already-documented pre-existing `ApplicationContext`/
  `SchedulingEnabledTest` failures (see PR #33 and prior Ch.12 B1 work).
