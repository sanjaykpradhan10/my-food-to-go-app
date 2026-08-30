# Ch.12 §12.4 B3c: Mesh Traffic Management (ServiceProfiles) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Linkerd `ServiceProfile` resources for `order-service`'s four outbound GET calls
(to `restaurant-service`, `kitchen-service`, `delivery-service`, `accounting-service`), retrying
only those idempotent routes, as a mesh-layer complement to the existing Resilience4j circuit
breakers — and capture live fault-injection evidence showing how the two layers behave together.

**Architecture:** Generate a `ServiceProfile` skeleton per callee via `linkerd profile --tap`
(none of the four services publish an OpenAPI spec), hand-edit each to mark only its GET route
`isRetryable: true` with a bounded retry budget, and add all four as one new Helm template
(`k8s/ftgo/templates/service-profiles.yaml`) applied through the normal `helm upgrade` cycle.
Verify by scaling each callee to 0, driving `GET /orders/{id}/view` (the existing API-composition
endpoint that calls all four downstream services concurrently), and comparing
`linkerd viz routes` output with and without each `ServiceProfile` in place.

**Tech Stack:** Linkerd 2.x `ServiceProfile` CRDs (`linkerd.io/v1alpha2`), the existing `kind`
cluster and `k8s/ftgo` Helm chart, `kubectl`, `linkerd`/`linkerd viz` CLI (installed at
`~/.linkerd2/bin/linkerd`, not on `$PATH` — use the full path).

**Spec:** `docs/superpowers/specs/2026-08-29-ch12-b3c-mesh-traffic-management-design.md`

## Global Constraints

- Namespace is always `ftgo` (`k8s/ftgo/values.yaml`'s `global.namespace`).
- No Java code changes. Resilience4j config/annotations/fallback methods are untouched.
- `ServiceProfile`s only for GET routes: `restaurant-service`'s `GET /restaurants/{id}`,
  `kitchen-service`'s `GET /tickets/order/{orderId}`, `delivery-service`'s
  `GET /deliveries/order/{orderId}`, `accounting-service`'s `GET /authorizations/order/{orderId}`
  (confirmed from `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/*Proxy.java`
  — all four downstream calls order-service makes are GETs; there is no POST/PUT route to
  exclude in practice, but the retryable flag must still be set explicitly per-route rather than
  defaulted, so a future non-GET route added to any of these profiles doesn't silently inherit
  retryability).
- Ports (from `k8s/ftgo/values.yaml`): `restaurant-service` 8085, `kitchen-service` 8083,
  `delivery-service` 8086, `accounting-service` 8084, `order-service` 8082.
- `ServiceProfile` names must be `<service>.<namespace>.svc.cluster.local` (Linkerd's required
  convention) — e.g. `restaurant-service.ftgo.svc.cluster.local`.
- Retry budget on each profile: `retryRatio: 0.2`, `minRetriesPerSecond: 10` (bounds retries to
  20% of underlying request rate plus a floor, per the spec's retry-storm safeguard).
- `linkerd` CLI is at `~/.linkerd2/bin/linkerd` in this environment, not on `$PATH` — every
  command below uses the full path.
- Documentation lands in the same commit as the config change it describes. Because this is the
  final §12.4 sub-project, the **last task** in this plan is the full Ch.12 chapter-completion
  documentation sweep required by `CLAUDE.md` (not just the sub-project's own doc update).

---

### Task 1: Generate and author the four `ServiceProfile`s

**Files:**
- Create: `k8s/ftgo/templates/service-profiles.yaml`
- Create: `docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md`

**Interfaces:**
- Consumes: the already-meshed `ftgo` namespace from B3a (all 13 app Deployments MESHED 1/1); no
  code from earlier tasks in this plan (first task).
- Produces: four `ServiceProfile` resources applied to the live cluster and versioned in the
  Helm chart — Task 2's fault-injection verification and Task 3's documentation both depend on
  these existing and being correctly authored (retryable GET routes only).

- [ ] **Step 1: Generate a tap-derived skeleton for `restaurant-service`**

In one terminal, start the tap:
```bash
~/.linkerd2/bin/linkerd profile --tap deploy/restaurant-service -n ftgo --tap-duration 10s \
  > /tmp/restaurant-service-profile.yaml &
TAP_PID=$!
```
While that's capturing (within the 10s window), trigger real traffic against
`restaurant-service`'s actual route:
```bash
sleep 2
kubectl exec -n ftgo deploy/order-service -- curl -s -o /dev/null -w "%{http_code}\n" \
  http://restaurant-service:8085/restaurants/1
wait $TAP_PID
cat /tmp/restaurant-service-profile.yaml
```
Expected: the generated YAML contains a `routes:` entry with `name: GET /restaurants/{id}` (or a
similarly-templated path — Linkerd's tap-derived profile groups path segments it detects as IDs).
If the route name differs from this (e.g. it captured the literal `/restaurants/1` instead of a
templated path), use the literal path as the `pathRegex` in Step 4 instead — the exact templating
behavior depends on the installed Linkerd version, so match what the tool actually produced
rather than forcing this expected shape.

- [ ] **Step 2: Repeat generation for the other three services**

```bash
~/.linkerd2/bin/linkerd profile --tap deploy/kitchen-service -n ftgo --tap-duration 10s \
  > /tmp/kitchen-service-profile.yaml &
TAP_PID=$!
sleep 2
kubectl exec -n ftgo deploy/order-service -- curl -s -o /dev/null -w "%{http_code}\n" \
  http://kitchen-service:8083/tickets/order/1
wait $TAP_PID

~/.linkerd2/bin/linkerd profile --tap deploy/delivery-service -n ftgo --tap-duration 10s \
  > /tmp/delivery-service-profile.yaml &
TAP_PID=$!
sleep 2
kubectl exec -n ftgo deploy/order-service -- curl -s -o /dev/null -w "%{http_code}\n" \
  http://delivery-service:8086/deliveries/order/1
wait $TAP_PID

~/.linkerd2/bin/linkerd profile --tap deploy/accounting-service -n ftgo --tap-duration 10s \
  > /tmp/accounting-service-profile.yaml &
TAP_PID=$!
sleep 2
kubectl exec -n ftgo deploy/order-service -- curl -s -o /dev/null -w "%{http_code}\n" \
  http://accounting-service:8084/authorizations/order/1
wait $TAP_PID
```
Expected: four files in `/tmp`, each with a `routes:` entry for that service's respective GET
path (order id `1` need not exist — a 404 still exercises and captures the route).

- [ ] **Step 3: Read all four generated files**

```bash
cat /tmp/restaurant-service-profile.yaml /tmp/kitchen-service-profile.yaml \
    /tmp/delivery-service-profile.yaml /tmp/accounting-service-profile.yaml
```
Use the actual `kind: ServiceProfile`, `metadata.name`, and `spec.routes` content from these four
outputs as the basis for Step 4 — do not hand-type the CRD structure from scratch, since the
exact schema (e.g. whether `isRetryable` sits under `routes[].isRetryable` or a separate
top-level block) must match what the installed Linkerd version's CLI actually generated.

- [ ] **Step 4: Write `k8s/ftgo/templates/service-profiles.yaml`**

Using the four generated skeletons as the structural template, write one file containing all
four `ServiceProfile`s. Each route's `isRetryable` must be set explicitly (`true` for the single
GET route each service exposes to order-service; this plan defines no other routes on these
profiles, so there is nothing to mark `false` — but do not omit the field, since an unset
`isRetryable` defaults to non-retryable and omitting it would make the retry intent implicit).
The shape (adjust path/route names to match what Step 3's actual output showed):

```yaml
apiVersion: linkerd.io/v1alpha2
kind: ServiceProfile
metadata:
  name: restaurant-service.{{ .Values.global.namespace }}.svc.cluster.local
  namespace: {{ .Values.global.namespace }}
spec:
  routes:
    - name: GET /restaurants/{id}
      condition:
        method: GET
        pathRegex: /restaurants/[^/]*
      isRetryable: true
  retryBudget:
    retryRatio: 0.2
    minRetriesPerSecond: 10
    ttl: 10s
---
apiVersion: linkerd.io/v1alpha2
kind: ServiceProfile
metadata:
  name: kitchen-service.{{ .Values.global.namespace }}.svc.cluster.local
  namespace: {{ .Values.global.namespace }}
spec:
  routes:
    - name: GET /tickets/order/{orderId}
      condition:
        method: GET
        pathRegex: /tickets/order/[^/]*
      isRetryable: true
  retryBudget:
    retryRatio: 0.2
    minRetriesPerSecond: 10
    ttl: 10s
---
apiVersion: linkerd.io/v1alpha2
kind: ServiceProfile
metadata:
  name: delivery-service.{{ .Values.global.namespace }}.svc.cluster.local
  namespace: {{ .Values.global.namespace }}
spec:
  routes:
    - name: GET /deliveries/order/{orderId}
      condition:
        method: GET
        pathRegex: /deliveries/order/[^/]*
      isRetryable: true
  retryBudget:
    retryRatio: 0.2
    minRetriesPerSecond: 10
    ttl: 10s
---
apiVersion: linkerd.io/v1alpha2
kind: ServiceProfile
metadata:
  name: accounting-service.{{ .Values.global.namespace }}.svc.cluster.local
  namespace: {{ .Values.global.namespace }}
spec:
  routes:
    - name: GET /authorizations/order/{orderId}
      condition:
        method: GET
        pathRegex: /authorizations/order/[^/]*
      isRetryable: true
  retryBudget:
    retryRatio: 0.2
    minRetriesPerSecond: 10
    ttl: 10s
```

- [ ] **Step 5: Apply via Helm and confirm all four exist**

```bash
helm upgrade ftgo ./k8s/ftgo --namespace ftgo
kubectl get serviceprofiles -n ftgo
```
Expected: four `ServiceProfile` resources listed, named
`restaurant-service.ftgo.svc.cluster.local`, `kitchen-service.ftgo.svc.cluster.local`,
`delivery-service.ftgo.svc.cluster.local`, `accounting-service.ftgo.svc.cluster.local`.

If `helm upgrade` fails with a field-manager ownership conflict (a known open issue from this
project's 2026-08-29 cluster-recovery session, see `CONTEXT.md`'s session log — a prior
`kubectl rollout restart` took ownership of some Deployments' `image` field away from Helm), this
change only adds new resources and doesn't touch any Deployment's `image` field, so it should not
trigger that specific conflict; if it does anyway, apply with
`kubectl apply --server-side --force-conflicts -f k8s/ftgo/templates/service-profiles.yaml`
(rendered via `helm template` first) as a scoped workaround, and note this in the evidence file
rather than silently resolving the broader chart-wide conflict as a side effect of this task.

- [ ] **Step 6: Start the evidence file**

Create `docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md`:

```markdown
# Ch.12 B3c mesh traffic management — captured evidence

Date: <fill in from `date -u +%Y-%m-%d` when this task actually runs>

## ServiceProfile generation and application

Four `ServiceProfile`s generated via `linkerd profile --tap` against live traffic (10s tap
windows, one `curl` per service to exercise its real route) and hand-edited to mark only each
service's single GET route `isRetryable: true` with a `retryRatio: 0.2`/`minRetriesPerSecond: 10`
budget. Applied via `helm upgrade ftgo ./k8s/ftgo --namespace ftgo`:

```
<paste the actual `kubectl get serviceprofiles -n ftgo` output>
```
```

- [ ] **Step 7: Commit**

```bash
git add k8s/ftgo/templates/service-profiles.yaml \
  docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md
git commit -m "feat: add ServiceProfiles for order-service's downstream GET routes (Ch.12 B3c)"
```

---

### Task 2: Fault-injection verification against all four callees

**Files:**
- Modify: `docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md`
  (append, do not replace)

**Interfaces:**
- Consumes: the four `ServiceProfile`s from Task 1; `ftgo-order-service`'s
  `GET /orders/{id}/view` endpoint (`OrderViewController.java`, unmodified — fires all four
  downstream proxy calls concurrently on virtual threads, exactly the composition point this
  verification needs).
- Produces: the full before/after evidence tables Task 3's documentation summarizes — no later
  task in this plan consumes this directly, but Task 3's ARCHITECTURE.md section references it.

- [ ] **Step 1: Create a real order to exercise `GET /orders/{id}/view` against**

The `/orders/{id}/view` endpoint 404s on a nonexistent order id, so an id is needed. Run the
existing Kubernetes-profile end-to-end suite's Create Order step once to seed one, or place an
order directly through the ingress (confirm exact ingress path/port against
`k8s/README.md`'s ingress section, e.g. `http://localhost:18000/public/api/v1/orders`). Record
the resulting order id — call it `<ORDER_ID>` for the rest of this task. If seeding via the e2e
suite, extract the id from its logs; if via direct `curl`, extract it from the `POST /orders`
response body's `id` field.

- [ ] **Step 2: Capture baseline `linkerd viz routes` for all four callees**

```bash
~/.linkerd2/bin/linkerd viz routes deploy/order-service -n ftgo -o wide
```
Expected: four route rows (one per downstream service, per the `ServiceProfile`s just applied),
`EFFECTIVE_SUCCESS` and `ACTUAL_SUCCESS` at parity (no retries needed — all callees healthy). Run
`kubectl exec -n ftgo deploy/order-service -- curl -s http://order-service:8082/orders/<ORDER_ID>/view`
a few times first if the routes table shows no traffic yet (the table only shows routes with
observed requests in its window).

- [ ] **Step 3: Fault-inject `restaurant-service` and observe retry behavior**

```bash
kubectl scale deployment/restaurant-service -n ftgo --replicas=0
kubectl wait --for=delete pod -n ftgo -l app=restaurant-service --timeout=30s
for i in 1 2 3 4 5; do
  kubectl exec -n ftgo deploy/order-service -- curl -s -o /dev/null -w "%{http_code}\n" \
    http://order-service:8082/orders/<ORDER_ID>/view
  sleep 2
done
~/.linkerd2/bin/linkerd viz routes deploy/order-service -n ftgo -o wide
```
Expected: the `GET /restaurants/{id}` route's `ACTUAL_SUCCESS` (raw, pre-retry request success)
drops toward 0% while `EFFECTIVE_SUCCESS` (what order-service ultimately observed, post-retry)
stays higher during the window the retry budget can still absorb failures, then converges toward
`ACTUAL_SUCCESS` once the budget is exhausted. The overall `/orders/{id}/view` response itself
should still return `200` throughout (the composition endpoint degrades that one section to
`Unavailable`/`NotFound` via `SectionResult`, per `RestaurantServiceProxy`'s existing
`findRestaurantForViewFallback` — this is Resilience4j's fallback taking over once the mesh's own
retries stop helping, confirming the two-layer story from the spec).

- [ ] **Step 4: Restore `restaurant-service`, repeat for the other three callees**

```bash
kubectl scale deployment/restaurant-service -n ftgo --replicas=1
kubectl rollout status deployment/restaurant-service -n ftgo --timeout=120s
```
Repeat Step 3's scale-to-0 / curl-loop / `linkerd viz routes` / scale-back-to-1 sequence for
`kitchen-service`, `delivery-service`, and `accounting-service` in turn (one at a time, restoring
each before injecting the next — do not fault multiple callees simultaneously, since
`GET /orders/{id}/view`'s degrade-independently design means a multi-fault run would conflate
each route's individual retry behavior in the same `linkerd viz routes` snapshot).

- [ ] **Step 5: Repeat the `restaurant-service` fault with its `ServiceProfile` removed**

This is the comparison evidence — the same fault, without mesh-level retries:
```bash
kubectl delete serviceprofile restaurant-service.ftgo.svc.cluster.local -n ftgo
kubectl scale deployment/restaurant-service -n ftgo --replicas=0
kubectl wait --for=delete pod -n ftgo -l app=restaurant-service --timeout=30s
for i in 1 2 3 4 5; do
  kubectl exec -n ftgo deploy/order-service -- curl -s -o /dev/null -w "%{http_code}\n" \
    http://order-service:8082/orders/<ORDER_ID>/view
  sleep 2
done
~/.linkerd2/bin/linkerd viz routes deploy/order-service -n ftgo -o wide
```
Expected: without the `ServiceProfile`, `linkerd viz routes` shows no distinct row for
`GET /restaurants/{id}` (it falls back to Linkerd's default unclassified-route behavior), and
`ACTUAL_SUCCESS`/`EFFECTIVE_SUCCESS` drop together immediately with no retry-smoothed gap between
them — recovery of the `/orders/{id}/view` response's 200-with-degraded-section behavior now
relies on Resilience4j's circuit breaker/fallback alone, with no mesh-level retry absorbing
transient blips first.

- [ ] **Step 6: Restore `restaurant-service`'s `ServiceProfile` and scale it back up**

```bash
helm upgrade ftgo ./k8s/ftgo --namespace ftgo
kubectl get serviceprofile restaurant-service.ftgo.svc.cluster.local -n ftgo
kubectl scale deployment/restaurant-service -n ftgo --replicas=1
kubectl rollout status deployment/restaurant-service -n ftgo --timeout=120s
```
Expected: the `ServiceProfile` is re-created (Helm re-applies it since it's still in the chart
template) and `restaurant-service` is back to `2/2 Ready`.

- [ ] **Step 7: Append all captured evidence to the evidence file**

Append to `docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md`:

```markdown
## Fault-injection verification

For each of the four callees, `linkerd viz routes deploy/order-service -n ftgo -o wide` was
captured before and during a scale-to-0 fault, with 5 requests to `/orders/{id}/view` driven
during the fault window.

### restaurant-service

Baseline:
```
<paste the actual baseline routes output>
```

During fault (ServiceProfile in place):
```
<paste the actual during-fault routes output>
```

During the same fault, with the ServiceProfile removed (comparison):
```
<paste the actual during-fault-no-profile routes output>
```

### kitchen-service / delivery-service / accounting-service

<repeat the baseline + during-fault pairing captured in Step 4, one subsection each>

## Conclusion

<summarize the actual observed EFFECTIVE_SUCCESS vs. ACTUAL_SUCCESS gap with the ServiceProfile
in place vs. removed, and confirm whether /orders/{id}/view kept returning 200 with a degraded
section throughout every fault, per Resilience4j's existing fallback behavior>
```

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md
git commit -m "docs: capture Ch.12 B3c fault-injection verification evidence"
```

---

### Task 3: Chapter-completion documentation sweep

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `ftgo-order-service/README.md` (if it references B3a/B3b without B3c, or documents
  `RestaurantServiceProxy`/`KitchenServiceProxy`/`DeliveryServiceProxy`/`AccountingServiceProxy`'s
  resilience behavior without mentioning the mesh-layer retry that now sits below it)

**Interfaces:**
- Consumes: the evidence file finalized in Task 2; the spec's Documentation section
  (`docs/superpowers/specs/2026-08-29-ch12-b3c-mesh-traffic-management-design.md`).
- Produces: nothing consumed by later tasks — this is the final task of B3c, and of Ch.12 §12.4
  as a whole (B1, B2, B3a, B3b, B3c all done after this task).

- [ ] **Step 1: Check `ftgo-order-service/README.md` for resilience/mesh references**

```bash
grep -n "Resilience4j\|circuit.breaker\|B3a\|B3b\|mesh" ftgo-order-service/README.md
```
If the README documents the four proxies' Resilience4j circuit breakers without noting the
mesh-level retry layer added by this sub-project, add a short paragraph (following whatever
section already documents resilience there) noting: `restaurant-service`, `kitchen-service`,
`delivery-service`, and `accounting-service` calls are now also retried at the mesh layer via
Linkerd `ServiceProfile`s (GET routes only, bounded by a retry budget) before Resilience4j's own
circuit breaker/fallback logic engages — link to `docs/ARCHITECTURE.md`'s new B3c section for the
full layering explanation rather than duplicating it here.

- [ ] **Step 2: Add the B3c section to `docs/ARCHITECTURE.md`**

Add a new subsection directly after the B3b "Mesh observability — linkerd-viz golden metrics
(§12.4, B3b)" section, titled
`### Mesh traffic management — ServiceProfiles (§12.4, B3c)`, covering:

- The four `ServiceProfile`s and what they target (`order-service`'s calls to `restaurant-service`,
  `kitchen-service`, `delivery-service`, `accounting-service`, all single GET routes).
- The tap-derived authoring approach and why (no OpenAPI spec on any of the four services).
- The retry-budget mechanism (`retryRatio: 0.2`, `minRetriesPerSecond: 10`) as the retry-storm
  safeguard.
- The layering diagram/explanation: mesh retry absorbs transient failures below the JVM;
  Resilience4j's circuit breaker and business-aware fallback methods (e.g.
  `findRestaurantFallback` mapping `RestaurantNotFoundException`) still own sustained-outage
  behavior — with a short comparison table (mesh: transport-level, transparent to app code,
  bounded retry budget, no domain-exception awareness; Resilience4j: business-aware fallback,
  circuit-breaker state machine, requires per-dependency Java config).
- The `ServiceProfile`-is-attached-to-the-destination-not-the-caller naming caveat from the spec.
- A summary of the captured evidence (link to
  `docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md`, inline the
  headline result comparing `EFFECTIVE_SUCCESS`/`ACTUAL_SUCCESS` with and without the profiles).
- Update the existing "Deferred to B3c" note left by the B3b section (added in the prior
  sub-project) to state B3c is now done, not deferred.

- [ ] **Step 3: Update `README.md`'s Book progress table**

Find the Ch.12 row (already extended for B1/B2/B3a/B3b) and append
`; B3c (mesh traffic management, ServiceProfiles) done — see docs/ARCHITECTURE.md` to the
existing cell text, following the same prose style as the prior additions. Since this is the last
§12.4 item, also update the row's overall Ch.12 status from "In progress" to "Done" if the table
has a separate status column (check the row's actual column structure before editing — do not
assume a column exists that isn't there).

- [ ] **Step 4: Update `CONTEXT.md`'s progress table, Current position, and Concept understanding**

Extend the Ch.12 row in the progress table analogously to Step 3. Update "Current position"'s
Ch.12 status line to mark B3c done and Ch.12 as a whole Done (following the same phrasing pattern
`CONTEXT.md`'s Ch.11 entry used when that chapter flipped to Done).

Add a new session-log entry (one line, following the existing entries' format) summarizing: the
four tap-derived `ServiceProfile`s, the GET-only retry scoping rationale, the retry-budget
values, and the headline fault-injection comparison result from the evidence file.

Per `CLAUDE.md`'s chapter-completion rule, review the "Concept understanding" section's
`Understood well`/`Needs more depth`/`Open questions` buckets: move any item there that this
sub-project resolves (e.g. an open question about mesh-level vs. application-level resilience, if
one exists) into `Understood well`, with a one-line note of what resolved it.

Update the footer `*Last updated*` line to describe Ch.12's completion, following the existing
footer format used for the Ch.11 completion entry.

- [ ] **Step 5: Full-repo grep sweep for anything the per-change rule missed**

```bash
grep -rln "ServiceProfile\|B3c\|mesh traffic management" --include="*.md" . \
  | grep -v "docs/session-\|docs/superpowers/plans/\|docs/superpowers/specs/"
```
Review each match; update any that still describe B3c as pending/future work.

- [ ] **Step 6: Commit**

```bash
git add docs/ARCHITECTURE.md README.md CONTEXT.md ftgo-order-service/README.md
git commit -m "docs: complete Ch.12 chapter-completion sweep for B3c (mesh traffic management)"
```

---

## Self-Review Notes

- **Spec coverage:** Task 1 covers the spec's Architecture section (ServiceProfile authoring,
  retry budget, Helm-template location, GET-only scoping) and the tap-derived-generation approach
  section. Task 2 covers the spec's full Verification section (baseline, per-callee fault
  injection, the with/without-ServiceProfile comparison). Task 3 covers the spec's Documentation
  section in full, including the chapter-completion sweep items specific to Ch.12 flipping to
  Done. The spec's explicit non-goals (no Java changes, no POST/PUT retries, no ServiceProfiles
  beyond order-service's four callees) are respected by every task above — no task touches
  application code or defines a non-GET route as retryable.
- **Placeholder scan:** Evidence-file templates in Tasks 1 and 2 contain `<fill in...>`/
  `<paste...>` markers, matching this project's existing evidence-file convention (B2, B3a) — the
  explicit output of a live cluster run performed during each task's own execution, not
  unresolved plan gaps. `<ORDER_ID>` in Task 2 is a genuine runtime value substituted during
  execution, not a plan placeholder. Task 1 Step 1's route-name divergence handling ("if the route
  name differs... use the literal path instead") is a legitimate "match live tool output" flag,
  not vague guidance — it names the exact fallback behavior.
- **Type/name consistency:** The four `ServiceProfile` names
  (`restaurant-service.ftgo.svc.cluster.local`, etc.) are used identically across Tasks 1 and 2.
  Route paths (`/restaurants/{id}`, `/tickets/order/{orderId}`, `/deliveries/order/{orderId}`,
  `/authorizations/order/{orderId}`) match the actual proxy source files read during planning.
  Ports (8082/8083/8084/8085/8086) match `k8s/ftgo/values.yaml`. The evidence file path
  (`docs/superpowers/plans/2026-08-29-ch12-b3c-mesh-traffic-management-evidence.md`) is created
  once in Task 1 and appended-to (never replaced) by Task 2, matching B2/B3a's evidence-file
  convention exactly.
