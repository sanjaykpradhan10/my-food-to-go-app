# Ch.12 §12.4 B3a: Linkerd Service Mesh (Install + Auto-mTLS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Install Linkerd on the existing `kind` cluster and get automatic, verified mutual TLS
between all meshed `ftgo` namespace app pods, with zero application code changes.

**Architecture:** Install the Linkerd control plane into its own `linkerd` namespace via the
Linkerd CLI, independent of the `k8s/ftgo` Helm chart's install/upgrade lifecycle. Annotate the
`ftgo` namespace (in `k8s/ftgo/templates/namespace.yaml`) for automatic proxy injection with
explicit, modest proxy resource requests/limits, so every pod scheduled into it gets a
`linkerd-proxy` sidecar on next rollout — no per-Deployment template changes needed. Roll out
injection incrementally (two services first, then the whole namespace) to avoid reproducing the
CPU-contention thundering herd Ch.12 B2 found on this same single-node cluster. Verify mTLS with
`linkerd viz tap`/`linkerd viz stat` against live traffic, captured as evidence the same way B2
captured its k6 rollout logs.

**Tech Stack:** Linkerd 2.x (CLI + control plane + viz extension), the existing `kind` cluster and
`k8s/ftgo` Helm chart, `kubectl`.

## Global Constraints

- Namespace is always `ftgo` (`k8s/ftgo/values.yaml`'s `global.namespace`).
- Registry is `localhost:5000/ftgo`; images are unchanged by this sub-project — B3a adds no new
  service images, only a sidecar container Linkerd injects at the cluster level.
- The Linkerd control plane is a cluster-level dependency, installed and managed independently of
  `helm upgrade --install ftgo ./k8s/ftgo` — the same relationship the chart already has with
  `kind` itself and the local image registry (see `k8s/README.md`'s Setup section).
- Mesh only the 13 app pods that participate in the request path: the 8 `businessServices`, the
  2 `gateways`, and the 3 `platformServices` (`service-registry`, `authorization-server`,
  `config-server`) — all defined in `k8s/ftgo/values.yaml`. Do NOT mesh MySQL, Kafka/Zookeeper,
  Elasticsearch/Logstash/Kibana/Filebeat, Prometheus/Grafana/Tempo, or GlitchTip+Postgres+Redis —
  these are stateful/infra services outside this sub-project's scope.
- Roll out proxy injection incrementally: verify on `order-service` + `restaurant-service` first,
  before annotating the whole `ftgo` namespace and restarting every pod simultaneously — B2's
  Task 5 found this single-node `kind` cluster's `ftgo-control-plane` Docker container (capped at
  10 CPU cores) prone to a CPU-starvation cascade when ~20+ JVM pods restart at once.
- Set explicit proxy resource requests/limits rather than relying on Linkerd's upstream defaults.
- No application code changes — this sub-project is entirely cluster/chart infrastructure.
- Documentation lands in the same commit as the code/config change it describes, per this
  project's `CLAUDE.md` convention; a full sweep (README.md, CONTEXT.md, docs/ARCHITECTURE.md)
  happens in the final task since B3a itself doesn't flip a chapter to Done (B3 as a whole isn't
  done until B3b/B3c also land), but the sub-project's own completion is still documented.

---

### Task 1: Install Linkerd CLI and control plane; add setup docs

**Files:**
- Modify: `k8s/README.md` (new "Install Linkerd (Ch.12 B3a)" section)
- Create: `/tmp/linkerd-check-task1.txt` (evidence capture, not committed — ignore per
  `.gitignore`'s existing scratch-file conventions; if `/tmp` isn't writable in the execution
  environment, use the project's designated scratch directory instead)

**Interfaces:**
- Consumes: nothing from earlier tasks (first task).
- Produces: a running, healthy Linkerd control plane in the `linkerd` namespace, and the
  `linkerd` CLI available on PATH — every later task's `linkerd viz tap`/`stat`/`check` commands
  depend on this.

- [ ] **Step 1: Install the Linkerd CLI**

Run:
```bash
curl --proto '=https' --tlsv1.2 -sSfL https://run.linkerd.io/install | sh
export PATH=$PATH:$HOME/.linkerd2/bin
```

Verify the CLI is present:
```bash
linkerd version --client
```
Expected: prints a client version (e.g. `Client version: stable-2.14.x`); no server version yet
since the control plane isn't installed.

- [ ] **Step 2: Pre-flight check against the running `kind` cluster**

```bash
linkerd check --pre
```
Expected: all pre-install checks pass (kubectl context is the `kind-ftgo` cluster, cluster is
reachable, no conflicting Linkerd installation already present). If `kubectl config current-context`
is not `kind-ftgo`, run `kubectl config use-context kind-ftgo` first — every command in this plan
assumes that context.

- [ ] **Step 3: Install the Linkerd control plane**

```bash
linkerd install --crds | kubectl apply -f -
linkerd install | kubectl apply -f -
```

- [ ] **Step 4: Wait for the control plane and verify health**

```bash
kubectl -n linkerd rollout status deploy --timeout=180s
linkerd check | tee /tmp/linkerd-check-task1.txt
```
Expected: `linkerd check` reports all checks passed, including
`linkerd-control-plane-proxy` and `control-plane-version` (or equivalent for the installed
version) with no `‼` failures. If a check fails on proxy image pulls, confirm `kind` can reach
the public internet (Linkerd's images are not built from this project's local registry) and
retry.

- [ ] **Step 5: Install the viz extension (verification tooling for later tasks)**

```bash
linkerd viz install | kubectl apply -f -
kubectl -n linkerd-viz rollout status deploy --timeout=180s
linkerd check --proxy | tee -a /tmp/linkerd-check-task1.txt
```
Expected: viz extension checks pass; `linkerd viz stat deploy -A` (run once here as a smoke test)
lists no meshed workloads yet, since no `ftgo` pods are injected.

- [ ] **Step 6: Document the install in `k8s/README.md`**

Add a new section after the existing "Install nginx-ingress" section:

```markdown
## Install Linkerd (Ch.12 B3a)

```bash
curl --proto '=https' --tlsv1.2 -sSfL https://run.linkerd.io/install | sh
export PATH=$PATH:$HOME/.linkerd2/bin
linkerd install --crds | kubectl apply -f -
linkerd install | kubectl apply -f -
kubectl -n linkerd rollout status deploy --timeout=180s
linkerd check
linkerd viz install | kubectl apply -f -
kubectl -n linkerd-viz rollout status deploy --timeout=180s
```

Linkerd's control plane (`linkerd` namespace) and viz extension (`linkerd-viz` namespace) are
installed independently of `helm upgrade --install ftgo ./k8s/ftgo` — a one-time cluster
dependency, the same relationship the chart already has with `kind` and the local registry. See
`docs/ARCHITECTURE.md`'s service mesh section for how the `ftgo` namespace opts into meshing.
```

- [ ] **Step 7: Commit**

```bash
git add k8s/README.md
git commit -m "docs: document Linkerd control-plane install (Ch.12 B3a)"
```

---

### Task 2: Annotate the `ftgo` namespace for auto-injection with pinned proxy resources

**Files:**
- Modify: `k8s/ftgo/templates/namespace.yaml`

**Interfaces:**
- Consumes: the running Linkerd control plane from Task 1 (the injector webhook it registers is
  what reads this annotation).
- Produces: a namespace-wide auto-injection policy — every pod scheduled into `ftgo` after a
  `helm upgrade` + rollout restart gets a `linkerd-proxy` sidecar with the pinned resource
  requests/limits set here. Task 3 relies on this annotation existing before restarting any pod.

- [ ] **Step 1: Add the injection and resource annotations**

Replace the full contents of `k8s/ftgo/templates/namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: {{ .Values.global.namespace }}
  annotations:
    # Auto-inject a linkerd-proxy sidecar into every pod scheduled here (Ch.12 B3a).
    # Explicit proxy resource pins rather than Linkerd's upstream defaults, since this
    # single-node kind cluster already showed CPU-starvation cascades under simultaneous
    # pod restarts (Ch.12 B2 Task 5) — a second uncapped container per pod is a needless
    # repeat of that risk.
    linkerd.io/inject: enabled
    config.linkerd.io/proxy-cpu-request: "50m"
    config.linkerd.io/proxy-cpu-limit: "100m"
    config.linkerd.io/proxy-memory-request: "20Mi"
    config.linkerd.io/proxy-memory-limit: "50Mi"
```

- [ ] **Step 2: Apply the chart change**

```bash
helm upgrade ftgo ./k8s/ftgo --namespace ftgo
kubectl get namespace ftgo -o jsonpath='{.metadata.annotations}'
```
Expected: the output includes `"linkerd.io/inject":"enabled"` and the four
`config.linkerd.io/proxy-*` keys. No pods restart yet — Kubernetes namespace annotations don't
retroactively affect already-running pods; injection only happens at pod creation time, which is
why Task 3 explicitly restarts specific Deployments rather than expecting this step alone to mesh
anything.

- [ ] **Step 3: Commit**

```bash
git add k8s/ftgo/templates/namespace.yaml
git commit -m "feat: annotate ftgo namespace for Linkerd auto-injection (Ch.12 B3a)"
```

---

### Task 3: Incrementally verify injection and mTLS on two services

**Files:**
- Create: `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`

**Interfaces:**
- Consumes: the namespace annotation from Task 2; `order-service` and `restaurant-service`
  Deployments already defined in `k8s/ftgo/templates/app-service.yaml` (unmodified by this task).
- Produces: the evidence file's initial section, extended in place by Task 5 with the full-mesh
  results — matching the pattern `docs/superpowers/plans/2026-08-15-ch12-zero-downtime-rollout-evidence.md`
  used, where a defect-finding run's evidence stays in the file as later sections are appended.

- [ ] **Step 1: Restart only `restaurant-service` and `order-service`**

```bash
kubectl rollout restart deployment/restaurant-service deployment/order-service -n ftgo
kubectl rollout status deployment/restaurant-service -n ftgo --timeout=180s
kubectl rollout status deployment/order-service -n ftgo --timeout=180s
```
Expected: both rollouts complete. If either times out, check
`kubectl get pods -n ftgo -l app=order-service` / `-l app=restaurant-service` for
`CrashLoopBackOff` before proceeding — do not continue to Step 2 with an unhealthy pod.

- [ ] **Step 2: Confirm 2/2 Ready (app container + injected proxy)**

```bash
kubectl get pods -n ftgo -l app=order-service -o wide
kubectl get pods -n ftgo -l app=restaurant-service -o wide
```
Expected: both pods show `2/2` in the `READY` column (previously `1/1` pre-injection).

- [ ] **Step 3: Trigger a real cross-service call and tap the connection**

In one terminal, start tapping `order-service`'s outbound traffic to `restaurant-service`:
```bash
linkerd viz tap deploy/order-service -n ftgo --to deploy/restaurant-service > /tmp/linkerd-tap-task3.log &
TAP_PID=$!
```

In another command, trigger a request that makes order-service call restaurant-service — the
simplest live trigger is order-service's existing `GET /orders/{id}/view` API-composition path
(Ch.7), which calls restaurant-service internally, but since no order exists yet on a freshly
injected cluster, instead trigger `restaurant-service` directly via its own healthcheck through
`order-service`'s Eureka-backed client is not exposed publicly — use the existing
Kubernetes-profile end-to-end test fixture creation flow instead:
```bash
kubectl exec -n ftgo deploy/order-service -- curl -s -o /dev/null -w "%{http_code}\n" \
  http://restaurant-service:8085/actuator/health
```
Expected: `200`. Let the tap run a few seconds after this call, then stop it:
```bash
sleep 5
kill $TAP_PID
cat /tmp/linkerd-tap-task3.log
```

- [ ] **Step 4: Confirm `tls=true` in the tap output**

```bash
grep -c "tls=true" /tmp/linkerd-tap-task3.log
```
Expected: at least 1. If the count is 0, check
`grep "tls=" /tmp/linkerd-tap-task3.log` for what value did appear (e.g. `tls=no_identity` or
`tls=disabled`) — this means injection or the identity issuer isn't working and must be resolved
before Task 4 rolls this out cluster-wide; do not proceed with a `tls=false`/`no_identity` result.

- [ ] **Step 5: Capture stat output and write the initial evidence file**

```bash
linkerd viz stat deploy/order-service deploy/restaurant-service -n ftgo
```

Create `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`:

```markdown
# Ch.12 B3a Linkerd service mesh — captured evidence

Date: <fill in from `date -u +%Y-%m-%d` when this task actually runs>

## Incremental verification: order-service + restaurant-service

After annotating the `ftgo` namespace for auto-injection (Task 2) and restarting only
`order-service` and `restaurant-service` (Task 3), both pods report `2/2 Ready`
(app container + `linkerd-proxy` sidecar).

`linkerd viz tap deploy/order-service -n ftgo --to deploy/restaurant-service`, captured while
`order-service` called `restaurant-service`'s `/actuator/health` in-cluster, shows:

```
<paste the actual tap output lines captured in Step 3, showing tls=true>
```

`linkerd viz stat deploy/order-service deploy/restaurant-service -n ftgo`:

```
<paste the actual stat table captured in this step>
```

This confirms mTLS is active between meshed pods before rolling injection out to the rest of the
`ftgo` namespace (Task 4).
```

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md
git commit -m "docs: capture Ch.12 B3a incremental mTLS verification evidence"
```

---

### Task 4: Roll out injection to the full `ftgo` namespace

**Files:**
- Modify: `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md` (append,
  do not replace, per the same pattern Task 3 established)

**Interfaces:**
- Consumes: the evidence file created in Task 3; the namespace annotation from Task 2; all
  Deployments across `k8s/ftgo/templates/app-service.yaml`, `mobile-gateway.yaml`,
  `public-gateway.yaml`, `service-registry.yaml`, `authorization-server.yaml`,
  `config-server.yaml` (none modified by this task — only restarted).
- Produces: the fully-meshed cluster state Task 5's e2e verification and final evidence write-up
  depend on.

- [ ] **Step 1: Restart every remaining app Deployment**

```bash
kubectl rollout restart deployment -n ftgo \
  -l 'app in (kitchen-service,consumer-service,accounting-service,delivery-service,order-history-service,audit-log-service,mobile-gateway,public-gateway,service-registry,authorization-server,config-server)'
```
If the `-l` selector's `in (...)` syntax isn't accepted by the installed `kubectl` version, list
each Deployment individually instead:
```bash
kubectl rollout restart deployment/kitchen-service deployment/consumer-service \
  deployment/accounting-service deployment/delivery-service \
  deployment/order-history-service deployment/audit-log-service \
  deployment/mobile-gateway deployment/public-gateway \
  deployment/service-registry deployment/authorization-server deployment/config-server -n ftgo
```

- [ ] **Step 2: Wait for each rollout, watching for the thundering-herd failure mode**

```bash
for d in kitchen-service consumer-service accounting-service delivery-service \
         order-history-service audit-log-service mobile-gateway public-gateway \
         service-registry authorization-server config-server; do
  kubectl rollout status deployment/$d -n ftgo --timeout=300s || echo "TIMEOUT: $d"
done
```
Expected: all rollouts complete within the timeout. If any pod enters `CrashLoopBackOff` driven
by probe timeouts under restart-induced CPU contention (the exact failure mode B2 Task 5 found
and fixed at `k8s/ftgo/templates/app-service.yaml:50-61`), this is a genuine finding, not a
transient blip to retry past — stop and report it rather than repeatedly restarting. B2's fix is
already in place on `main`, so a recurrence here would mean the proxy's own resource pins (Task
2) are still under-provisioned for this restart pattern; if that happens, raise the proxy CPU
request pinned in `namespace.yaml` and retry once, then treat a second failure as a blocker to
escalate rather than silently loosen further.

- [ ] **Step 3: Confirm 2/2 Ready across the whole namespace**

```bash
kubectl get pods -n ftgo -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[*].ready
```
Expected: every app pod shows `[true true]` (two containers, both ready). MySQL, Kafka,
Elasticsearch, and the other unmeshed infra pods still show a single-container ready state — this
is expected and correct, not a gap (Global Constraints explicitly excludes them).

- [ ] **Step 4: Confirm mesh-wide stat**

```bash
linkerd viz stat deploy -n ftgo
```
Expected: all 13 app Deployments (8 business services + 2 gateways + 3 platform services) appear
in the output with a `MESHED` column showing `1/1`. If any of the 13 shows `0/1`, that pod's
rollout restart from Step 1 didn't take — re-run Step 1 for that specific Deployment.

- [ ] **Step 5: Append the full-mesh results to the evidence file**

Append a new section to `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`:

```markdown
## Full-namespace rollout

All 13 app Deployments (8 business services, 2 gateways, 3 platform services) restarted and
confirmed `2/2 Ready` with no CrashLoopBackOff — the incremental rollout in Task 3, plus the
pinned proxy resource requests/limits from Task 2, avoided the CPU-starvation cascade Ch.12 B2
Task 5 found when this cluster's pods all restarted simultaneously.

`linkerd viz stat deploy -n ftgo`:

```
<paste the actual stat table captured above, showing all 13 Deployments MESHED 1/1>
```
```

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md
git commit -m "docs: capture Ch.12 B3a full-namespace mesh rollout evidence"
```

---

### Task 5: Re-run the Kubernetes-profile end-to-end suite; finalize evidence

**Files:**
- Modify: `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md` (append
  final conclusion section)

**Interfaces:**
- Consumes: the fully-meshed cluster from Task 4; the existing `ftgo-end-to-end-test` module's
  Kubernetes profile, added in Ch.12 B1 (base URLs pointed at nginx-ingress on host port 18000,
  per `k8s/README.md`'s ingress section).
- Produces: the final pass/fail confirmation this sub-project's success criteria require — no
  later task consumes this directly, but the completion ledger (per the
  subagent-driven-development process) records its outcome.

- [ ] **Step 1: Confirm nginx-ingress is still healthy**

```bash
kubectl get pods -n ingress-nginx
```
Expected: the ingress controller pod is `Running`/`1/1` (nginx-ingress itself is not meshed —
it isn't part of the `ftgo` namespace — so it needs no changes from this sub-project).

- [ ] **Step 2: Run the Kubernetes-profile end-to-end suite**

```bash
./gradlew :ftgo-end-to-end-test:e2eTest -Dspring.profiles.active=kubernetes
```
(Confirm the exact Gradle task name and profile-activation flag against
`ftgo-end-to-end-test`'s existing configuration — Ch.12 B1 already wired this profile; do not
invent a new one. If the profile is activated differently there, e.g. via a Gradle project
property or a different system property key, use that existing mechanism instead of the flag
shown here.)

Expected: the suite passes — the Create→Revise→Cancel Order journey completes successfully
through the now-meshed services, confirming injection didn't break inter-service calls (e.g. via
unexpected TLS termination issues or the sidecar delaying startup past a dependency's
`ftgo.waitFor` init-container check).

If the suite fails specifically on a startup-ordering timeout (not a business-logic assertion),
check whether the injected proxy's own startup delay is racing `ftgo.waitFor`'s TCP-port polling
init containers — this is a plausible integration gap between two separate startup-ordering
mechanisms (Linkerd's proxy init, and this chart's own dependency-wait helper) that the spec's
resource-risk section anticipated in general terms but didn't name specifically; document
whatever is found rather than silently retrying past it.

- [ ] **Step 3: Append the final conclusion to the evidence file**

Append to `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`:

```markdown
## Conclusion

Linkerd is installed, all 13 app pods in the `ftgo` namespace are meshed with automatic mutual
TLS (confirmed live via `linkerd viz tap` showing `tls=true` on inter-service calls), and the
existing Kubernetes-profile `ftgo-end-to-end-test` suite passes unchanged against the meshed
cluster — <fill in the actual pass/fail result and any notable findings from Step 2>. No
application code was changed; this sub-project is entirely cluster/chart infrastructure (a
namespace annotation plus a documented, independently-lifecycled control-plane install).
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md
git commit -m "docs: finalize Ch.12 B3a evidence with e2e verification result"
```

---

### Task 6: Documentation sweep (README.md, CONTEXT.md, docs/ARCHITECTURE.md)

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: the evidence file finalized in Task 5.
- Produces: nothing consumed by later tasks — this is the final task of B3a. (B3b and B3c are
  separate future sub-projects, each getting their own brainstorm→spec→plan cycle; this task
  documents B3a's own completion, not B3 as a whole.)

- [ ] **Step 1: Update README.md's Book progress table**

Find the Ch.12 row in `README.md`'s Book progress table (the same row extended for B1 and B2) and
append `; B3a (Linkerd service mesh, auto-mTLS) done — see docs/ARCHITECTURE.md` to the existing
cell text, following the exact prose style already used for the B1/B2 additions.

- [ ] **Step 2: Update CONTEXT.md's progress table and Current position**

In `CONTEXT.md`, extend the Ch.12 row in the progress table analogously to Step 1. Update the
"Current position" section's Ch.12 status line from "...B3 (service mesh) not yet started." to
"...B3a (Linkerd service mesh, auto-mTLS) done — see docs/ARCHITECTURE.md. B3b (mesh
observability) and B3c (mesh traffic management) not yet started."

Add a new Session log entry (one line, following the existing entries' format) summarizing: the
Linkerd control-plane install (independent of the `k8s/ftgo` chart lifecycle), the namespace-wide
auto-injection annotation with pinned proxy resources (avoiding a repeat of B2's CPU-contention
incident), the incremental-then-full rollout sequencing, and the captured mTLS evidence (tap
`tls=true`, e2e suite result).

Update the footer `*Last updated*` line to reference this sub-project's completion, following the
existing footer's format from the B1/B2 updates.

- [ ] **Step 3: Add a new subsection to docs/ARCHITECTURE.md**

Add a new subsection directly after the "Zero-downtime rolling deployment (§12.4.4)" subsection
added in B2, titled `### Service mesh — Linkerd install and auto-mTLS (§12.4, B3a)`, covering:

- Why Linkerd was chosen over Istio for this cluster (lighter proxy footprint, given B2's
  CPU-contention history on this single-node `kind` cluster — see the design spec's rationale at
  `docs/superpowers/specs/2026-08-16-ch12-b3a-service-mesh-linkerd-design.md`).
- The control plane's independent lifecycle (installed via `linkerd install`, not part of
  `helm upgrade --install ftgo`).
- The namespace-annotation injection mechanism (`k8s/ftgo/templates/namespace.yaml`) and why it
  needed no per-Deployment template changes.
- The pinned proxy resource requests/limits and the incremental-rollout mitigation for B2's
  known CPU-contention risk.
- A summary of the captured evidence (link to
  `docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`, inline the
  headline result: all 13 app pods meshed, `tls=true` confirmed live, e2e suite result).
- What's explicitly deferred to B3b (mesh observability dashboard) and B3c (mesh-level traffic
  management via `ServiceProfiles`, contrasted with the existing Resilience4j-based resilience).

- [ ] **Step 4: Commit**

```bash
git add README.md CONTEXT.md docs/ARCHITECTURE.md
git commit -m "docs: document Ch.12 sub-project B3a (Linkerd service mesh, auto-mTLS)"
```

---

## Self-Review Notes

- **Spec coverage:** Task 1 covers control-plane install; Task 2 covers the namespace-injection
  mechanism and the spec's resource-risk mitigation (pinned requests/limits); Tasks 3-4 cover the
  spec's incremental-rollout mitigation and the mesh-scope constraint (13 app pods, not infra);
  Task 5 covers the spec's e2e-verification success criterion; Task 6 covers the spec's
  documentation-sweep success criterion. The spec's explicit out-of-scope items (dashboard,
  ServiceProfiles, meshing infra services, app code changes) are untouched by every task above.
- **Placeholder scan:** Evidence-file templates in Tasks 3-5 contain `<fill in...>`/`<paste...>`
  markers, matching B2's evidence-file pattern exactly — these are the explicit output of a live
  cluster run performed during each task's own execution, not unresolved plan gaps. Task 5 Step 2
  flags one genuine unknown (the exact e2e Gradle task/profile invocation) with an explicit
  instruction to confirm against existing configuration rather than inventing one — this is a
  legitimate "verify before use" flag, not a placeholder.
- **Type/name consistency:** `k8s/ftgo/templates/namespace.yaml`'s annotation keys
  (`linkerd.io/inject`, `config.linkerd.io/proxy-*`) are referenced identically in Tasks 2-4. The
  evidence file path (`docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md`)
  is created once in Task 3 and appended-to (never replaced) by Tasks 4 and 5, matching B2's
  evidence-file convention exactly. All 13 app-pod names used in Task 4's rollout-restart commands
  match `k8s/ftgo/values.yaml`'s `businessServices`/`gateways`/`platformServices` lists exactly.
