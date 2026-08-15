# Ch.12 §12.4 Sub-project B2: Zero-Downtime Rolling Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove, with captured evidence, that `k8s/ftgo`'s existing rolling-update configuration delivers a genuinely zero-downtime deploy of a real code change to `ftgo-order-service`, and demonstrate rollback.

**Architecture:** Add an optional per-service `imageTag` override to the Helm chart's `businessServices` list (currently a single global tag). Add a trivial, observable `X-Service-Version` response header to `ftgo-order-service` via a `jakarta.servlet.Filter`, driven by a new `ftgo.service-version` property. Add a standalone (non-Helm-managed) k6 Job manifest that hammers `order-service`'s `/actuator/health` in-cluster throughout a live rollout and rollback, capturing pass/fail evidence.

**Tech Stack:** Helm (existing `k8s/ftgo` chart), Spring Boot `jakarta.servlet.Filter`, k6 (`grafana/k6` image) as a plain Kubernetes Job, `kubectl rollout status`/`undo`.

## Global Constraints

- Namespace is always `ftgo` (per the existing chart's `global.namespace` default and every existing manifest in `k8s/`).
- `order-service`'s in-cluster Service name is `order-service`, port `8082` (from `k8s/ftgo/values.yaml`'s `businessServices` entry) — this is the address the k6 Job targets, not the ingress host port 18000.
- The local kind image registry is `localhost:5000/ftgo` (`values.yaml` `global.imageRegistry`); images are pushed there via `k8s/scripts/build-and-push.sh` (check this script's existing usage pattern before Task 2 — do not invent a new build/push mechanism).
- Every other business service must be completely unaffected by this change — no other chart template, values entry, or service's Java code changes.
- Follow existing code-comment convention: comments explain *why*, not *what* (see root `CLAUDE.md`).
- Documentation updates (`README.md`, `CONTEXT.md`, `docs/ARCHITECTURE.md`) land in the same commit/PR as the code change they describe, per root `CLAUDE.md`.

---

### Task 1: Add `X-Service-Version` response header to ftgo-order-service

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/config/ServiceVersionHeaderFilter.java`
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/config/ServiceVersionHeaderFilterTest.java`
- Modify: `ftgo-order-service/src/main/resources/application.yml`

**Interfaces:**
- Produces: an `X-Service-Version` HTTP response header on every response from `ftgo-order-service`, value sourced from the `ftgo.service-version` Spring property (default `1.0.0`).

- [ ] **Step 1: Write the failing unit test**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/config/ServiceVersionHeaderFilterTest.java`:

```java
package com.sanjay.ftgo.order.config;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ServiceVersionHeaderFilterTest {

    @Test
    void setsConfiguredVersionOnResponseHeader() throws Exception {
        ServiceVersionHeaderFilter filter = new ServiceVersionHeaderFilter("1.1.0");
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        assertThat(response.getHeader("X-Service-Version")).isEqualTo("1.1.0");
        verify(chain).doFilter(request, response);
    }

    @Test
    void defaultsToUnknownWhenPropertyMissing() throws Exception {
        ServiceVersionHeaderFilter filter = new ServiceVersionHeaderFilter(null);
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        assertThat(response.getHeader("X-Service-Version")).isEqualTo("unknown");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.config.ServiceVersionHeaderFilterTest"`
Expected: FAIL (compilation error — `ServiceVersionHeaderFilter` does not exist yet)

- [ ] **Step 3: Write minimal implementation**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/config/ServiceVersionHeaderFilter.java`:

```java
package com.sanjay.ftgo.order.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;

import java.io.IOException;

// Exists solely as this sub-project's (Ch.12 B2) zero-downtime rollout evidence: k6 traces this
// header across a live rolling update to prove requests are never dropped and the version
// transition (e.g. 1.0.0 -> 1.1.0) is clean. Not a general-purpose versioning mechanism.
@Component
public class ServiceVersionHeaderFilter extends GenericFilterBean {

    private final String serviceVersion;

    public ServiceVersionHeaderFilter(@Value("${ftgo.service-version:unknown}") String serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        ((HttpServletResponse) response).setHeader("X-Service-Version", serviceVersion);
        chain.doFilter(request, response);
    }
}
```

- [ ] **Step 4: Add the property to application.yml**

Modify `ftgo-order-service/src/main/resources/application.yml`, adding at the top level (alongside the existing `server:` and `outbox:` blocks):

```yaml
ftgo:
  service-version: "1.0.0"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.config.ServiceVersionHeaderFilterTest"`
Expected: PASS (2 tests)

- [ ] **Step 6: Run the full ftgo-order-service test suite to check for regressions**

Run: `./gradlew :ftgo-order-service:test`
Expected: BUILD SUCCESSFUL, no new failures (pre-existing `contextLoads()`-style failures, if any, are out of scope — compare against `git stash` baseline if any new failure appears)

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/config/ServiceVersionHeaderFilter.java \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/config/ServiceVersionHeaderFilterTest.java \
        ftgo-order-service/src/main/resources/application.yml
git commit -m "feat: add X-Service-Version response header to order-service

Zero-downtime rollout evidence for Ch.12 B2: k6 traces this header across
a live rolling update to prove the version transition is clean and no
requests are dropped."
```

---

### Task 2: Add per-service `imageTag` override to the Helm chart

**Files:**
- Modify: `k8s/ftgo/values.yaml`
- Modify: `k8s/ftgo/templates/app-service.yaml`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: an optional `imageTag` field on any `businessServices` entry in `values.yaml`, honored by `app-service.yaml`'s image reference. Absent on an entry, behavior is byte-for-byte identical to today (falls through to `global.imageTag`).

- [ ] **Step 1: Modify the image line in app-service.yaml**

In `k8s/ftgo/templates/app-service.yaml`, change line 20 from:

```yaml
          image: "{{ $.Values.global.imageRegistry }}/ftgo-{{ .name }}:{{ $.Values.global.imageTag }}"
```

to:

```yaml
          image: "{{ $.Values.global.imageRegistry }}/ftgo-{{ .name }}:{{ .imageTag | default $.Values.global.imageTag }}"
```

- [ ] **Step 2: Verify the default chart still renders identically (no imageTag override present)**

Run: `helm template k8s/ftgo | grep -A1 "name: order-service$" | grep image:`
Expected: shows `localhost:5000/ftgo/ftgo-order-service:local` (unchanged from before this task — `default` falls through since no entry sets `imageTag` yet)

- [ ] **Step 3: Add a commented-out example in values.yaml (documentation, not active config)**

Modify `k8s/ftgo/values.yaml`'s `order-service` entry (lines 49-57) to add a comment showing the override syntax, directly above the entry:

```yaml
  # imageTag: per-entry override (falls back to global.imageTag when unset) — used by
  # Ch.12 B2's zero-downtime rollout demo to move only order-service to a new version
  # without retagging every other business service. Example: imageTag: "1.1.0"
  - name: order-service
    port: 8082
    db: ftgo_order
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}, {name: restaurant-service, port: 8085}]
    extraEnv:
      OUTBOX_PUBLISH_MODE: polling
      SAGA_MODE: choreography
      PERSISTENCE_MODE: jpa
```

- [ ] **Step 4: Verify an explicit override renders correctly**

Run: `helm template k8s/ftgo --set 'businessServices[1].imageTag=1.1.0' | grep -A1 "name: order-service$" | grep image:`

(Confirm index 1 is `order-service` in the current `businessServices` list order in `values.yaml` before running — it is `restaurant-service` (0), `order-service` (1) as of this plan's writing; adjust the index if the list has changed.)

Expected: shows `localhost:5000/ftgo/ftgo-order-service:1.1.0`

Run the same command without the `--set` flag again to confirm it still falls back to `local`.

- [ ] **Step 5: Commit**

```bash
git add k8s/ftgo/values.yaml k8s/ftgo/templates/app-service.yaml
git commit -m "feat: support per-service imageTag override in businessServices chart values

Lets one business service move to a new image version independently of
the shared global.imageTag — needed for Ch.12 B2's zero-downtime rollout
demo, which only redeploys order-service."
```

---

### Task 3: Build and push order-service:1.0.0 and order-service:1.1.0 images

**Files:**
- None (build/deploy actions only — no source changes in this task)

**Interfaces:**
- Consumes: `ServiceVersionHeaderFilter`/`ftgo.service-version` from Task 1, `imageTag` override from Task 2.
- Produces: two tagged images (`localhost:5000/ftgo/ftgo-order-service:1.0.0`, `localhost:5000/ftgo/ftgo-order-service:1.1.0`) in the local kind registry, differing only in the `ftgo.service-version` value baked into `application.yml`.

- [ ] **Step 1: Confirm the kind cluster and local registry from B1 are still up**

Run: `kubectl get nodes` and `docker ps --filter name=kind-registry`
Expected: one `ftgo-control-plane` node `Ready`, `kind-registry` container running. If either is missing, re-run B1's cluster setup (`k8s/scripts/setup-cluster.sh`) before continuing — this task assumes a live cluster, it does not recreate one.

- [ ] **Step 2: Build and push the 1.0.0 baseline image**

Confirm `ftgo.service-version` in `application.yml` currently reads `1.0.0` (it does, from Task 1 Step 4). Build and tag:

```bash
docker build -t localhost:5000/ftgo/ftgo-order-service:1.0.0 -f ftgo-order-service/Dockerfile .
docker push localhost:5000/ftgo/ftgo-order-service:1.0.0
```

(Use whatever build context/Dockerfile path `k8s/scripts/build-and-push.sh` already uses for `order-service` — read that script first and match its exact `docker build` invocation rather than guessing the context path; the command above is illustrative.)

- [ ] **Step 3: Bump the version property and build the 1.1.0 image**

Modify `ftgo-order-service/src/main/resources/application.yml`, changing:

```yaml
ftgo:
  service-version: "1.0.0"
```

to:

```yaml
ftgo:
  service-version: "1.1.0"
```

Build and tag:

```bash
docker build -t localhost:5000/ftgo/ftgo-order-service:1.1.0 -f ftgo-order-service/Dockerfile .
docker push localhost:5000/ftgo/ftgo-order-service:1.1.0
```

- [ ] **Step 4: Verify both tags are present in the local registry**

Run: `curl -s http://localhost:5000/v2/ftgo/ftgo-order-service/tags/list`
Expected: `{"name":"ftgo/ftgo-order-service","tags":["1.0.0","1.1.0"]}` (order may vary)

- [ ] **Step 5: Commit the version bump**

```bash
git add ftgo-order-service/src/main/resources/application.yml
git commit -m "chore: bump order-service ftgo.service-version to 1.1.0

Second build for Ch.12 B2's zero-downtime rollout demo — 1.0.0 stays
available in the local registry as the rollback target."
```

---

### Task 4: Add standalone k6 rollout-verification Job manifest

**Files:**
- Create: `k8s/verification/k6-rollout-check-configmap.yaml`
- Create: `k8s/verification/k6-rollout-check-job.yaml`
- Create: `k8s/README.md` (modify — add a "Zero-downtime rollout verification" section)

**Interfaces:**
- Consumes: `order-service` Service (name `order-service`, port `8082`, namespace `ftgo`) from the existing chart; `/actuator/health` endpoint (permitAll, no auth) from Task 1's target service.
- Produces: a `kubectl apply`-able Job that runs for a fixed duration hammering `order-service`, printing a k6 summary to its logs (readable via `kubectl logs job/k6-rollout-check`) showing per-response HTTP status and the `X-Service-Version` header value.

This Job is deliberately **not** placed under `k8s/ftgo/templates/` — Helm applies everything in that directory on every `helm upgrade --install`, and a `batch/v1` Job's `spec.template` is immutable, so a Helm-managed copy would break every subsequent `helm upgrade` once the Job exists. It lives in a sibling `k8s/verification/` directory instead, applied and deleted manually per demo run.

- [ ] **Step 1: Write the k6 script as a ConfigMap**

Create `k8s/verification/k6-rollout-check-configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: k6-rollout-check-script
  namespace: ftgo
data:
  rollout-check.js: |
    import http from 'k6/http';
    import { check } from 'k6';

    export const options = {
      scenarios: {
        rollout_check: {
          executor: 'constant-vus',
          vus: 5,
          duration: __ENV.K6_DURATION || '60s',
        },
      },
    };

    export default function () {
      const res = http.get('http://order-service:8082/actuator/health');
      check(res, {
        'status is 200': (r) => r.status === 200,
        'has X-Service-Version header': (r) => r.headers['X-Service-Version'] !== undefined,
      });
      console.log(`status=${res.status} version=${res.headers['X-Service-Version']}`);
    }
```

- [ ] **Step 2: Write the Job manifest**

Create `k8s/verification/k6-rollout-check-job.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: k6-rollout-check
  namespace: ftgo
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: k6
          image: grafana/k6:0.55.0
          args: ["run", "/scripts/rollout-check.js"]
          env:
            - name: K6_DURATION
              value: "90s"
          volumeMounts:
            - name: script
              mountPath: /scripts
      volumes:
        - name: script
          configMap:
            name: k6-rollout-check-script
```

- [ ] **Step 3: Dry-run validate both manifests**

Run: `kubectl apply --dry-run=client -f k8s/verification/k6-rollout-check-configmap.yaml -f k8s/verification/k6-rollout-check-job.yaml`
Expected: both resources print `... created (dry run)` with no validation errors

- [ ] **Step 4: Smoke-test the Job against the live cluster (order-service still on 1.0.0 at this point)**

```bash
kubectl apply -f k8s/verification/k6-rollout-check-configmap.yaml
kubectl apply -f k8s/verification/k6-rollout-check-job.yaml
kubectl wait --for=condition=complete job/k6-rollout-check -n ftgo --timeout=120s
kubectl logs job/k6-rollout-check -n ftgo | tail -30
```

Expected: log output shows `status=200 version=1.0.0` repeated, and k6's own summary at the end reports `checks_succeeded: 100.00%`.

Clean up after the smoke test so Task 5's real run starts fresh:

```bash
kubectl delete job/k6-rollout-check -n ftgo
```

- [ ] **Step 5: Document the verification tool in k8s/README.md**

Modify `k8s/README.md`, adding a new section (placed after the existing deployment-instructions content, matching that file's existing heading style):

```markdown
## Zero-downtime rollout verification (Ch.12 B2)

`k8s/verification/k6-rollout-check-job.yaml` is a standalone k6 Job — deliberately kept
outside `k8s/ftgo/templates/` since a Job's `spec.template` is immutable and Helm applies
every template on each `helm upgrade`, which would break subsequent upgrades once the Job
exists. Apply/delete it manually around a rollout:

    kubectl apply -f k8s/verification/k6-rollout-check-configmap.yaml
    kubectl apply -f k8s/verification/k6-rollout-check-job.yaml
    # ... trigger the rollout in another terminal ...
    kubectl logs -f job/k6-rollout-check -n ftgo
    kubectl delete job/k6-rollout-check -n ftgo   # before re-running
```

It hits `order-service`'s `/actuator/health` in-cluster (no auth required) for a fixed
duration, logging the HTTP status and `X-Service-Version` response header on every request —
a clean rollout shows 100% `status=200` and a gap-free transition between version values.
```

- [ ] **Step 6: Commit**

```bash
git add k8s/verification/k6-rollout-check-configmap.yaml k8s/verification/k6-rollout-check-job.yaml k8s/README.md
git commit -m "feat: add standalone k6 Job to verify zero-downtime rollouts

Kept outside k8s/ftgo/templates/ deliberately -- Job specs are immutable
and Helm applies every template on each upgrade, which would break future
helm upgrades once the Job existed once."
```

---

### Task 5: Run the live zero-downtime rollout and rollback demo, capture evidence

**Files:**
- Create: `docs/superpowers/plans/2026-08-15-ch12-zero-downtime-rollout-evidence.md`

**Interfaces:**
- Consumes: everything from Tasks 1-4 (both image tags in the registry, the `imageTag` override, the k6 Job).
- Produces: a captured evidence file showing the rollout was zero-downtime and rollback worked — this is the sub-project's acceptance artifact, referenced by Task 6's documentation.

- [ ] **Step 1: Confirm order-service is currently on 1.0.0**

Run: `helm template k8s/ftgo | grep -A1 "name: order-service$" | grep image:` (no override set yet)
Expected: `localhost:5000/ftgo/ftgo-order-service:local` — this is B1's original deploy, not yet pointed at either new tag. Deploy the 1.0.0 baseline explicitly first:

```bash
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --set 'businessServices[1].imageTag=1.0.0'
kubectl rollout status deployment/order-service -n ftgo --timeout=120s
```

(Confirm index 1 is still `order-service` — see Task 2 Step 4's note.)

- [ ] **Step 2: Start the k6 Job**

```bash
kubectl apply -f k8s/verification/k6-rollout-check-configmap.yaml
kubectl apply -f k8s/verification/k6-rollout-check-job.yaml
```

- [ ] **Step 3: Trigger the rollout to 1.1.0 while k6 is running**

Immediately (k6's Job runs for 90s per Task 4's `K6_DURATION`):

```bash
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --set 'businessServices[1].imageTag=1.1.0'
kubectl rollout status deployment/order-service -n ftgo --timeout=120s
```

- [ ] **Step 4: Wait for k6 to finish and capture its full log output**

```bash
kubectl wait --for=condition=complete job/k6-rollout-check -n ftgo --timeout=120s
kubectl logs job/k6-rollout-check -n ftgo > /tmp/k6-rollout-forward.log
cat /tmp/k6-rollout-forward.log
```

Inspect the output: every line must read `status=200 ...`; scan for the point where `version=1.0.0` lines stop and `version=1.1.0` lines start — there must be no non-200 status between them. If any non-200 status appears, this is a genuine defect (readinessProbe timing, chart resource limits, etc.) — stop and investigate per this plan's Global Constraints before proceeding; do not weaken or skip the check.

- [ ] **Step 5: Clean up the forward-rollout k6 Job**

```bash
kubectl delete job/k6-rollout-check -n ftgo
```

- [ ] **Step 6: Demonstrate rollback with a second, shorter k6 burst**

```bash
kubectl apply -f k8s/verification/k6-rollout-check-configmap.yaml
kubectl apply -f k8s/verification/k6-rollout-check-job.yaml
kubectl rollout undo deployment/order-service -n ftgo
kubectl rollout status deployment/order-service -n ftgo --timeout=120s
kubectl wait --for=condition=complete job/k6-rollout-check -n ftgo --timeout=120s
kubectl logs job/k6-rollout-check -n ftgo > /tmp/k6-rollback.log
cat /tmp/k6-rollback.log
kubectl delete job/k6-rollout-check -n ftgo
```

Inspect: every line `status=200`, and `version` transitions cleanly from `1.1.0` back to `1.0.0` with no non-200 status.

- [ ] **Step 7: Write the evidence file**

Create `docs/superpowers/plans/2026-08-15-ch12-zero-downtime-rollout-evidence.md` containing:

```markdown
# Ch.12 B2 zero-downtime rollout — captured evidence

Date: <fill in actual run date>

## Forward rollout (1.0.0 -> 1.1.0)

Total requests: <count from /tmp/k6-rollout-forward.log>
Non-200 responses: <count, must be 0>
Version transition: <paste the last few 1.0.0 lines and first few 1.1.0 lines,
showing no gap and no non-200 status between them>

## Rollback (1.1.0 -> 1.0.0)

Total requests: <count from /tmp/k6-rollback.log>
Non-200 responses: <count, must be 0>
Version transition: <paste the last few 1.1.0 lines and first few 1.0.0 lines>

## Conclusion

<one paragraph: the rollout and rollback were both zero-downtime per k6's evidence,
readinessProbe gating + the default surge-1/unavailable-0 RollingUpdate strategy at
replicas:1 delivered exactly the behavior described in book §12.4.4.>
```

Fill in the actual numbers and log excerpts from Steps 4 and 6 — do not fabricate placeholder values.

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/plans/2026-08-15-ch12-zero-downtime-rollout-evidence.md
git commit -m "docs: capture Ch.12 B2 zero-downtime rollout and rollback evidence"
```

---

### Task 6: Documentation sweep (README.md, CONTEXT.md, docs/ARCHITECTURE.md)

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: the evidence file from Task 5, the `imageTag` mechanism from Task 2, the k6 Job from Task 4.
- Produces: nothing consumed by later tasks — this is the final task.

- [ ] **Step 1: Update README.md's Book progress table**

Find the Ch.12 row in `README.md`'s Book progress table (matching the row already updated for sub-project B1 in PR #33) and extend its description to mention B2 is done, following the exact same prose style/format already used for the B1 addition — e.g. append `; B2 (zero-downtime rolling deployment, §12.4.4) done — see docs/ARCHITECTURE.md` to the existing cell text.

- [ ] **Step 2: Update CONTEXT.md's Book structure & progress table and Current position**

In `CONTEXT.md`, find the Ch.12 row (same row updated for B1) and extend it analogously to Step 1. In the "Current position" section, update the Ch.12 status line (the one reading "...B2 (zero-downtime) and B3 (service mesh) not yet started.") to instead read "...B2 (zero-downtime rolling deployment) done — see docs/ARCHITECTURE.md. B3 (service mesh) not yet started."

Add a new Session log entry (one line, following the existing entries' format) summarizing this sub-project: the `X-Service-Version` header mechanism, the per-service `imageTag` override, the standalone k6 Job kept outside `templates/` and why, and a one-sentence summary of the captured evidence result (zero dropped requests on both the forward rollout and the rollback).

Update the footer `*Last updated*` line to reference this sub-project's completion, following the existing footer's format from the B1 update.

- [ ] **Step 3: Add a new subsection to docs/ARCHITECTURE.md**

Add a new subsection directly after the existing "Kubernetes deployment (Ch.12, §12.4)" section (added in PR #33), titled `### Zero-downtime rolling deployment (§12.4.4)`, covering:

- How the existing `readinessProbe` (already present on every business service, `app-service.yaml`) is what makes `kubectl rollout`/`helm upgrade` zero-downtime: Kubernetes won't terminate an old pod until the new one passes it.
- The default `RollingUpdate` strategy (`maxSurge: 25%`, `maxUnavailable: 25%`) at `replicas: 1` rounding to surge-1/unavailable-0 — no chart change was needed for the mechanism itself.
- The one real gap this sub-project closed: `global.imageTag` was previously a single value shared by all business services; `businessServices[].imageTag` (Task 2) lets one service move independently.
- The `X-Service-Version` header (Task 1) and the standalone k6 verification Job (Task 4, `k8s/verification/`, deliberately outside Helm's `templates/` because Job specs are immutable) as the demo/verification mechanism.
- A summary of the captured evidence (link to `docs/superpowers/plans/2026-08-15-ch12-zero-downtime-rollout-evidence.md`, and inline the headline numbers: zero non-200 responses across both the forward rollout and rollback, clean version transitions).
- `kubectl rollout undo deployment/order-service` as the rollback mechanism, with Kubernetes' rollout history making it a single command.

- [ ] **Step 4: Commit**

```bash
git add README.md CONTEXT.md docs/ARCHITECTURE.md
git commit -m "docs: document Ch.12 sub-project B2 (zero-downtime rolling deployment)"
```

---

## Self-Review Notes

- **Spec coverage:** Task 2 covers spec §1 (imageTag override), Task 1 covers §2 (version header), Task 4 covers §3 (k6 Job), Task 5 covers §4 (demo procedure), Task 6 covers §5 (documentation). Out-of-scope items (autoscaling, replica bump, CI integration, other services) are untouched by every task above.
- **Placeholder scan:** Task 5's evidence file template contains `<fill in...>` markers, but these are explicitly the *output* of a live cluster run performed during that task's own execution (Step 7 explicitly instructs "do not fabricate placeholder values" and to fill them from Steps 4/6's actual captured logs) — not an unresolved plan gap.
- **Type/name consistency:** `ftgo.service-version` (Task 1 Step 4, Task 3 Step 3) and `ServiceVersionHeaderFilter`/`X-Service-Version` (Task 1) are used identically everywhere they recur (Tasks 3, 4, 5, 6). `businessServices[1].imageTag` (Task 2, Task 5) is flagged twice as index-order-dependent with an explicit instruction to verify before use, since Helm list `--set` addressing is fragile to list reordering.
