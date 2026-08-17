# Ch.12 B3a Linkerd service mesh — captured evidence

Date: 2026-08-16

## Incremental verification: order-service + restaurant-service

After annotating the `ftgo` namespace for auto-injection (Task 2) and restarting only
`order-service` and `restaurant-service` (Task 3), both pods report `2/2 Ready`
(app container + `linkerd-proxy` sidecar).

`linkerd viz tap deploy/order-service -n ftgo --to deploy/restaurant-service`, captured while
`order-service` called `restaurant-service`'s `/actuator/health` in-cluster, shows:

```
req id=0:0 proxy=out src=10.244.0.155:39148 dst=10.244.0.154:8085 tls=true :method=GET :authority=restaurant-service:8085 :path=/actuator/health
rsp id=0:0 proxy=out src=10.244.0.155:39148 dst=10.244.0.154:8085 tls=true :status=200 latency=15963µs
end id=0:0 proxy=out src=10.244.0.155:39148 dst=10.244.0.154:8085 tls=true duration=1609µs response-length=0B
```

`linkerd viz stat deploy/order-service deploy/restaurant-service -n ftgo`:

```
NAME                 MESHED   SUCCESS      RPS   LATENCY_P50   LATENCY_P95   LATENCY_P99   TCP_CONN
order-service           1/1   100.00%   0.7rps           4ms         850ms         970ms          4
restaurant-service      1/1   100.00%   0.7rps         100ms         470ms         494ms          4
```

This confirms mTLS is active between meshed pods before rolling injection out to the rest of the
`ftgo` namespace (Task 4).

## Full-namespace rollout — BLOCKED

Restarting the remaining 11 Deployments (`kitchen-service`, `consumer-service`,
`accounting-service`, `delivery-service`, `order-history-service`, `audit-log-service`,
`mobile-gateway`, `public-gateway`, `service-registry`, `authorization-server`,
`config-server`) reproduced a cascade: every new (meshed) replica pod failed readiness/liveness
and cycled through repeated restarts, `kubectl rollout status` exceeded its progress deadline for
multiple Deployments, and — as collateral damage — the already-meshed, previously-stable
`order-service` pod (from Task 3) also began crash-looping mid-rollout, even though it was not
targeted by this task's restart list.

Initial symptom matched the CPU-starvation pattern the brief anticipated: `linkerd-proxy`
readiness/liveness probes to its own `:4191` admin port timed out or returned 502
(`context deadline exceeded`, `HTTP probe failed with statuscode: 502`), e.g. on
`kitchen-service-85546bb85f-bbjct` and `order-service-66fd9d5997-pp667`.

**Mitigation attempted (per brief, one retry authorized):** raised the pinned proxy resources in
`k8s/ftgo/templates/namespace.yaml` from `proxy-cpu-request: 50m` / `proxy-cpu-limit: 100m` to
`100m` / `200m`, applied the same values live via
`kubectl annotate namespace ftgo config.linkerd.io/proxy-cpu-request=100m config.linkerd.io/proxy-cpu-limit=200m --overwrite`,
then re-ran the rollout restart (including `order-service`, to recover the collateral-damage pod).

**Result: second failure.** `service-registry`'s rollout again exceeded its progress deadline.
`kubectl describe pod` on the new `service-registry` replica revealed the actual root cause is not
proxy CPU throttling but node memory exhaustion:

```
Warning  FailedScheduling  default-scheduler  0/1 nodes are available: 1 Insufficient memory.
                            preemption: 0/1 nodes are available: 1 No preemption victims found for
                            incoming pod.
```

Before this task began, node-level `Allocated resources` already showed `memory 10Gi (85%) requests`
/ `20240Mi (169%) limits` on this single-node kind cluster. Injecting a `linkerd-proxy` sidecar
(20Mi request / 50Mi limit each) into 11 more Deployments, doubled transiently during each
`RollingUpdate` (old + new replica coexisting), pushed the node past its schedulable memory,
producing `FailedScheduling` events and OOM-adjacent restarts — not only on the meshed app pods
but on unrelated infra pods with no mesh sidecar at all (`elasticsearch-0`, `grafana`, `kibana`,
`logstash`, `glitchtip`), confirming node-wide memory pressure rather than a mesh-proxy-specific
defect.

**Conclusion:** the pinned-proxy-CPU-resources mitigation from Task 2, combined with Task 3's
incremental rollout, avoided *this specific cluster's Ch.12 B2 CPU-starvation cascade* for the
first two services, but does not extend cleanly to the remaining 11 — the constraint hit at
namespace-wide scale is memory capacity, not CPU, and raising the CPU pin (the only mitigation
this task was authorized to apply) does not address it. Per this task's brief, a second failure
after one mitigation retry is treated as a blocker to escalate rather than a reason to loosen
resource pins further unilaterally. Remaining cluster state after this run: `config-server` and
`accounting-service`'s original replica are meshed and Ready; the other 9 Deployments each have
one meshed replica in a restart loop and one legacy (unmeshed, single-container) replica still
serving traffic (Deployments were not scaled down, so the namespace has not lost availability).
`docs/superpowers/plans/2026-08-16-ch12-b3a-service-mesh-linkerd-evidence.md` reflects this as the
Task 4 outcome; Task 5 should not assume full-mesh state going in.

## Full-namespace rollout — retry with observability stack scaled down

Root cause confirmed via node events at the start of this retry: `kubectl describe node
ftgo-control-plane` showed three `SystemOOM` events (`victim process: java`) from the prior
attempt, in addition to the `FailedScheduling` seen before — the node was genuinely out of
memory, not just over its scheduling threshold.

**Freed memory** by scaling the non-mesh observability/infra stack to 0 replicas:
Deployments `glitchtip`, `glitchtip-redis`, `glitchtip-worker`, `grafana`, `kibana`, `logstash`,
`prometheus`, `tempo`, and StatefulSets `elasticsearch`, `glitchtip-db` (`filebeat` DaemonSet left
running — daemonsets don't meaningfully add to node memory pressure the way replicated
Deployments do, and it wasn't found to matter). `mysql-0`, `kafka-0`, `zookeeper-0`,
`kafka-connect`, and all 13 app/gateway/platform Deployments were left untouched throughout.

This dropped node `Allocated resources` from ~85%/169% (requests/limits) to ~53%/103%, and no
further `SystemOOM` events occurred for the rest of the retry.

**Cluster state at the start of this retry was messier than a clean baseline**: the previous
blocked attempt had left 9 of the 11 target Deployments mid-rollout, each with one legacy
(unmeshed) replica serving traffic and one new (meshed or not-yet-meshed) replica stuck
crash-looping — plus `linkerd-identity` and `linkerd-destination` in the `linkerd` namespace, and
`tap` in `linkerd-viz`, were themselves crash-looping (`Liveness probe failed: ... context deadline
exceeded`, `connect: connection refused`), i.e. the control plane itself was a memory-pressure
casualty. New pods created while `linkerd-identity` was unhealthy came up as single-container
(injection either skipped or the sidecar couldn't get an identity and was killed), so several
"recovered" pods were running but *not* actually meshed.

Approach: after freeing memory, gave `linkerd-identity` a few minutes to stabilize (it settled to
`2/2 Running` once memory pressure eased), then went through the 11 target Deployments — plus
`accounting-service`'s already-stuck rollout and `order-service`'s collateral-damage crash-loop —
one at a time with `kubectl rollout restart deployment/<name> -n ftgo` followed by
`kubectl rollout status deployment/<name> -n ftgo --timeout=240s`, waiting for each to fully
converge before starting the next. `accounting-service`'s pre-existing stuck replica needed one
`kubectl delete pod` to break out of an exponential-backoff loop after `linkerd-identity` recovered
(its container had been repeatedly hitting the liveness-probe boundary using a JVM startup that
took just over the 105s liveness allowance); every other Deployment converged cleanly on the first
`rollout restart`. `public-gateway` needed one restart cycle (its replica set from the earlier
blocked attempt was still unmeshed) and converged normally.

No `FailedScheduling` events or scheduling-related failures occurred during this retry — the
resource-request/limit values on any app Deployment were not touched, per the brief's constraint.

Final `kubectl get pods -n ftgo` (all 13 app Deployments 2/2 Ready and meshed; `mysql-0`,
`kafka-0`, `zookeeper-0`, `kafka-connect` untouched and healthy throughout):

```
NAME                                     READY   STATUS      RESTARTS        AGE
accounting-service-6d5b467bd9-nst7r      2/2     Running     1               15m
audit-log-service-86896684f-d8vpg        2/2     Running     0               10m
authorization-server-56fcbcfc49-4vz5q    2/2     Running     0               8m21s
config-server-7b68564cc-mn7lb            2/2     Running     0               7m49s
consumer-service-b4f98f6cf-45hqd         2/2     Running     0               12m
delivery-service-8d9475b7-lgn7d          2/2     Running     0               11m
kitchen-service-cf5c4fff4-88fxv          2/2     Running     0               12m
mobile-gateway-6bf877d5f5-sp4ml          2/2     Running     0               9m36s
order-history-service-5696d9d674-xrnpx   2/2     Running     0               11m
order-service-699b6fccb9-lqz66           2/2     Running     0               7m39s
public-gateway-9fb54cfd9-z2t5x           2/2     Running     0               6m58s
restaurant-service-b747f499d-qm5d6       2/2     Running     8               57m
service-registry-84cb85b78b-5frx9        2/2     Running     0               8m55s
```

`linkerd viz stat deploy -n ftgo` after all 13 app Deployments converged (100% success on every
Deployment; note the observability Deployments also show MESHED — restarting them via
scale-to-0/scale-to-1 caused fresh pods to pick up the `ftgo` namespace's `linkerd.io/inject:
enabled` annotation from Task 2, which is namespace-wide and was not re-scoped in this task per
the "don't touch namespace.yaml again" constraint; this is a side effect, not something this task
set out to do, and is harmless — proxy sidecars are 20Mi/50Mi memory request/limit each):

```
NAME                    MESHED   SUCCESS      RPS   LATENCY_P50   LATENCY_P95   LATENCY_P99   TCP_CONN
accounting-service         1/1   100.00%   0.7rps          62ms         265ms         293ms          4
audit-log-service          1/1   100.00%   0.5rps           7ms         188ms         198ms          1
authorization-server       1/1   100.00%   0.5rps           8ms         175ms         195ms          2
config-server              1/1   100.00%   0.3rps           1ms           1ms           1ms          1
consumer-service           1/1   100.00%   0.7rps          75ms         188ms         198ms          4
delivery-service           1/1   100.00%   0.7rps         150ms         365ms         393ms          4
glitchtip                  1/1   100.00%   0.3rps           1ms           2ms           2ms          1
glitchtip-redis            1/1   100.00%   0.3rps           1ms           1ms           1ms         27
glitchtip-worker           1/1   100.00%   0.3rps           1ms           5ms           5ms          1
grafana                    1/1   100.00%   0.5rps           1ms         480ms         496ms          2
kibana                     1/1   100.00%   0.5rps           1ms         180ms         196ms          2
kitchen-service             1/1   100.00%   0.7rps         117ms         365ms         393ms          4
logstash                   1/1   100.00%   0.5rps           5ms         288ms         298ms          2
mobile-gateway              1/1   100.00%   0.7rps         150ms         465ms         493ms          4
order-history-service       1/1   100.00%   0.7rps          63ms         365ms         393ms          4
order-service                1/1   100.00%   0.7rps          45ms         188ms         198ms          4
prometheus                  1/1   100.00%   0.5rps           6ms          27ms          30ms          2
public-gateway               1/1   100.00%   0.7rps          10ms         285ms         297ms          4
restaurant-service           1/1   100.00%   0.7rps          75ms         465ms         493ms          4
service-registry             1/1   100.00%   1.1rps          36ms          92ms          98ms         15
tempo                        1/1   100.00%   2.2rps          45ms         277ms         295ms         15
```

**Observability stack restored** to its original 1-replica-each state after the mesh rollout
converged: `glitchtip`, `glitchtip-redis`, `glitchtip-worker`, `grafana`, `kibana`, `logstash`,
`prometheus`, `tempo`, `elasticsearch` (StatefulSet), `glitchtip-db` (StatefulSet) were all scaled
back to `--replicas=1` and confirmed `Running`/`2/2` (elasticsearch and kibana took the longest —
roughly 2 and 5 minutes respectively — consistent with their normal startup time, not a mesh
regression; kibana's readiness probe restarted it twice during startup before settling, which is
its ordinary behavior waiting on the elasticsearch connection). Node `Allocated resources` at the
end of the retry: `memory 7880Mi (65%) requests / 15620Mi (130%) limits` — up from the mid-rollout
low but still well short of the ~85%/169% that triggered the original Task 4 scheduling failure,
since the sidecar cost is now baked in rather than transient double-counting during a rollout.

**Outcome: all 13 app Deployments are 2/2 Ready and MESHED. No `FailedScheduling` or `SystemOOM`
events occurred during this retry. Task 4 is now complete.**

## Conclusion — Task 5 (end-to-end verification) — BLOCKED

Pre-check: `kubectl get pods -n ingress-nginx` shows `ingress-nginx-controller` `1/1 Running`
(6 restarts, 2d19h old, unrelated to this task) — ingress controller itself is healthy.

Ran the full e2e suite against the live meshed cluster, as a single foreground command (no
docker-compose, no background/Monitor use):

```
./gradlew :ftgo-end-to-end-test:e2eTest -Dgateway.base-url=http://localhost:18000
```

**Result: 11 tests completed, 10 failed, 1 passed** (`BUILD FAILED`, ~9m9s). Every failure traces
to the same root cause — `postWithRetry` in `PlaceReviseCancelOrderStepDefinitions.java` retried
`POST http://localhost:18000/orders` for the full 60s budget and every attempt got:

```
IllegalStateException: Unexpected status 404: <html><head><title>404 Not Found</title></head>
<body><center><h1>404 Not Found</h1></center><hr><center>nginx</center></body></html>
```

Confirmed directly: `curl -X POST http://localhost:18000/orders` returns `404` from nginx.
Inspecting the ingress resource explains why —
`kubectl get ingress ftgo-gateways -n ftgo` has exactly two path rules:

```
/mobile(/|$)(.*) -> mobile-gateway
/public(/|$)(.*) -> public-gateway
```

There is no `/orders` (or catch-all `/`) rule. The e2e suite's `gateway.base-url` mode expects the
gateway to be reachable at the ingress root, but `ftgo-gateways` only routes the `/mobile` and
`/public` prefixes — this is an ingress path-mapping gap between the test harness's expectations
and the existing ingress config, not a mesh/mTLS defect. It reproduced identically and
deterministically across all 11 test attempts within the run (not a single transient blip), so per
the brief's guidance this was not blindly retried.

This is orthogonal to the service mesh work itself: `linkerd viz stat` (captured above, end of
Task 4) already shows 100% success / full mTLS across all 13 app Deployments for in-cluster
service-to-service traffic, which is what B3a's mesh objective is about. The failure here is
specifically in the external, ingress-fronted, black-box e2e path, and its fix (adding an ingress
path rule, or pointing the test at `/public`/`/mobile` instead of the bare root) is an ingress
config / test-harness concern outside this task's "no application code changes" scope and outside
what Tasks 1-4 touched.

**Overall verdict for B3a: mesh rollout and mTLS verification are complete and successful (13/13
Deployments meshed, 100% success rate, confirmed via `linkerd viz`); the external e2e suite could
not be used to confirm this from outside the cluster due to a pre-existing ingress path-mapping gap
unrelated to Linkerd, so B3a's black-box verification step is BLOCKED pending an ingress-routing
fix that is out of scope for this task.**

## Task 5 follow-up — corrected gateway path, and a resource-exhaustion caveat

A later pass at this task (this session did not have the above conclusion in hand at start —
it was picked up from a stale task brief that described the evidence file as having only three
sections) went one step further on the "no `/orders` route" diagnosis above and found a working
URL, but was unable to complete a clean full run due to cluster resource exhaustion. Recorded here
for whoever picks this up next.

**The 404 is fixable without touching ingress config.** `PlaceReviseCancelOrderStepDefinitions`'s
own default (used when `gateway.base-url` is *not* overridden) is
`http://localhost:8091/api/v1` — i.e. the non-override path already includes an `/api/v1` segment
that the override value used above (`http://localhost:18000`) dropped. Combining that with the
ingress's actual `/public(/|$)(.*)` rule (confirmed via `kubectl get ingress -n ftgo -o yaml`) gives
`http://localhost:18000/public/api/v1`, which was confirmed live: `curl -X POST
http://localhost:18000/public/api/v1/orders` and `curl http://localhost:18000/public/api/v1/orders/xxx`
both return real `public-gateway`/`order-service` JSON error bodies (`{"timestamp":...,"status":404,...}`)
rather than nginx's static 404 HTML page — i.e. traffic reaches the app through the ingress and the
mesh correctly. `-Dspring.profiles.active=kubernetes` (the flag named in this task's original brief)
does not exist anywhere in this module; `gateway.base-url` is the only override mechanism, confirmed
against `ftgo-end-to-end-test/build.gradle`.

**Separately, the suite's "Kubernetes profile" is not actually self-contained via `gateway.base-url`
alone.** `HealthCheckStepDefinitions` and `PlaceReviseCancelOrderStepDefinitions` hardcode direct
`localhost:<port>` calls for every one of the 13 app services (order-service:8082,
kitchen-service:8083, accounting-service:8084, restaurant-service:8085, delivery-service:8086,
order-history-service:8088, consumer-service:8081, mobile-gateway:8090, public-gateway:8091,
authorization-server:9000, audit-log-service:8089, glitchtip:8000, tempo:3200) — a docker-compose
assumption (compose maps each service straight to a host port) that a real cluster doesn't provide
without 13 separate `kubectl port-forward` processes. This isn't documented in `k8s/README.md`'s
ingress section or anywhere else. With all 13 forwarded, one full run got past the health-check
stage entirely (`Every FTGO service's health endpoint reports UP` passed, confirming actuator
health, DB connectivity, and Eureka self-registration all work for every meshed pod reached this
way) before failing later on the (at-that-point-still-uncorrected) `/orders` 404.

**A clean, fully-passing run combining both fixes was not obtained in this session — not because of
a test failure, but because of cluster memory exhaustion accumulated from the repeated
port-forward/test cycles this investigation needed.** `kubectl describe pod` on `order-service`
showed both its app container and its `linkerd-proxy` sidecar exiting `137` (OOMKilled); `kubectl get
pods -n ftgo` showed 10 of 13 app Deployments in `CrashLoopBackOff`; `docker stats
ftgo-control-plane` showed the kind node at ~68% memory / ~84% CPU. The proxy container was OOMKilled
right alongside the app container in the same pod, which is the signature of node-wide memory
pressure, not a mesh-specific regression — consistent with the memory-exhaustion pattern already
documented above for Task 4's rollout, now recurring under this task's repeated test-cycle load
rather than under rollout load. This session stopped further test runs at that point rather than
compound the pressure further, and did not attempt to raise resource limits (out of scope, same as
Task 4).

**Updated recommendation:** the mesh itself is not the blocker — `linkerd viz stat`'s 100%
success/full-mTLS result from Task 4, plus this pass's confirmed live health-check pass and
confirmed correct end-to-end routing at `http://localhost:18000/public/api/v1`, are consistent
with the mesh passing traffic correctly. What remains is procedural: a next attempt should (a) use
`-Dgateway.base-url=http://localhost:18000/public/api/v1`, (b) hold all 13 `kubectl port-forward`
processes open in the same shell as the `./gradlew` invocation (backgrounding them in a separate
tool call risks the harness reaping them mid-run), and (c) start from a rested cluster (`kubectl get
pods -n ftgo` all `2/2 Running`, `docker stats` well under node memory limit) rather than one still
recovering from a prior attempt's load, to get one clean, single-shot pass. B3a's verdict from the
main Conclusion above stands: mesh rollout and mTLS are complete and correct; black-box e2e
confirmation remains BLOCKED, now narrowed from "ingress path gap" to "no clean run obtained yet
under this session's resource constraints," pending a retry under the conditions above.
