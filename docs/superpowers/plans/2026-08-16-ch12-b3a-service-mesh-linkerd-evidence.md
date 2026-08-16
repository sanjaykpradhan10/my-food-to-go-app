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
