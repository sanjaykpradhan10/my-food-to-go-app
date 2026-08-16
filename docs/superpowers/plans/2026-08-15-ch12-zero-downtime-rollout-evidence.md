# Ch.12 B2 zero-downtime rollout — captured evidence

Date: 2026-08-16

## Status: defect found, rollout is NOT currently zero-downtime

Two independent forward-rollout attempts (1.0.0 -> 1.1.0) were run against a live k6
verification Job hitting `order-service`'s `/actuator/health` endpoint every request
during a real `helm upgrade`. Both attempts showed a sustained window of connection
failures (`status=0`, i.e. connection refused / no backing Service endpoint) that
started when the old pod's readiness probe began failing and did not fully recover
until the new pod passed its own readiness probe. This is a genuine outage, not a
single dropped request, so per this plan's Step 4 instruction ("if any non-200 status
appears... stop and investigate... do not weaken or skip the check"), Task 5 stops
here rather than reporting a fabricated clean pass. Rollback (Step 6) was not
attempted because Step 4 never passed.

## Forward rollout attempt 1 (1.0.0 -> 1.1.0)

Total requests: 283 (`http_reqs` from k6 summary)
Non-200 / failed responses: 181 (`http_req_failed`: 63.95%)
Version transition: last `1.0.0` line and first `1.1.0` line, with the outage between them:

```
status=200 version=1.0.0   (17:30:57Z, last of a run of 90 consecutive 200s)
status=0 version=undefined (17:30:58Z through 17:32:14Z — 78 seconds, ~181 requests)
status=200 version=1.1.0   (17:32:16Z, first of 12 consecutive 200s)
```

## Forward rollout attempt 2 (1.0.0 -> 1.1.0, retried to check for a one-off flake)

Total requests: 15,528
Non-200 / failed responses: 11,996 (`http_req_failed`: 77.25%)
Version transition: the outage did not occur once — it recurred in multiple bursts
throughout the whole 90s window, alternating between short bursts of `1.1.0` 200s and
sustained `status=0` gaps, e.g.:

```
status=200 version=1.0.0    (17:37:25Z)
status=0 version=undefined  (17:37:25Z–17:38:07Z, ~700+3572 requests in two bursts)
status=200 version=1.1.0    (17:38:08Z, burst of ~2700 successes)
status=0 version=undefined  (17:38:17Z–17:38:40Z, another gap)
status=200 version=1.1.0    (17:38:41Z, another burst)
status=0 version=undefined  (17:38:44Z onward, gap continues past job completion)
```

Inspecting the `order-service` pod directly during attempt 2 confirmed the root cause
is not solely rollout timing: the *new* pod (already on `1.1.0`, after the rollout had
already completed per `kubectl rollout status`) continued to restart on its own —
`RESTARTS: 2 (61s ago)` on a pod that was 2m13s old — driven by readiness/liveness
probe failures (`context deadline exceeded` and `connection refused` against
`:8082/actuator/health`) that recurred independently of the deployment's surge
sequencing. This matches the environment brief's documented "known history of
transient crash-loop flakiness" for `order-service`/`restaurant-service`, but here it
directly produces observable client-facing downtime during a rollout window, which is
exactly what this chapter's zero-downtime work is meant to prevent — so it is a real
finding, not just background noise.

## Rollback (1.1.0 -> 1.0.0)

Not attempted. Step 4's gate (clean, zero-non-200 forward rollout) never passed in two
attempts, so per the plan's explicit instruction the rollback demonstration (Step 6)
was not run against a broken baseline — doing so would not have produced meaningful
evidence.

## Conclusion

The `X-Service-Version` header, per-service `imageTag` override, and k6 verification
Job (Tasks 1-4) all work correctly as instrumentation — they clearly showed the
version transition and precisely quantified the outage. However, the rollout itself is
not currently zero-downtime in this cluster: `order-service` exhibits probe-driven
restarts (both on the outgoing and incoming pod) that remove it from the Service's
endpoint list for tens of seconds at a time, independent of the deployment
controller's `maxSurge`/`maxUnavailable` accounting. This reproduced across two
separate attempts with different failure shapes (single long gap vs. multiple
recurring gaps), so it does not look like a one-off scheduling fluke. Suspected
contributing factors, not yet isolated: readinessProbe `timeoutSeconds: 1` is tight
for a JVM under any contention, and no CPU `limits` are set on `order-service` (only a
100m `request`), which combined with the shared kind node's variable load can starve
probe response time enough to flip a healthy pod to `NotReady`. This is a real defect
in the chart's probe/resource tuning that pre-dates and is independent of Task 5's own
work — fixing it is out of Task 5's scope (Task 5 only runs and captures the
verification, per the plan's Global Constraints, which restrict this sub-project to
the version-header/imageTag/k6 pieces already built in Tasks 1-4) and should be raised
as follow-up work before this chapter's B2 sub-project can be marked Done.
