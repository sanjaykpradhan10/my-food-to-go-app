# Task 3 report: Stateful infrastructure Helm templates

## Note on execution environment

This continuation ran in a different worktree than the original Task 3 work
(`agent-ae13f84c8059efc94` instead of `ch12-deployment`, which the harness
kept locked/isolated from this agent). To keep the fix on the correct base,
a new local branch `ch12-task3-fix` was created directly from commit
`f30c97e` (the merged Task 3 commit) inside this worktree, and all work
below was done and committed there. The two fix commits
(`d8b2656`, `7b71328`) need to be merged/cherry-picked into the
`ch12-deployment`/main line by whoever integrates this branch.

## What Task 3 originally created (commit f30c97e)

- `k8s/ftgo/templates/mysql-statefulset.yaml` — MySQL 8.4 StatefulSet + headless Service, with
  binlog args for CDC, credentials from the `ftgo-credentials` Secret, and an init-SQL ConfigMap
  mount.
- `k8s/ftgo/templates/kafka-statefulset.yaml` — Zookeeper StatefulSet + Service, and a Kafka
  StatefulSet + Service (internal/external listeners), with an init container that waits for
  Zookeeper to be reachable before starting Kafka.
- `k8s/ftgo/templates/elasticsearch-statefulset.yaml` — single-node Elasticsearch 8.15.3
  StatefulSet + Service, security disabled, `ES_JAVA_OPTS` fixed at `-Xms512m -Xmx512m`.
- `k8s/ftgo/templates/glitchtip-db-statefulset.yaml` — Postgres StatefulSet + Service for
  GlitchTip's database.
- `k8s/ftgo/templates/glitchtip-redis-deployment.yaml` — Redis Deployment + Service for
  GlitchTip.
- `k8s/ftgo/templates/init-sql-configmap.yaml` — ConfigMap built from
  `.Files.Glob "infrastructure/mysql/init.sql"` (via the chart's `infrastructure` symlink).
  Reviewer confirmed this path/approach is correct as-is; no change made here.

None of these five workloads (six containers) set `resources.requests`/`resources.limits`, so
all pods ran as `BestEffort` QoS.

## What the reviewer found

**Critical**: `kafka-0` was crash-looping from node-level OOM kills, confirmed via
`dmesg | grep -i oom` on the kind node — the kernel OOM-killer was repeatedly killing the
Kafka JVM process (containerd surfaces this as a generic exit code 1, not classic
`OOMKilled`/137). Root cause: with no memory ceiling on any Task 3 container, the Kafka JVM's
heap ergonomics scaled off full node memory, and combined with Elasticsearch/MySQL/Zookeeper
JVMs plus kind's own control-plane processes on a single node, memory got oversubscribed.

## Fixes applied

### Commit `d8b2656` — resource requests/limits on all 6 containers

Added `resources: {requests, limits}` to every container in the 5 Task 3 template files, sized
to keep this task's total footprint under budget (target: comfortably under ~3Gi) while leaving
headroom for the ~13 app-service pods, platform services, and observability stack that land in
later tasks on the same kind node:

| Container | Request (mem/cpu) | Limit (mem) |
|---|---|---|
| mysql | 256Mi / 100m | 512Mi |
| zookeeper | 128Mi / 50m | 256Mi |
| kafka | 512Mi / 100m | 768Mi |
| elasticsearch | 512Mi / 100m | 1Gi |
| glitchtip-db | 128Mi / 50m | 256Mi |
| glitchtip-redis | 64Mi / 25m | 128Mi |

**Total limits: ~2.9Gi** — under the ~3Gi target.

Also added `KAFKA_HEAP_OPTS: "-Xmx512m -Xms512m"` to the kafka container so the JVM heap is
explicitly bounded rather than derived from cgroup ergonomics (the direct fix for the
node-level OOM: previously the JVM would size its heap off the full node, not the container's
new 768Mi limit).

For Elasticsearch: it already had `ES_JAVA_OPTS: "-Xms512m -Xmx512m"` fixed, so no heap change
was needed — the container's memory limit was set to 1Gi, comfortably above the 512Mi heap to
cover Elasticsearch's significant off-heap usage (Lucene, native memory, etc.), per the
reviewer's note.

`helm template ftgo k8s/ftgo` was confirmed to render cleanly after this change, and it was
committed before any live-cluster verification, per the task's risk-mitigation instruction.

### Commit `7b71328` — kafka `enableServiceLinks: false` (found during live verification)

After applying the resource-limit fix and running `helm upgrade --install` against the live
kind cluster, `elasticsearch-0` recovered immediately (once its stale pod, still running with
resources from an earlier ad-hoc/manual attempt, was deleted so the StatefulSet controller
could recreate it with the new template spec). `kafka-0`, however, kept exiting with
`exitCode: 1` in well under a second — too fast to be a JVM/memory issue, and confirmed via
`kubectl logs --previous` to die during the entrypoint's *configure* step, before the JVM even
starts.

Root-caused by exec'ing a throwaway debug pod with the same image/env and running
`bash -x /etc/confluent/docker/configure`: Kubernetes automatically injects legacy
Docker-links-style environment variables for every Service visible in the namespace
(`<SVCNAME>_PORT`, etc.). Because the Kafka Service is also named `kafka`, this injects
`KAFKA_PORT=tcp://<clusterIP>:29092` into the kafka pod's own environment. The cp-kafka image's
`configure` script treats any non-empty `KAFKA_PORT` as legacy single-port configuration and
calls `exit 1` (after printing the "port is deprecated" message visible in the pod logs).

This is unrelated to the OOM issue from the brief — it's a second, independent bug that was
masked by the OOM crash loop in the reviewer's original diagnosis (both produce container
restarts/CrashLoopBackOff, but this one has nothing to do with memory). Fixed by setting
`enableServiceLinks: false` on the kafka pod spec in `k8s/ftgo/templates/kafka-statefulset.yaml`,
which stops Kubernetes from injecting the colliding service-link env vars.

`helm template` was re-verified to render cleanly, and the fix was committed separately from the
resource-limit commit.

## Live verification

`helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace` was run twice against
the live `kind` cluster (`ftgo` cluster, context `kind-ftgo`) — once after each fix commit. Stale
pods left over from a prior ad-hoc debugging attempt (before this session) were deleted so the
StatefulSet controllers would recreate them from the current template spec.

Final polled state (all pods `1/1 Running`, kafka and elasticsearch each freshly recreated with
0 restarts since the fix):

```
NAME                               READY   STATUS    RESTARTS   AGE
elasticsearch-0                    1/1     Running   0          3m37s
glitchtip-db-0                     1/1     Running   0          64m
glitchtip-redis-5cf58467d6-zmx5f   1/1     Running   0          64m
kafka-0                            1/1     Running   0          37s
mysql-0                            1/1     Running   0          64m
zookeeper-0                        1/1     Running   0          64m
```

## Concerns / follow-ups for whoever integrates this

- The two fix commits live on branch `ch12-task3-fix`, based on `f30c97e`, inside worktree
  `agent-ae13f84c8059efc94` — they are **not** on `ch12-deployment` or `main` and need to be
  merged/cherry-picked there.
- `k8s/ftgo` had visible drift from a prior manual/ad-hoc attempt on the live cluster (stale pod
  resource values that matched no committed template). That drift is now gone (pods were deleted
  and recreated from the current Helm release), but it's worth double-checking there's no other
  leftover manual `kubectl patch`/`kubectl apply` state on the `ftgo` namespace before the next
  task builds on top of it.
- The `enableServiceLinks: false` fix only addresses the `kafka` Service/pod name collision.
  Later tasks that name a Service identically to its own pod's container should watch for the
  same class of bug if they use an image whose entrypoint is sensitive to `<NAME>_PORT`-style
  env vars.
