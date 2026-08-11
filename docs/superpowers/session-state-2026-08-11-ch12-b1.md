# Session State — Ch.12 Sub-project B1 (Kubernetes core deployment)

**Date paused:** 2026-08-11
**Worktree:** `/Users/sanjaypradhan/Sanjay/Projects/Spring/my-food-to-go-app/.claude/worktrees/ch12-deployment`
**Branch:** `worktree-ch12-deployment` (based off `main`, sub-project A already merged into it)
**Plan file:** `docs/superpowers/plans/2026-08-11-ch12-k8s-core-deployment.md`
**Spec:** `docs/superpowers/specs/2026-08-11-ch12-k8s-core-deployment-design.md`
**SDD ledger:** `.superpowers/sdd/2026-08-11-ch12-k8s-core-deployment/progress.md` (git-ignored, lives only in the worktree — this doc is the durable backup of its content)

## How to resume

1. Read this file, then read `docs/superpowers/plans/2026-08-11-ch12-k8s-core-deployment.md` in full.
2. `cd` into the worktree above (it should still exist; if not, recreate via `superpowers:using-git-worktrees` — branch already exists with commits, just needs a worktree checkout).
3. Invoke `superpowers:subagent-driven-development` to continue executing the plan, starting at **Task 6** (Tasks 1-5 are done — see ledger below, do NOT re-dispatch them).
4. **First**, before dispatching Task 6, complete the paused Docker Desktop step (see "Immediate next step" below) — the cluster needs to be recreated after any Docker Desktop restart.

## Why paused

Sub-project A (Ch.12 §12.1-12.3 docs) and B1 Tasks 1-5 of 13 are complete and merged on this branch. After Task 5, total container memory limits in the `ftgo` namespace reached ~8.3Gi against the kind Docker VM's 7.8Gi cap (only ~88Mi physically free at the time). Tasks 6-9 still need to add ~12+ more containers (observability stack, GlitchTip app/worker, Kafka Connect, gateways), which would not fit.

Docker Desktop's settings file (`~/Library/Group Containers/group.com.docker/settings-store.json`) already specifies `MemoryMiB: 12288` (12Gi), but the running VM was still capped at 7.8Gi — the setting needs a Docker Desktop restart to take effect. The user chose to restart Docker Desktop manually (not via CLI) since a restart kills the `kind` cluster and every other running container, including other worktree sessions' compose stacks — a disruptive action requiring their own timing.

## Immediate next step (before Task 6)

Once the user confirms Docker Desktop has been restarted (12Gi should now be live — verify with `docker info | grep -i memory` or `docker exec ftgo-control-plane free -h` after cluster recreation):

1. Recreate the kind cluster + local registry: `./k8s/scripts/setup-cluster.sh` (idempotent script from Task 1; the previous cluster/registry were destroyed by the Docker restart).
2. Redeploy the full chart: `helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace`
3. Poll `kubectl -n ftgo get pods` until all Tasks 1-5 pods (mysql-0, zookeeper-0, kafka-0, elasticsearch-0, glitchtip-db-0, glitchtip-redis-*, service-registry, authorization-server, config-server, and the 8 business services) reach `1/1 Running`. Rebuilding/pushing images to the fresh local registry may be needed too — `k8s/scripts/build-and-push.sh` builds all services added through Task 5.
4. Confirm new memory headroom via `docker exec ftgo-control-plane free -h` before dispatching Task 6.
5. Only then generate Task 6's brief and dispatch its implementer.

## Ledger state (Tasks 1-5 complete)

```
# SDD ledger — plan: docs/superpowers/plans/2026-08-11-ch12-k8s-core-deployment.md
COORDINATOR NOTE: host port 8000 (kind-config.yaml's ingress hostPort) was occupied by an
unrelated running container (ch11-audit-logging-glitchtip-1) in another worktree. Changed to
18000 (commit 8259ffe). Later tasks/dispatches referencing "http://localhost:8000" (Task 9
nginx-ingress doc, Task 11 verification, Task 12 e2e base URLs) must use http://localhost:18000
instead.

Task 1: complete (commits 364c215..8259ffe, review clean)
Task 2: complete (commits 8259ffe..800e0aa, review clean)
Task 3: complete (commits 800e0aa..90c5479, review clean after 1 fix round — kafka OOM/
  resource-limits + service-link env collision, both fixed and re-verified live)
Task 4: complete (commits 90c5479..f936429, review clean; minor deferred: _helpers.tpl doc
  comment for ftgo.waitFor could note the initContainers splice location)
Task 5: complete (commits f936429..fcccc22, review clean; minor note: memory headroom thin,
  prompted a pause for Docker Desktop memory increase)

COORDINATOR NOTE: paused after Task 5 waiting for user to restart Docker Desktop to apply a
12Gi memory setting (was capped at 7.8Gi at runtime despite config already saying 12288 MiB).
Once restarted: re-run k8s/scripts/setup-cluster.sh (kind cluster + registry were destroyed by
the restart), then helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace,
then re-verify Tasks 1-5's pods are healthy before dispatching Task 6.
```

**Git commit history for the branch (most recent first):**
```
fcccc22 feat: add business services via generic values-driven Deployment template
f936429 docs: add Task 4 report (platform services + wait-for helper)
641e5a3 feat: add platform services (service-registry, authorization-server, config-server) and wait-for helper
90c5479 docs: add Task 3 fix report (OOM resource limits + kafka service-link bug)
7b71328 fix: disable service-link env injection on kafka pod to fix startup
d8b2656 fix: set resource requests/limits on Task 3 stateful infra to prevent node OOM
f30c97e feat: add stateful infrastructure (MySQL, Kafka, Elasticsearch, GlitchTip DB/Redis)
800e0aa feat: add namespace, shared Secret, and common ConfigMap templates
8259ffe fix: use free host port 18000 for kind ingress mapping (8000 was occupied)
94e0a48 Merge branch 'worktree-agent-adf7f82fe3cf49584' into worktree-ch12-deployment
4716263 feat: scaffold Helm chart, kind cluster config, and local registry setup
364c215 Merge branch 'worktree-agent-a09cf7d005805eaaf' into worktree-ch12-deployment
```
(Below `364c215` is sub-project A's single-task plan — already complete, merged, workspace cleaned up.)

## What's built so far (Tasks 1-5)

- `k8s/kind-config.yaml` — kind cluster `ftgo`, ingress `hostPort: 18000` (not the plan's literal 8000 — see coordinator note above, port 8000 was occupied by another worktree's container).
- `k8s/scripts/setup-cluster.sh` — idempotent: creates `kind-registry` container (localhost:5000) + `ftgo` kind cluster, connects registry to kind's docker network.
- `k8s/scripts/build-and-push.sh` — builds/pushes all service images to the local registry; currently covers service-registry, authorization-server, config-server (Task 4) + all 8 business services (Task 5).
- `k8s/ftgo/` Helm chart:
  - `Chart.yaml`, `values.yaml` (`global:`, `secrets:`, `commonEnv:`, `kafka:`, `mysql:`/etc infra values, `platformServices:` list, `businessServices:` 8-entry list)
  - `templates/namespace.yaml`, `secrets.yaml` (`ftgo-credentials`), `configmap-common.yaml` (`ftgo-common-config`)
  - `templates/_helpers.tpl` — `ftgo.waitFor` named-template helper: takes a list of `{name, port}` dicts, emits `initContainers` YAML (busybox `nc`-loop wait), splice under `spec.template.spec.initContainers:`. Handles empty list safely (renders nothing).
  - `templates/{mysql,kafka,elasticsearch,glitchtip-db}-statefulset.yaml`, `glitchtip-redis-deployment.yaml`, `init-sql-configmap.yaml` (reuses `infrastructure/mysql/init.sql` via a chart-root symlink `k8s/ftgo/infrastructure -> ../../infrastructure` + `.Files.Glob`)
  - `templates/{service-registry,authorization-server,config-server}.yaml`
  - `templates/app-service.yaml` — single generic template, `{{- range .Values.businessServices }}`, renders 8 Deployments + 8 Services from `values.yaml`'s `businessServices` list.
- **Resource limits are mandatory on every container** — this was a hard lesson from Task 3 (see below). Current per-task budgets: infra ~2.9Gi limits, platform services ~1.5Gi limits (512Mi×3), business services ~4Gi limits (512Mi×8). Total already ~8.3Gi against the (pre-restart) 7.8Gi node cap — this is exactly why the Docker Desktop memory bump was needed.

## Key incidents / lessons this session (apply to remaining tasks)

1. **Port 8000 conflict** (Task 1): another worktree's container held host port 8000. Fixed by using `18000` instead — **propagate this to Task 9 (nginx-ingress doc), Task 11 (verification), Task 12 (e2e base URLs)**, all of which reference the ingress URL and must use port 18000, not the plan's literal 8000.
2. **Missing resource limits caused a real node-OOM crash-loop** (Task 3): kafka-0 crash-looped with generic exit code 1 (not classic 137/OOMKilled — cgroup v2 reporting quirk) because no container in Task 3 had `resources.requests`/`limits` set, so the kafka JVM's heap ergonomics scaled off full node memory. Root-caused via `docker exec ftgo-control-plane dmesg | grep -i oom`. Fixed by adding explicit limits to all 6 Task 3 containers + `KAFKA_HEAP_OPTS` bounding kafka's JVM heap. **Every container in every remaining task must have `resources.requests`/`resources.limits` set from the start** — don't repeat this fix-loop.
3. **A second, independent kafka bug found during the same fix round**: Kubernetes auto-injects Docker-links-style `<SVCNAME>_PORT`/`<SVCNAME>_SERVICE_HOST` env vars into every pod from every Service in the namespace; the `kafka` Service's name collided with the kafka pod itself, injecting `KAFKA_PORT` which the `cp-kafka` entrypoint's configure script treats as deprecated single-port config and exits 1 on. Fixed with `enableServiceLinks: false` on the kafka pod spec. **Consider `enableServiceLinks: false` defensively on any future workload whose container name could collide with env-var-shaped Service names** (Task 5's business services already added this defensively per the implementer's own initiative).
4. **Agent stalls/API errors are common and require checking for uncommitted work before re-dispatching**: at least 3 times this session a dispatched agent either stalled (600s no-progress watchdog) or hit a raw API connection error mid-task. In every case, checked `git log <branch> --oneline` / `git diff <base> <branch> --stat` on the agent's worktree branch before concluding whether to resume or redispatch from scratch — sometimes real commits existed on an unexpected branch name (one agent committed to a manually-created local branch `ch12-task3-fix` instead of its own `worktree-agent-<id>` branch; had to search `git branch -a` to find it before merging). **Always verify via git before assuming an agent's work is lost.**
5. **Backgrounding long-running builds inside a dispatched agent causes stalls**: Task 5's first attempt kicked off a Docker build as a background/detached process inside its own sandbox, then sat polling/monitoring it indefinitely and stalled with zero commits after 10+ minutes. Fixed by explicit instruction in the redispatch: commit template/config changes BEFORE touching Docker, then run builds as synchronous foreground commands (even if a single command takes 20-40 minutes, that's fine — backgrounding-and-losing-track is not). **Apply this instruction to any future task involving Docker builds** (there likely aren't more full 8-image builds needed, but worth remembering for Task 7's GlitchTip images if it builds custom images).
6. **Docker memory ceiling was silently stale**: Docker Desktop's settings file said 12Gi but the live VM enforced 7.8Gi — settings changes need an actual Docker Desktop restart, not just being present in the config file. If future memory issues recur even after a stated increase, verify the *running* VM's actual allocation (`docker exec ftgo-control-plane free -h`), don't trust the settings file alone.

## Remaining work (Tasks 6-13 of B1, then finish)

Per `docs/superpowers/plans/2026-08-11-ch12-k8s-core-deployment.md`:

- **Task 6**: Observability stack (Tempo, Prometheus, Grafana, Logstash, Kibana, Filebeat DaemonSet) — reuses existing local config files via `.Files.Glob(...).AsConfig` (same symlink pattern as Task 3's init-sql ConfigMap will likely be needed here too, since these configs also live outside `k8s/ftgo/`).
- **Task 7**: GlitchTip app + worker Deployments + REST-API-based provisioning Job (RBAC ServiceAccount/Role/RoleBinding), producing a `glitchtip-dsn` Secret; also modifies Task 5's `app-service.yaml` to add an `optional: true` `SENTRY_DSN` env var.
- **Task 8**: Kafka Connect Deployment + connector-registrar Job (reuses `infrastructure/debezium/outbox-connector.json`).
- **Task 9**: Gateway Deployments (mobile-gateway, public-gateway) + nginx-ingress installation doc + Ingress resource — **use port 18000, not the plan's literal 8000**.
- **Task 10**: Kibana index-pattern registration Job (reuses `kibana/saved-objects/index-pattern.ndjson`).
- **Task 11**: Full-cluster fresh-deploy verification (delete/recreate cluster, `helm install`, wait for all pods Ready, spot-check hook Jobs) — **use port 18000**.
- **Task 12**: E2E test Kubernetes profile — add a K8s-targeting profile to `ftgo-end-to-end-test` pointing at the ingress, run the full Cucumber suite as the acceptance gate — **use port 18000 for base URLs**.
- **Task 13**: Documentation sweep (root `README.md`, `CONTEXT.md`, `docs/ARCHITECTURE.md` Kubernetes section).

After all 13 tasks are complete and reviewed clean:
- Run the final whole-branch review (most capable model available), MERGE_BASE = `364c215` (where B1 started) through current HEAD.
- Fix loop if findings (max one fix dispatch + one scoped re-review, then adjudicate residuals).
- Delete the SDD workspace `.superpowers/sdd/2026-08-11-ch12-k8s-core-deployment/` once clean.
- Only then invoke `superpowers:finishing-a-development-branch` **once**, for the combined branch (both sub-project A and B1) — per explicit earlier user instruction not to hand off until both plans are done. This presents the standard merge/PR/keep-as-is menu.

**Do not flip Ch.12 to "Done" in `CONTEXT.md`** — sub-projects B2 (zero-downtime deploys) and B3 (service mesh) are separate, explicitly out-of-scope future work per the spec.

## Task-loop procedure (unchanged, for reference)

For each remaining task: `task-brief` script → dispatch implementer (sonnet for multi-file/integration tasks, per Model Selection guidance) with `isolation: "worktree"` → on DONE, `git merge worktree-agent-<id> --no-edit` into `worktree-ch12-deployment` (check `git branch -a` if "already up to date" looks wrong — an agent may have committed to a differently-named branch) → `review-package` script → dispatch reviewer (sonnet) → if Critical/Important findings, fix loop (resume original implementer rounds 1-3, escalate model rounds 4-5, 5-round cap) → log ledger entry → next task. Minor findings get logged and deferred, not fixed inline.
