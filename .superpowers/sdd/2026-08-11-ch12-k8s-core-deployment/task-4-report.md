# Task 4 report: Platform services + wait-for helper

## What was created

- `k8s/ftgo/templates/_helpers.tpl` — new file, defines the `ftgo.waitFor` named
  template (see interface below). Verbatim from the brief.
- `k8s/ftgo/templates/service-registry.yaml` — Deployment + Service, port 8761,
  `/actuator/health` readiness (15s delay/10s period) and liveness (30s/15s) probes,
  `enableServiceLinks: false`, `resources.requests: {memory: 256Mi, cpu: 100m}` /
  `limits: {memory: 512Mi}`.
- `k8s/ftgo/templates/authorization-server.yaml` — same shape, port 9000, probes
  target `/oauth2/jwks` (matching compose.yml's own healthcheck endpoint for this
  service, not `/actuator/health`), same resources/enableServiceLinks.
- `k8s/ftgo/templates/config-server.yaml` — same shape, port 8888, `CONFIG_SERVER_GIT_URI`
  env var sourced from `.Values.platformServices` (looked up by `name: config-server`
  via a `range`/`if` at the top of the template, captured into `$gitUri`), no
  readiness/liveness probe (compose.yml has none for this service either), same
  resources/enableServiceLinks.
- `k8s/scripts/build-and-push.sh` — new script, `chmod +x`, with `build_and_push`
  calls for `ftgo-service-registry`, `ftgo-authorization-server`, `ftgo-config-server`
  only (Task 10 appends the remaining 10).
- `k8s/ftgo/values.yaml` — appended the `platformServices:` list exactly as specified
  in the brief (service-registry/authorization-server/config-server entries, each with
  `dependsOn: []`, config-server also carrying `extraEnv.CONFIG_SERVER_GIT_URI`).

All three containers have `resources.requests` and `resources.limits` set (256Mi
request / 512Mi limit each), per the Task 3 OOM lesson — 3 x 512Mi = 1.5Gi added to
the ~2.9Gi Task 3 already uses, still well under a typical local `kind` node budget.

All three Deployments set `enableServiceLinks: false` defensively, since each Service
name exactly matches its Deployment/pod name (`service-registry`, `authorization-server`,
`config-server`) — the same pattern that caused the Task 3 kafka service-link bug.
None of these three Spring services currently read a colliding `<NAME>_PORT` env var
(checked: none of the three consume `SERVICE_REGISTRY_PORT`, `AUTHORIZATION_SERVER_PORT`,
or `CONFIG_SERVER_PORT`), so this is precautionary rather than fixing an observed bug.

`dependsOn: []` for all three services in this task, so no template currently calls
`ftgo.waitFor` — it is wired up and ready for later tasks (5-9) whose services do
depend on service-registry/authorization-server/config-server/mysql/kafka.

## `ftgo.waitFor` interface (for tasks 5-9)

```
{{ include "ftgo.waitFor" (list (dict "name" "mysql" "port" 3306) (dict "name" "kafka" "port" 29092)) }}
```

- Input: a Helm list of dicts, each `{name: <k8s Service DNS name>, port: <int>}`.
- Output: a YAML block of `initContainers` list items (busybox:1.36, `nc -z` polling
  loop with 2s sleep) — splice it directly under a `spec.template.spec.initContainers:`
  key in a Deployment/StatefulSet template.
- In-cluster DNS names now available for callers: `service-registry:8761`,
  `authorization-server:9000`, `config-server:8888` (in addition to the Task 3 names
  `mysql:3306`, `kafka:29092`).

## Verification

`helm template ftgo k8s/ftgo` rendered cleanly (exit 0), all three new manifests
present with correct Source headers, correct image refs
(`localhost:5000/ftgo/ftgo-{service}:local`), correct probes, and the config-server
`CONFIG_SERVER_GIT_URI` env resolved to `"file:///config-source-repo"` in the
rendered output — confirming the values lookup works.

Images were buildable (Dockerfiles exist for all three services), so per the brief's
guidance this task built and pushed them and did a live deploy rather than stopping
at manifest-only verification:

- `./k8s/scripts/build-and-push.sh` — all three images built and pushed successfully
  to `localhost:5000/ftgo/ftgo-{service-registry,authorization-server,config-server}:local`.
- `helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace` — succeeded,
  `STATUS: deployed`, `REVISION: 7`.
- `kubectl -n ftgo get pods -l 'app in (service-registry,authorization-server,config-server)'`
  polled at ~20s intervals: all three pods went `ContainerCreating` -> `Running` ->
  `1/1 Running`, 0 restarts, within under a minute. Final observed state:

```
NAME                                   READY   STATUS    RESTARTS   AGE
authorization-server-7468688d4-dzk5c   1/1     Running   0          10m
config-server-7db9bf78f6-hsqg9         1/1     Running   0          10m
service-registry-68764659c9-dhnkr      1/1     Running   0          10m
```

No `ImagePullBackOff` or crash-looping observed — this task fully completes both the
manifest and the live-deployment verification the brief called for.

## Note on worktree/branch state

This agent's assigned worktree (`agent-a71b3195e8ad3e13b`) started from `main` and did
not yet contain Tasks 1-3's work. Before starting, `worktree-ch12-deployment` (which
had Tasks 1-3 merged) was fast-forward-merged into this branch to get the required
base (`ftgo-credentials` Secret, `ftgo-common-config` ConfigMap, mysql/zookeeper/kafka/
elasticsearch/glitchtip StatefulSets/Deployment). Task 4's changes are committed on top
of that on branch `worktree-agent-a71b3195e8ad3e13b`.

## Commit

`641e5a3` — "feat: add platform services (service-registry, authorization-server,
config-server) and wait-for helper"
(preceded by fast-forward merge commit `90c5479` bringing in `worktree-ch12-deployment`,
i.e. Tasks 1-3, which this branch did not otherwise have)
