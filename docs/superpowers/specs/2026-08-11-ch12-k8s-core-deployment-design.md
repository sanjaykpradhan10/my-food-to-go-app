# Ch.12 §12.4 — Core Kubernetes Deployment (Sub-project B1)

**Status:** Approved design, spec for Ch.12 sub-project B1 (Deploying microservices — Kubernetes)

## Goal

Deploy the entire FTGO stack (all app services + all infrastructure currently in `compose.yml`) to a local Kubernetes cluster via a Helm chart, replacing `compose.yml` as this chapter's target runtime, with functional parity proven by the existing end-to-end test suite.

## Background

Chapter 12 §12.4 walks through deploying FTGO's `restaurant-service` and an API gateway to Kubernetes as an illustrative example. This project already runs its entire stack via `compose.yml` (~30 services: 13 buildable app images, MySQL, Kafka/Zookeeper, the ELK stack, Prometheus/Grafana/Tempo, GlitchTip + its Postgres/Redis, and several one-shot setup containers). Rather than a partial illustrative deployment, this sub-project converts the whole stack, since compose already proves the full system works together and a partial K8s conversion would leave two parallel, drifting deployment definitions.

This is sub-project B1 of three planned Ch.12 §12.4 sub-projects:
- **B1 (this spec):** core deployment — get everything running on Kubernetes.
- **B2 (future):** zero-downtime rolling deployments.
- **B3 (future):** service mesh (Istio or Linkerd), closing out the topic deferred from Ch.11 §11.4.

## Decisions

These were settled during brainstorming and are binding for this sub-project:

1. **Scope:** entire stack (all `compose.yml` services), not just `restaurant-service` + gateway.
2. **Cluster target:** `kind` (Kubernetes-in-Docker).
3. **Manifest format:** Helm charts (not plain YAML).
4. **Image delivery:** a local Docker registry container; built images are pushed there, and Helm `values.yaml` points each Deployment at that registry (not `kind load docker-image`).
5. **GlitchTip provisioning:** replace the compose `glitchtip-provisioner` (which shells out to `docker run` via a host `docker.sock` mount — inapplicable inside a pod) with a Kubernetes `Job` that calls GlitchTip's REST API directly to create the org/project/DSN, writing the result into a `Secret` that app pods mount. Same end state as today (auto-provisioned DSN, no manual step), different mechanism.
6. **Secrets:** Kubernetes `Secret` objects, Helm-templated, using the same dev-only credential values `compose.yml` already uses (e.g. `mysql`/`ftgo`, GlitchTip's `local-dev-only-not-a-real-secret`). Not externalized to an untracked values file — this is a local learning deployment, and compose already keeps these values in-repo.
7. **External access:** an Ingress controller (nginx-ingress, installed into the `kind` cluster as a documented prerequisite) fronts `mobile-gateway` and `public-gateway`.
8. **Verification:** the existing `ftgo-end-to-end-test` Cucumber suite gets a Kubernetes profile (base URLs pointed at the ingress instead of `localhost:<port>`) and is run against the live `kind` cluster as this sub-project's acceptance gate — not just `kubectl get pods` / manual curls.

## Architecture

A single umbrella Helm chart at `k8s/ftgo/`, with subcharts or grouped templates per service. `values.yaml` holds per-service image references (pointing at the local registry), resource requests, and environment-specific config (replica counts, etc.), following the same "one place to change environment-specific values" principle `config-repo/` already uses for application config.

### Resource mapping from `compose.yml`

| Compose concept | Kubernetes equivalent | Services |
|---|---|---|
| Service with a named volume | `StatefulSet` + `PersistentVolumeClaim` | `mysql`, `zookeeper`+`kafka`, `elasticsearch`, `glitchtip-db`, `glitchtip-redis` |
| Stateless service, published port | `Deployment` + `Service` (ClusterIP) | 13 app services, `authorization-server`, `config-server`, `service-registry`, `tempo`, `prometheus`, `grafana`, `logstash`, `kibana`, `glitchtip`, `glitchtip-worker` |
| Host-mount-based log/metric collector | `DaemonSet` | `filebeat` (reads `/var/log/pods` instead of compose's Docker-socket + container-log-dir mounts) |
| One-shot setup container (`restart: "no"`) | Helm hook `Job` (`post-install,post-upgrade`) | `connector-registrar`, `kibana-index-pattern-registrar`, GlitchTip provisioning (replaces `glitchtip-provisioner`, per Decision 5) |
| `depends_on: condition: service_healthy/service_started` | `initContainers` (wait-for-dependency busybox/curl loops) + Helm hook weights for ordering across Jobs | All services with `depends_on` |
| Plaintext `environment:` values (non-secret) | `ConfigMap`, Helm-templated | All services |
| Plaintext `environment:` values (credentials) | `Secret`, Helm-templated (Decision 6) | `mysql`, `glitchtip*`, any service consuming those credentials |
| `ports:` host publish | `Service` (ClusterIP) for internal traffic; `Ingress` for the two gateways only (Decision 7) | All |
| Docker-socket-mounted `glitchtip-provisioner` | K8s `Job` calling GlitchTip's REST API (Decision 5) | `glitchtip-provisioner` → new job |

### Config-server

`config-server`'s existing Dockerfile approach (copying the build-stage git checkout into the image so JGit has a real `.git` to open, per the existing inline comment in `compose.yml`) carries over unchanged — this is an image-build concern, not a runtime/orchestration concern, so it needs no Kubernetes-specific handling.

### Health checks and readiness

Every service's existing `/actuator/health` endpoint (Ch.11 §11.3.1) backs both `readinessProbe` and `livenessProbe` in its Deployment/StatefulSet template — no new health-check code, this sub-project only wires K8s to consume what already exists.

### Kafka Connect / Debezium

`kafka-connect` and its `connector-registrar` job carry over as a `Deployment` + `Job` pair, same as compose, since this isn't a stateful-volume concern beyond what Kafka itself needs.

## Image build & delivery pipeline

1. A new script (e.g. `k8s/scripts/build-and-push.sh`) builds each of the 13 app-service Dockerfiles the same way `docker compose build` does today, tags each image for the local registry (e.g. `localhost:5000/ftgo/<service>:local`), and pushes.
2. The local registry itself runs as a plain `docker run registry:2` container connected to the `kind` network (documented setup step, not part of the Helm chart — infrastructure-for-the-tooling, not part of the deployed application).
3. `values.yaml` per-service `image.repository`/`image.tag` values point at that registry.

## Testing

- `ftgo-end-to-end-test` gains a Kubernetes profile: a new properties file (or Cucumber profile) that swaps every `http://localhost:<port>` base URL for the ingress host/paths for `mobile-gateway` and `public-gateway` (all other services are only reached indirectly through those two gateways in the existing e2e scenarios, so no other base-URL changes are needed).
- Acceptance for this sub-project: `helm install` into a fresh `kind` cluster succeeds, all pods reach `Running`/`Ready`, and the full existing Cucumber suite passes against the cluster's ingress with no scenario changes beyond the base-URL swap.

## Non-goals (deferred to B2/B3)

- Rolling-update strategy tuning / zero-downtime deployment demonstration (B2).
- Service mesh installation, sidecar injection, canary/traffic-split releases (B3).
- CI integration for the `kind` cluster (out of scope entirely for this book-learning project unless requested later).

## Documentation / CLAUDE.md sync

Per-change: root `README.md` (tech stack, running-locally section gains a "Kubernetes (kind)" alternative alongside the existing compose instructions, Book progress table row for Ch.12 updated to reflect B1 done), `CONTEXT.md` (Current position, Services to build table if applicable, Patterns reference, session log entry). No chapter-completion full sweep yet — Ch.12 doesn't flip to Done until B2 and B3 (and sub-project A) also land.
