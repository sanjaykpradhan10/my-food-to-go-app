# Ch.12 §12.4 Core Kubernetes Deployment (Sub-project B1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the entire FTGO stack to a local `kind` Kubernetes cluster via a Helm chart, with the existing `ftgo-end-to-end-test` Cucumber suite passing against it as the acceptance gate.

**Architecture:** One umbrella Helm chart at `k8s/ftgo/`. Stateful infrastructure (MySQL, Kafka, Elasticsearch, GlitchTip's Postgres/Redis) becomes `StatefulSet`+`PersistentVolumeClaim`; the 8 business services + 2 gateways + 3 platform services (service-registry, authorization-server, config-server) share one generic, values-driven Deployment+Service template to avoid 13 near-duplicate template files; one-shot compose containers (`connector-registrar`, `kibana-index-pattern-registrar`, GlitchTip provisioning) become Helm-hook `Job`s. Images are built locally and pushed to a registry container running alongside `kind`.

**Tech Stack:** Helm 3, Kubernetes (`kind`), nginx-ingress, the existing Spring Boot / Dockerfile / `compose.yml` toolchain (unchanged).

## Global Constraints

- Cluster target is `kind`; manifest format is Helm charts (spec Decision 2, 3).
- Images are delivered via a local Docker registry container, not `kind load docker-image` (spec Decision 4).
- GlitchTip provisioning is a Kubernetes `Job` calling GlitchTip's REST API directly — never a pod with a `docker.sock` mount (spec Decision 5).
- Secrets use the same dev-only credential values already in `compose.yml` (`mysql`/`ftgo`, GlitchTip's `local-dev-only-not-a-real-secret`), templated into K8s `Secret` objects, not externalized to an untracked values file (spec Decision 6).
- `mobile-gateway` and `public-gateway` are reachable only via an nginx-ingress `Ingress`; no other service gets a `NodePort`/`LoadBalancer` (spec Decision 7).
- Acceptance for this sub-project is the full existing `ftgo-end-to-end-test` Cucumber suite passing against the live `kind` cluster via its ingress — not `kubectl get pods` alone (spec Decision 8).
- Every service's `readinessProbe`/`livenessProbe` must use its existing `/actuator/health` endpoint (Ch.11 §11.3.1) — no new health-check code.
- No rolling-update strategy tuning or service mesh work in this plan — those are sub-projects B2/B3 (spec Non-goals).

---

### Task 1: Chart scaffold, kind cluster config, local registry

**Files:**
- Create: `k8s/ftgo/Chart.yaml`
- Create: `k8s/ftgo/values.yaml`
- Create: `k8s/kind-config.yaml`
- Create: `k8s/scripts/setup-cluster.sh`
- Create: `k8s/README.md`

**Interfaces:**
- Produces: a running `kind` cluster named `ftgo`, a registry container reachable from cluster nodes at `localhost:5000` (host) / `kind-registry:5000` (in-cluster), and `k8s/ftgo/values.yaml`'s top-level `global:` block (`namespace`, `imageRegistry`) that every later task's templates read.

- [ ] **Step 1: Write the kind cluster config**

`k8s/kind-config.yaml`:
```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ftgo
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:5000"]
      endpoint = ["http://kind-registry:5000"]
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 80
        hostPort: 8000
        protocol: TCP
```

The `extraPortMappings` entry forwards the ingress controller's port 80 to host port 8000, so `curl localhost:8000/...` reaches the cluster's Ingress once nginx-ingress is installed (Task 9).

- [ ] **Step 2: Write the cluster + registry setup script**

`k8s/scripts/setup-cluster.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail

REGISTRY_NAME="kind-registry"
REGISTRY_PORT=5000

# Start the local registry if it isn't already running.
if ! docker inspect "${REGISTRY_NAME}" >/dev/null 2>&1; then
  docker run -d --restart=always -p "127.0.0.1:${REGISTRY_PORT}:5000" \
    --name "${REGISTRY_NAME}" registry:2
fi

# Create the kind cluster if it doesn't already exist.
if ! kind get clusters | grep -q '^ftgo$'; then
  kind create cluster --config "$(dirname "$0")/../kind-config.yaml"
fi

# Connect the registry container to the kind network so nodes can reach it
# by the "kind-registry" name used in kind-config.yaml's mirror endpoint.
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${REGISTRY_NAME}")" = 'null' ]; then
  docker network connect kind "${REGISTRY_NAME}"
fi

echo "kind cluster 'ftgo' ready. Registry reachable at localhost:${REGISTRY_PORT}."
```

- [ ] **Step 3: Run it and verify**

```bash
chmod +x k8s/scripts/setup-cluster.sh
./k8s/scripts/setup-cluster.sh
kubectl cluster-info --context kind-ftgo
docker ps --filter name=kind-registry --format '{{.Names}}: {{.Status}}'
```
Expected: `kubectl cluster-info` prints the control-plane URL; the registry container shows `Up`.

- [ ] **Step 4: Scaffold the Helm chart**

`k8s/ftgo/Chart.yaml`:
```yaml
apiVersion: v2
name: ftgo
description: FTGO microservices stack (Ch.12 §12.4)
version: 0.1.0
appVersion: "1.0"
```

`k8s/ftgo/values.yaml` (starting point — later tasks append sections to this same file):
```yaml
global:
  namespace: ftgo
  imageRegistry: "localhost:5000/ftgo"
  imageTag: local
```

- [ ] **Step 5: Write `k8s/README.md`**

```markdown
# FTGO on Kubernetes (Ch.12 §12.4)

Local Kubernetes deployment via Helm + kind, alongside the existing
compose.yml-based workflow (compose remains the default for day-to-day
development; this is the Ch.12 learning deployment).

## Prerequisites

- `kind`, `kubectl`, `helm` installed
- Docker running

## Setup

\`\`\`bash
./k8s/scripts/setup-cluster.sh          # creates the kind cluster + local registry
./k8s/scripts/build-and-push.sh         # builds and pushes all service images (Task 10)
helm install ftgo ./k8s/ftgo --namespace ftgo --create-namespace
\`\`\`

See `docs/ARCHITECTURE.md`'s Kubernetes section for the full resource
mapping from `compose.yml`.
```

- [ ] **Step 6: Commit**

```bash
git add k8s/kind-config.yaml k8s/scripts/setup-cluster.sh k8s/ftgo/Chart.yaml k8s/ftgo/values.yaml k8s/README.md
git commit -m "feat: scaffold Helm chart, kind cluster config, and local registry setup"
```

---

### Task 2: Namespace, shared Secret and ConfigMap templates

**Files:**
- Create: `k8s/ftgo/templates/namespace.yaml`
- Create: `k8s/ftgo/templates/secrets.yaml`
- Create: `k8s/ftgo/templates/configmap-common.yaml`
- Modify: `k8s/ftgo/values.yaml` (append `secrets:` and `commonEnv:` sections)

**Interfaces:**
- Consumes: `global.namespace` from Task 1's `values.yaml`.
- Produces: Secret `ftgo-credentials` (keys: `mysql-root-password`, `mysql-password`, `glitchtip-secret-key`) and ConfigMap `ftgo-common-config` (keys: `eureka-url`, `config-server-url`, `jwks-uri`, `tempo-otlp-endpoint`) — every later Deployment/StatefulSet task references these by name.

- [ ] **Step 1: Append secret/config values**

Append to `k8s/ftgo/values.yaml`:
```yaml
secrets:
  mysqlRootPassword: root
  mysqlPassword: ftgo
  glitchtipSecretKey: local-dev-only-not-a-real-secret

commonEnv:
  eurekaUrl: "http://service-registry:8761/eureka/"
  configServerUrl: "http://config-server:8888"
  jwksUri: "http://authorization-server:9000/oauth2/jwks"
  tempoOtlpEndpoint: "http://tempo:4318/v1/traces"
```
These mirror the literal values already in `compose.yml` (spec Decision 6 — same dev-only credentials, just relocated into K8s objects).

- [ ] **Step 2: Write the Namespace template**

`k8s/ftgo/templates/namespace.yaml`:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: {{ .Values.global.namespace }}
```

- [ ] **Step 3: Write the Secret template**

`k8s/ftgo/templates/secrets.yaml`:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ftgo-credentials
  namespace: {{ .Values.global.namespace }}
type: Opaque
stringData:
  mysql-root-password: {{ .Values.secrets.mysqlRootPassword | quote }}
  mysql-password: {{ .Values.secrets.mysqlPassword | quote }}
  glitchtip-secret-key: {{ .Values.secrets.glitchtipSecretKey | quote }}
```

- [ ] **Step 4: Write the common ConfigMap template**

`k8s/ftgo/templates/configmap-common.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ftgo-common-config
  namespace: {{ .Values.global.namespace }}
data:
  eureka-url: {{ .Values.commonEnv.eurekaUrl | quote }}
  config-server-url: {{ .Values.commonEnv.configServerUrl | quote }}
  jwks-uri: {{ .Values.commonEnv.jwksUri | quote }}
  tempo-otlp-endpoint: {{ .Values.commonEnv.tempoOtlpEndpoint | quote }}
```

- [ ] **Step 5: Verify the chart renders**

```bash
helm template ftgo k8s/ftgo | grep -A5 "kind: Secret"
```
Expected: the rendered Secret shows the three `stringData` keys with the values from `values.yaml`.

- [ ] **Step 6: Commit**

```bash
git add k8s/ftgo/templates/namespace.yaml k8s/ftgo/templates/secrets.yaml k8s/ftgo/templates/configmap-common.yaml k8s/ftgo/values.yaml
git commit -m "feat: add namespace, shared Secret, and common ConfigMap templates"
```

---

### Task 3: Stateful infrastructure (MySQL, Kafka, Elasticsearch, GlitchTip DB/Redis)

**Files:**
- Create: `k8s/ftgo/templates/mysql-statefulset.yaml`
- Create: `k8s/ftgo/templates/kafka-statefulset.yaml`
- Create: `k8s/ftgo/templates/elasticsearch-statefulset.yaml`
- Create: `k8s/ftgo/templates/glitchtip-db-statefulset.yaml`
- Create: `k8s/ftgo/templates/glitchtip-redis-deployment.yaml`
- Create: `k8s/ftgo/templates/init-sql-configmap.yaml`
- Modify: `k8s/ftgo/values.yaml` (append `mysql:`, `kafka:`, `elasticsearch:`, `glitchtipDb:` sections)

**Interfaces:**
- Consumes: `ftgo-credentials` Secret (Task 2) for `mysql-root-password`/`mysql-password`; `global.namespace`.
- Produces: in-cluster DNS names `mysql:3306`, `kafka:29092` (internal listener, matching compose's `KAFKA_ADVERTISED_LISTENERS` internal port), `elasticsearch:9200`, `glitchtip-db:5432`, `glitchtip-redis:6379` — every later task that needs these dependencies references these exact names.

- [ ] **Step 1: Append infra values**

Append to `k8s/ftgo/values.yaml`:
```yaml
mysql:
  image: mysql:8.4
  storageSize: 2Gi
kafka:
  zookeeperImage: confluentinc/cp-zookeeper:7.9.0
  kafkaImage: confluentinc/cp-kafka:7.9.0
elasticsearch:
  image: docker.elastic.co/elasticsearch/elasticsearch:8.15.3
  storageSize: 2Gi
glitchtipDb:
  image: postgres:16
  storageSize: 1Gi
```
Image tags copied verbatim from `compose.yml` — do not change versions.

- [ ] **Step 2: Write the MySQL init-sql ConfigMap**

`k8s/ftgo/templates/init-sql-configmap.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mysql-init-sql
  namespace: {{ .Values.global.namespace }}
data:
{{ (.Files.Glob "../../infrastructure/mysql/init.sql").AsConfig | indent 2 }}
```
This reuses the exact same `infrastructure/mysql/init.sql` that `compose.yml` bind-mounts (`./infrastructure/mysql/init.sql:/docker-entrypoint-initdb.d/init.sql`) — no duplicated SQL.

- [ ] **Step 3: Write the MySQL StatefulSet**

`k8s/ftgo/templates/mysql-statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql
  namespace: {{ .Values.global.namespace }}
spec:
  serviceName: mysql
  replicas: 1
  selector:
    matchLabels: {app: mysql}
  template:
    metadata:
      labels: {app: mysql}
    spec:
      containers:
        - name: mysql
          image: {{ .Values.mysql.image }}
          args: ["--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL", "--server-id=1"]
          ports: [{containerPort: 3306}]
          env:
            - name: MYSQL_ROOT_PASSWORD
              valueFrom: {secretKeyRef: {name: ftgo-credentials, key: mysql-root-password}}
            - name: MYSQL_USER
              value: ftgo
            - name: MYSQL_PASSWORD
              valueFrom: {secretKeyRef: {name: ftgo-credentials, key: mysql-password}}
          volumeMounts:
            - {name: mysql-data, mountPath: /var/lib/mysql}
            - {name: init-sql, mountPath: /docker-entrypoint-initdb.d}
          readinessProbe:
            exec: {command: ["mysqladmin", "ping", "-h", "localhost", "-u", "ftgo", "-pftgo"]}
            initialDelaySeconds: 10
            periodSeconds: 10
      volumes:
        - name: init-sql
          configMap: {name: mysql-init-sql}
  volumeClaimTemplates:
    - metadata: {name: mysql-data}
      spec:
        accessModes: ["ReadWriteOnce"]
        resources: {requests: {storage: "{{ .Values.mysql.storageSize }}"}}
---
apiVersion: v1
kind: Service
metadata:
  name: mysql
  namespace: {{ .Values.global.namespace }}
spec:
  clusterIP: None
  selector: {app: mysql}
  ports: [{port: 3306}]
```
The `readinessProbe` is the same `mysqladmin ping` command as `compose.yml`'s healthcheck — this is infrastructure health, not the app-level `/actuator/health` constraint, so reusing the exact mysql-native check is correct here.

- [ ] **Step 4: Write the Kafka/Zookeeper StatefulSets**

`k8s/ftgo/templates/kafka-statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: zookeeper
  namespace: {{ .Values.global.namespace }}
spec:
  serviceName: zookeeper
  replicas: 1
  selector: {matchLabels: {app: zookeeper}}
  template:
    metadata: {labels: {app: zookeeper}}
    spec:
      containers:
        - name: zookeeper
          image: {{ .Values.kafka.zookeeperImage }}
          ports: [{containerPort: 2181}]
          env:
            - {name: ZOOKEEPER_CLIENT_PORT, value: "2181"}
            - {name: ZOOKEEPER_TICK_TIME, value: "2000"}
---
apiVersion: v1
kind: Service
metadata: {name: zookeeper, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: zookeeper}
  ports: [{port: 2181}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: {{ .Values.global.namespace }}
spec:
  serviceName: kafka
  replicas: 1
  selector: {matchLabels: {app: kafka}}
  template:
    metadata: {labels: {app: kafka}}
    spec:
      initContainers:
        - name: wait-for-zookeeper
          image: busybox:1.36
          command: ["sh", "-c", "until nc -z zookeeper 2181; do sleep 2; done"]
      containers:
        - name: kafka
          image: {{ .Values.kafka.kafkaImage }}
          ports: [{containerPort: 29092}, {containerPort: 9092}]
          env:
            - {name: KAFKA_BROKER_ID, value: "1"}
            - {name: KAFKA_ZOOKEEPER_CONNECT, value: "zookeeper:2181"}
            - {name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"}
            - {name: KAFKA_LISTENERS, value: "INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092"}
            - {name: KAFKA_ADVERTISED_LISTENERS, value: "INTERNAL://kafka:29092,EXTERNAL://kafka:9092"}
            - {name: KAFKA_INTER_BROKER_LISTENER_NAME, value: "INTERNAL"}
            - {name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1"}
---
apiVersion: v1
kind: Service
metadata: {name: kafka, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: kafka}
  ports: [{name: internal, port: 29092}, {name: external, port: 9092}]
```
`KAFKA_ADVERTISED_LISTENERS`'s external entry uses the in-cluster service name `kafka` (not `localhost`, since compose's `localhost:9092` only made sense for host-machine access) — every consumer of Kafka in this cluster (in-cluster only) always resolves `kafka` via the `INTERNAL` listener on 29092 regardless, so this affects only clients connecting from outside the cluster on 9092.

- [ ] **Step 5: Write Elasticsearch and GlitchTip-DB/Redis**

`k8s/ftgo/templates/elasticsearch-statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: elasticsearch, namespace: {{ .Values.global.namespace }}}
spec:
  serviceName: elasticsearch
  replicas: 1
  selector: {matchLabels: {app: elasticsearch}}
  template:
    metadata: {labels: {app: elasticsearch}}
    spec:
      containers:
        - name: elasticsearch
          image: {{ .Values.elasticsearch.image }}
          ports: [{containerPort: 9200}]
          env:
            - {name: discovery.type, value: single-node}
            - {name: xpack.security.enabled, value: "false"}
            - {name: ES_JAVA_OPTS, value: "-Xms512m -Xmx512m"}
          volumeMounts: [{name: es-data, mountPath: /usr/share/elasticsearch/data}]
          readinessProbe:
            httpGet: {path: /_cluster/health, port: 9200}
            initialDelaySeconds: 20
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata: {name: es-data}
      spec:
        accessModes: ["ReadWriteOnce"]
        resources: {requests: {storage: "{{ .Values.elasticsearch.storageSize }}"}}
---
apiVersion: v1
kind: Service
metadata: {name: elasticsearch, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: elasticsearch}
  ports: [{port: 9200}]
```

`k8s/ftgo/templates/glitchtip-db-statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: glitchtip-db, namespace: {{ .Values.global.namespace }}}
spec:
  serviceName: glitchtip-db
  replicas: 1
  selector: {matchLabels: {app: glitchtip-db}}
  template:
    metadata: {labels: {app: glitchtip-db}}
    spec:
      containers:
        - name: glitchtip-db
          image: {{ .Values.glitchtipDb.image }}
          ports: [{containerPort: 5432}]
          env:
            - {name: POSTGRES_DB, value: glitchtip}
            - {name: POSTGRES_USER, value: glitchtip}
            - {name: POSTGRES_PASSWORD, value: glitchtip}
          volumeMounts: [{name: glitchtip-db-data, mountPath: /var/lib/postgresql/data}]
          readinessProbe:
            exec: {command: ["pg_isready", "-U", "glitchtip"]}
  volumeClaimTemplates:
    - metadata: {name: glitchtip-db-data}
      spec:
        accessModes: ["ReadWriteOnce"]
        resources: {requests: {storage: "{{ .Values.glitchtipDb.storageSize }}"}}
---
apiVersion: v1
kind: Service
metadata: {name: glitchtip-db, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: glitchtip-db}
  ports: [{port: 5432}]
```
`POSTGRES_PASSWORD` stays the literal `glitchtip` value here (matching `compose.yml` exactly) rather than the shared `ftgo-credentials` Secret — this is GlitchTip's own internal DB, unrelated to the FTGO MySQL credentials that Secret holds.

`k8s/ftgo/templates/glitchtip-redis-deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: glitchtip-redis, namespace: {{ .Values.global.namespace }}}
spec:
  replicas: 1
  selector: {matchLabels: {app: glitchtip-redis}}
  template:
    metadata: {labels: {app: glitchtip-redis}}
    spec:
      containers:
        - {name: glitchtip-redis, image: "redis:7", ports: [{containerPort: 6379}]}
---
apiVersion: v1
kind: Service
metadata: {name: glitchtip-redis, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: glitchtip-redis}
  ports: [{port: 6379}]
```
No PVC — matches `compose.yml`, where `glitchtip-redis` has no volume either (queue-only, disposable).

- [ ] **Step 6: Verify chart renders and deploy**

```bash
helm template ftgo k8s/ftgo | grep "kind: StatefulSet"
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get pods -w
```
Expected: 4 StatefulSets render (`mysql`, `zookeeper`, `kafka`, `elasticsearch`, `glitchtip-db` — 5 total); wait until `mysql-0`, `zookeeper-0`, `kafka-0`, `elasticsearch-0`, `glitchtip-db-0` all show `1/1 Running`.

- [ ] **Step 7: Commit**

```bash
git add k8s/ftgo/templates/mysql-statefulset.yaml k8s/ftgo/templates/kafka-statefulset.yaml k8s/ftgo/templates/elasticsearch-statefulset.yaml k8s/ftgo/templates/glitchtip-db-statefulset.yaml k8s/ftgo/templates/glitchtip-redis-deployment.yaml k8s/ftgo/templates/init-sql-configmap.yaml k8s/ftgo/values.yaml
git commit -m "feat: add stateful infrastructure (MySQL, Kafka, Elasticsearch, GlitchTip DB/Redis)"
```

---

### Task 4: Platform services (service-registry, authorization-server, config-server) + wait-for helper

**Files:**
- Create: `k8s/ftgo/templates/_helpers.tpl`
- Create: `k8s/ftgo/templates/service-registry.yaml`
- Create: `k8s/ftgo/templates/authorization-server.yaml`
- Create: `k8s/ftgo/templates/config-server.yaml`
- Create: `k8s/scripts/build-and-push.sh` (partial — service-registry/authorization-server/config-server entries only; Task 10 completes it)
- Modify: `k8s/ftgo/values.yaml` (append `platformServices:` section)

**Interfaces:**
- Consumes: `global.imageRegistry`/`global.imageTag` (Task 1), `ftgo-common-config` ConfigMap (Task 2), `mysql`/`kafka` Services (Task 3).
- Produces: the `ftgo.waitFor` named template in `_helpers.tpl` (input: a list of `{name, port}` pairs; output: an `initContainers` YAML block) — every later Deployment task (5 through 9) uses this instead of hand-writing wait loops. Also produces in-cluster DNS names `service-registry:8761`, `authorization-server:9000`, `config-server:8888`.

- [ ] **Step 1: Write the reusable wait-for-dependency helper**

`k8s/ftgo/templates/_helpers.tpl`:
```
{{/*
Renders an initContainers block that blocks until each named TCP dependency
is accepting connections, replacing compose.yml's depends_on/condition
chains (Kubernetes has no native equivalent — see spec Decision/mapping
table row "depends_on: condition:").
Usage: {{ include "ftgo.waitFor" (list (dict "name" "mysql" "port" 3306) (dict "name" "kafka" "port" 29092)) }}
*/}}
{{- define "ftgo.waitFor" -}}
{{- range . }}
- name: wait-for-{{ .name }}
  image: busybox:1.36
  command: ["sh", "-c", "until nc -z {{ .name }} {{ .port }}; do echo waiting for {{ .name }}:{{ .port }}; sleep 2; done"]
{{- end }}
{{- end -}}
```

- [ ] **Step 2: Append platform-service values**

Append to `k8s/ftgo/values.yaml`:
```yaml
platformServices:
  - name: service-registry
    port: 8761
    dependsOn: []
  - name: authorization-server
    port: 9000
    dependsOn: []
  - name: config-server
    port: 8888
    dependsOn: []
    extraEnv:
      CONFIG_SERVER_GIT_URI: "file:///config-source-repo"
```

- [ ] **Step 3: Write the three platform-service manifests**

`k8s/ftgo/templates/service-registry.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: service-registry, namespace: {{ .Values.global.namespace }}}
spec:
  replicas: 1
  selector: {matchLabels: {app: service-registry}}
  template:
    metadata: {labels: {app: service-registry}}
    spec:
      containers:
        - name: service-registry
          image: "{{ .Values.global.imageRegistry }}/ftgo-service-registry:{{ .Values.global.imageTag }}"
          ports: [{containerPort: 8761}]
          readinessProbe: {httpGet: {path: /actuator/health, port: 8761}, initialDelaySeconds: 15, periodSeconds: 10}
          livenessProbe: {httpGet: {path: /actuator/health, port: 8761}, initialDelaySeconds: 30, periodSeconds: 15}
---
apiVersion: v1
kind: Service
metadata: {name: service-registry, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: service-registry}
  ports: [{port: 8761}]
```

`k8s/ftgo/templates/authorization-server.yaml` follows the identical shape (image `ftgo-authorization-server`, port 9000, same probes pointed at `/oauth2/jwks` per `compose.yml`'s healthcheck — use `httpGet: {path: /oauth2/jwks, port: 9000}` instead of `/actuator/health`, matching `compose.yml`'s own choice of endpoint for this service).

`k8s/ftgo/templates/config-server.yaml` follows the same shape (image `ftgo-config-server`, port 8888, env var `CONFIG_SERVER_GIT_URI` from `.Values.platformServices` entry's `extraEnv`, no readiness probe — `compose.yml` has none for this service either, so don't add one it doesn't have).

- [ ] **Step 4: Start `k8s/scripts/build-and-push.sh`**

`k8s/scripts/build-and-push.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
REGISTRY="localhost:5000/ftgo"
TAG="local"

build_and_push() {
  local service="$1"
  docker build -t "${REGISTRY}/${service}:${TAG}" -f "${service}/Dockerfile" .
  docker push "${REGISTRY}/${service}:${TAG}"
}

build_and_push ftgo-service-registry
build_and_push ftgo-authorization-server
build_and_push ftgo-config-server
```
(Task 10 appends the remaining 10 services to this same function-call list.)

- [ ] **Step 5: Build, push, deploy, verify**

```bash
chmod +x k8s/scripts/build-and-push.sh
./k8s/scripts/build-and-push.sh
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get pods -l app=service-registry -w
```
Expected: `service-registry`, `authorization-server`, `config-server` pods reach `1/1 Running`.

- [ ] **Step 6: Commit**

```bash
git add k8s/ftgo/templates/_helpers.tpl k8s/ftgo/templates/service-registry.yaml k8s/ftgo/templates/authorization-server.yaml k8s/ftgo/templates/config-server.yaml k8s/scripts/build-and-push.sh k8s/ftgo/values.yaml
git commit -m "feat: add platform services (service-registry, authorization-server, config-server) and wait-for helper"
```

---

### Task 5: Business services — generic values-driven Deployment template

**Files:**
- Create: `k8s/ftgo/templates/app-service.yaml`
- Modify: `k8s/ftgo/values.yaml` (append `businessServices:` list — all 8 entries)
- Modify: `k8s/scripts/build-and-push.sh` (append 8 `build_and_push` calls)

**Interfaces:**
- Consumes: `ftgo.waitFor` helper (Task 4), `ftgo-common-config` ConfigMap (Task 2), `ftgo-credentials` Secret (Task 2), `mysql`/`kafka` Services (Task 3), `service-registry`/`authorization-server`/`config-server`/`tempo` Services (Task 4, Task 6).
- Produces: one `Deployment`+`Service` pair per entry in `values.yaml`'s `businessServices` list, iterated via a single `range` block — later tasks (gateways, e2e config) reference these Services by the same `{{ .name }}` DNS names `compose.yml` already uses (e.g. `restaurant-service:8085`).

This is the DRY point of the plan: rather than 8 near-identical template files (one per business service, differing only in name/port/db/env), one template iterates `.Values.businessServices`. This mirrors the actual duplication already visible in `compose.yml` itself (every business service repeats the same `depends_on`/`environment` shape) — collapsing it here is an intentional improvement, not scope creep, since 8 copy-pasted 40-line templates would themselves be a maintenance problem the next Ch.12 sub-project (B2) would have to touch 8 times instead of once.

- [ ] **Step 1: Append business-service values**

Append to `k8s/ftgo/values.yaml` — one entry per service, `env` holds only what's unique per service (datasource DB name, Kafka bootstrap flag, extra vars); everything common (Eureka URL, JWKS URI, Tempo endpoint) comes from the `ftgo-common-config` ConfigMap the template wires in for every entry:

```yaml
businessServices:
  - name: restaurant-service
    port: 8085
    db: ftgo_restaurant
    kafka: false
    dependsOn: [{name: mysql, port: 3306}]
  - name: order-service
    port: 8082
    db: ftgo_order
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}, {name: restaurant-service, port: 8085}]
    extraEnv:
      OUTBOX_PUBLISH_MODE: polling
      SAGA_MODE: choreography
      PERSISTENCE_MODE: jpa
  - name: kitchen-service
    port: 8083
    db: ftgo_kitchen
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}]
    extraEnv:
      SAGA_MODE: choreography
  - name: consumer-service
    port: 8081
    db: ftgo_consumer
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}]
    extraEnv:
      SAGA_MODE: choreography
  - name: accounting-service
    port: 8084
    db: ftgo_accounting
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}]
    extraEnv:
      SAGA_MODE: choreography
  - name: delivery-service
    port: 8086
    db: ftgo_delivery
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}]
    extraEnv:
      SAGA_MODE: choreography
  - name: order-history-service
    port: 8088
    db: ftgo_order_history
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}]
  - name: audit-log-service
    port: 8089
    db: ftgo_audit_log
    kafka: true
    dependsOn: [{name: mysql, port: 3306}, {name: kafka, port: 29092}]
```
Values (db names, ports, `SAGA_MODE`/`OUTBOX_PUBLISH_MODE`/`PERSISTENCE_MODE` defaults) copied verbatim from each service's block in `compose.yml`.

- [ ] **Step 2: Write the generic app-service template**

`k8s/ftgo/templates/app-service.yaml`:
```yaml
{{- range .Values.businessServices }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .name }}
  namespace: {{ $.Values.global.namespace }}
spec:
  replicas: 1
  selector: {matchLabels: {app: {{ .name }}}}
  template:
    metadata: {labels: {app: {{ .name }}}}
    spec:
      initContainers:
        {{- include "ftgo.waitFor" .dependsOn | nindent 8 }}
      containers:
        - name: {{ .name }}
          image: "{{ $.Values.global.imageRegistry }}/ftgo-{{ .name }}:{{ $.Values.global.imageTag }}"
          ports: [{containerPort: {{ .port }}}]
          env:
            - name: SPRING_DATASOURCE_URL
              value: "jdbc:mysql://mysql:3306/{{ .db }}"
            - name: SPRING_CONFIG_IMPORT
              value: "optional:configserver:$(CONFIG_SERVER_URL)"
            - name: CONFIG_SERVER_URL
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: config-server-url}}
            - name: MANAGEMENT_OTLP_TRACING_ENDPOINT
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: tempo-otlp-endpoint}}
            - name: EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: eureka-url}}
            - name: SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWKSETURI
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: jwks-uri}}
            {{- if .kafka }}
            - {name: SPRING_KAFKA_BOOTSTRAP_SERVERS, value: "kafka:29092"}
            {{- end }}
            {{- range $k, $v := .extraEnv }}
            - {name: {{ $k }}, value: {{ $v | quote }}}
            {{- end }}
          readinessProbe: {httpGet: {path: /actuator/health, port: {{ .port }}}, initialDelaySeconds: 15, periodSeconds: 10}
          livenessProbe: {httpGet: {path: /actuator/health, port: {{ .port }}}, initialDelaySeconds: 30, periodSeconds: 15}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ .name }}
  namespace: {{ $.Values.global.namespace }}
spec:
  selector: {app: {{ .name }}}
  ports: [{port: {{ .port }}}]
---
{{- end }}
```
Note the `$.Values`/`.Values` distinction: inside `range`, `.` is rebound to the current list element, so `$` (the root context) is required to reach `global.namespace`/`global.imageRegistry`.

- [ ] **Step 3: Append the 8 services to `build-and-push.sh`**

Append inside the same script after the Task 4 calls:
```bash
build_and_push ftgo-restaurant-service
build_and_push ftgo-order-service
build_and_push ftgo-kitchen-service
build_and_push ftgo-consumer-service
build_and_push ftgo-accounting-service
build_and_push ftgo-delivery-service
build_and_push ftgo-order-history-service
build_and_push ftgo-audit-log-service
```

- [ ] **Step 4: Build, deploy, verify**

```bash
./k8s/scripts/build-and-push.sh
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get pods -l 'app in (restaurant-service,order-service,kitchen-service,consumer-service,accounting-service,delivery-service,order-history-service,audit-log-service)' -w
```
Expected: all 8 pods reach `1/1 Running` (order-service and later ones may cycle through init-container waiting states first — that's expected while `restaurant-service` etc. come up).

- [ ] **Step 5: Commit**

```bash
git add k8s/ftgo/templates/app-service.yaml k8s/ftgo/values.yaml k8s/scripts/build-and-push.sh
git commit -m "feat: add business services via generic values-driven Deployment template"
```

---

### Task 6: Observability stack (Tempo, Prometheus, Grafana, Logstash, Kibana, Filebeat)

**Files:**
- Create: `k8s/ftgo/templates/tempo.yaml`
- Create: `k8s/ftgo/templates/prometheus.yaml`
- Create: `k8s/ftgo/templates/grafana.yaml`
- Create: `k8s/ftgo/templates/logstash.yaml`
- Create: `k8s/ftgo/templates/kibana.yaml`
- Create: `k8s/ftgo/templates/filebeat-daemonset.yaml`
- Create: `k8s/ftgo/templates/observability-configmaps.yaml`
- Modify: `k8s/ftgo/values.yaml` (append `observability:` section)

**Interfaces:**
- Consumes: `elasticsearch:9200` (Task 3).
- Produces: in-cluster DNS names `tempo:4318`, `prometheus:9090`, `grafana:3000`, `logstash:5044`, `kibana:5601` — already consumed by Task 5's `tempo-otlp-endpoint` reference and by Task 4's platform services (which also point `MANAGEMENT_OTLP_TRACING_ENDPOINT` at `tempo`, matching `compose.yml`).

- [ ] **Step 1: Append observability values**

```yaml
observability:
  tempoImage: grafana/tempo:2.6.1
  prometheusImage: prom/prometheus:v2.55.1
  grafanaImage: grafana/grafana:11.3.1
  logstashImage: docker.elastic.co/logstash/logstash:8.15.3
  kibanaImage: docker.elastic.co/kibana/kibana:8.15.3
  filebeatImage: docker.elastic.co/beats/filebeat:8.15.3
```

- [ ] **Step 2: ConfigMaps for existing config files**

`k8s/ftgo/templates/observability-configmaps.yaml` reuses the existing local config files the same way Task 3 reused `init.sql`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: {name: tempo-config, namespace: {{ .Values.global.namespace }}}
data:
{{ (.Files.Glob "../../tempo/tempo.yaml").AsConfig | indent 2 }}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: prometheus-config, namespace: {{ .Values.global.namespace }}}
data:
{{ (.Files.Glob "../../prometheus/*.yml").AsConfig | indent 2 }}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: logstash-pipeline, namespace: {{ .Values.global.namespace }}}
data:
{{ (.Files.Glob "../../logstash/pipeline/*").AsConfig | indent 2 }}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: filebeat-config, namespace: {{ .Values.global.namespace }}}
data:
{{ (.Files.Glob "../../filebeat/filebeat.yml").AsConfig | indent 2 }}
```
(Grafana's `provisioning`/`dashboards` directories are multiple files each — reuse the same `.Files.Glob(...).AsConfig` pattern for those in Step 3's Grafana manifest; skip repeating the snippet here.)

- [ ] **Step 3: Write Tempo, Prometheus, Grafana, Logstash, Kibana Deployments**

Each follows the same `Deployment` + `Service` + (where the compose service had a `volumes:` mount of a local config file) a `volumeMounts`/`volumes` pair pointing at the matching ConfigMap from Step 2. Concretely for Tempo (`k8s/ftgo/templates/tempo.yaml`):
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: tempo, namespace: {{ .Values.global.namespace }}}
spec:
  replicas: 1
  selector: {matchLabels: {app: tempo}}
  template:
    metadata: {labels: {app: tempo}}
    spec:
      containers:
        - name: tempo
          image: {{ .Values.observability.tempoImage }}
          args: ["-config.file=/etc/tempo.yaml"]
          ports: [{containerPort: 3200}, {containerPort: 4318}]
          volumeMounts: [{name: config, mountPath: /etc/tempo.yaml, subPath: tempo.yaml}]
      volumes: [{name: config, configMap: {name: tempo-config}}]
---
apiVersion: v1
kind: Service
metadata: {name: tempo, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: tempo}
  ports: [{name: http, port: 3200}, {name: otlp, port: 4318}]
```
Apply the same pattern (image from `values.yaml`, matching ports from `compose.yml`, ConfigMap-backed config file mount) to `prometheus.yaml` (port 9090, mounts `prometheus-config` at `/etc/prometheus`), `grafana.yaml` (port 3000, env `GF_AUTH_ANONYMOUS_ENABLED=true`/`GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer` copied from `compose.yml`, depends on `prometheus` via the `ftgo.waitFor` helper), `logstash.yaml` (port 5044, mounts `logstash-pipeline`, depends on `elasticsearch`), `kibana.yaml` (port 5601, env `ELASTICSEARCH_HOSTS=http://elasticsearch:9200`, readinessProbe `httpGet: {path: /api/status, port: 5601}` matching compose's healthcheck, depends on `elasticsearch`).

- [ ] **Step 4: Write the Filebeat DaemonSet**

`k8s/ftgo/templates/filebeat-daemonset.yaml` — per spec's resource-mapping table, Filebeat becomes a DaemonSet reading Kubernetes' pod-log directory instead of compose's Docker-socket + container-log-dir host mounts:
```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: filebeat, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {matchLabels: {app: filebeat}}
  template:
    metadata: {labels: {app: filebeat}}
    spec:
      serviceAccountName: default
      containers:
        - name: filebeat
          image: {{ .Values.observability.filebeatImage }}
          args: ["--strict.perms=false"]
          volumeMounts:
            - {name: config, mountPath: /usr/share/filebeat/filebeat.yml, subPath: filebeat.yml}
            - {name: varlogpods, mountPath: /var/log/pods, readOnly: true}
            - {name: varlibdockercontainers, mountPath: /var/lib/docker/containers, readOnly: true}
      volumes:
        - {name: config, configMap: {name: filebeat-config}}
        - {name: varlogpods, hostPath: {path: /var/log/pods}}
        - {name: varlibdockercontainers, hostPath: {path: /var/lib/docker/containers}}
```
`filebeat/filebeat.yml`'s autodiscovery config (read in Task 8 for the e2e verification, not modified here) still targets Docker container metadata, which `kind` nodes expose the same way as any Docker host — no filebeat.yml changes needed for this DaemonSet to work, since kind's node containers are themselves plain Docker containers.

- [ ] **Step 5: Deploy and verify**

```bash
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get pods -l 'app in (tempo,prometheus,grafana,logstash,kibana)' -w
kubectl -n ftgo get daemonset filebeat
```
Expected: all 5 Deployment pods `1/1 Running`; `filebeat` DaemonSet shows `DESIRED` == `CURRENT` == `READY` (1, matching kind's single-node default).

- [ ] **Step 6: Commit**

```bash
git add k8s/ftgo/templates/tempo.yaml k8s/ftgo/templates/prometheus.yaml k8s/ftgo/templates/grafana.yaml k8s/ftgo/templates/logstash.yaml k8s/ftgo/templates/kibana.yaml k8s/ftgo/templates/filebeat-daemonset.yaml k8s/ftgo/templates/observability-configmaps.yaml k8s/ftgo/values.yaml
git commit -m "feat: add observability stack (Tempo, Prometheus, Grafana, ELK, Filebeat)"
```

---

### Task 7: GlitchTip app + worker + REST-API provisioning Job

**Files:**
- Create: `k8s/ftgo/templates/glitchtip.yaml`
- Create: `k8s/ftgo/templates/glitchtip-provisioner-job.yaml`
- Create: `k8s/ftgo/templates/glitchtip-provisioner-configmap.yaml`
- Modify: `k8s/ftgo/values.yaml` (append `glitchtip:` section)

**Interfaces:**
- Consumes: `glitchtip-db:5432`, `glitchtip-redis:6379` (Task 3), `ftgo-credentials` Secret (`glitchtip-secret-key`, Task 2).
- Produces: Secret `glitchtip-dsn` (key `sentry-dsn`) — Task 5's business-service template and Task 9's gateway template both mount this in Task 8's rework... **correction**: Task 5 already deployed without Sentry wiring since GlitchTip didn't exist yet. This task adds the DSN Secret; Task 5's `app-service.yaml` template (already written) needs one small addition here to read it. See Step 4.

- [ ] **Step 1: Append GlitchTip values**

```yaml
glitchtip:
  image: glitchtip/glitchtip:v4.2.9
  domain: "http://glitchtip:8000"
```

- [ ] **Step 2: Write the GlitchTip app + worker Deployments**

`k8s/ftgo/templates/glitchtip.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: glitchtip, namespace: {{ .Values.global.namespace }}}
spec:
  replicas: 1
  selector: {matchLabels: {app: glitchtip}}
  template:
    metadata: {labels: {app: glitchtip}}
    spec:
      initContainers:
        {{- include "ftgo.waitFor" (list (dict "name" "glitchtip-db" "port" 5432) (dict "name" "glitchtip-redis" "port" 6379)) | nindent 8 }}
      containers:
        - name: glitchtip
          image: {{ .Values.glitchtip.image }}
          command: ["sh", "-c", "./bin/run-migrate.sh && ./bin/run-uwsgi.sh"]
          ports: [{containerPort: 8000}]
          env:
            - {name: DATABASE_URL, value: "postgres://glitchtip:glitchtip@glitchtip-db:5432/glitchtip"}
            - name: SECRET_KEY
              valueFrom: {secretKeyRef: {name: ftgo-credentials, key: glitchtip-secret-key}}
            - {name: REDIS_URL, value: "redis://glitchtip-redis:6379/0"}
            - {name: CELERY_BROKER_URL, value: "redis://glitchtip-redis:6379/0"}
            - {name: GLITCHTIP_DOMAIN, value: {{ .Values.glitchtip.domain | quote }}}
            - {name: DEFAULT_FROM_EMAIL, value: "glitchtip@localhost"}
            - {name: ENABLE_OPEN_USER_REGISTRATION, value: "false"}
            - {name: PORT, value: "8000"}
          readinessProbe:
            exec: {command: ["python3", "-c", "import urllib.request as u; u.urlopen('http://localhost:8000/_health/')"]}
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata: {name: glitchtip, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: glitchtip}
  ports: [{port: 8000}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: glitchtip-worker, namespace: {{ .Values.global.namespace }}}
spec:
  replicas: 1
  selector: {matchLabels: {app: glitchtip-worker}}
  template:
    metadata: {labels: {app: glitchtip-worker}}
    spec:
      initContainers:
        {{- include "ftgo.waitFor" (list (dict "name" "glitchtip-db" "port" 5432) (dict "name" "glitchtip-redis" "port" 6379)) | nindent 8 }}
      containers:
        - name: glitchtip-worker
          image: {{ .Values.glitchtip.image }}
          command: ["celery", "-A", "glitchtip", "worker", "-B", "-l", "info"]
          env:
            - {name: DATABASE_URL, value: "postgres://glitchtip:glitchtip@glitchtip-db:5432/glitchtip"}
            - name: SECRET_KEY
              valueFrom: {secretKeyRef: {name: ftgo-credentials, key: glitchtip-secret-key}}
            - {name: REDIS_URL, value: "redis://glitchtip-redis:6379/0"}
            - {name: CELERY_BROKER_URL, value: "redis://glitchtip-redis:6379/0"}
            - {name: GLITCHTIP_DOMAIN, value: {{ .Values.glitchtip.domain | quote }}}
            - {name: DEFAULT_FROM_EMAIL, value: "glitchtip@localhost"}
            - {name: ENABLE_OPEN_USER_REGISTRATION, value: "false"}
```
Env vars, images, commands copied verbatim from `compose.yml`'s `glitchtip`/`glitchtip-worker` blocks, including the existing comment there explaining why both `REDIS_URL` and `CELERY_BROKER_URL` must point at `glitchtip-redis`.

- [ ] **Step 3: Write the provisioning Job (replaces docker.sock-based provisioner — spec Decision 5)**

`k8s/ftgo/templates/glitchtip-provisioner-configmap.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: {name: glitchtip-provisioner-script, namespace: {{ .Values.global.namespace }}}
data:
  provision.sh: |
    #!/bin/sh
    set -eu
    # GlitchTip's REST API needs an authenticated session; the image ships
    # a superuser bootstrap via its Django management command, which this
    # Job execs directly (no docker.sock — unlike compose's provisioner,
    # this Job talks to the glitchtip Deployment's API from inside the
    # cluster, per spec Decision 5).
    echo "Provisioning GlitchTip org/project/DSN via REST API..."
    # (Full curl-based org/project/token creation sequence — same shape as
    # compose.yml's glitchtip/provision.sh script, reused verbatim here via
    # the ConfigMap below instead of re-deriving the API calls.)
```

Reuse the existing `glitchtip/provision.sh` script's logic rather than rewriting it: mount it via `.Files.Glob "../../glitchtip/provision.sh"` the same way Task 3/6 reused `init.sql`/`tempo.yaml`, replacing only its docker-specific invocation wrapper (compose's `entrypoint: sh -c "cd /glitchtip && sh provision.sh && cp dsn.env /shared/dsn.env"`) with a Job that runs the same script against `http://glitchtip:8000` and writes the result into a Secret via `kubectl create secret` (requires a `ServiceAccount` with `create secrets` RBAC — add a minimal `Role`/`RoleBinding` scoped to the `ftgo` namespace in this same file).

`k8s/ftgo/templates/glitchtip-provisioner-job.yaml`:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata: {name: glitchtip-provisioner, namespace: {{ .Values.global.namespace }}}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: glitchtip-provisioner, namespace: {{ .Values.global.namespace }}}
rules:
  - {apiGroups: [""], resources: ["secrets"], verbs: ["create", "get", "update"]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: glitchtip-provisioner, namespace: {{ .Values.global.namespace }}}
subjects: [{kind: ServiceAccount, name: glitchtip-provisioner}]
roleRef: {kind: Role, name: glitchtip-provisioner, apiGroup: rbac.authorization.k8s.io}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: glitchtip-provisioner
  namespace: {{ .Values.global.namespace }}
  annotations: {"helm.sh/hook": post-install,post-upgrade}
spec:
  template:
    spec:
      serviceAccountName: glitchtip-provisioner
      restartPolicy: Never
      initContainers:
        {{- include "ftgo.waitFor" (list (dict "name" "glitchtip" "port" 8000)) | nindent 8 }}
      containers:
        - name: provisioner
          image: bitnami/kubectl:1.31
          volumeMounts: [{name: script, mountPath: /scripts}]
          command: ["sh", "-c", "/scripts/provision.sh"]
      volumes: [{name: script, configMap: {name: glitchtip-provisioner-script, defaultMode: 0755}}]
```
The Helm `post-install,post-upgrade` hook annotation replaces compose's `depends_on: glitchtip-provisioner: condition: service_completed_successfully` chain (Kubernetes Jobs have no equivalent "block other resources until I finish" primitive — the hook ordering is the closest analog, and is sufficient here since `SPRING_SECURITY...` env vars aren't consumed until pod startup, which happens after `helm install` returns).

- [ ] **Step 4: Wire the DSN Secret into business services**

Modify `k8s/ftgo/templates/app-service.yaml` (from Task 5): add an `envFrom` entry reading the DSN Secret this Job produces, matching compose's approach of sourcing `SENTRY_DSN` from a shared volume at container startup:
```yaml
          env:
            - name: SPRING_DATASOURCE_URL
              value: "jdbc:mysql://mysql:3306/{{ .db }}"
            # ... (existing env entries unchanged) ...
            - name: SENTRY_DSN
              valueFrom: {secretKeyRef: {name: glitchtip-dsn, key: sentry-dsn, optional: true}}
```
`optional: true` is required here: this Deployment template renders before the provisioner Job necessarily completes on first install, and pods must not crash-loop waiting on a Secret key that appears moments later post-hook.

- [ ] **Step 5: Deploy and verify**

```bash
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get pods -l app=glitchtip -w
kubectl -n ftgo get job glitchtip-provisioner
kubectl -n ftgo get secret glitchtip-dsn -o jsonpath='{.data.sentry-dsn}' | base64 -d
```
Expected: `glitchtip`/`glitchtip-worker` pods `1/1 Running`; the Job shows `COMPLETIONS: 1/1`; the decoded DSN is a non-empty `http://...@glitchtip:8000/...` URL.

- [ ] **Step 6: Commit**

```bash
git add k8s/ftgo/templates/glitchtip.yaml k8s/ftgo/templates/glitchtip-provisioner-job.yaml k8s/ftgo/templates/glitchtip-provisioner-configmap.yaml k8s/ftgo/templates/app-service.yaml k8s/ftgo/values.yaml
git commit -m "feat: add GlitchTip app/worker and REST-API-based provisioning Job"
```

---

### Task 8: Kafka Connect + connector registration Job

**Files:**
- Create: `k8s/ftgo/templates/kafka-connect.yaml`
- Create: `k8s/ftgo/templates/connector-registrar-job.yaml`
- Modify: `k8s/ftgo/values.yaml` (append `kafkaConnect:` section)

**Interfaces:**
- Consumes: `kafka:29092` (Task 3), `mysql:3306` (Task 3).
- Produces: `kafka-connect:8083` Service, and a completed Debezium connector registration (no other task depends on this one's outputs directly — it's a leaf in the dependency graph, matching `compose.yml` where nothing depends on `connector-registrar`).

- [ ] **Step 1: Append values**

```yaml
kafkaConnect:
  image: debezium/connect:3.0.0.Final
  outboxPublishMode: polling
```
`outboxPublishMode: polling` matches `compose.yml`'s default (`${OUTBOX_PUBLISH_MODE:-polling}`) — the CDC path stays available by overriding this value, exactly as compose supports via its env var override.

- [ ] **Step 2: Write the Kafka Connect Deployment**

`k8s/ftgo/templates/kafka-connect.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: kafka-connect, namespace: {{ .Values.global.namespace }}}
spec:
  replicas: 1
  selector: {matchLabels: {app: kafka-connect}}
  template:
    metadata: {labels: {app: kafka-connect}}
    spec:
      initContainers:
        {{- include "ftgo.waitFor" (list (dict "name" "kafka" "port" 29092) (dict "name" "mysql" "port" 3306)) | nindent 8 }}
      containers:
        - name: kafka-connect
          image: {{ .Values.kafkaConnect.image }}
          ports: [{containerPort: 8083}]
          env:
            - {name: BOOTSTRAP_SERVERS, value: "kafka:29092"}
            - {name: GROUP_ID, value: "1"}
            - {name: CONFIG_STORAGE_TOPIC, value: "connect-configs"}
            - {name: OFFSET_STORAGE_TOPIC, value: "connect-offsets"}
            - {name: STATUS_STORAGE_TOPIC, value: "connect-status"}
          readinessProbe: {httpGet: {path: /, port: 8083}, initialDelaySeconds: 15, periodSeconds: 10}
---
apiVersion: v1
kind: Service
metadata: {name: kafka-connect, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: kafka-connect}
  ports: [{port: 8083}]
```
Image pinned to `3.0.0.Final` — do not change; `compose.yml`'s inline comment documents that `2.7.x` silently drops CDC events against MySQL 8.4.

- [ ] **Step 3: Write the connector-registrar Job**

`k8s/ftgo/templates/connector-registrar-job.yaml` reuses the existing connector JSON config file the same way earlier tasks reused `init.sql`/`tempo.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: {name: outbox-connector-config, namespace: {{ .Values.global.namespace }}}
data:
{{ (.Files.Glob "../../infrastructure/debezium/outbox-connector.json").AsConfig | indent 2 }}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: connector-registrar
  namespace: {{ .Values.global.namespace }}
  annotations: {"helm.sh/hook": post-install,post-upgrade}
spec:
  template:
    spec:
      restartPolicy: Never
      initContainers:
        {{- include "ftgo.waitFor" (list (dict "name" "kafka-connect" "port" 8083)) | nindent 8 }}
      containers:
        - name: connector-registrar
          image: curlimages/curl:8.10.1
          volumeMounts: [{name: config, mountPath: /outbox-connector.json, subPath: outbox-connector.json}]
          env: [{name: OUTBOX_PUBLISH_MODE, value: {{ .Values.kafkaConnect.outboxPublishMode | quote }}}]
          command:
            - sh
            - -c
            - |
              if [ "$OUTBOX_PUBLISH_MODE" = "cdc" ]; then
                curl -s -X PUT http://kafka-connect:8083/connectors/outbox-connector/config -H "Content-Type: application/json" -d @/outbox-connector.json
              else
                curl -s -X DELETE http://kafka-connect:8083/connectors/outbox-connector || true
              fi
      volumes: [{name: config, configMap: {name: outbox-connector-config}}]
```
Logic copied verbatim from `compose.yml`'s `connector-registrar` entrypoint.

- [ ] **Step 4: Deploy and verify**

```bash
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get pods -l app=kafka-connect -w
kubectl -n ftgo get job connector-registrar
```
Expected: `kafka-connect` pod `1/1 Running`; `connector-registrar` Job `COMPLETIONS: 1/1`.

- [ ] **Step 5: Commit**

```bash
git add k8s/ftgo/templates/kafka-connect.yaml k8s/ftgo/templates/connector-registrar-job.yaml k8s/ftgo/values.yaml
git commit -m "feat: add Kafka Connect and connector-registrar Job"
```

---

### Task 9: Gateways + nginx-ingress + Ingress resources

**Files:**
- Create: `k8s/ftgo/templates/mobile-gateway.yaml`
- Create: `k8s/ftgo/templates/public-gateway.yaml`
- Create: `k8s/ftgo/templates/ingress.yaml`
- Modify: `k8s/README.md` (add nginx-ingress install step)
- Modify: `k8s/scripts/build-and-push.sh` (append 2 gateway `build_and_push` calls)
- Modify: `k8s/ftgo/values.yaml` (append `gateways:` section)

**Interfaces:**
- Consumes: all 8 business-service Services (Task 5), `service-registry`/`authorization-server` (Task 4).
- Produces: externally reachable HTTP routes `http://localhost:8000/mobile/...` → `mobile-gateway:8090` and `http://localhost:8000/public/...` → `public-gateway:8091` — Task 12's e2e Kubernetes profile targets these exact paths.

- [ ] **Step 1: Append gateway values**

```yaml
gateways:
  - name: mobile-gateway
    port: 8090
    ingressPath: /mobile
    dependsOn: [{name: service-registry, port: 8761}, {name: order-service, port: 8082}, {name: kitchen-service, port: 8083}, {name: accounting-service, port: 8084}, {name: delivery-service, port: 8086}]
  - name: public-gateway
    port: 8091
    ingressPath: /public
    dependsOn: [{name: service-registry, port: 8761}, {name: order-service, port: 8082}, {name: kitchen-service, port: 8083}, {name: accounting-service, port: 8084}, {name: delivery-service, port: 8086}, {name: order-history-service, port: 8088}, {name: restaurant-service, port: 8085}]
```

- [ ] **Step 2: Write gateway Deployments**

`k8s/ftgo/templates/mobile-gateway.yaml` and `public-gateway.yaml` each hard-code their own `dependsOn`/`port` (not looped like Task 5's business services, since there are only 2 and their `dependsOn` lists differ enough — and their env vars, unlike the business services, are identical to each other, `GATEWAY_JWT_JWKSETURI`/`EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE`/`MANAGEMENT_OTLP_TRACING_ENDPOINT`/`SPRING_CONFIG_IMPORT` — all sourced from `ftgo-common-config`, same as Task 5's pattern):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: mobile-gateway, namespace: {{ .Values.global.namespace }}}
spec:
  replicas: 1
  selector: {matchLabels: {app: mobile-gateway}}
  template:
    metadata: {labels: {app: mobile-gateway}}
    spec:
      initContainers:
        {{- include "ftgo.waitFor" (index .Values.gateways 0).dependsOn | nindent 8 }}
      containers:
        - name: mobile-gateway
          image: "{{ .Values.global.imageRegistry }}/ftgo-mobile-gateway:{{ .Values.global.imageTag }}"
          ports: [{containerPort: 8090}]
          env:
            - name: EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: eureka-url}}
            - name: GATEWAY_JWT_JWKSETURI
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: jwks-uri}}
            - name: MANAGEMENT_OTLP_TRACING_ENDPOINT
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: tempo-otlp-endpoint}}
            - name: SPRING_CONFIG_IMPORT
              value: "optional:configserver:$(CONFIG_SERVER_URL)"
            - name: CONFIG_SERVER_URL
              valueFrom: {configMapKeyRef: {name: ftgo-common-config, key: config-server-url}}
          readinessProbe: {httpGet: {path: /actuator/health, port: 8090}, initialDelaySeconds: 15, periodSeconds: 10}
---
apiVersion: v1
kind: Service
metadata: {name: mobile-gateway, namespace: {{ .Values.global.namespace }}}
spec:
  selector: {app: mobile-gateway}
  ports: [{port: 8090}]
```
`public-gateway.yaml` is identical except `name: public-gateway`, `port: 8091`, and `(index .Values.gateways 1).dependsOn`.

- [ ] **Step 3: Write the Ingress**

`k8s/ftgo/templates/ingress.yaml`:
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ftgo-gateways
  namespace: {{ .Values.global.namespace }}
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /$2
spec:
  ingressClassName: nginx
  rules:
    - http:
        paths:
          - path: /mobile(/|$)(.*)
            pathType: ImplementationSpecific
            backend: {service: {name: mobile-gateway, port: {number: 8090}}}
          - path: /public(/|$)(.*)
            pathType: ImplementationSpecific
            backend: {service: {name: public-gateway, port: {number: 8091}}}
```
The `rewrite-target`/capture-group pattern strips the `/mobile` or `/public` prefix before forwarding, so `http://localhost:8000/mobile/orders/1` reaches `mobile-gateway`'s `/orders/1` route unchanged — the gateways' own route definitions need no path-prefix awareness added for Kubernetes.

- [ ] **Step 4: Document nginx-ingress installation**

Append to `k8s/README.md`, after the existing Setup section:
```markdown
## Install nginx-ingress (one-time, into the kind cluster)

\`\`\`bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.3/deploy/static/provider/kind/deploy.yaml
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=120s
\`\`\`

Then `curl http://localhost:8000/public/...` reaches `public-gateway` through the Ingress (port 8000 is forwarded to the ingress controller's port 80 by `k8s/kind-config.yaml`'s `extraPortMappings`).
```

- [ ] **Step 5: Append gateways to `build-and-push.sh`**

```bash
build_and_push ftgo-mobile-gateway
build_and_push ftgo-public-gateway
```

- [ ] **Step 6: Deploy and verify end-to-end reachability**

```bash
./k8s/scripts/build-and-push.sh
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get pods -l 'app in (mobile-gateway,public-gateway)' -w
curl -f http://localhost:8000/public/actuator/health
```
Expected: both gateway pods `1/1 Running`; the curl returns `{"status":"UP"}` (or the actuator health JSON shape this project already uses elsewhere).

- [ ] **Step 7: Commit**

```bash
git add k8s/ftgo/templates/mobile-gateway.yaml k8s/ftgo/templates/public-gateway.yaml k8s/ftgo/templates/ingress.yaml k8s/README.md k8s/scripts/build-and-push.sh k8s/ftgo/values.yaml
git commit -m "feat: add gateways, nginx-ingress, and Ingress routing"
```

---

### Task 10: Kibana index-pattern registration Job

**Files:**
- Create: `k8s/ftgo/templates/kibana-index-pattern-job.yaml`

**Interfaces:**
- Consumes: `kibana:5601` (Task 6).
- Produces: nothing further consumed by later tasks — a leaf Job, same as `compose.yml`'s `kibana-index-pattern-registrar`.

- [ ] **Step 1: Write the Job**

Reuses the existing `kibana/saved-objects/index-pattern.ndjson` file the same way earlier tasks reused other local config files:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: {name: kibana-index-pattern, namespace: {{ .Values.global.namespace }}}
data:
{{ (.Files.Glob "../../kibana/saved-objects/index-pattern.ndjson").AsConfig | indent 2 }}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: kibana-index-pattern-registrar
  namespace: {{ .Values.global.namespace }}
  annotations: {"helm.sh/hook": post-install,post-upgrade}
spec:
  template:
    spec:
      restartPolicy: Never
      initContainers:
        {{- include "ftgo.waitFor" (list (dict "name" "kibana" "port" 5601)) | nindent 8 }}
      containers:
        - name: registrar
          image: curlimages/curl:8.10.1
          volumeMounts: [{name: pattern, mountPath: /index-pattern.ndjson, subPath: index-pattern.ndjson}]
          command:
            - sh
            - -c
            - |
              curl -s -X POST http://kibana:5601/api/saved_objects/_import?overwrite=true \
                -H "kbn-xsrf: true" \
                --form file=@/index-pattern.ndjson
      volumes: [{name: pattern, configMap: {name: kibana-index-pattern}}]
```
Logic copied verbatim from `compose.yml`'s `kibana-index-pattern-registrar` entrypoint.

- [ ] **Step 2: Deploy and verify**

```bash
helm upgrade --install ftgo k8s/ftgo --namespace ftgo --create-namespace
kubectl -n ftgo get job kibana-index-pattern-registrar
```
Expected: `COMPLETIONS: 1/1`.

- [ ] **Step 3: Commit**

```bash
git add k8s/ftgo/templates/kibana-index-pattern-job.yaml
git commit -m "feat: add Kibana index-pattern registration Job"
```

---

### Task 11: Full-cluster deployment verification

**Files:** none created — verification-only task.

**Interfaces:**
- Consumes: everything from Tasks 1–10.
- Produces: a confirmed-healthy cluster state that Task 12's e2e run depends on.

- [ ] **Step 1: Fresh deploy from scratch**

```bash
kind delete cluster --name ftgo || true
docker rm -f kind-registry || true
./k8s/scripts/setup-cluster.sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.3/deploy/static/provider/kind/deploy.yaml
kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=120s
./k8s/scripts/build-and-push.sh
helm install ftgo k8s/ftgo --namespace ftgo --create-namespace
```

- [ ] **Step 2: Wait for and confirm every pod is Ready**

```bash
kubectl -n ftgo wait --for=condition=Ready pod --all --timeout=600s
kubectl -n ftgo get pods
```
Expected: every listed pod shows `1/1 Running` (Jobs show `Completed`, which `wait --for=condition=Ready` treats as satisfied once they've run).

- [ ] **Step 3: Spot-check each Helm-hook Job actually did its job**

```bash
kubectl -n ftgo get secret glitchtip-dsn -o jsonpath='{.data.sentry-dsn}' | base64 -d | grep -q '^http' && echo "DSN OK"
kubectl -n ftgo logs job/connector-registrar | tail -5
kubectl -n ftgo logs job/kibana-index-pattern-registrar | tail -5
```
Expected: `DSN OK` printed; connector-registrar log shows either a successful DELETE (default `polling` mode) or a connector-config PUT response; kibana-index-pattern-registrar log shows a JSON response with no `"error"` key.

- [ ] **Step 4: No commit** — this task only verifies prior commits; nothing new to stage.

---

### Task 12: E2E test Kubernetes profile and full-suite run

**Files:**
- Create: `ftgo-end-to-end-test/src/test/resources/application-k8s.yml` (or the equivalent existing profile-config location — check `ftgo-end-to-end-test/src/test/resources/` for the current `application.yml`'s structure before creating this, since base URLs may already be centralized in one file rather than per-profile)
- Modify: whichever step-definition file(s) currently hardcode `http://localhost:<gateway-port>` base URLs, to read from the profile instead

**Interfaces:**
- Consumes: `http://localhost:8000/mobile` and `http://localhost:8000/public` (Task 9's Ingress).
- Produces: a passing `ftgo-end-to-end-test` run against the K8s cluster — the sub-project's acceptance gate (spec Decision 8).

- [ ] **Step 1: Locate the existing base-URL configuration**

```bash
grep -rn "localhost:809" ftgo-end-to-end-test/src/test/
```
This finds every place the mobile/public gateway ports are hardcoded — read each hit before writing the profile, since the exact mechanism (Spring `@Value`, a Cucumber `World` constant, a properties file) determines where the K8s profile's values need to plug in.

- [ ] **Step 2: Add the K8s-targeting profile**

Using whatever mechanism Step 1 found, add a profile/properties variant that sets:
```
mobile-gateway.base-url=http://localhost:8000/mobile
public-gateway.base-url=http://localhost:8000/public
```
in place of the existing `http://localhost:8090`/`http://localhost:8091` compose-targeting values. Every other service in the existing scenarios is reached indirectly through these two gateways (per spec's Testing section), so no other base-URL changes are needed.

- [ ] **Step 3: Run the full suite against the live cluster**

With Task 11's cluster still up:
```bash
./gradlew :ftgo-end-to-end-test:test -Dspring.profiles.active=k8s
```
(Substitute the actual profile-activation mechanism found in Step 1 if it isn't a Spring profile flag.)

Expected: all existing Cucumber scenarios pass with 0 failures — this is the sub-project's acceptance gate. If any scenario fails, treat it as a defect in Tasks 1–10's manifests (a missing env var, wrong dependency-wait target, wrong port) and fix the relevant task's template before proceeding — do not weaken the test.

- [ ] **Step 4: Commit**

```bash
git add ftgo-end-to-end-test/
git commit -m "test: add Kubernetes profile for e2e suite, verified against live kind cluster"
```

---

### Task 13: Documentation sweep

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: the completed, verified deployment from Tasks 1–12.
- Produces: docs reflecting Ch.12 sub-project B1 done — no further tasks in this plan.

- [ ] **Step 1: Update root `README.md`**

- Tech stack section: add a row/line noting Kubernetes (`kind` + Helm) as an alternative local deployment target alongside `compose.yml`.
- "Running locally" section: add a subsection pointing at `k8s/README.md` for the Kubernetes path, without removing or restructuring the existing compose instructions (compose remains the default/primary path).
- Book progress table: update the Ch.12 row — mark §12.4 sub-project B1 (core deployment) done, note B2 (zero-downtime) and B3 (service mesh) as not yet started, and reference sub-project A's `docs/CH12-DEPLOYMENT.md` for §12.1–12.3.

- [ ] **Step 2: Update `CONTEXT.md`**

- Current position: note Ch.12 B1 (Kubernetes core deployment) done, B2/B3 pending, sub-project A done.
- Book progress table: same Ch.12 row update as README, at the level of detail this table already carries for Ch.11's rows (see the existing Ch.11 row for the expected depth).
- Session log: append an entry following the existing format (see the 2026-08-11 audit-logging entry as a template) describing what B1 built: Helm chart structure, the generic app-service template decision and why, the GlitchTip provisioning-Job replacement, the e2e K8s profile, and the verification result from Task 11/12.
- Footer line: update `*Last updated: ...*` to reflect this change, following the existing pattern.

- [ ] **Step 3: Add a Kubernetes section to `docs/ARCHITECTURE.md`**

Add a new `## Kubernetes deployment (Ch.12, §12.4)` section, matching the depth of existing pattern sections (e.g. the Log aggregation or Audit logging sections) — cover: the compose-to-K8s resource mapping table (from the spec), the generic app-service template and why it exists instead of 8 separate files, the `ftgo.waitFor` helper as the `depends_on` replacement, and the GlitchTip provisioning-Job flow (with a short sequence description: Job waits for `glitchtip` readiness → calls REST API → writes `glitchtip-dsn` Secret → business-service pods pick it up via `optional: true` secretKeyRef on next restart or if not yet started).

- [ ] **Step 4: Commit**

```bash
git add README.md CONTEXT.md docs/ARCHITECTURE.md
git commit -m "docs: chapter-completion-style sweep for Ch.12 §12.4 sub-project B1 (Kubernetes core deployment)"
```
