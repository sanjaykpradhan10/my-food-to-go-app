# FTGO on Kubernetes (Ch.12 §12.4)

Local Kubernetes deployment via Helm + kind, alongside the existing
compose.yml-based workflow (compose remains the default for day-to-day
development; this is the Ch.12 learning deployment).

## Prerequisites

- `kind`, `kubectl`, `helm` installed
- Docker running

## Setup

```bash
./k8s/scripts/setup-cluster.sh          # creates the kind cluster + local registry
./k8s/scripts/build-and-push.sh         # builds and pushes all service images (Task 10)
helm install ftgo ./k8s/ftgo --namespace ftgo --create-namespace
```

See `docs/ARCHITECTURE.md`'s Kubernetes section for the full resource
mapping from `compose.yml`.

## Install nginx-ingress (one-time, into the kind cluster)

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.3/deploy/static/provider/kind/deploy.yaml
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=120s
```

Then `curl http://localhost:18000/public/...` reaches `public-gateway` through the Ingress (port 18000 is forwarded to the ingress controller's port 80 by `k8s/kind-config.yaml`'s `extraPortMappings`).

## Install Linkerd (Ch.12 B3a)

```bash
curl --proto '=https' --tlsv1.2 -sSfL https://run.linkerd.io/install | sh
export PATH=$PATH:$HOME/.linkerd2/bin
kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.1/standard-install.yaml
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

Note: as of the `run.linkerd.io/install` edge-channel installer, `linkerd check --pre` requires
the Gateway API CRDs to already be present in the cluster (the `kubectl apply --server-side`
step above) — this is a step beyond what the stable-channel installer historically required.

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

It hits `order-service`'s `/actuator/health` in-cluster (no auth required) for a fixed
duration, logging the HTTP status and `X-Service-Version` response header on every request —
a clean rollout shows 100% `status=200` and a gap-free transition between version values.
