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
