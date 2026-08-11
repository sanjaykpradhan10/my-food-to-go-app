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
