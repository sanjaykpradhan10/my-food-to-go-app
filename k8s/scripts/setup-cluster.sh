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
