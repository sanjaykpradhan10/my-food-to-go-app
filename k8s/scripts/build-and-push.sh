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
# Task 10 appends the remaining 10 services to this same function-call list.
