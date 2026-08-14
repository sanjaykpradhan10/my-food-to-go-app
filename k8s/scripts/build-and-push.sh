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
build_and_push ftgo-restaurant-service
build_and_push ftgo-order-service
build_and_push ftgo-kitchen-service
build_and_push ftgo-consumer-service
build_and_push ftgo-accounting-service
build_and_push ftgo-delivery-service
build_and_push ftgo-order-history-service
build_and_push ftgo-audit-log-service
build_and_push ftgo-mobile-gateway
build_and_push ftgo-public-gateway
