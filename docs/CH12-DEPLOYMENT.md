# Chapter 12 — Deploying Microservices

This document maps FTGO's actual deployment setup onto the four patterns
*Microservices Patterns* Ch.12 covers. §12.1–§12.3 describe what already
exists (built up since Ch.4 and extended through Ch.11); §12.4
(Kubernetes) is new work — see `docs/ARCHITECTURE.md`'s Kubernetes
section once sub-project B1 lands.

## §12.1 — Language-specific packaging format

Each service is packaged as a Spring Boot executable ("fat") JAR via the
`bootJar` Gradle task, e.g. `./gradlew :ftgo-restaurant-service:bootJar`.
The JAR bundles the service's compiled classes plus every runtime
dependency (Spring, Jackson, the MySQL driver, etc.) into a single
artifact — no separate classpath assembly step is needed at deploy time.

The root `build.gradle`'s `actuatorModules` block (lines 69–72) centralizes
the Spring Boot Actuator, Prometheus metrics, OpenTelemetry tracing, JSON
logging, and Sentry exception tracking setup; it applies to 10 services
(`ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`,
`ftgo-restaurant-service`, `ftgo-accounting-service`, `ftgo-delivery-service`,
`ftgo-order-history-service`, `ftgo-mobile-gateway`, `ftgo-public-gateway`,
and `ftgo-audit-log-service`) that run as standalone processes/containers
and need observability endpoints. This keeps per-service `build.gradle` files
focused on that service's own dependencies rather than repeating
observability/actuator setup in all of them.

Each service's Dockerfile packages this pattern into a container image in
two stages — see `ftgo-restaurant-service/Dockerfile` as the
representative example:

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-restaurant-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-restaurant-service/build/libs/*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["sh", "-c", "[ -f /shared/dsn.env ] && export $(grep '^SENTRY_DSN=' /shared/dsn.env) ; exec java -jar app.jar"]
```

Stage 1 (`build`) runs the Gradle `bootJar` task inside a JDK image.
Stage 2 copies only the resulting JAR into a slim JRE base image — the
book's language-specific packaging pattern gives fast startup (no
container build step at deploy time, just `java -jar`) at the cost of no
OS-level isolation between services sharing a host, which is exactly what
§12.3's container wrapping (below) addresses.

## §12.2 — Service as a VM

The book's VM pattern packages each service as a machine image (e.g. an
AMI) and deploys one VM instance per service instance.

**This project does not implement this pattern.** There is no VM
provisioning tooling (no Packer configs, no cloud image definitions) in
this repo. This mirrors how Ch.11 §11.4 (microservice chassis / service
mesh) was left as a conceptual, not-implemented topic in that chapter's
sweep.

The book itself motivates skipping this pattern: VM images are slow to
build and provision (minutes, not seconds), and running one full guest OS
per service instance wastes memory and disk compared to containers, which
share the host kernel. §12.3 below is the pattern this project uses
instead.

## §12.3 — Service as a container

Every FTGO service already runs as a container, orchestrated locally by
`compose.yml`. 31 service definitions cover the app services, infrastructure
(MySQL, Kafka, the ELK stack, Prometheus/Grafana/Tempo, GlitchTip and its
Postgres/Redis), and several one-shot setup containers.

**Two-stage builds.** Every service Dockerfile follows the same
build-then-runtime-stage pattern shown in §12.1 above: a `-jdk` base image
runs the Gradle build, a separate `-jre` base image runs only the
resulting JAR. This keeps the final image free of the JDK's full compiler
toolchain and the Gradle wrapper/cache.

**Startup ordering via health checks.** `compose.yml`'s `depends_on:
condition: service_healthy` entries consume the `/actuator/health`
endpoints every service exposes (Ch.11 §11.3.1) — e.g. the restaurant-service's dependency on MySQL:

```yaml
depends_on:
  mysql:
    condition: service_healthy
```

Compose won't start a dependent service until its dependency's Actuator health
check reports `UP`.

**Gaps relative to the book's container pattern.** Compose covers
single-host container orchestration but stops there:

- **No image registry.** Images exist only in the local Docker daemon,
  built by `docker compose build` — there's no push/pull step, so images
  can't be distributed to another host.
- **No orchestrator.** Compose starts and stops containers on one
  machine; it has no concept of scheduling across multiple hosts,
  automatic rescheduling on failure, or horizontal scaling.
- **No managed rollout.** `docker compose up` recreates containers
  in-place; there's no rolling-update mechanism that keeps old instances
  serving traffic while new ones start.

These three gaps are exactly what Kubernetes (§12.4) adds — see the
Kubernetes deployment work described in `docs/ARCHITECTURE.md`.
