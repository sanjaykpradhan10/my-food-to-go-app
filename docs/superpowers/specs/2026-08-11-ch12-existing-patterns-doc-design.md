# Ch.12 §12.1–§12.3 — Existing Deployment Patterns Documentation

**Status:** Approved design, spec for sub-project A of Ch.12 (Deploying microservices)

## Goal

Document how this project already implements (or deliberately doesn't implement) the book's first three deployment patterns from Chapter 12, mapping current reality onto the book's terminology. No code changes — this sub-project is purely descriptive.

## Background

Chapter 12 covers four deployment patterns in order: §12.1 language-specific packaging, §12.2 service as a VM, §12.3 service as a container, §12.4 Kubernetes. This project already builds and runs every service as a container via `compose.yml` and per-service Dockerfiles (established since Ch.4 and extended through Ch.11). Rather than re-implement what already exists, this sub-project formalizes it against the book's patterns, and sets up the contrast that motivates §12.4 (sub-project B): compose has no orchestrator, registry, or rolling-deployment story — which is exactly what Kubernetes adds.

## Scope

One new file: `docs/CH12-DEPLOYMENT.md`, with three sections.

### §12.1 — Language-specific packaging format

Document how each service's `build.gradle` + Dockerfile produces a runnable artifact:
- Spring Boot's `bootJar` task producing an executable fat JAR (all dependencies bundled) per service module.
- The shared root `build.gradle`'s common plugin/dependency setup (e.g. the `actuatorModules` block referenced throughout Ch.11 docs) and how it flows into each service's jar.
- Each service's Dockerfile build stage: copies source, runs the Gradle build, produces the jar; runtime stage copies just the jar into a slim JRE base image.
- Map this explicitly to the book's "language-specific packaging" pattern: benefits (fast startup, no separate packaging step needed beyond what Gradle+Spring Boot already do) and the book's noted drawback (no OS-level isolation) — which is why §12.3 wraps it in a container anyway.

### §12.2 — Service as a VM

Conceptual only, no implementation:
- Define the pattern per the book: package each service as a VM image (e.g. an AMI), deploy VM instances per service instance.
- State plainly that this project does not implement this pattern and has no VM tooling (no Packer configs, no cloud VM provisioning).
- Note the book's own tradeoffs for why this project (and most modern deployments) skip it: slow to provision, wasteful of resources (whole OS per service instance), and heavyweight compared to containers.
- Cross-reference: mirrors how §11.4 (microservice chassis / service mesh) was deliberately left conceptual-only in the Ch.11 sweep.

### §12.3 — Service as a container

Document what already exists in detail:
- `compose.yml` as the container-orchestration layer: one service definition per FTGO service, each pointing at its own Dockerfile.
- The two-stage Dockerfile pattern (build stage → runtime stage) used across all service Dockerfiles, and why (smaller final image, no Gradle/JDK-full-SDK baggage at runtime).
- How `depends_on` + `condition: service_healthy`/`service_started` encodes startup ordering — tie this back to the Ch.11 §11.3.1 health-check work, since compose's healthcheck blocks consume those `/actuator/health` endpoints.
- Explicitly name the gaps relative to the book's container pattern that motivate §12.4: no image registry (images only exist locally, built by `docker compose build`), no orchestrator managing placement/scaling/self-healing across multiple hosts, no rolling-update mechanism — compose recreates containers on `up`, it doesn't perform a managed rollout.

## Non-goals

- No code, Dockerfile, or compose.yml changes.
- No new diagrams beyond what's useful inline in the new doc (a simple textual mapping table is sufficient; this doesn't need the sequence-diagram depth of `docs/ARCHITECTURE.md`'s pattern sections, since there's no new runtime behavior to trace).

## Testing

None — documentation only. Self-review: read the new doc back against the actual `compose.yml` and a representative Dockerfile (e.g. `ftgo-restaurant-service/Dockerfile`) to confirm every claim matches current code exactly.

## Documentation / CLAUDE.md sync

This sub-project itself doesn't touch `CONTEXT.md`'s Book progress table status for Ch.12 (still "Not started" until at least B1 lands) — it only adds the new `docs/CH12-DEPLOYMENT.md` file. `CONTEXT.md`'s Ch.12 row gets updated once sub-project B1 (Kubernetes) also has a result to report, at which point this sub-project's doc gets referenced from there.
