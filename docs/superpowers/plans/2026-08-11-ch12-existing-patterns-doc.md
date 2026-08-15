# Ch.12 §12.1–§12.3 Existing Patterns Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `docs/CH12-DEPLOYMENT.md`, documenting how this project already implements (or deliberately doesn't implement) the book's §12.1 (language-specific packaging), §12.2 (service as a VM), and §12.3 (service as a container) patterns.

**Architecture:** Single new markdown file with three sections, one per pattern, each stating the book's pattern definition then mapping it to this project's actual `build.gradle`/Dockerfile/`compose.yml` content. No code changes.

**Tech Stack:** Markdown only.

## Global Constraints

- No code, Dockerfile, or `compose.yml` changes — this plan is documentation-only, per the spec's Non-goals section.
- Every factual claim in the new doc must be verified against the actual current file it describes (spec's "Testing" section) — not written from memory of the compose.yml/Dockerfile content summarized in the spec.
- This sub-project does not flip Ch.12's `CONTEXT.md` progress-table status — that happens once sub-project B1 also lands (spec's "Documentation / CLAUDE.md sync" section).

---

### Task 1: Write `docs/CH12-DEPLOYMENT.md`

**Files:**
- Create: `docs/CH12-DEPLOYMENT.md`

**Interfaces:**
- Consumes: nothing (first and only task).
- Produces: `docs/CH12-DEPLOYMENT.md`, referenced by sub-project B1's future documentation task.

- [ ] **Step 1: Read the source files this doc will describe**

Read these four files in full before writing anything, since every claim in the new doc must match them exactly:
- `compose.yml` (repo root)
- `ftgo-restaurant-service/Dockerfile` (representative service Dockerfile — two-stage build)
- `build.gradle` (repo root — confirm the shared plugin/dependency setup, e.g. the `actuatorModules` block referenced in Ch.11 docs)
- `ftgo-restaurant-service/build.gradle` (a representative per-service build file, to confirm the `bootJar` task is Spring Boot's standard one, not custom)

- [ ] **Step 2: Write the file**

Create `docs/CH12-DEPLOYMENT.md` with this structure (fill in the `<...>` placeholders with the exact values you read in Step 1 — do not invent values):

```markdown
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

The root `build.gradle`'s <describe the actuatorModules block or
equivalent shared config you found in Step 1: what it centralizes and
which modules it applies to> keeps per-service `build.gradle` files
focused on that service's own dependencies rather than repeating
observability/actuator setup in all <N> of them.

Each service's Dockerfile packages this pattern into a container image in
two stages — see `ftgo-restaurant-service/Dockerfile` as the
representative example:

\`\`\`dockerfile
<paste the actual Dockerfile content you read in Step 1>
\`\`\`

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
`compose.yml`. <count the service: entries in compose.yml you read in
Step 1> service definitions cover the app services, infrastructure
(MySQL, Kafka, the ELK stack, Prometheus/Grafana/Tempo, GlitchTip and its
Postgres/Redis), and several one-shot setup containers.

**Two-stage builds.** Every service Dockerfile follows the same
build-then-runtime-stage pattern shown in §12.1 above: a `-jdk` base image
runs the Gradle build, a separate `-jre` base image runs only the
resulting JAR. This keeps the final image free of the JDK's full compiler
toolchain and the Gradle wrapper/cache.

**Startup ordering via health checks.** `compose.yml`'s `depends_on:
condition: service_healthy` entries consume the `/actuator/health`
endpoints every service exposes (Ch.11 §11.3.1) — e.g. <quote one
representative depends_on block from compose.yml showing
condition: service_healthy referencing a service's healthcheck>. Compose
won't start a dependent service until its dependency's Actuator health
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
```

- [ ] **Step 3: Verify every filled-in value against source**

Re-open `compose.yml` and `ftgo-restaurant-service/Dockerfile` side-by-side with the new doc. Confirm:
- The pasted Dockerfile content is character-for-character identical to the real file.
- The service count you wrote matches the actual number of top-level entries under `services:` in `compose.yml` (count them, don't estimate).
- The `depends_on` block you quoted is copied verbatim, not paraphrased.

Fix any mismatch before proceeding.

- [ ] **Step 4: Commit**

```bash
git add docs/CH12-DEPLOYMENT.md
git commit -m "docs: document Ch.12 §12.1-12.3 against existing deployment setup"
```
