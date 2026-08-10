# Ch.11 §11.3.5 Exception Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy self-hosted GlitchTip and wire all 9 FTGO services to auto-report uncaught exceptions to it via the official Sentry Spring Boot starter, with zero `@ExceptionHandler`/`@ControllerAdvice` code changes.

**Architecture:** 3 new `compose.yml` containers (`glitchtip`, `glitchtip-db`, `glitchtip-redis`) plus a one-shot registrar container that provisions a GlitchTip organization/project via its REST API and writes the resulting DSN to a file shared (via a named volume) with all 9 service containers; each service's entrypoint reads that file into `SENTRY_DSN` before starting the JVM. `sentry-spring-boot-starter` auto-registers a `HandlerExceptionResolver` that only sees exceptions Spring's default handling would otherwise turn into a 500 — existing local `@ExceptionHandler` methods (which map domain exceptions to 4xx) run first and are untouched.

**Tech Stack:** GlitchTip (`glitchtip/glitchtip` image), PostgreSQL 16, Redis 7, `io.sentry:sentry-spring-boot-starter`, Docker Compose, Cucumber (e2e verification).

## Global Constraints

- Instrument all 9 services: `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-restaurant-service`, `ftgo-accounting-service`, `ftgo-delivery-service`, `ftgo-order-history-service`, `ftgo-mobile-gateway`, `ftgo-public-gateway` — same `actuatorModules` list in root `build.gradle:69-71`.
- No changes to any existing `@ExceptionHandler` method or `@ControllerAdvice` class — auto-capture only.
- Every new compose service follows the existing observability-stack pattern (see `tempo`, `prometheus`, `grafana`, `elasticsearch`/`kibana`/`kibana-index-pattern-registrar` in `compose.yml`): plain `image:`, `depends_on` with `condition:`, no custom Dockerfiles for third-party images.
- Per this repo's `CLAUDE.md`: documentation updates (`README.md`, `CONTEXT.md`, `docs/ARCHITECTURE.md`) land in the same commit/PR as the code, not a follow-up.
- Spec: `docs/superpowers/specs/2026-08-10-exception-tracking-design.md`.

---

### Task 1: GlitchTip containers + DSN provisioning

**Files:**
- Modify: `compose.yml` (add 4 new services after the `kibana-index-pattern-registrar` block, i.e. after line 519)
- Create: `glitchtip/provision.sh`

**Interfaces:**
- Produces: a named Docker volume `sentry-dsn` containing `/shared/dsn.env` (format: `SENTRY_DSN=<url>`), written by `glitchtip-provisioner` once GlitchTip is healthy. Task 2 mounts this same volume read-only into all 9 service containers.

- [ ] **Step 1: Add the 3 GlitchTip infra containers to `compose.yml`**

Add after the `kibana-index-pattern-registrar` block (after line 519, before `volumes:`):

```yaml
  glitchtip-db:
    image: postgres:16
    environment:
      POSTGRES_DB: glitchtip
      POSTGRES_USER: glitchtip
      POSTGRES_PASSWORD: glitchtip
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U glitchtip"]
      interval: 5s
      timeout: 5s
      retries: 20

  glitchtip-redis:
    image: redis:7

  glitchtip:
    image: glitchtip/glitchtip:v4.2.9
    depends_on:
      glitchtip-db:
        condition: service_healthy
      glitchtip-redis:
        condition: service_started
    environment:
      DATABASE_URL: postgres://glitchtip:glitchtip@glitchtip-db:5432/glitchtip
      SECRET_KEY: local-dev-only-not-a-real-secret
      CELERY_BROKER_URL: redis://glitchtip-redis:6379/0
      GLITCHTIP_DOMAIN: http://localhost:8000
      DEFAULT_FROM_EMAIL: glitchtip@localhost
      ENABLE_OPEN_USER_REGISTRATION: "false"
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8000/_health/ || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
```

Note: pin the image tag to whatever the latest stable GlitchTip release is at implementation time (check https://hub.docker.com/r/glitchtip/glitchtip/tags) rather than trusting `v4.2.9` above verbatim — it was not verified against the live registry while writing this plan.

- [ ] **Step 2: Write the provisioning script**

Create `glitchtip/provision.sh`:

```bash
#!/bin/sh
set -eu

# Bootstraps a superuser, organization, team, and project via GlitchTip's Django management
# commands (more reliable than guessing REST payload shapes for a first-run, unauthenticated
# instance), then reads back the project's DSN via the REST API using a freshly minted API
# token, and writes it where the app containers can pick it up.
#
# This runs `docker compose exec` against the already-running glitchtip container rather than
# being a container of its own, because Django management commands need the app's actual
# environment/venv, which is simplest to reach by executing inside the existing image.

GLITCHTIP_CID=$(docker compose ps -q glitchtip)

docker exec "$GLITCHTIP_CID" python manage.py shell -c "
from django.contrib.auth import get_user_model
from organizations_ext.models import Organization
from projects.models import Project
from apps.projects.models import ProjectKey

User = get_user_model()
user, _ = User.objects.get_or_create(email='admin@localhost', defaults={'username': 'admin', 'is_superuser': True, 'is_staff': True})
user.set_password('admin')
user.save()

org, _ = Organization.objects.get_or_create(name='ftgo')
org.add_user(user)

project, _ = Project.objects.get_or_create(name='ftgo', organization=org)

key = ProjectKey.objects.filter(project=project).first()
if key is None:
    key = ProjectKey.objects.create(project=project)

print('DSN=' + key.get_dsn())
" > /tmp/glitchtip-shell-output.txt

DSN_LINE=$(grep '^DSN=' /tmp/glitchtip-shell-output.txt)
echo "SENTRY_${DSN_LINE}" | sed 's/^SENTRY_DSN=/SENTRY_DSN=/' > glitchtip/dsn.env
cat glitchtip/dsn.env
```

**This step's exact Django model import paths (`organizations_ext.models.Organization`, `projects.models.Project`, `apps.projects.models.ProjectKey`) are a best-effort guess based on GlitchTip being a Django app derived from old Sentry's codebase — they are NOT verified against the actual running image.** Before wiring this into compose automation, run it once manually against the started `glitchtip` container (`docker compose up -d glitchtip-db glitchtip-redis glitchtip`, wait for healthy, then `docker exec -it <cid> python manage.py shell` and explore `from django.apps import apps; apps.get_models()` to find the real model locations) and fix the import paths/field names before proceeding. This mirrors how the log-aggregation sub-project's `filebeat.yml` needed a live-discovery fix — expect the same here.

- [ ] **Step 3: Wire the script into compose as a one-shot registrar**

Add to `compose.yml` right after the `glitchtip` service block from Step 1:

```yaml
  glitchtip-provisioner:
    image: docker:27-cli
    depends_on:
      glitchtip:
        condition: service_healthy
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./glitchtip:/glitchtip
      - sentry-dsn:/shared
    entrypoint: ["sh", "-c", "cd /glitchtip && sh provision.sh && cp dsn.env /shared/dsn.env"]
    restart: "no"
```

Add `sentry-dsn:` to the top-level `volumes:` block at the end of `compose.yml` (alongside the existing `mysql-data:` entry).

- [ ] **Step 4: Verify manually**

```bash
docker compose up -d glitchtip-db glitchtip-redis glitchtip glitchtip-provisioner
docker compose logs glitchtip-provisioner
```

Expected: log output ending in a line like `SENTRY_DSN=http://<key>@localhost:8000/1`. Fix `provision.sh` per Step 2's note until this succeeds. Then confirm `docker compose exec glitchtip-provisioner cat /shared/dsn.env` shows the same line.

- [ ] **Step 5: Commit**

```bash
git add compose.yml glitchtip/provision.sh
git commit -m "feat: add self-hosted GlitchTip stack with DSN auto-provisioning"
```

---

### Task 2: `sentry-spring-boot-starter` dependency + config on all 9 services

**Files:**
- Modify: `build.gradle:73-83` (the `actuatorModules` dependencies block)
- Modify: `config-repo/application.yml` (add shared Sentry defaults)
- Modify: `compose.yml` (each of the 9 service definitions — add the `sentry-dsn` volume mount + entrypoint wrapper)

**Interfaces:**
- Consumes: `/shared/dsn.env` from the `sentry-dsn` volume (Task 1's output — format `SENTRY_DSN=<url>`).
- Produces: every one of the 9 services has `io.sentry:sentry-spring-boot-starter` on its classpath and `SENTRY_DSN` set at JVM startup.

- [ ] **Step 1: Add the dependency**

In `build.gradle`, inside the existing block at lines 73-83:

```groovy
configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        implementation 'io.micrometer:micrometer-registry-prometheus'
        implementation 'io.micrometer:micrometer-tracing-bridge-otel'
        implementation 'io.opentelemetry:opentelemetry-exporter-otlp'
        implementation 'net.logstash.logback:logstash-logback-encoder:7.4'
        // Ch.11 §11.3.5 Exception tracking: auto-captures any exception that reaches Spring's
        // default error handling (i.e. NOT already caught by a local @ExceptionHandler) and
        // reports it to GlitchTip, tagged with the active trace/span ID from the tracing bridge
        // dependency above for cross-referencing with Tempo/Kibana.
        implementation 'io.sentry:sentry-spring-boot-starter:7.14.0'
    }
}
```

Verify `7.14.0` is a real published version before committing (`./gradlew dependencies --configuration compileClasspath` on one module after adding it, or check Maven Central) — pin whatever the latest stable 7.x release actually is.

- [ ] **Step 2: Add shared Sentry config to `config-repo/application.yml`**

Add to `config-repo/application.yml`:

```yaml
sentry:
  environment: local
  # traces-sample-rate left unset (defaults to 0): this sub-project only needs error capture,
  # not Sentry's separate performance-tracing feature, which would duplicate what Tempo already
  # does via micrometer-tracing-bridge-otel.
  send-default-pii: false
```

`sentry.dsn` is deliberately NOT set here — it comes from the `SENTRY_DSN` environment variable injected per-container in Step 3, sourced from Task 1's provisioned DSN, matching how `SPRING_KAFKA_BOOTSTRAP_SERVERS` and other environment-specific values are already handled as compose env vars rather than config-repo values.

- [ ] **Step 3: Inject `SENTRY_DSN` into each of the 9 service containers**

Each service currently starts via `ENTRYPOINT ["java", "-jar", "app.jar"]` (see e.g. `ftgo-order-service/Dockerfile`) with all config coming from compose `environment:` blocks — but `SENTRY_DSN`'s value isn't known until Task 1's provisioner runs, so it can't be a static `environment:` entry. Change each of the 9 Dockerfiles' entrypoint to read the shared file at container start:

For each of `ftgo-order-service/Dockerfile`, `ftgo-kitchen-service/Dockerfile`, `ftgo-consumer-service/Dockerfile`, `ftgo-restaurant-service/Dockerfile`, `ftgo-accounting-service/Dockerfile`, `ftgo-delivery-service/Dockerfile`, `ftgo-order-history-service/Dockerfile`, `ftgo-mobile-gateway/Dockerfile`, `ftgo-public-gateway/Dockerfile`, replace:

```dockerfile
ENTRYPOINT ["java", "-jar", "app.jar"]
```

with:

```dockerfile
ENTRYPOINT ["sh", "-c", "[ -f /shared/dsn.env ] && export $(cat /shared/dsn.env) ; exec java -jar app.jar"]
```

Then in `compose.yml`, add to each of the 9 service definitions (e.g. `order-service:`, `kitchen-service:`, ...):

```yaml
    volumes:
      - sentry-dsn:/shared:ro
    depends_on:
      glitchtip-provisioner:
        condition: service_completed_successfully
      # ...(keep the service's existing depends_on entries)
```

If a service block already has a `volumes:`/`depends_on:` key, merge into the existing one rather than duplicating the key (YAML doesn't allow duplicate keys in the same mapping).

- [ ] **Step 4: Verify manually**

```bash
docker compose up -d --build order-service
docker compose logs order-service | grep -i sentry
```

Expected: a Sentry SDK init log line (the starter logs its DSN/environment on startup) and no `SENTRY_DSN` warnings. Repeat spot-check for at least one more service (e.g. `kitchen-service`) to confirm the Dockerfile change applies uniformly.

- [ ] **Step 5: Commit**

```bash
git add build.gradle config-repo/application.yml compose.yml ftgo-order-service/Dockerfile ftgo-kitchen-service/Dockerfile ftgo-consumer-service/Dockerfile ftgo-restaurant-service/Dockerfile ftgo-accounting-service/Dockerfile ftgo-delivery-service/Dockerfile ftgo-order-history-service/Dockerfile ftgo-mobile-gateway/Dockerfile ftgo-public-gateway/Dockerfile
git commit -m "feat: wire sentry-spring-boot-starter + DSN injection into all 9 services"
```

---

### Task 3: Diagnostic trigger endpoint + Cucumber verification scenario

**Rationale for a dedicated diagnostic endpoint (not a naturally-occurring bug):** a code review of `OrderController`, `OrderService`, `Order`, and `OrderTransitions` (see spec discussion) found the existing domain code is defensively written — every plausible bad-input path already routes through one of the 3 existing `@ExceptionHandler` groups (`OrderNotFoundException`/`MenuItemNotFoundException`/`RestaurantNotFoundException`, `RestaurantServiceUnavailableException`, `OrderCannotBeCancelledException`/`UnsupportedStateTransitionException`). Rather than manufacture a fragile, contrived input to hit some incidental unguarded line (which would silently break this test the next time that code is refactored), add one small, explicitly-labeled diagnostic endpoint whose only purpose is exercising the uncaught-exception path — the same pattern Sentry's own onboarding docs recommend for verifying an integration.

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`
- Create: `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/ExceptionTrackingStepDefinitions.java`
- Modify: `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`

**Interfaces:**
- Produces: `GET /orders/_diagnostics/trigger-exception` on order-service (port 8082), `ADMIN`-only, always throws `IllegalStateException`, resulting in a 500 with no matching `@ExceptionHandler`.

- [ ] **Step 1: Add the diagnostic endpoint**

In `OrderController.java`, add above the existing `@ExceptionHandler` methods:

```java
    // Deliberately uncaught (no matching @ExceptionHandler below) — exists solely to verify the
    // Ch.11 §11.3.5 exception-tracking pipeline end-to-end (GlitchTip capture). ADMIN-gated since
    // it has no other purpose and shouldn't be reachable by regular consumer traffic.
    @PreAuthorize("hasRole('ADMIN')")
    @GetMapping("/_diagnostics/trigger-exception")
    public ResponseEntity<Void> triggerDiagnosticException() {
        throw new IllegalStateException("Deliberate exception for exception-tracking verification (Ch.11 §11.3.5)");
    }
```

- [ ] **Step 2: Verify locally it produces a 500 and reaches GlitchTip**

```bash
docker compose up -d --build order-service
TOKEN=$(curl -s -X POST http://localhost:9000/oauth2/token -u ftgo-client:secret \
  -d "grant_type=password&username=admin1&password=password" | jq -r .access_token)
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $TOKEN" \
  http://localhost:8082/orders/_diagnostics/trigger-exception
```

Expected: `500`. Then open `http://localhost:8000` (GlitchTip UI, login `admin@localhost` / `admin` from Task 1's provisioning), confirm an issue titled `IllegalStateException` appears under the `ftgo` project within ~10s.

(Adjust the token-request curl to match this project's actual authorization-server grant/credentials — check `ftgo-authorization-server`'s seed users and the existing `TokenClient` class in `ftgo-end-to-end-test` referenced below rather than trusting the placeholder client id/secret above.)

- [ ] **Step 3: Write the Cucumber step definitions**

Create `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/ExceptionTrackingStepDefinitions.java`:

```java
package com.sanjay.ftgo.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExceptionTrackingStepDefinitions {

    private static final String ORDER_SERVICE_BASE_URL = "http://localhost:8082";
    // GlitchTip's REST API is Sentry-API-compatible; "ftgo" is the organization slug the
    // Task 1 provisioning script creates. Adjust the slug here if provisioning ends up choosing
    // a different one in practice.
    private static final String GLITCHTIP_ISSUES_URL = "http://localhost:8000/api/0/organizations/ftgo/issues/";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TokenClient tokenClient = new TokenClient();

    private int diagnosticResponseStatus;

    @When("an admin triggers the order-service diagnostic exception endpoint")
    public void anAdminTriggersTheDiagnosticExceptionEndpoint() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ORDER_SERVICE_BASE_URL + "/orders/_diagnostics/trigger-exception"))
                .header("Authorization", "Bearer " + tokenClient.tokenFor("admin1", "password"))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        diagnosticResponseStatus = response.statusCode();
    }

    @Then("the diagnostic endpoint responds with a server error")
    public void theDiagnosticEndpointRespondsWithAServerError() {
        assertEquals(500, diagnosticResponseStatus);
    }

    @Then("GlitchTip eventually reports an IllegalStateException issue for ftgo-order-service")
    public void glitchtipEventuallyReportsAnIssue() throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (Instant.now().isBefore(deadline)) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(GLITCHTIP_ISSUES_URL + "?query=IllegalStateException"))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JsonNode issues = objectMapper.readTree(response.body());
                if (issues.isArray() && !issues.isEmpty()) {
                    for (JsonNode issue : issues) {
                        String title = issue.path("title").asText("");
                        String culprit = issue.path("culprit").asText("");
                        if (title.contains("IllegalStateException")
                                && culprit.contains("OrderController")) {
                            return;
                        }
                    }
                }
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("Expected a GlitchTip issue for IllegalStateException in OrderController within 30s");
    }
}
```

**This step's exact GlitchTip issues-API response shape (`title`/`culprit` fields, `?query=` param) is a best-effort guess based on Sentry's own (compatible) API — verify it against the actual running GlitchTip instance in Step 2 above (`curl http://localhost:8000/api/0/organizations/ftgo/issues/` with an auth token) and adjust field names before finalizing this file.** GlitchTip's API requires an auth token (create one via its UI under Settings → API Tokens, or extend `provision.sh` from Task 1 to mint one and add it to `dsn.env`) — add an `Authorization: Bearer <token>` header to both requests above once you've confirmed the auth requirement.

- [ ] **Step 4: Add the Cucumber scenario**

Add to `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`:

```gherkin
  Scenario: Triggering an uncaught exception reports it to GlitchTip
    When an admin triggers the order-service diagnostic exception endpoint
    Then the diagnostic endpoint responds with a server error
    And GlitchTip eventually reports an IllegalStateException issue for ftgo-order-service
```

- [ ] **Step 5: Run the full e2e suite**

```bash
docker compose up -d --build
./gradlew :ftgo-end-to-end-test:test --tests "*PlaceReviseCancelOrder*"
```

Expected: all scenarios in `PlaceReviseCancelOrder.feature` pass, including the new one.

- [ ] **Step 6: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java \
        ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/ExceptionTrackingStepDefinitions.java \
        ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature
git commit -m "test: verify exception tracking end-to-end via diagnostic endpoint + GlitchTip polling"
```

---

### Task 4: Documentation sync

**Files:**
- Modify: `README.md` (tech-stack/observability row, "Book progress" table, running-locally port list)
- Modify: `CONTEXT.md` ("Current position" section, "Patterns reference" checklist, session log entry)
- Modify: `docs/ARCHITECTURE.md` (new "Exception tracking (Ch.11, §11.3.5)" section)

**Interfaces:**
- Consumes: final, verified state of Tasks 1-3 (exact image tags, model/API field names used, DSN provisioning mechanics) — write this documentation only after Tasks 1-3's live-verification steps confirm what was actually built, since several details in this plan were marked unverified-at-write-time.

- [ ] **Step 1: Update `README.md`**

Add a row/bullet describing GlitchTip alongside the existing ELK/Prometheus/Tempo/Grafana entries in the tech-stack and observability sections, and add `http://localhost:8000` to the running-locally port list. Update the Ch.11 row of the "Book progress" table: append a description of §11.3.5 in the same style as the existing §11.3.2/§11.3.3/§11.3.4 sentences, and change "not started" to reflect §11.3.5 done (§11.3.6 still not started).

- [ ] **Step 2: Update `CONTEXT.md`**

In "Current position": append a paragraph describing the exception-tracking sub-project in the same style/detail level as the log-aggregation paragraph already there (component list, key design decisions, what was verified). In "Patterns reference": check off Exception tracking. Add a new dated session-log entry following the existing entries' format (see the 2026-08-08 log-aggregation entry).

- [ ] **Step 3: Add the `docs/ARCHITECTURE.md` section**

Add a new "Exception tracking (Ch.11, §11.3.5)" section after the existing "Log aggregation" section, matching its depth: an architecture description (GlitchTip + Postgres + Redis, DSN provisioning flow), a description of the auto-capture-only-uncaught-exceptions design decision and why (existing `@ExceptionHandler`s stay untouched), and how it correlates with the tracing/logging sub-projects via `traceId`.

- [ ] **Step 4: Commit**

```bash
git add README.md CONTEXT.md docs/ARCHITECTURE.md
git commit -m "docs: document Ch.11 §11.3.5 exception tracking"
```

Note: per this repo's `CLAUDE.md`, this documentation update is a per-change sync only — §11.3.6 (audit logging) remains unstarted, so Ch.11/§11.3 stay "In progress" and the full chapter-completion sweep (ARCHITECTURE.md sequence diagrams for every saga, full README parity per service, CONTEXT.md "Concept understanding" section) is NOT triggered by this task. Do not perform that sweep here.
