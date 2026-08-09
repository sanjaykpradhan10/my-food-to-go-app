# Ch.11 §11.3.2 Log Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an ELK (Elasticsearch, Logstash, Kibana) log aggregation stack so logs from all 9 FTGO services can be searched from one place, correlated with existing distributed traces via `traceId`.

**Architecture:** Each of the 9 services switches its console log encoding to structured JSON (via `logstash-logback-encoder`), which already carries `traceId`/`spanId` from Micrometer Tracing's MDC. Docker's default `json-file` log driver captures that JSON as each container's stdout. A new Filebeat sidecar autodiscovers all containers and forwards their log lines to Logstash, which parses the JSON and writes to Elasticsearch (`ftgo-logs-%{+YYYY.MM.dd}` daily index). Kibana provides the search UI, with an index pattern (`ftgo-logs-*`) auto-imported at startup so there's no manual setup step.

**Tech Stack:** `net.logstash.logback:logstash-logback-encoder`, `docker.elastic.co/elasticsearch/elasticsearch:8.15.3`, `docker.elastic.co/logstash/logstash:8.15.3`, `docker.elastic.co/kibana/kibana:8.15.3`, `docker.elastic.co/beats/filebeat:8.15.3`.

## Global Constraints

- All 4 new Elastic images pin the same version: `8.15.3` (matches Spring Boot 3.5.16 / current stack's practice of pinning exact image tags, e.g. `grafana/tempo:2.6.1`, `prom/prometheus:v2.55.1`).
- `logstash-logback-encoder` goes on the same 9 services already grouped as `actuatorModules` in the root `build.gradle` (`ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-consumer-service`, `ftgo-restaurant-service`, `ftgo-accounting-service`, `ftgo-delivery-service`, `ftgo-order-history-service`, `ftgo-mobile-gateway`, `ftgo-public-gateway`) — the same 9 services already wired for tracing/metrics.
- No change to any service's business logic, log statements, or `logging.level` config — this plan only changes log *encoding* and adds new infrastructure containers.
- Follow this repo's `CLAUDE.md` per-change doc-sync rule: `README.md`, `CONTEXT.md`, and `docs/ARCHITECTURE.md` get updated in the same commits that add the feature they describe.

---

### Task 1: Structured JSON logging on all 9 services

**Files:**
- Modify: `build.gradle` (the `actuatorModules` dependency block, currently at the bottom of the file)
- Create: `ftgo-order-service/src/main/resources/logback-spring.xml`
- Create: `ftgo-kitchen-service/src/main/resources/logback-spring.xml`
- Create: `ftgo-consumer-service/src/main/resources/logback-spring.xml`
- Create: `ftgo-restaurant-service/src/main/resources/logback-spring.xml`
- Create: `ftgo-accounting-service/src/main/resources/logback-spring.xml`
- Create: `ftgo-delivery-service/src/main/resources/logback-spring.xml`
- Create: `ftgo-order-history-service/src/main/resources/logback-spring.xml`
- Create: `ftgo-mobile-gateway/src/main/resources/logback-spring.xml`
- Create: `ftgo-public-gateway/src/main/resources/logback-spring.xml`

**Interfaces:**
- Produces: every one of the 9 services now emits one JSON object per log line on stdout, with fields `@timestamp`, `level`, `logger_name`, `message`, `service` (from `spring.application.name`), and — once a request carries an active trace — `traceId`/`spanId` (added automatically by `micrometer-tracing-bridge-otel`'s MDC integration, already a dependency on all 9 services per `build.gradle`'s existing `actuatorModules` block). Task 2's Logstash pipeline consumes this JSON shape directly; do not rename these field names without updating Task 2's `json` filter to match.

- [ ] **Step 1: Add the `logstash-logback-encoder` dependency**

Open `build.gradle`. Find the existing block:

```groovy
configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        implementation 'io.micrometer:micrometer-registry-prometheus'
        implementation 'io.micrometer:micrometer-tracing-bridge-otel'
        implementation 'io.opentelemetry:opentelemetry-exporter-otlp'
    }
}
```

Add one line so it reads:

```groovy
configure(subprojects.findAll { actuatorModules.contains(it.name) }) {
    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        implementation 'io.micrometer:micrometer-registry-prometheus'
        implementation 'io.micrometer:micrometer-tracing-bridge-otel'
        implementation 'io.opentelemetry:opentelemetry-exporter-otlp'
        // Emits JSON-structured console logs (Ch.11 §11.3.2 Log aggregation) so Logstash can
        // parse them without a grok pattern; the JSON automatically carries the traceId/spanId
        // already in MDC from micrometer-tracing-bridge-otel above, correlating logs to traces.
        implementation 'net.logstash.logback:logstash-logback-encoder:7.4'
    }
}
```

- [ ] **Step 2: Create `logback-spring.xml` in each of the 9 services**

Use this exact content for all 9 files listed above (identical in every service — the `springProperty` line pulls each service's own `spring.application.name` at startup, so no per-service edits are needed):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <springProperty scope="context" name="appName" source="spring.application.name"/>

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <customFields>{"service":"${appName}"}</customFields>
        </encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="STDOUT" />
    </root>
</configuration>
```

- [ ] **Step 3: Build to confirm the dependency resolves and Spring context still starts**

Run: `./gradlew :ftgo-order-service:compileJava :ftgo-order-service:test`
Expected: BUILD SUCCESSFUL, existing tests still pass (this config only swaps the console encoder — no test asserts on log format today, so no test should break).

- [ ] **Step 4: Spot-check the JSON output locally**

Run: `./gradlew :ftgo-order-service:bootRun` (stop it with Ctrl-C once you've confirmed the output, no need to leave it running)
Expected: console lines are single-line JSON objects, e.g. `{"@timestamp":"...","level":"INFO","service":"ftgo-order-service","message":"..."}` — not the old plain-text pattern.

- [ ] **Step 5: Repeat Steps 1-4's file creation for the remaining 8 services, then commit**

```bash
git add build.gradle ftgo-order-service/src/main/resources/logback-spring.xml \
  ftgo-kitchen-service/src/main/resources/logback-spring.xml \
  ftgo-consumer-service/src/main/resources/logback-spring.xml \
  ftgo-restaurant-service/src/main/resources/logback-spring.xml \
  ftgo-accounting-service/src/main/resources/logback-spring.xml \
  ftgo-delivery-service/src/main/resources/logback-spring.xml \
  ftgo-order-history-service/src/main/resources/logback-spring.xml \
  ftgo-mobile-gateway/src/main/resources/logback-spring.xml \
  ftgo-public-gateway/src/main/resources/logback-spring.xml
git commit -m "feat: emit structured JSON logs on all 9 services for log aggregation"
```

---

### Task 2: ELK stack + Filebeat shipping in compose.yml

**Files:**
- Create: `logstash/pipeline/logstash.conf`
- Create: `filebeat/filebeat.yml`
- Modify: `compose.yml` (add `elasticsearch`, `logstash`, `kibana`, `filebeat` services after the existing `grafana` service, before the trailing `volumes:` block)

**Interfaces:**
- Consumes: the JSON log lines Task 1 produces on stdout of the 9 services (field names `@timestamp`, `level`, `logger_name`, `message`, `service`, `traceId`, `spanId`).
- Produces: an `elasticsearch` service reachable at `http://elasticsearch:9200` inside the compose network (and `http://localhost:9200` from the host), holding indices named `ftgo-logs-YYYY.MM.dd`. Task 3 imports a Kibana index pattern matching `ftgo-logs-*` against this same index name.

- [ ] **Step 1: Create the Logstash pipeline config**

Create `logstash/pipeline/logstash.conf`:

```
input {
  beats {
    port => 5044
  }
}

filter {
  # The 9 FTGO services emit one JSON object per log line (Task 1). Non-JSON container logs
  # (mysql, kafka, zookeeper, etc.) fail this filter and get dropped below rather than indexed
  # as noise — this stack only aggregates the FTGO services' own structured logs.
  json {
    source => "message"
    skip_on_invalid_json => true
  }
  if "_jsonparsefailure" in [tags] {
    drop { }
  }
}

output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "ftgo-logs-%{+YYYY.MM.dd}"
  }
}
```

- [ ] **Step 2: Create the Filebeat config**

Create `filebeat/filebeat.yml`:

```yaml
filebeat.autodiscover:
  providers:
    - type: docker
      hints.enabled: true

processors:
  - add_docker_metadata: ~

output.logstash:
  hosts: ["logstash:5044"]

logging.level: info
```

- [ ] **Step 3: Add the 4 new services to `compose.yml`**

Insert after the existing `grafana:` service block (before the trailing `volumes:` section):

```yaml
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.3
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
    ports:
      - "9200:9200"
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9200/_cluster/health || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 20

  logstash:
    image: docker.elastic.co/logstash/logstash:8.15.3
    depends_on:
      elasticsearch:
        condition: service_healthy
    volumes:
      - ./logstash/pipeline:/usr/share/logstash/pipeline:ro
    ports:
      - "5044:5044"
    environment:
      LS_JAVA_OPTS: "-Xms256m -Xmx256m"

  kibana:
    image: docker.elastic.co/kibana/kibana:8.15.3
    depends_on:
      elasticsearch:
        condition: service_healthy
    ports:
      - "5601:5601"
    environment:
      ELASTICSEARCH_HOSTS: http://elasticsearch:9200
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:5601/api/status || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 20

  filebeat:
    image: docker.elastic.co/beats/filebeat:8.15.3
    user: root
    depends_on:
      logstash:
        condition: service_started
    volumes:
      - ./filebeat/filebeat.yml:/usr/share/filebeat/filebeat.yml:ro
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    command: ["--strict.perms=false"]
```

- [ ] **Step 4: Validate the compose file parses**

Run: `docker compose config --quiet`
Expected: no output, exit code 0 (a non-zero exit or YAML error means a syntax mistake in the block just added).

- [ ] **Step 5: Bring up the new services and verify ingestion end to end**

Run: `docker compose up -d mysql kafka zookeeper service-registry authorization-server tempo order-service elasticsearch logstash kibana filebeat`

Wait for `order-service` to report healthy (`docker compose ps order-service`), then generate a log line by hitting its health endpoint a few times:

Run: `for i in 1 2 3; do curl -s http://localhost:8082/actuator/health; done`

Then query Elasticsearch directly:

Run: `curl -s "http://localhost:9200/ftgo-logs-*/_search?q=service:ftgo-order-service&size=1" | head -c 2000`

Expected: a JSON response with at least one hit whose `_source.service` is `ftgo-order-service` and whose `_source.message` looks like a log line. If there are zero hits, check `docker compose logs filebeat` and `docker compose logs logstash` for connection errors before proceeding.

- [ ] **Step 6: Tear down and commit**

```bash
docker compose down
git add compose.yml logstash/pipeline/logstash.conf filebeat/filebeat.yml
git commit -m "feat: add ELK stack and Filebeat log shipping to compose.yml"
```

---

### Task 3: Kibana index pattern auto-provisioning

**Files:**
- Create: `kibana/saved-objects/index-pattern.ndjson`
- Modify: `compose.yml` (add a `kibana-index-pattern-registrar` one-shot container after the `kibana` and `filebeat` services)

**Interfaces:**
- Consumes: the `kibana` service from Task 2 (`http://kibana:5601` inside the compose network) and the `ftgo-logs-*` index name Task 2's Logstash output writes to.
- Produces: a Kibana index pattern named `ftgo-logs-*` visible in Kibana's Discover view with no manual setup, satisfying the design's "auto-imported at startup" requirement.

- [ ] **Step 1: Create the Kibana saved-object export**

Create `kibana/saved-objects/index-pattern.ndjson` (Kibana's saved-objects import format is newline-delimited JSON, one object per line):

```
{"attributes":{"title":"ftgo-logs-*","timeFieldName":"@timestamp"},"id":"ftgo-logs-pattern","type":"index-pattern"}
```

- [ ] **Step 2: Add the registrar container to `compose.yml`**

Insert after the `filebeat:` service block added in Task 2, following the same one-shot-curl-container pattern already used by `connector-registrar` earlier in this file:

```yaml
  kibana-index-pattern-registrar:
    image: curlimages/curl:8.10.1
    depends_on:
      kibana:
        condition: service_healthy
    volumes:
      - ./kibana/saved-objects/index-pattern.ndjson:/index-pattern.ndjson:ro
    entrypoint:
      - sh
      - -c
      - |
        curl -s -X POST http://kibana:5601/api/saved_objects/_import?overwrite=true \
          -H "kbn-xsrf: true" \
          --form file=@/index-pattern.ndjson
    restart: "no"
```

- [ ] **Step 3: Validate the compose file parses**

Run: `docker compose config --quiet`
Expected: no output, exit code 0.

- [ ] **Step 4: Bring up Kibana and the registrar, verify the index pattern lands**

Run: `docker compose up -d elasticsearch kibana kibana-index-pattern-registrar`

Wait for the registrar to exit (`docker compose ps kibana-index-pattern-registrar` shows `Exited (0)`), then:

Run: `curl -s http://localhost:5601/api/saved_objects/index-pattern/ftgo-logs-pattern -H "kbn-xsrf: true"`

Expected: a JSON response with `"attributes":{"title":"ftgo-logs-*", ...}` — not a 404.

- [ ] **Step 5: Tear down and commit**

```bash
docker compose down
git add compose.yml kibana/saved-objects/index-pattern.ndjson
git commit -m "feat: auto-provision Kibana index pattern for ftgo-logs-*"
```

---

### Task 4: Documentation sweep

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: the finished stack from Tasks 1-3 (service names, ports, image versions) as the source of truth for what to document.

- [ ] **Step 1: Update `README.md`**

Find the section listing the tech stack / observability tooling (near the existing Tempo/Prometheus/Grafana entries added for the tracing/metrics sub-projects) and add a line for the new stack, e.g.:

```markdown
- **Log aggregation**: ELK stack (Elasticsearch, Logstash, Kibana) + Filebeat — all 9 services log structured JSON (via `logstash-logback-encoder`) to stdout, which Filebeat ships to Logstash and indexes into Elasticsearch as `ftgo-logs-*`. Search and correlate logs by `traceId` in Kibana at `http://localhost:5601`.
```

Also add `elasticsearch` (9200), `logstash` (5044), `kibana` (5601) to any existing port-listing table, following the same row format already used for `tempo`/`prometheus`/`grafana`.

- [ ] **Step 2: Update `CONTEXT.md`**

In the "Services to build" or equivalent status table, mark Log aggregation (§11.3.2) as done. In the session log, add an entry dated today summarizing this sub-project (ELK stack added, structured JSON logging on all 9 services, Kibana index pattern auto-provisioned). Move "log aggregation" out of any "remaining work" / "not yet started" list it currently appears in (per the per-change doc-sync rule — do not leave stale references to this being unstarted).

- [ ] **Step 3: Add a "Log aggregation" section to `docs/ARCHITECTURE.md`**

Add a new top-level section (this is the first observability pattern to get a dedicated section in this file — match the heading depth/style already used for the saga sections). Cover: the pattern (aggregate logs from all services into a searchable central store), the components (Elasticsearch/Logstash/Kibana/Filebeat, with a short diagram of the flow: service stdout → Docker json-file → Filebeat autodiscover → Logstash beats input → json filter → Elasticsearch → Kibana), why structured JSON logging was chosen over grok-parsing plain text (queryable fields, no brittle pattern matching, multi-line stack traces stay as one event), and how it correlates with the existing distributed tracing pattern (§11.3.3) via the shared `traceId`/`spanId` MDC fields.

- [ ] **Step 4: Commit**

```bash
git add README.md CONTEXT.md docs/ARCHITECTURE.md
git commit -m "docs: document Ch.11 §11.3.2 log aggregation"
```

---

## Manual acceptance check (performed after all tasks land, not a subagent task)

Start the full stack (`docker compose up -d`), wait for all services healthy, place an order through `public-gateway`, note the `traceId` Grafana/Tempo shows for that request, then search for that exact `traceId` in Kibana's Discover view against the `ftgo-logs-*` index pattern. Expect matching log lines from at least `order-service` and `public-gateway` for that request — confirming the design's acceptance criterion (trace↔log correlation without new application code beyond Task 1's encoder swap).
