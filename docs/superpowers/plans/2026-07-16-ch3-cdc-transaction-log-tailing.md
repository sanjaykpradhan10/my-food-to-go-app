# Ch.3 Transaction Log Tailing (CDC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add CDC-based event publishing (Debezium tailing MySQL's binlog on the existing `outbox_events` table) as a second delivery mechanism, switchable against the existing polling publisher via one `OUTBOX_PUBLISH_MODE` env var — with zero changes to `OrderService`'s write side or kitchen-service's consumer.

**Architecture:** A new `kafka-connect` container (Debezium MySQL connector plugin) tails MySQL's binlog on `outbox_events`, and its Outbox Event Router SMT republishes each row's `payload` column, keyed by `order_id`, to Kafka topic `order.events` — byte-identical to what the existing polling publisher already sends. A one-shot `connector-registrar` container and order-service's `OutboxPublisher` bean (now `@ConditionalOnProperty`-gated) both read the same `OUTBOX_PUBLISH_MODE` variable and reconcile independently on every `docker compose up`, guaranteeing exactly one publish path is ever active.

**Tech Stack:** Debezium 2.7 (Kafka Connect + MySQL connector plugin, `debezium/connect` image), MySQL 8.4 binlog (ROW format), Spring Boot `@ConditionalOnProperty`, `curlimages/curl` for the registrar.

## Global Constraints

- Java 21, Spring Boot 3.5.3 — do not change
- `OrderService.java`, `OutboxEvent.java`, `OutboxEventRepository.java`, `OrderCreatedEvent.java`, `KafkaProducerConfig.java`, and kitchen-service's entire consumer stack (`OrderEventListener`, `TicketService`, `ProcessedEvent`) are **not modified by this plan** — this feature only changes how outbox rows get published, never how they're written or consumed
- Kafka topic name stays `order.events` (exact string) — the CDC path must publish to the same topic the polling path uses
- Kafka message key stays the string form of `order_id` (`String.valueOf(orderId)` on the polling side; the CDC side must produce the identical string representation) — matches the existing keying decision from the messaging feature
- `outbox_events` column names (`event_id`, `event_type`, `order_id`, `payload`, `sent_at`) are not renamed — the Debezium connector config maps onto the existing schema exactly as-is
- One source of truth: `OUTBOX_PUBLISH_MODE` (`polling` default, or `cdc`) — both order-service's `outbox.publish-mode` property and the `connector-registrar` container read this same variable, nothing else introduces a second flag
- No new Dockerfiles needed — `kafka-connect` and `connector-registrar` both use prebuilt images (`debezium/connect`, `curlimages/curl`), no custom builds

Full context: `docs/superpowers/specs/2026-07-16-ch3-cdc-transaction-log-tailing-design.md`

---

### Task 1: order-service — gate the polling publisher behind a mode flag

**Files:**
- Modify: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherConditionalTest.java`

**Interfaces:**
- Produces: `OutboxPublisher` bean exists only when `outbox.publish-mode` is `polling` or unset — consumed implicitly by the rest of the app (nothing else references this bean directly; its mere presence/absence is the effect)

- [ ] **Step 1: Write the failing test**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherConditionalTest.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class OutboxPublisherConditionalTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withBean(PropertySourcesPlaceholderConfigurer.class)
            .withUserConfiguration(CollaboratorConfig.class, OutboxPublisher.class);

    @Test
    void beanExistsWhenPublishModeIsPolling() {
        contextRunner.withPropertyValues("outbox.publish-mode=polling")
                .run(context -> assertThat(context).hasSingleBean(OutboxPublisher.class));
    }

    @Test
    void beanExistsWhenPublishModeIsUnset() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(OutboxPublisher.class));
    }

    @Test
    void beanAbsentWhenPublishModeIsCdc() {
        contextRunner.withPropertyValues("outbox.publish-mode=cdc")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxPublisher.class));
    }

    @Configuration
    static class CollaboratorConfig {

        @Bean
        OutboxEventRepository outboxEventRepository() {
            return mock(OutboxEventRepository.class);
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }
    }
}
```

`PropertySourcesPlaceholderConfigurer` is registered explicitly because `ApplicationContextRunner.withUserConfiguration(...)` does not automatically add placeholder resolution — without it, `OutboxPublisher`'s constructor-level `@Value("${outbox.batch-size:20}")` would fail to resolve its default value. If this test still fails to start the context with a placeholder-resolution error after adding it, that is the specific thing to debug — don't guess at other fixes first.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.OutboxPublisherConditionalTest"`
Expected: FAIL — `beanAbsentWhenPublishModeIsCdc` fails because `OutboxPublisher` has no conditional gating yet (bean always exists).

- [ ] **Step 3: Gate `OutboxPublisher` behind the mode flag**

Modify `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java` — add the import and class-level annotation (the rest of the class body is unchanged):

```java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.OutboxEvent;
import com.sanjay.ftgo.order.domain.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final String TOPIC = "order.events";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${outbox.batch-size:20}") int batchSize) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms:2000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findBySentAtIsNullOrderByIdAsc()
                .stream()
                .limit(batchSize)
                .toList();

        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(TOPIC, String.valueOf(event.getOrderId()), event.getPayload()).get();
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }
}
```

- [ ] **Step 4: Add the default property**

Modify `ftgo-order-service/src/main/resources/application.yml` — add `publish-mode` under the existing `outbox:` key:

```yaml
outbox:
  poll-fixed-delay-ms: 2000
  batch-size: 20
  publish-mode: polling
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.OutboxPublisherConditionalTest"`
Expected: PASS, all 3 cases.

- [ ] **Step 6: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS — `OutboxPublisherTest` (the existing mock-based unit test, which constructs `OutboxPublisher` directly rather than through a Spring context) is unaffected by the class-level annotation. `FtgoOrderServiceApplicationTests` boots with `outbox.publish-mode` unset in the test profile, so `matchIfMissing = true` keeps the bean present there — this preserves the existing full-context test's behavior.

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisher.java \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/OutboxPublisherConditionalTest.java
git commit -m "feat(order-service): gate OutboxPublisher behind outbox.publish-mode flag"
```

---

### Task 2: MySQL binlog + debezium user

**Files:**
- Modify: `compose.yml`
- Modify: `infrastructure/mysql/init.sql`

No application code changes — this is infrastructure-only, verified by manually inspecting MySQL's runtime state.

- [ ] **Step 1: Enable binlog on the `mysql` service**

Modify `compose.yml` — add a `command` key to the existing `mysql` service (do not change any other key in this service block):

```yaml
  mysql:
    image: mysql:8.4
    command: --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_USER: ftgo
      MYSQL_PASSWORD: ftgo
    ports:
      - "3306:3306"
    volumes:
      - mysql-data:/var/lib/mysql
      - ./infrastructure/mysql/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "ftgo", "-pftgo"]
      interval: 10s
      timeout: 5s
      retries: 5
```

- [ ] **Step 2: Add the dedicated `debezium` replication user**

Modify `infrastructure/mysql/init.sql` — append after the existing `GRANT`/`FLUSH PRIVILEGES` block:

```sql
CREATE DATABASE IF NOT EXISTS ftgo_consumer;
CREATE DATABASE IF NOT EXISTS ftgo_order;
CREATE DATABASE IF NOT EXISTS ftgo_kitchen;
CREATE DATABASE IF NOT EXISTS ftgo_accounting;
CREATE DATABASE IF NOT EXISTS ftgo_restaurant;
CREATE DATABASE IF NOT EXISTS ftgo_delivery;

GRANT ALL PRIVILEGES ON ftgo_consumer.*   TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_order.*      TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_kitchen.*    TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_accounting.* TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_restaurant.* TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_delivery.*   TO 'ftgo'@'%';
FLUSH PRIVILEGES;

CREATE USER 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

- [ ] **Step 3: Recreate the MySQL volume and verify binlog is active**

`init.sql` only runs on first container creation with an empty data volume, so the existing `mysql-data` volume must be removed for the new user/binlog settings to take effect:

```bash
docker compose down
docker volume rm my-food-to-go-app_mysql-data
docker compose up -d mysql
sleep 15
docker compose exec mysql mysql -uroot -proot -e "SHOW VARIABLES LIKE 'log_bin';"
docker compose exec mysql mysql -uroot -proot -e "SHOW VARIABLES LIKE 'binlog_format';"
docker compose exec mysql mysql -udebezium -pdebezium -e "SHOW DATABASES;"
```
Expected: `log_bin` is `ON`, `binlog_format` is `ROW`, and the `debezium` user can authenticate and list databases (proving the grants took effect).

- [ ] **Step 4: Commit**

```bash
git add compose.yml infrastructure/mysql/init.sql
git commit -m "feat: enable MySQL binlog and add a debezium replication user"
```

---

### Task 3: Kafka Connect + connector registrar

**Files:**
- Modify: `compose.yml`
- Create: `infrastructure/debezium/outbox-connector.json`

No application code changes — infrastructure-only, verified via Kafka Connect's REST API.

- [ ] **Step 1: Create the connector config**

Create `infrastructure/debezium/outbox-connector.json`:

```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "mysql",
  "database.port": "3306",
  "database.user": "debezium",
  "database.password": "debezium",
  "database.server.id": "184054",
  "topic.prefix": "ftgo",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
  "schema.history.internal.kafka.topic": "schema-changes.ftgo_order",
  "database.include.list": "ftgo_order",
  "table.include.list": "ftgo_order.outbox_events",
  "snapshot.mode": "no_data",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "event_id",
  "transforms.outbox.table.field.event.key": "order_id",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.route.by.field": "event_type",
  "transforms.outbox.route.topic.regex": ".*",
  "transforms.outbox.route.topic.replacement": "order.events"
}
```

**Before moving on:** the `debezium/connect:2.7` image tag referenced in Step 2 and the `"snapshot.mode": "no_data"` value above both need verification against whichever Debezium version actually gets pulled — this property's exact value has shifted across Debezium versions (some use `schema_only`). After pulling the image in Step 4, check the connector's actual accepted values (e.g. via `GET /connector-plugins/io.debezium.connector.mysql.MySqlConnector/config` on the running Kafka Connect REST API, or the image's bundled documentation) and correct this file if `no_data` is rejected. If you're not confident which value is correct for the pulled version, stop and report NEEDS_CONTEXT rather than guessing — this is exactly the kind of version-sensitive detail worth confirming over assuming.

- [ ] **Step 2: Add Kafka Connect to `compose.yml`**

Modify `compose.yml` — add this service after `service-registry` and before `restaurant-service` (or anywhere in the `services:` block; ordering within the file doesn't matter to Compose):

```yaml
  kafka-connect:
    image: debezium/connect:2.7
    depends_on:
      kafka:
        condition: service_started
      mysql:
        condition: service_healthy
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:29092
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: connect-configs
      OFFSET_STORAGE_TOPIC: connect-offsets
      STATUS_STORAGE_TOPIC: connect-status
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8083/"]
      interval: 10s
      timeout: 5s
      retries: 10
```

- [ ] **Step 3: Add the connector registrar to `compose.yml`**

Modify `compose.yml` — add this service after `kafka-connect`:

```yaml
  connector-registrar:
    image: curlimages/curl:8.10.1
    depends_on:
      kafka-connect:
        condition: service_healthy
    volumes:
      - ./infrastructure/debezium/outbox-connector.json:/outbox-connector.json:ro
    environment:
      OUTBOX_PUBLISH_MODE: ${OUTBOX_PUBLISH_MODE:-polling}
    entrypoint:
      - sh
      - -c
      - |
        if [ "$$OUTBOX_PUBLISH_MODE" = "cdc" ]; then
          curl -s -X PUT http://kafka-connect:8083/connectors/outbox-connector/config -H "Content-Type: application/json" -d @/outbox-connector.json
        else
          curl -s -X DELETE http://kafka-connect:8083/connectors/outbox-connector || true
        fi
    restart: "no"
```

`$$OUTBOX_PUBLISH_MODE` (doubled `$`) is required, not a typo — Docker Compose interpolates `$VAR`/`${VAR}` in YAML string values itself before the container ever sees them; `$$` is how you escape a literal `$` so the shell inside the container receives `$OUTBOX_PUBLISH_MODE` as intended, rather than Compose substituting an empty/host value first.

- [ ] **Step 4: Bring up the stack in CDC mode and verify registration**

```bash
docker compose down
OUTBOX_PUBLISH_MODE=cdc docker compose up -d --build
sleep 30
docker compose ps
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/outbox-connector/status
```
Expected: `docker compose ps` shows `kafka-connect` and `connector-registrar` (the latter as `Exited (0)`, since it's one-shot). `curl /connectors` lists `["outbox-connector"]`. `curl /connectors/outbox-connector/status` shows `"state": "RUNNING"` for both the connector and its task. If the state is `FAILED`, read the `trace` field in the response — this is almost certainly where the `snapshot.mode` version mismatch from Step 1 would surface; fix `outbox-connector.json` and re-run this step (re-running `docker compose up -d` re-triggers the registrar, which will re-`PUT` the corrected config).

- [ ] **Step 5: Bring the stack back to polling mode and verify deregistration**

```bash
docker compose down
docker compose up -d --build
sleep 15
curl -s http://localhost:8083/connectors
```
Expected: `[]` (empty array) — the registrar's `DELETE` call removed the connector since `OUTBOX_PUBLISH_MODE` was unset (defaults to `polling`).

- [ ] **Step 6: Commit**

```bash
git add compose.yml infrastructure/debezium/outbox-connector.json
git commit -m "feat: add Kafka Connect + Debezium connector registrar, gated by OUTBOX_PUBLISH_MODE"
```

---

### Task 4: order-service compose wiring + full end-to-end verification

**Files:**
- Modify: `compose.yml`
- Modify: `CONTEXT.md`

No application code changes — this task wires the flag into `order-service`'s environment and manually verifies both modes work end-to-end, then updates `CONTEXT.md`.

- [ ] **Step 1: Wire the flag into order-service**

Modify `compose.yml` — add one line to `order-service`'s existing `environment` block (do not change any other key):

```yaml
  order-service:
    build:
      context: .
      dockerfile: ftgo-order-service/Dockerfile
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_started
      service-registry:
        condition: service_started
      restaurant-service:
        condition: service_started
    ports:
      - "8082:8082"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/ftgo_order
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      EUREKA_CLIENT_SERVICE_URL_DEFAULTZONE: http://service-registry:8761/eureka/
      OUTBOX_PUBLISH_MODE: ${OUTBOX_PUBLISH_MODE:-polling}
```

- [ ] **Step 2: Verify polling mode (regression check)**

```bash
docker compose down
docker compose up -d --build
sleep 30
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 1}]}' | jq
sleep 5
docker compose exec mysql mysql -uftgo -pftgo ftgo_order -e "SELECT id, event_type, sent_at FROM outbox_events ORDER BY id DESC LIMIT 1;"
docker compose exec mysql mysql -uftgo -pftgo ftgo_kitchen -e "SELECT * FROM tickets ORDER BY id DESC LIMIT 1;"
```
Expected: order created (`201`), the outbox row's `sent_at` is populated (poller ran), and a matching `Ticket` exists — exactly the behavior verified when the messaging feature was first built. This confirms CDC infrastructure being present doesn't interfere when unused.

- [ ] **Step 3: Verify CDC mode**

```bash
docker compose down
OUTBOX_PUBLISH_MODE=cdc docker compose up -d --build
sleep 40
curl -s http://localhost:8083/connectors/outbox-connector/status
curl -s -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"restaurantId": 1, "lineItems": [{"menuItemId": 1, "quantity": 1}]}' | jq
sleep 10
docker compose exec mysql mysql -uftgo -pftgo ftgo_order -e "SELECT id, event_type, sent_at FROM outbox_events ORDER BY id DESC LIMIT 1;"
docker compose exec mysql mysql -uftgo -pftgo ftgo_kitchen -e "SELECT * FROM tickets ORDER BY id DESC LIMIT 1;"
```
Expected: connector status `RUNNING`, order created (`201`), the new outbox row's `sent_at` is **`NULL`** (the poller did not run — `OutboxPublisher` bean doesn't exist in this mode), and a matching `Ticket` still exists — proving Debezium alone delivered the event to kitchen-service via the exact same topic/payload shape.

- [ ] **Step 4: Verify mode-switch replay is handled safely**

Using the orders already created in Steps 2-3 (which have existing `processed_events` rows in kitchen-service from prior delivery):
```bash
docker compose exec mysql mysql -uftgo -pftgo ftgo_kitchen -e "SELECT COUNT(*) FROM tickets;"
```
Record this count, then:
```bash
docker compose down
OUTBOX_PUBLISH_MODE=cdc docker compose up -d --build
sleep 40
docker compose exec mysql mysql -uftgo -pftgo ftgo_kitchen -e "SELECT COUNT(*) FROM tickets;"
```
Expected: the count is unchanged even though the connector re-registered fresh (a new container, re-running Kafka Connect's registration flow) — either because `snapshot.mode: no_data` prevented any historical replay, or because kitchen-service's `processed_events` dedup ledger silently absorbed any replay that did occur. Either outcome is correct; note in your report which one was actually observed (check `docker compose logs kafka-connect` for snapshot-related log lines, and compare `processed_events` row count before/after if you want to distinguish them).

- [ ] **Step 5: Leave the stack in polling mode (the default) and confirm clean state**

```bash
docker compose down
docker compose up -d --build
sleep 15
curl -s http://localhost:8083/connectors
```
Expected: `[]` — no connector registered, stack is back to the default mode for anyone running `docker compose up` without setting the env var.

- [ ] **Step 6: Update `CONTEXT.md`**

Read the current file fully before editing so changes are surgical, matching the existing format exactly:

1. In the "Communication" patterns checklist, change `- [ ] Transaction log tailing (Ch. 3)` to `- [x] Transaction log tailing (Ch. 3)`. This is the last unchecked item under Communication — after this edit, every Ch. 3 IPC pattern is done.
2. Update the `ftgo-order-service` services-table row's Notes to mention the new CDC delivery path alongside the existing polling one, and that it's switchable via `OUTBOX_PUBLISH_MODE`.
3. Add one new session-log line, dated with today's actual date (check `date +%Y-%m-%d` — don't assume), in the same terse dense past-tense style as the existing entries, summarizing: Debezium + Kafka Connect added as a second outbox delivery mechanism (transaction log tailing), using the Outbox Event Router SMT to publish the existing `outbox_events.payload` column unchanged to `order.events`; switchable against the existing polling publisher via one `OUTBOX_PUBLISH_MODE` env var reconciled by both `OutboxPublisher`'s `@ConditionalOnProperty` gate and a new idempotent `connector-registrar` container; `OrderService`'s write side and kitchen-service's consumer are both unchanged; manually verified both modes plus mode-switch replay safety via `processed_events` dedup. Note whether this is still on a feature branch or merged, checking `git branch --show-current` first.
4. Update "## Current position" — the Status line should now read that all four Ch.3 IPC patterns (RPI/circuit breaker, messaging, transactional outbox, discovery, and now transaction log tailing) are done. Check whether the "Chapter" field should still say Ch. 4, since the user may have made progress reading it independently — if uncertain, leave the Chapter field as whatever it currently says and only update the Ch.3-specific status text.
5. In the "Patterns reference" section, change `- [ ] Transaction log tailing (Ch. 3)` to `- [x] Transaction log tailing (Ch. 3)`.

- [ ] **Step 7: Commit**

```bash
git add compose.yml CONTEXT.md
git commit -m "docs: update CONTEXT.md — Ch.3 transaction log tailing (CDC) implemented; all Ch.3 IPC patterns complete"
```
