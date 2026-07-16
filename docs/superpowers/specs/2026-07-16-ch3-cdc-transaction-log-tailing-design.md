# Design: Transaction Log Tailing (CDC) via a Publish-Mode Flag (Ch. 3)

**Date**: 2026-07-16
**Status**: Approved

## Goal

Add CDC-based event publishing (Debezium tailing MySQL's binlog) as a
second delivery mechanism for the *same* `outbox_events` table
order-service already writes to, switchable against the existing polling
publisher via one flag — with zero changes to `OrderService`'s write side
or kitchen-service's consumer.

This closes the fourth and final remaining Ch. 3 IPC pattern (transaction
log tailing). All four Ch. 3 IPC patterns — RPI/circuit breaker,
messaging, transactional outbox, client-side discovery, and now log
tailing — will be complete after this pass.

## Scope decisions

- **Debezium deployment**: Kafka Connect + the Debezium MySQL connector,
  running as a separate container (`debezium/connect` image). Not an
  embedded engine inside order-service — this keeps CDC delivery
  completely outside application code, which is what makes "zero changes
  to the outbox write-side" possible.
- **Message shape parity**: Debezium's Outbox Event Router SMT
  (`io.debezium.transforms.outbox.EventRouter`) unwraps the raw CDC
  envelope and republishes just the `payload` column's value, keyed by
  `order_id` — identical to what the polling publisher already sends.
  kitchen-service's `OrderEventListener` needs zero changes in either
  mode.
- **Mode switching**: a single environment variable,
  `OUTBOX_PUBLISH_MODE` (`polling` default, or `cdc`), is the one source
  of truth. It's read by two independent, idempotent reconciliation
  points on every `docker compose up`:
  - order-service's `OutboxPublisher` bean only exists when the mapped
    Spring property `outbox.publish-mode` is `polling` (or unset) —
    `@ConditionalOnProperty`.
  - A new one-shot `connector-registrar` container reads the same
    variable and either registers (`PUT`) or removes (`DELETE`) the
    Debezium connector via Kafka Connect's REST API accordingly.

  This guarantees exactly one publish path is ever active — no scenario
  where both run simultaneously and double-publish. Chosen over a
  manually-run register/unregister script because a single exported env
  var, reconciled automatically on every startup, is the standard
  production pattern for a feature flag; a script someone has to remember
  to run is not.
- **Existing outbox logic is untouched**: `OrderService.createOrder()`
  still writes `Order` and `OutboxEvent` in one transaction, exactly as
  before this pass. In CDC mode, `sent_at` simply stays `NULL` forever
  (nothing polls it) — CDC's delivery guarantee comes from the binlog
  itself, not from marking rows sent.
- **Snapshot replay is a known, accepted edge case, not a bug to
  engineer around**: switching into CDC mode for the first time on a
  table with pre-existing rows could, depending on `snapshot.mode`,
  replay historical rows as fresh events. `snapshot.mode: no_data` avoids
  this by skipping the initial data snapshot entirely, but even without
  it, kitchen-service's `processed_events` dedup ledger (from the
  messaging feature) would silently absorb any replay — this is exactly
  the scenario that dedup exists for.

## Architecture

```
                    OUTBOX_PUBLISH_MODE=polling|cdc  (one env var, both paths read it)
                              │
        ┌─────────────────────┴─────────────────────┐
        ▼                                            ▼
┌─────────────────┐                     ┌──────────────────────────┐
│  order-service    │                     │  connector-registrar       │
│                    │                     │  (one-shot, idempotent)    │
│  OutboxPublisher   │                     │  PUT/DELETE the Debezium   │
│  @ConditionalOnProperty                  │  MySQL connector config    │
│  (polling only)    │                     │  via Kafka Connect REST API│
└─────────┬──────────┘                     └─────────────┬─────────────┘
          │ polls outbox_events                            │ registers/removes
          │ (sent_at IS NULL)                               ▼
          │                                    ┌──────────────────────────┐
          │                                    │  kafka-connect             │
          │                                    │  (debezium/connect image)  │
          │                                    │  tails MySQL binlog on     │
          │                                    │  outbox_events table       │
          │                                    │  → Outbox Event Router SMT │
          │                                    └─────────────┬─────────────┘
          │                                                    │
          └──────────────────► Kafka topic "order.events" ◄────┘
                                (identical payload shape either way)
                                        │
                                        ▼
                              kitchen-service (unchanged)
```

## Debezium connector configuration

Maps directly onto the existing `outbox_events` schema (`id`, `event_id`,
`event_type`, `order_id`, `payload`, `sent_at`) — no column renames, no new
columns.

```json
{
  "name": "outbox-connector",
  "config": {
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
}
```

Notes on the choices:

- **`key.converter`/`value.converter: StringConverter`** instead of
  Debezium's default `JsonConverter` — the `payload` column already
  stores a complete JSON string, and the poller already keys by
  `String.valueOf(orderId)`. `StringConverter` writes the value
  byte-identical to what the poller writes, so
  `OrderEventListener.onMessage(String payload)` cannot tell the two
  paths apart.
- **`route.topic.regex`/`route.topic.replacement`** forces every row to
  the constant topic `order.events`, regardless of the `event_type`
  column's actual value (always the literal `"OrderCreated"` today).
- **`snapshot.mode: no_data`** — the exact property/value name has
  shifted across Debezium versions (`schema_only` in some, `no_data` in
  others). Verify the correct value for whichever Debezium image version
  actually gets pulled during implementation rather than assuming; don't
  guess at implementation time.

The connector's JSON config lives in a new file,
`infrastructure/debezium/outbox-connector.json`, holding just the
`"config"` object above (a `PUT /connectors/<name>/config` call takes the
config object directly, not the `{"name": ..., "config": {...}}` envelope
a `POST /connectors` call would need).

## MySQL setup

- `compose.yml`'s `mysql` service needs binlog enabled:
  `command: --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1`
- `infrastructure/mysql/init.sql` gets a dedicated least-privilege
  `debezium` user (not reusing the app's `ftgo` user):
  ```sql
  CREATE USER 'debezium'@'%' IDENTIFIED BY 'debezium';
  GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
  FLUSH PRIVILEGES;
  ```

## Docker Compose wiring

**`kafka-connect`** (new service, `debezium/connect:2.7` image — bundles
Kafka Connect + the Debezium MySQL connector plugin):
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

**`connector-registrar`** (new one-shot service — reconciles connector
state to the flag on every `docker compose up`):
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
  entrypoint: >
    sh -c '
    if [ "$$OUTBOX_PUBLISH_MODE" = "cdc" ]; then
      curl -s -X PUT http://kafka-connect:8083/connectors/outbox-connector/config
        -H "Content-Type: application/json" -d @/outbox-connector.json;
    else
      curl -s -X DELETE http://kafka-connect:8083/connectors/outbox-connector || true;
    fi'
  restart: "no"
```

**`order-service`** gets one new env var:
```yaml
order-service:
  environment:
    OUTBOX_PUBLISH_MODE: ${OUTBOX_PUBLISH_MODE:-polling}
    # ...existing env vars unchanged
```
Spring's relaxed binding maps `OUTBOX_PUBLISH_MODE` → `outbox.publish-mode`
automatically. `application.yml` also gets `outbox.publish-mode: polling`
as an explicit default for local (non-Docker) runs.

**Switching modes**, end to end:
```bash
docker compose down
OUTBOX_PUBLISH_MODE=cdc docker compose up -d --build
```
Both order-service (poller disabled) and the registrar (connector
registered) pick up the same value from one shell export.

## Testing & verification

**Automated (unit-level, no Debezium/Kafka Connect infra needed):**
- A focused test on `OutboxPublisher`'s `@ConditionalOnProperty` gating,
  using Spring's `ApplicationContextRunner`: with
  `outbox.publish-mode=polling` (or unset), the bean exists; with
  `outbox.publish-mode=cdc`, it doesn't. This is the one piece of new
  application code in this whole feature.
- No new tests for `OrderService`/`OutboxEvent` — genuinely unchanged,
  already covered by existing tests.

**Manual e2e verification (docker-compose, both modes):**
1. **Polling mode (regression check)**: `docker compose up -d --build`
   (default mode), place an order, confirm it flows through exactly as
   the messaging feature already verified — outbox row unsent → sent,
   Ticket created.
2. **CDC mode**: `docker compose down && OUTBOX_PUBLISH_MODE=cdc docker
   compose up -d --build`, confirm the connector registered
   (`curl http://localhost:8083/connectors/outbox-connector/status` shows
   `RUNNING`), place an order, confirm a `Ticket` appears in
   kitchen-service **without** the outbox row's `sent_at` ever being set
   (proving the poller truly didn't run).
3. **Mode-switch replay behavior**: with existing orders already present
   from step 1, switch to CDC mode and confirm kitchen-service's
   `processed_events` dedup correctly absorbs any replay without creating
   duplicate tickets.

## Error handling

- If Kafka Connect or MySQL isn't reachable when the registrar runs, the
  registrar's `curl` calls fail non-fatally (container exits non-zero but
  nothing else depends on it succeeding) — a known rough edge for a
  learning project, not hardened with retries.
- Malformed connector config would show up as `FAILED` status in
  `GET /connectors/outbox-connector/status` — same manual-check discovery
  method as verification step 2, no special handling needed.

## Deferred (not in this pass)

- Automatic retry/backoff in the registrar if Kafka Connect isn't ready in
  time (relies on `depends_on: condition: service_healthy` being
  sufficient)
- A teardown/cleanup path for Debezium's internal topics
  (`connect-configs`, `connect-offsets`, `connect-status`, schema history)
  if the stack is torn down and rebuilt repeatedly — they accumulate
  harmlessly in Kafka for a learning setup
