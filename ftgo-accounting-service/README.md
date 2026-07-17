# ftgo-accounting-service

**Port:** 8084
**Bounded context:** Payment authorization

## Role

`ftgo-accounting-service` authorizes (or declines) the consumer's card for an order. It participates in the Create Order saga as the final decision point before an order is approved, and it's the service with the most interesting architectural difference between the two saga implementations in this repo: in choreography mode it has to independently coordinate two events arriving from two other services in either order; in orchestration mode that coordination problem simply doesn't exist, because the orchestrator has already done it.

## API

None. This service has no REST endpoints — it participates in the saga entirely through Kafka.

## Events

### Publishes

| Mode | Topic | Event type | When |
|---|---|---|---|
| Choreography | `accounting.events` | `CardAuthorized` | Join resolved, quantity within limit |
| Choreography | `accounting.events` | `CardAuthorizationFailed` | Join resolved, quantity over limit |
| Orchestration | `saga.replies` | `CardAuthorized` | `AuthorizeCard` command processed, quantity within limit |
| Orchestration | `saga.replies` | `CardAuthorizationFailed` | `AuthorizeCard` command processed, quantity over limit, or `totalQuantity` missing |

### Consumes

| Mode | Topic | What it reacts to |
|---|---|---|
| Choreography | `consumer.events` | `ConsumerVerified` / `ConsumerVerificationFailed` |
| Choreography | `kitchen.events` | `TicketCreated` / `TicketCreationFailed` only — `TicketConfirmed`/`TicketCancelled` are ignored, since those are downstream echoes of this service's own eventual output |
| Orchestration | `accounting.commands` | `AuthorizeCard` — no filtering needed, this topic never carries anything else |

## The join (choreography only)

`ConsumerVerified` and `TicketCreated` are published by two independent services reacting in parallel to the same `OrderCreated` event, consumed by two separate Kafka listener threads in this service, with no ordering guarantee between the two topics. `SagaJoinState` (keyed by `orderId`) is the local bookkeeping that makes correct behavior possible without knowing which one arrives first: `handleConsumerEvent` and `handleKitchenEvent` each record their own flag and call `tryResolve`, which only proceeds once *both* `consumerVerified` and `ticketCreated` are true — so the authorization decision fires exactly once, regardless of arrival order. A `resolved`/`failed` guard at the top of both handlers makes the join idempotent against duplicate or late events, and a `@Version` field on `SagaJoinState` guards against the genuine concurrency case: the two handler threads racing to update the same row for the same order. A lost-update race throws `ObjectOptimisticLockingFailureException`, rolling back the transaction (including the `processed_events` insert) and relying on Kafka's redelivery-on-exception as the retry — no custom retry loop needed.

**In orchestration mode, none of this exists.** `handleAuthorizeCardCommand` applies the authorization decision directly, because the orchestrator already waited for both `ConsumerVerified` and `TicketCreated` replies before it ever sent the `AuthorizeCard` command — the join responsibility has moved to the central coordinator. This is the clearest concrete illustration in this codebase of what centralizing coordination actually buys you: an entire class of local state-tracking disappears.

## Domain model

`Authorization(orderId, status)` — one row per order, `status` is `"AUTHORIZED"` or `"DECLINED"`.

The authorization decision is a simple threshold: `totalQuantity <= AUTHORIZATION_QUANTITY_LIMIT` (currently 10). Total line-item quantity substitutes for a real order-total/price signal, since no pricing data flows through any saga event — order-service validates prices against restaurant-service at order-creation time but never persists or forwards them. This is a deliberate simplification for the learning exercise, not a real payment integration.

## Idempotency & reliability

Every Kafka-driven handler dedupes via a `processed_events` ledger (insert-then-act in one local transaction) before doing anything else. Outgoing events use the same transactional-outbox pattern as every other service in this repo: `Authorization`/join-state changes and the corresponding `OutboxEvent` row are written in one transaction, and a `@Scheduled` `OutboxPublisher` polls unsent rows and publishes them to Kafka (topic read per-row, not hardcoded), marking them sent only after a successful publish.

## Running standalone

```bash
./gradlew :ftgo-accounting-service:test
```

Needs the full `docker-compose` stack (MySQL, Kafka) to run live — see the root [`README.md`](../README.md).
