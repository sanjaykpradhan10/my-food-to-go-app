# ftgo-accounting-service

**Port:** 8084
**Bounded context:** Payment authorization

## Role

`ftgo-accounting-service` authorizes, declines, reverses, or revises the consumer's card authorization for an order. It participates in all three order-related sagas — Create Order (initial authorization), Cancel Order (reversal), and Revise Order (re-authorization for a new quantity) — and is the service with the most interesting architectural difference between the two saga implementations for Create Order specifically: in choreography mode it has to independently coordinate two events arriving from two other services in either order; in orchestration mode that coordination problem simply doesn't exist, because the orchestrator has already done it. Cancel Order and Revise Order don't have this join at all in either mode — both are simple linear pipeline steps.

## API

None. This service has no REST endpoints — it participates entirely through Kafka.

## Events

### Publishes (`accounting.events`, choreography)

| eventType | When |
|---|---|
| `CardAuthorized` | Create Order join resolved, quantity within limit |
| `CardAuthorizationFailed` | Create Order join resolved, quantity over limit |
| `AuthorizationReversed` | Cancel Order: kitchen confirmed the ticket cancellable, authorization reversed |
| `AuthorizationRevised` | Revise Order: kitchen confirmed the revised quantity, re-authorization succeeded |
| `AuthorizationRevisionRejected` | Revise Order: re-authorization declined (over threshold) — `Authorization` itself is left unchanged, this doesn't fire a real state transition |

### Publishes (`saga.replies`, orchestration)

| Reply eventType | Reply to command | When |
|---|---|---|
| `CardAuthorized` / `CardAuthorizationFailed` | `AuthorizeCard` | Quantity within/over limit, or `totalQuantity` missing |
| `AuthorizationReversed` | `ReverseAuthorization` | Always succeeds — `reverse()` is unconditional once `AUTHORIZED` |
| `AuthorizationRevised` / `AuthorizationRevisionRejected` | `ReviseAuthorization` | Quantity within/over limit |

### Consumes

| Mode | Topic | What it reacts to |
|---|---|---|
| Choreography | `consumer.events` | `ConsumerVerified` / `ConsumerVerificationFailed` |
| Choreography | `kitchen.events` | `TicketCreated` / `TicketCreationFailed` (Create Order join); `TicketCancelled` (Cancel Order — triggers reversal); `TicketQuantityRevised` (Revise Order — triggers re-authorization). `TicketConfirmed`/`TicketCancellationRejected`/etc. are ignored — those are downstream echoes this service has no reason to react to |
| Orchestration | `accounting.commands` | `AccountingCommand{commandType=AuthorizeCard\|ReverseAuthorization\|ReviseAuthorization}`, dispatched by `commandType` |

## The join (Create Order, choreography only)

`ConsumerVerified` and `TicketCreated` are published by two independent services reacting in parallel to the same `OrderCreated` event, consumed by two separate Kafka listener threads in this service, with no ordering guarantee between the two topics. `SagaJoinState` (keyed by `orderId`) is the local bookkeeping that makes correct behavior possible without knowing which one arrives first: `handleConsumerEvent` and `handleKitchenEvent` each record their own flag and call `tryResolve`, which only proceeds once *both* `consumerVerified` and `ticketCreated` are true — so the authorization decision fires exactly once, regardless of arrival order. A `resolved`/`failed` guard at the top of both handlers makes the join idempotent against duplicate or late events, and a `@Version` field on `SagaJoinState` guards against the genuine concurrency case: the two handler threads racing to update the same row for the same order. A lost-update race throws `ObjectOptimisticLockingFailureException`, rolling back the transaction (including the `processed_events` insert) and relying on Kafka's redelivery-on-exception as the retry — no custom retry loop needed.

**In orchestration mode, none of this exists.** `handleAuthorizeCardCommand` applies the authorization decision directly, because the orchestrator already waited for both `ConsumerVerified` and `TicketCreated` replies before it ever sent the `AuthorizeCard` command. **Cancel Order and Revise Order never needed a join at all, in either mode** — both are strict linear pipelines where accounting is only ever asked one thing, after kitchen has already confirmed its own step.

## Domain model

`Authorization` — `id`, `orderId`, `status` (`AuthorizationStatus`: `AUTHORIZED`/`DECLINED`/`REVERSED`), `totalQuantity` (persisted). Guarded methods: static factories `authorize(orderId, totalQuantity)`/`decline(orderId, reason, totalQuantity)`, instance methods `reverse()` (legal only from `AUTHORIZED`) and `reviseAuthorization(newTotalQuantity)` (legal only from `AUTHORIZED`, mirrors `reverse()`'s guard exactly). State-changing methods return class-per-event `AuthorizationDomainEvent`s (sealed interface).

The authorization/re-authorization decision is a simple threshold: `totalQuantity <= AUTHORIZATION_QUANTITY_LIMIT` (currently 10). Total line-item quantity substitutes for a real order-total/price signal, since no pricing data flows through any saga event — a deliberate simplification for the learning exercise, not a real payment integration. Threshold checking stays in the service layer (`SagaJoinService.isAuthorized`, `AuthorizationReviseService.isAuthorized`), not the aggregate — the aggregate's guarded methods never self-reject on the threshold, only on illegal state.

**`AuthorizationCancelService`** (Cancel Order) and **`AuthorizationReviseService`** (Revise Order) each split into two entry points by channel, not by decision logic: `reverseForChoreography`/`reviseForChoreography` publish a domain event to `accounting.events`; `reverseForCommand`/`reviseForCommand` publish a `SagaReply` to `saga.replies`. This split matters — publishing a domain event in orchestration mode would leave the orchestrator waiting forever on a `saga.replies` message that never arrives, a real bug caught during Cancel Order's Docker verification and deliberately avoided from the start when Revise Order was built the same way.

## Idempotency & reliability

Every Kafka-driven handler dedupes via a `processed_events` ledger (insert-then-act in one local transaction) before doing anything else. Outgoing events use the same transactional-outbox pattern as every other service in this repo: `Authorization`/join-state changes and the corresponding `OutboxEvent` row are written in one transaction, and a `@Scheduled` `OutboxPublisher` polls unsent rows and publishes them to Kafka (topic read per-row, not hardcoded), marking them sent only after a successful publish. `OutboxEvent`/`OutboxPublisher`/`KafkaProducerConfig` themselves live in the shared `ftgo-common` module (see the root `docs/ARCHITECTURE.md`), not this service's own source tree.

## Running standalone

```bash
./gradlew :ftgo-accounting-service:test
```

Needs the full `docker-compose` stack (MySQL, Kafka) to run live — see the root [`README.md`](../README.md).
