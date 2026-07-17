# Session — 2026-07-17 (orchestration follow-up)

**Tool:** Claude Code
**Duration:** Full brainstorm → spec → plan → subagent-driven-development cycle for the Ch.4 Create Order saga (orchestration style) + full documentation site, plus setup work (CLAUDE.md)
**Repo:** https://github.com/sanjaykpradhan10/my-food-to-go-app
**PRs:** #7 (orchestration saga, merged); documentation site committed directly to `main` (docs-only, low risk)

Continuation of `docs/session-2026-07-17.md` (choreography, PR #6, merged earlier the same day).

---

## What we did

### Standing process rules (CLAUDE.md)

Added `CLAUDE.md` codifying two rules the user asked to apply going forward: documentation updates land in the same change as the code they describe (not a separate follow-up), and code comments should explain *why* not *what*.

### Ch.4 Create Order saga — orchestration style, switchable via `SAGA_MODE`

Full brainstorm → spec → plan → subagent-driven-development cycle (7 tasks), same rigor as the choreography pass. Built the same saga a second time using orchestration, switchable against the already-merged choreography implementation via one `SAGA_MODE` env var per service (`choreography` default, `orchestration` alternate).

**Architecture decisions made during brainstorming:**
- Separate command/reply Kafka topics (`consumer.commands`/`kitchen.commands`/`accounting.commands`/`saga.replies`) rather than mixing command/reply event types into the existing 4 choreography topics — keeps the two communication styles cleanly comparable.
- Saga progress persisted in a new `CreateOrderSagaInstance` table (not in-memory only) — an order-service restart mid-saga shouldn't silently strand an order.
- `OutboxEvent` generalized to carry a `topic` column per row (rather than each service hardcoding one topic constant), since order-service now needs to fan out to 3 different command topics from one outbox table.

**Built:**
- A new `CreateOrderSagaOrchestrator` in order-service — the centerpiece. Sends `VerifyConsumerCommand`/`CreateTicketCommand` in parallel, waits for both replies before sending `AuthorizeCardCommand`, and approves the order directly on `CardAuthorized` without waiting for any downstream confirmation echo (a real, worth-noting difference from choreography, where order-service had to wait for kitchen's `TicketConfirmed` as an indirect signal). `CreateOrderSagaInstance` gets `@Version` optimistic locking for the same reason choreography's `SagaJoinState` needed it — two Kafka consumer threads can race on the same order's saga state.
- Each participant (consumer/kitchen/accounting) got its existing decision logic (verification, capacity check, authorization threshold) extracted into a small shared method, reused by both the choreography handler and a new command handler — so the business rules are never duplicated, only the outbound wire format (domain event vs. reply) differs.
- **Accounting-service needs zero join logic in orchestration mode** — the clearest architectural contrast between the two styles. The orchestrator already waited for both prerequisites before ever sending `AuthorizeCardCommand`, so `SagaJoinService`'s entire reason for existing (the choreography path's local join table) simply doesn't apply here.
- Kitchen-service's orchestration path also skips the `FailedOrder` pre-check table choreography needed — that race is now absorbed centrally by the orchestrator itself.

**One real defect found via code review (not manual testing this time) and fixed:**
- Task 4's `handleCreateTicketCommand` unboxed an `Integer totalQuantity` parameter without a null guard — a malformed command could throw an unhandled NPE inside a `@Transactional` method. Fixed with a guard that publishes `TicketCreationFailed` and returns before unboxing, plus a test. The same class of gap was proactively pre-empted in Task 5's dispatch instructions (accounting-service's equivalent), so it landed correctly on the first pass there with no separate fix round needed.

**Manually verified via Docker** with `SAGA_MODE=orchestration`, reusing the exact same four scenarios as the choreography pass for direct comparison — all reaching identical end states:
1. Happy path — `Order.APPROVED`, `Ticket.AWAITING_ACCEPTANCE`, `Authorization.AUTHORIZED`, saga instance shows `consumer_verified=1, ticket_created=1, failed=0`.
2. Case A (consumer verification fails) — `Order.REJECTED`, `Ticket.CANCELLED`, no authorization.
3. Case B (kitchen capacity exceeded) — `Order.REJECTED`, no ticket row at all, no authorization.
4. Case C (card authorization declined) — `Order.REJECTED`, `Ticket.CANCELLED`, `Authorization.DECLINED`.
5. Redelivery/idempotency — forced Kafka redelivery of a reply, confirmed no double-processing (authorization count stayed at 1).

One transient hiccup during manual verification: the first `POST /orders` call failed with "Restaurant service unavailable" because order-service's Eureka client hadn't finished its first registry-refresh cycle yet (~30s after container start) — not a code defect, just startup timing; resolved by retrying after the refresh completed.

PR #7 opened, reviewed (whole-branch review: Ready to merge, no blockers — one intentional design point flagged for conscious sign-off: a brief `Order=APPROVED`-while-`Ticket=CREATE_PENDING` window on the direct-approve path, which converges correctly since `ConfirmTicket` rides the same ordered Kafka partition), and merged.

### Full documentation site

Once both saga styles were merged, built the deferred documentation site: **7 per-service `README.md` files** (all 4 saga services + restaurant-service + service-registry + the still-stub delivery-service), each written by a parallel fork that read the actual current source before writing (role, API, event catalog for both saga modes where applicable, domain model, how to run standalone) — dispatched all 7 in parallel since they're independent of each other. Then **`docs/ARCHITECTURE.md`** written directly (needs the holistic cross-service view a single fork can't have): the shared transactional-outbox pattern explained once, the full 8-topic Kafka catalog, the `SAGA_MODE` switch, and **8 mermaid sequence diagrams** — choreography's happy path + all 3 compensation cases, and orchestration's same 4 scenarios, laid out in matching pairs so the two styles can be flipped between directly — plus a comparison table of what actually differs (the join, the `FailedOrder` race table, the direct-approve shortcut, new topic count). Root `README.md` updated to link both. Committed directly to `main` (docs-only, no code risk, no PR needed).

---

## What's implemented now (full picture, both saga styles)

- **ftgo-consumer-service** (8081): verifies consumer; choreography publishes to `consumer.events`, orchestration replies on `saga.replies` to a `VerifyConsumerCommand` from `consumer.commands`.
- **ftgo-order-service** (8082): `POST /orders` → `Order{APPROVAL_PENDING}`; choreography reacts to 3 event topics via `OrderSagaService`; orchestration's `CreateOrderSagaOrchestrator` drives the flow via commands/replies, persisted in `CreateOrderSagaInstance`.
- **ftgo-kitchen-service** (8083): capacity-gated `Ticket` creation (limit 20 total quantity); confirms/cancels based on saga outcome in either style.
- **ftgo-accounting-service** (8084): authorizes by quantity threshold (limit 10); choreography joins via `SagaJoinState`, orchestration needs no join at all.
- **ftgo-restaurant-service** (8085), **ftgo-service-registry** (8761): unchanged.
- **docker-compose**: all 4 saga-participant services now accept `SAGA_MODE` (default `choreography`), mirroring the existing `OUTBOX_PUBLISH_MODE` convention.

## Ch.4 status: both saga styles complete

- [x] Saga — choreography
- [x] Saga — orchestration

Ch. 4's core saga pattern work — including documentation — is fully done.

## Documentation site status: complete

- [x] Per-service READMEs (all 7 services)
- [x] `docs/ARCHITECTURE.md` (event catalog, outbox pattern, `SAGA_MODE` switch, 8 sequence diagrams, choreography-vs-orchestration comparison table)
- [x] Root `README.md` linked to both

---

## Project state at end of session

```
Git log (main, HEAD):
60ebc17 docs: add full documentation site — per-service READMEs + architecture doc
59d5b8a Merge pull request #7 from sanjaykpradhan10/worktree-ch4-create-order-saga-orchestration
4211d34 docs: update CONTEXT.md/README.md — Ch.4 Create Order saga (orchestration) implemented
07fd3e1 feat: wire SAGA_MODE env var through docker-compose for all 4 saga participants
b566d8c feat(accounting-service): handle AuthorizeCardCommand for SAGA_MODE=orchestration
```

All commits pushed to `main`. No open PRs, no open branches, no worktrees.

## Next actions

- [ ] **Move on to Ch. 5** — "Designing business logic in a microservice architecture" (DDD aggregates, domain events, transaction script). This is the recommended next step — the natural continuation, nothing else is blocking it.
- [ ] Optional, not blocking: shared module extraction for the now-4x-duplicated (and orchestration-doubled) outbox/dedup/event-record infrastructure across all 4 saga services. No urgency — both saga styles work correctly as-is; this is a code-quality cleanup, not a functional gap.

---

## Resuming in a new session

### In Claude Code
Open the project and say:
> "Read CONTEXT.md and docs/session-2026-07-17-orchestration.md. Let's start Ch. 5 — designing business logic."

### In Claude Chat
Paste `CONTEXT.md` and this file, then say:
> "I'm working through Microservices Patterns Ch. 5. Here is my context and last session notes. Resume from where I left off."
