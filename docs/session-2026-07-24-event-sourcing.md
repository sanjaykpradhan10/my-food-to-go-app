# Session — 2026-07-24 (Ch.6 — Event sourcing, `Order` aggregate)

**Tool:** Claude Code
**Duration:** Continuation of a full brainstorm → spec → plan → subagent-driven-development cycle spanning two sessions (25-task plan, 22 subagent-dispatched review rounds), a full Docker e2e verification pass across three persistence/saga-mode combinations that caught and fixed one real bug, a full documentation sweep, and PR review/merge.
**Repo:** https://github.com/sanjaykpradhan10/my-food-to-go-app
**Branch:** `worktree-order-event-sourcing` (merged and deleted — see below)
**Spec:** `docs/superpowers/specs/2026-07-22-order-event-sourcing-design.md`
**Plan:** `docs/superpowers/plans/2026-07-22-order-event-sourcing.md`
**PR:** [#15](https://github.com/sanjaykpradhan10/my-food-to-go-app/pull/15), merged via merge commit `6fb3edf`

Continuation of an earlier same-topic session that started this plan (design decisions, Tasks 1–20) and was interrupted mid-Task-21-review by a weekly API rate limit.

---

## What we did

### Scope: event sourcing for the `Order` aggregate — all three sagas, one pass

Design decisions (made in the earlier session via brainstorming, not repeated here): hand-roll the event store (no Eventuate, consistent with every other pattern in this codebase); make it switchable via `PERSISTENCE_MODE=jpa|event-sourcing` (default `jpa`) rather than a hard replacement; cover **all three** `Order` sagas (Create, Cancel, Revise) in **one pass**, not just Create; publish choreography-mode events by **extending the existing Ch.3 Debezium/CDC pipeline**, not a new poller; build the book's **full pseudo-event mechanism** for orchestration-mode saga commands (a `SagaCommandEvent`-style poller) rather than the same-transaction shortcut; **implement snapshots** even though `Order`'s short lifecycle doesn't strictly need them, for the learning exercise; use a **dedicated optimistic-lock version table**, not one derived from event count.

### What got built

- `OrderAggregate` — the book's `process(Command)`/`apply(Event)` split, 9 command types.
- `OrderEventStore` — the hand-rolled event store: `save`/`find`/`update`, replay from a snapshot plus the event tail since it, optimistic locking via a dedicated `order_aggregate_version` table (Hibernate's own dirty-checking flush on a still-managed row, not `merge()` on a detached one — see the Task 4 bug below for why that distinction matters).
- `OrderSnapshot`/`OrderSnapshotData` — written every 5 events, pure performance optimization.
- `OrderTransitions` facade (`JpaOrderTransitions` / `EventSourcedOrderTransitions`) — every call site that used to depend on `OrderRepository` directly now depends on this instead: `OrderController`, `OrderService`, all three choreography saga services, and all three orchestration saga orchestrators. Two method contracts: `create`/`findById`/`cancel`/`revise` throw on invalid state; `approve`/`reject`/`noteCancelled`/`undoCancel`/`confirmRevision`/`rejectRevision`/`requestRevisionCompensation` silently no-op (matching what a saga reply handler already needs for duplicate/late replies).
- `SagaCommandPublisher` facade (`OutboxSagaCommandPublisher` / `EventSourcedSagaCommandPublisher`) — the same abstraction for orchestration-mode outbound saga commands, backed in event-sourcing mode by a new `order_saga_command_requests` table (deliberately **separate** from `order_events`, so the CDC connector that unconditionally routes `order_events` rows to `order.events` can never leak a saga command meant for `kitchen.commands`/`accounting.commands`) and polled by `SagaCommandRequestPublisher`.
- Debezium connector's `table.include.list` extended to also tail `order_events` (columns deliberately named to match `outbox_events`' so one connector config handles both tables).

### Subagent-driven-development execution (25 tasks total)

Executed via the `subagent-driven-development` skill: implementer subagent → task-reviewer subagent → fix loop → next task, with a durable progress ledger at `.superpowers/sdd/progress.md` (worktree-local, git-ignored). All 25 tasks passed review clean (some after fixes).

**Two genuinely subtle Hibernate/JPA bugs**, found and fixed via direct instrumentation (Task 4): the original optimistic-lock test never triggered a real conflict, because Hibernate's `merge()` silently reused a still-managed instance instead of treating two loads as independent stale reads; the first fix's own test then turned out to rely on an incorrect JPA identity-map assumption (two `findById` calls without a `clear()` between them return the *same* object, not independent copies) — fixed a second time, verified via `==` identity checks that the two loads were genuinely independent before trusting the test.

**A recurring coverage-gap pattern** — "shared helper method, only one variant has a dedicated test" (e.g. `JpaOrderTransitions.reject()` sharing `approve()`'s orchestration logic but starting with zero tests of its own) — was caught three times in early tasks (11, 12, 13) and proactively pre-empted in every later task's dispatch, so it never recurred in Tasks 17–20.

**Two implementer-subagent connection drops** (Tasks 7 and 21), both resolved by the controller verifying the in-flight partial work matched the plan exactly and completing it directly rather than re-dispatching (which risked duplicating or losing already-correct work).

**A real plan-authoring gap** (Task 7): `requestRevisionCompensation`'s extracted implementation reused the inbound `eventId` for its outbound publish instead of minting a fresh one, unlike the original code it was extracted from — caught by the task reviewer, fixed with `UUID.randomUUID()`.

Parts of the plan, all complete: event-store JPA entities & `OrderEventStore` (Tasks 1–5); `OrderTransitions` facade + all call-site rewiring across `OrderController`/`OrderService`/all three choreography saga services, including a double-publish fix (Tasks 6–14); `PERSISTENCE_MODE` env wiring + Debezium connector extension (Tasks 15–16); the orchestration-mode pseudo-event mechanism — `findById`, `OrderSagaCommandRequest`, `SagaCommandPublisher`, `SagaCommandRequestPublisher` (Tasks 17–20); rewiring all three orchestrators, `CreateOrderSagaOrchestrator` → `CancelOrderSagaOrchestrator` → `ReviseOrderSagaOrchestrator` (Tasks 21–23); full regression build + grep verification (Task 24); Docker e2e + documentation sweep (Task 25).

### Docker e2e verification found and fixed a real bug

Verified across three combinations: `PERSISTENCE_MODE=event-sourcing × SAGA_MODE=choreography`, `event-sourcing × orchestration`, and a `jpa` regression pass — create/cancel/revise happy paths plus the Revise Order saga's accounting-decline compensation case in each.

The accounting-decline case crashed order-service permanently for that order under `event-sourcing` + `choreography`: the compensation trigger (`OrderRevisionCompensationRequested`, a wire-only pseudo-event — in JPA mode it's published straight to the outbox, never touching `Order`'s own state) was being appended into `order_events` itself, the same table `OrderEventStore.replay()` unconditionally treated as the complete domain-event log to feed into `OrderAggregate.apply()`. Root-caused via systematic debugging (read the stack trace, traced it to `OrderEventStore.appendCompensationRequestedEvent`, compared against JPA-mode's equivalent), reproduced first with a dedicated failing unit test (`replaySkipsNonReplayableCompensationRequestedEvent`), then fixed by adding a `replayable` boolean flag to `OrderEventEntity` (`true` by default, `false` only for this one pseudo-event) and filtering `OrderEventStore.replay()`'s two event queries on it. Re-verified clean live in Docker afterward, both choreography and orchestration modes.

Separately, an attempt to verify the optimistic-lock concurrent-cancel scenario live in Docker was undermined by an environment artifact (restarting only the order-service container reused order/ticket IDs against still-populated sibling-service databases from an earlier test run). Resolved by code inspection instead: `OrderController` has no `@ExceptionHandler` for any optimistic-lock exception in either persistence mode, so a raw 500 on a genuine concurrent conflict is identical, pre-existing behavior in both modes — not a Ch.6 regression.

### Documentation sweep

Per this repo's `CLAUDE.md` chapter-completion rule: `README.md` (progress summary, order-service row, Book progress table), `CONTEXT.md` (progress table, current position, a detailed session-log entry, concept understanding, services table, patterns checklist, footer), `docs/ARCHITECTURE.md` (new "Event sourcing — `Order` aggregate (Ch.6)" section with sequence diagrams for the event-store update/replay cycle and the orchestration-mode pseudo-event poller, plus the CDC-reuse/wire-only-pseudo-event gotcha writeup), and `ftgo-order-service/README.md` (new "Persistence" section).

### PR review and merge

```
6fb3edf Merge pull request #15 from sanjaykpradhan10/worktree-order-event-sourcing
a3ba614 docs: full Ch.6 documentation sweep (event sourcing, Order aggregate)
fb2dd27 fix: exclude OrderRevisionCompensationRequested pseudo-event from aggregate replay
0bff3eb refactor: rewire ReviseOrderSagaOrchestrator through OrderTransitions/SagaCommandPublisher
3ef5f88 refactor: rewire CancelOrderSagaOrchestrator through OrderTransitions/SagaCommandPublisher
9449f8f refactor: rewire CreateOrderSagaOrchestrator through OrderTransitions/SagaCommandPublisher
... (34 commits total)
```

PR #15 pushed, reviewed (full test suite green, no CI configured for this repo), and merged via `gh pr merge --merge`. Post-merge: local `main` had two stale duplicate commits (the design spec and plan, originally committed on `main` directly before being cherry-picked into the worktree) — confirmed byte-identical in content to what the merge brought in, then reconciled via `git reset --hard origin/main` rather than a real merge. A small amount of leftover uncommitted debris was also found in the main checkout (a stray partial edit from this plan's own Task 1 wrong-directory incident, from earlier in the cycle) — stashed as a safety net, verified fully superseded by the merged content, then dropped. Worktree and both local/remote `worktree-order-event-sourcing` branches removed via `ExitWorktree`.

## Next actions

- [ ] Still-deferred from earlier sessions: consider a Spring Boot 4.x migration now that 3.5.x is permanently frozen (no more OSS patches). Not part of any recent session's scope.
- [ ] Chapter 7 (implementing queries — API composition, CQRS) — not started, no session yet.

---

## Resuming in a new session

### In Claude Code
Open the project and say:
> "Read CONTEXT.md. Let's start Chapter 7 — implementing queries."

### In Claude Chat
Paste `CONTEXT.md`, then say:
> "I'm working through Microservices Patterns. Chapter 6 (event sourcing) is fully done — `Order` has a hand-rolled, switchable event-sourced persistence path (`PERSISTENCE_MODE`) covering all three `Order` sagas in both saga modes, Docker-verified end to end, merged via PR #15. Ready to move to Chapter 7, implementing queries."
