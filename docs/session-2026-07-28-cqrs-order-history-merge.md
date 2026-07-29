# Session — 2026-07-28 (Ch.7 wrap-up — CQRS order-history: e2e verification, critical bug fix, merge)

**Tool:** Claude Code
**Duration:** Single session — resumed a previously-blocked sub-project, ran manual Docker e2e verification, ran a final whole-branch review that found and fixed a real concurrency bug, re-verified the fix, opened and merged the PR, and cleaned up.
**Repo:** https://github.com/sanjaykpradhan10/my-food-to-go-app
**Branch:** `worktree-cqrs-order-history` (merged and deleted — see below)
**Spec:** `docs/superpowers/specs/2026-07-25-cqrs-order-history-design.md`
**Plan:** `docs/superpowers/plans/2026-07-25-cqrs-order-history-plan.md`
**PR:** [#18](https://github.com/sanjaykpradhan10/my-food-to-go-app/pull/18), merged via merge commit `daa51f5`

Picked up from a prior session (recorded in a now-deleted `docs/session-2026-07-29-cqrs-order-history-resume.md`) where all 10 plan tasks except Task 10 (manual Docker e2e verification) were complete and independently reviewed; Task 10 was blocked by a local Docker Desktop DNS networking issue unrelated to the code.

## What we did

### Resumed and unblocked

Re-entered the existing worktree at `.claude/worktrees/cqrs-order-history` (branch `worktree-cqrs-order-history`, HEAD `6c5cb5c` at session start). The prior session's Docker DNS failures (6 consecutive `docker compose build` failures against Gradle/Maven infra) had resolved on their own — `docker compose build`/`up` succeeded cleanly on the first retry this session, standing up all 12 containers.

### Task 10: manual Docker e2e verification — passed clean

- **Happy path**: `POST /orders` (consumerId 1, restaurantId 1) → `GET /order-views/1` on order-history-service (8088) and `GET /orders/1/view` on order-service (8082, the API-composition endpoint from PR #17) agree exactly: `orderStatus=APPROVED`, `ticketStatus=AWAITING_ACCEPTANCE`, `authorizationStatus=AUTHORIZED`, `deliveryStatus=SCHEDULED`, `courierId=1`.
- **Decline scenario**: `POST /orders` with consumerId 2 (seeded inactive via `ConsumerService`'s `DataSeeder`) → both endpoints agree on `orderStatus=REJECTED`/`ticketStatus=CANCELLED`/`deliveryStatus=CANCELLED`. Noted one **intentional** divergence: order-history clears `courierId` to `null` on `DeliveryCancelled` per the design spec, while order-service's composite view still shows delivery-service's raw uncleared `courierId` — two independently-designed read models, not a bug.
- **404**: `GET /order-views/999999` → `404`.
- **Idempotency**: reset order-service's `OrderApproved` outbox row's `sent_at` to `NULL` to force Kafka redelivery; confirmed the `order_views` row for order 1 was byte-identical before/after, single row, no errors in order-history-service logs.
- Stack torn down via `docker compose down` (volume preserved). No bugs found — no commit needed for this task per the plan.

### Final whole-branch review — found and fixed a real Critical bug

Dispatched a full whole-branch review (Opus) independent of the 9 per-task reviews already done. It found:

- **Critical**: `OrderView` had no `@Version`. Because the entity's `@Id` is externally assigned, every `save()` goes through `EntityManager.merge()`, which writes *every* column, not just the ones a given handler touched. The four `@KafkaListener`s (`OrderEventListener`/`KitchenEventListener`/`AccountingEventListener`/`DeliveryEventListener`) run on independent consumer threads and can race to update the same row — whichever transaction commits second silently overwrites the first's columns with a stale read snapshot, *permanently*, since fields like `consumerId`/`lineItems` only ever arrive on one event (`OrderCreated`) that's already been marked processed.
- **Important #1**: `OrderRevised` events updated `orderStatus` but never called `setLineItems`, so line items went permanently stale after any order revision, even though the wire payload (verified against `OrderEventSerializer` in ftgo-order-service) already carried the revised items.
- **Important #2**: `OrderEventListener` passed an immutable `Stream.toList()` list straight into `OrderView.setLineItems`, an `@ElementCollection` field — this repo already hit `UnsupportedOperationException` from this exact shape once in `Order.java`, and the same hazard was unguarded here.
- **Important #3**: `OrderViewServiceTest` covered only ~11 of the ~25 real event-type-to-status mappings across the four topics.
- **Important #4**: the resume doc (`docs/session-2026-07-29-cqrs-order-history-resume.md`) was already stale, claiming Task 10 and the final review were "not started."

A fix subagent addressed all five: added `@Version` to `OrderView` plus a new `KafkaConsumerConfig` bean (`DefaultErrorHandler` + `FixedBackOff(200ms, 3 retries)`) so `OptimisticLockingFailureException` triggers a retry of the whole listener invocation (each handler already re-`findById`s fresh at the top of its own `@Transactional` method, so the retry sees the winner's committed write); proved this with a new `@DataJpaTest` (`OrderViewPersistenceTest`) that races two genuinely separate committed transactions on the same row and asserts the loser throws; fixed the `OrderRevised` line-items gap; added the defensive `ArrayList` copy in `setLineItems`; added parameterized tests for the full 24-mapping table; deleted the stale resume doc. Result: 61/61 tests passing (up from 32), full `./gradlew build` green.

**Re-review, not rubber-stamped**: dispatched a second independent review specifically to verify the fix report's own claims against the actual diff — confirmed the `@Version`/retry wiring is genuinely applied to all four listeners (no `containerFactory` override, so they resolve the new default bean), confirmed `OrderViewPersistenceTest` really uses two separate `PROPAGATION_REQUIRES_NEW` transactions (not one transaction calling two methods, which would prove nothing), confirmed the `OrderRevised` fix reads from the correct wire field by checking `OrderEventSerializer` directly, and confirmed the new parameterized tests are anchored to the real producer state machines (e.g. `Order.java`'s `rejectCancel`/`rejectRevision`) rather than being tautological re-assertions of the read model's own switch. Verdict: **Ready to merge: Yes**, with 4 Minor (non-blocking) nits — fixed the cheapest one (an off-by-one in a retry-count comment/doc: "3 attempts" → "3 retries, 4 attempts total") inline.

### PR review and merge

```
daa51f5 Merge pull request #18 from sanjaykpradhan10/worktree-cqrs-order-history
80a88aa docs: correct retry-count off-by-one in KafkaConsumerConfig comment and README
a5e9e6a docs: remove stale resume doc, clarify PersistenceConfig comment, extend DeliveryEventListenerTest
98b68f7 fix: apply revised line items on OrderRevised, expand mapping test coverage
7eb1026 fix: add optimistic locking to OrderView to prevent lost updates
... (14 commits total, base 4868718)
```

PR #18 pushed and merged via `gh pr merge --merge --delete-branch` (no CI configured for this repo; merge gated on the two independent subagent reviews above plus a green full-workspace build). Post-merge: local `main` had 3 commits not yet on origin (the design spec and plan, committed directly on `main` before the worktree existed — the same pattern noted in the Ch.6 session log) that diverged from origin's PR #18 merge commit; reconciled via a real merge (`git merge origin/main`, following the same pattern as the prior `a5da635` "Merge origin/main after PR #17" commit) rather than a reset, then pushed. Worktree and both local/remote `worktree-cqrs-order-history` branches removed.

This completes **Chapter 7** (both sub-projects — API composition via PR #17, CQRS via PR #18) — already flipped to Done in `CONTEXT.md`'s progress table during the prior session's documentation sweep; no further doc changes needed this session beyond the resume-doc deletion and the retry-count doc correction, both already covered above.

## Next actions

- [ ] Still-deferred from earlier sessions: consider a Spring Boot 4.x migration now that 3.5.x is permanently frozen (no more OSS patches). Not part of any recent session's scope.
- [ ] Chapter 8 — External API patterns — not started, no session yet.

---

## Resuming in a new session

### In Claude Code
Open the project and say:
> "Read CONTEXT.md. Let's start Chapter 8 — external API patterns."

### In Claude Chat
Paste `CONTEXT.md`, then say:
> "I'm working through Microservices Patterns. Chapter 7 (implementing queries) is fully done — both the API composition sub-project (PR #17) and the CQRS sub-project (PR #18, `ftgo-order-history-service`) are merged and Docker-verified end to end, including a real concurrency bug (missing `@Version` on the CQRS read model, causing silent lost updates across racing Kafka consumer threads) found during final review and fixed with optimistic locking + retry. Ready to move to Chapter 8, external API patterns."
