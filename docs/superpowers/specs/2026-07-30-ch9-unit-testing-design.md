# Ch.9 — Testing microservices: Part 1 — design

**Date:** 2026-07-30
**Status:** Approved, pending plan

## Background

Chapter 9 of *Microservices Patterns* covers only two sections: 9.1 (testing strategy,
the test pyramid, solitary vs. sociable unit tests — conceptual, not code) and 9.2
(unit-testing patterns for six kinds of class: entities, value objects, sagas, domain
services, controllers, event/message handlers). Consumer-driven contract testing —
previously mis-filed under Ch.9 in this project's `CONTEXT.md` patterns-reference
table — actually belongs to Ch.10 (§10.1.2–10.1.4), confirmed by reading the book PDF
directly. This spec corrects that filing error as part of its scope.

Because Ch.9 is a testing-discipline chapter rather than a new-feature chapter, this
session opened with a gap audit of the existing FTGO test suite against the book's six
§9.2 techniques, rather than assuming net-new work was needed.

## Audit findings

| Technique | Verdict |
|---|---|
| 9.2.1 Entities (sociable) | Already matches the book closely. `OrderTest`/`TicketTest` build via real factories, invoke real methods, assert both end-state and `containsExactly(new XxxEvent(...))` on returned domain events — stronger than the book's own worked example. No gap. |
| 9.2.2 Value objects | No dedicated worked example exists. `OrderLineItem` (an `@Embeddable` record) is the closest candidate but has no standalone test file — only exercised indirectly inside `OrderTest`. |
| 9.2.3 Sagas (exact message sequence) | **Real gap.** Orchestration-mode saga orchestrator tests (`CreateOrderSagaOrchestratorTest`, `CancelOrderSagaOrchestratorTest`, `ReviseOrderSagaOrchestratorTest`) verify `sagaCommandPublisher.publish(eq(topic), any(), eq(commandType), eq(orderId), any())` — the actual command **payload** is always `any()`, never asserted. A wrong-field regression (e.g. wrong `totalQuantity` in `CreateTicket`) would pass every existing test. Choreography-side saga triggers (`ChoreographyOrderCreationSagaTriggerTest` etc.) are thin delegators that already assert the exact `Order` object passed through — no gap there. |
| 9.2.3 (cont'd) Choreography domain services | **Real gap**, found on expanding the audit: `DeliveryServiceTest` (delivery-service) has 3 spots (`handleOrderCreatedSchedulesWhenCourierAvailable`, `handleScheduleDeliveryCommandRepliesDeliveryScheduled`, `handleReleaseDeliveryCommandRepliesDeliveryCancelled`) that verify `domainEventPublisher.publish(any(Delivery.class), any())` / `outboxEventRepository.save(any())` without checking event/topic content — unlike `SagaJoinServiceTest`'s equivalent assertions, which use `argThat` to check real field values. `TicketServiceTest`, `OrderCancelSagaServiceTest`, `OrderReviseSagaServiceTest` were checked and are already tight or intentionally narrow-scoped (delegation-only, transition logic tested elsewhere) — no further gaps. |
| 9.2.4 Domain services (solitary) | Already matches the book closely. `SagaJoinServiceTest` follows setup/execute/verify precisely, verifies repository saves and event publishes with `argThat` predicates on real field values. No gap. |
| 9.2.5 Controllers (MockMvc) | Already matches the book exactly. `OrderControllerTest` — `@WebMvcTest`, mocked services, status codes, `jsonPath` body assertions, full error-path coverage. No gap. |
| 9.2.6 Event/message handlers | Already correctly scoped. `KitchenEventListenerTest` tests routing/dispatch only, doesn't duplicate domain-service-level assertions — exactly the book's intended split between handler tests and domain-service tests. No gap. |

**Conclusion:** the FTGO test suite already independently converged on 4 of 6 book
techniques almost exactly as written, without ever having read this chapter. The real,
actionable gaps are narrow: loose command/event-payload assertions in a specific set of
saga-related tests, and a missing (but easy) value-object worked example.

## Scope

### 1. Tighten saga payload assertions

Replace the trailing `any()` payload matcher with an `argThat`/`eq(new XxxCommand(...))`
assertion on the real payload, in:

- `ftgo-order-service/.../domain/CreateOrderSagaOrchestratorTest.java`
- `ftgo-order-service/.../domain/CancelOrderSagaOrchestratorTest.java`
- `ftgo-order-service/.../domain/ReviseOrderSagaOrchestratorTest.java`
- `ftgo-delivery-service/.../domain/DeliveryServiceTest.java` (3 identified spots:
  `handleOrderCreatedSchedulesWhenCourierAvailable`,
  `handleScheduleDeliveryCommandRepliesDeliveryScheduled`,
  `handleReleaseDeliveryCommandRepliesDeliveryCancelled`)

No production code changes — these are test-only changes tightening existing
assertions to check payload contents, not just that *a* call happened. Every asserted
payload must be built from the same test fixtures already used in each test (no new
production types).

### 2. Value-object worked example

Add `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderLineItemTest.java`
testing `OrderLineItem` (an existing `@Embeddable` record: `menuItemId`, `quantity`) per
the book's §9.2.2 pattern — solitary tests, no dependencies, covering record equality/
immutability semantics. No behavior methods exist on `OrderLineItem` today, so this is a
test-only addition; no production code changes (adding synthetic behavior to match the
book's `Money.add()/multiply()` shape would be scope creep this domain doesn't need).

### 3. Documentation

- `CONTEXT.md` patterns-reference table: move "Consumer-driven contract test" from the
  Ch.9 row to the Ch.10 row (correcting the mis-filing this session discovered);
  check off the two real Ch.9 items once done (unit testing techniques, test pyramid
  understanding).
- `CONTEXT.md` book-progress table: Ch.9 → Done.
- `CONTEXT.md` "Concept understanding" section: add a Ch.9 "Understood well" entry
  summarizing the audit's actual finding (this codebase converged on the book's
  patterns independently) rather than a generic restatement of the book's techniques.
- Session log entry in `CONTEXT.md` following the existing per-session convention.
- No `docs/ARCHITECTURE.md` changes — this chapter adds no new pattern, saga, or
  service; it strengthens existing test coverage. The chapter-completion full-sweep
  rule doesn't apply here in the same way it did for feature chapters, since there's
  no new architecture to diagram.

## Out of scope

- Chapter 10 (component tests, contract tests, end-to-end tests) — separate chapter,
  separate session.
- Any production code changes — this is a test-only chapter.
- Auditing every remaining test file in the repo beyond the representative sample this
  session's fork already covered (entities, value objects, sagas — both orchestration
  and choreography, domain services, controllers, event/message handlers, across
  order/kitchen/accounting/delivery-service).

## Verification

- `./gradlew test` across `ftgo-order-service` and `ftgo-delivery-service` after the
  assertion changes — all existing tests must still pass (this is a tightening of
  existing green tests, not new behavior, so no new failures are expected; a
  deliberately-broken payload in one test, verified to fail, then reverted, would
  confirm the tightened assertion is actually load-bearing before finalizing — left
  to plan-time judgment on whether that's worth doing for all 7 spots or a
  representative subset).
