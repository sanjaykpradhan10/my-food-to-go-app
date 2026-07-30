# Ch.9 Unit-Testing Tightening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two real gaps found by this session's audit against Ch.9's (§9.2) unit-testing techniques — loose (`any()`) saga command/event payload assertions in a specific set of tests, and a missing value-object worked example for `OrderLineItem` — then correct a mis-filed patterns-reference entry and record the chapter in `CONTEXT.md`.

**Architecture:** Test-only changes. No production code is touched anywhere in this plan. Tightened assertions replace trailing Mockito `any()` payload matchers with `argThat`/`eq(...)` checks against the real command/event field values (already the pattern `SagaJoinServiceTest` and `ReviseOrderSagaOrchestratorTest` use in this codebase), and one new test file exercises `OrderLineItem` as a standalone value object per the book's §9.2.2 pattern.

**Tech Stack:** JUnit 5, Mockito (`argThat`, `ArgumentCaptor`), AssertJ (`assertThat`) — all already in use throughout this repo's test suites, no new dependencies.

## Global Constraints

- No production code changes in any task — every change is confined to `src/test/java`.
- Every asserted payload must be built from values already present in each test's own fixtures (`pendingOrder()`, `instanceWith(...)`, the `courier`/`delivery` locals) — never a new hardcoded literal that duplicates a fixture value by coincidence.
- Existing test method names, `@BeforeEach`/`setUp()` wiring, and mock field declarations in each touched file stay unchanged — only the specific `verify(...)` lines identified below change.
- Run each touched module's tests after every task and confirm 0 failures before committing.

---

### Task 1: Tighten `CreateOrderSagaOrchestratorTest` payload assertions

**Files:**
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java`

**Interfaces:**
- Consumes: `SagaCommandPublisher.publish(String topic, String eventId, String eventType, Long orderId, Object command)` (`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/SagaCommandPublisher.java`); wire records `KitchenCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType)`, `DeliveryCommand(String eventId, String commandType, Long orderId, Long restaurantId, String sagaType)`, `AccountingCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType)`, `VerifyConsumerCommand(String eventId, Long orderId, Long consumerId)` (same package). `eventId` is a fresh `UUID.randomUUID().toString()` per call in `CreateOrderSagaOrchestrator`, so it can never be matched with `eq(...)` — every matcher below ignores that field by construction (it isn't compared).
- Produces: nothing consumed by later tasks — this file has no other test dependents.

This test class currently verifies every outbound command with a trailing `any()` payload matcher (e.g. `verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CreateTicket"), eq(42L), any())`), so a regression that put the wrong `totalQuantity` or `sagaType` into a real command would pass every test in this file today. Add four private static matcher-builder helpers (mirroring the `argThat` style already used in `ReviseOrderSagaOrchestratorTest`) and use them to replace every trailing `any()` in this file's existing `verify(...)` calls — do not add, remove, or rename any `@Test` method.

- [ ] **Step 1: Add the four matcher helper methods**

Add these as private static methods inside `CreateOrderSagaOrchestratorTest`, right after the existing `instanceWith(...)` helper (after line 41):

```java
    private static boolean isKitchenCommand(Object command, String commandType, Integer totalQuantity, String sagaType) {
        return command instanceof KitchenCommand kitchenCommand
                && commandType.equals(kitchenCommand.commandType())
                && java.util.Objects.equals(totalQuantity, kitchenCommand.totalQuantity())
                && sagaType.equals(kitchenCommand.sagaType());
    }

    private static boolean isDeliveryCommand(Object command, String commandType, Long restaurantId, String sagaType) {
        return command instanceof DeliveryCommand deliveryCommand
                && commandType.equals(deliveryCommand.commandType())
                && java.util.Objects.equals(restaurantId, deliveryCommand.restaurantId())
                && sagaType.equals(deliveryCommand.sagaType());
    }

    private static boolean isAccountingCommand(Object command, String commandType, Integer totalQuantity, String sagaType) {
        return command instanceof AccountingCommand accountingCommand
                && commandType.equals(accountingCommand.commandType())
                && java.util.Objects.equals(totalQuantity, accountingCommand.totalQuantity())
                && sagaType.equals(accountingCommand.sagaType());
    }

    private static boolean isVerifyConsumerCommand(Object command, Long orderId, Long consumerId) {
        return command instanceof VerifyConsumerCommand verifyConsumerCommand
                && orderId.equals(verifyConsumerCommand.orderId())
                && consumerId.equals(verifyConsumerCommand.consumerId());
    }
```

Add `import static org.mockito.ArgumentMatchers.argThat;` to the existing import block (after the `any` import).

- [ ] **Step 2: Tighten `startSendsThreeParallelCommands`**

Replace the three `verify(sagaCommandPublisher)` lines (currently lines 50–52) with:

```java
        verify(sagaCommandPublisher).publish(eq("consumer.commands"), any(), eq("VerifyConsumerCommand"), eq(42L),
                argThat(command -> isVerifyConsumerCommand(command, 42L, 1L)));
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CreateTicket"), eq(42L),
                argThat(command -> isKitchenCommand(command, "CreateTicket", 2, "CreateOrder")));
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ScheduleDelivery"), eq(42L),
                argThat(command -> isDeliveryCommand(command, "ScheduleDelivery", 1L, "CreateOrder")));
```

(`consumerId` is `1L` and `restaurantId` is `1L` because `pendingOrder()` constructs `new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING)` — consumerId and restaurantId are both the second/third constructor args; total line-item quantity is `2`.)

- [ ] **Step 3: Tighten `authorizesOnlyAfterAllThreeReplies` and `authorizesRegardlessOfReplyOrder`**

In both tests, replace:
```java
        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("AuthorizeCard"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("AuthorizeCard"), eq(42L),
                argThat(command -> isAccountingCommand(command, "AuthorizeCard", 5, "CreateOrder")));
```
(`5` because `instanceWith(42L, ..., ...)` constructs `new CreateOrderSagaInstance(orderId, 5)` — the saga instance's own `totalQuantity`, unrelated to `pendingOrder()`'s line items.)

Also replace the loose never() check in `authorizesOnlyAfterAllThreeReplies`:
```java
        verify(sagaCommandPublisher, never()).publish(eq("accounting.commands"), any(), any(), any(), any());
```
This line is already fully scoped (an entire topic/eventType wildcard `never()`) and needs no payload matcher — leave it unchanged.

- [ ] **Step 4: Tighten the four compensation tests**

In `deliverySchedulingFailedCompensatesTicketIfCreated`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L),
                argThat(command -> isKitchenCommand(command, "CancelTicket", null, "CreateOrder")));
```

In `ticketCreationFailedCompensatesDeliveryIfScheduled`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L),
                argThat(command -> isDeliveryCommand(command, "ReleaseDelivery", null, "CreateOrder")));
```

In `accountingDeclineCompensatesBothTicketAndDelivery`, replace both:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L),
                argThat(command -> isKitchenCommand(command, "CancelTicket", null, "CreateOrder")));
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L),
                argThat(command -> isDeliveryCommand(command, "ReleaseDelivery", null, "CreateOrder")));
```

In `accountingDeclineDoesNotReAuthorizeOnLateCompensationReplies`, replace the same two lines with the same two replacements as above (`never()` line for `AuthorizeCard` stays unchanged, same reasoning as Step 3).

- [ ] **Step 5: Tighten the remaining three single-command assertions**

In `compensatesLateDeliveryScheduledReplyAfterAlreadyFailed`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L),
                argThat(command -> isDeliveryCommand(command, "ReleaseDelivery", null, "CreateOrder")));
```

In `approvesOrderDirectlyOnCardAuthorizedWithoutWaitingForConfirmation`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("ConfirmTicket"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("ConfirmTicket"), eq(42L),
                argThat(command -> isKitchenCommand(command, "ConfirmTicket", null, "CreateOrder")));
```

In `rejectsOrderAndCancelsTicketOnCardAuthorizationFailed`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L),
                argThat(command -> isKitchenCommand(command, "CancelTicket", null, "CreateOrder")));
```

In `compensatesLateTicketCreatedReplyAfterAlreadyFailed`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L),
                argThat(command -> isKitchenCommand(command, "CancelTicket", null, "CreateOrder")));
```

Leave `rejectsOrderWithoutCompensatingWhenConsumerVerificationFailsBeforeTicketCreated`, `rejectsOrderOnTicketCreationFailedWithNoCompensationNeeded`, and `skipsDuplicateReplyDelivery` unchanged — their only `sagaCommandPublisher` assertions are already fully scoped `never()` wildcards.

- [ ] **Step 6: Run the tests**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestratorTest"`
Expected: all 12 tests pass.

- [ ] **Step 7: Commit**

```bash
git add ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CreateOrderSagaOrchestratorTest.java
git commit -m "test: assert real command payloads in CreateOrderSagaOrchestratorTest"
```

---

### Task 2: Tighten `CancelOrderSagaOrchestratorTest` payload assertions

**Files:**
- Modify: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java`

**Interfaces:**
- Consumes: same `SagaCommandPublisher`/`KitchenCommand`/`DeliveryCommand`/`AccountingCommand` records as Task 1 (same package, `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/`). Production behavior confirmed by reading `CancelOrderSagaOrchestrator.java`: `start()` sends `KitchenCommand(eventId, "CancelTicket", orderId, null, "CancelOrder")`; a kitchen `TicketCancelled` reply sends `DeliveryCommand(eventId, "ReleaseDelivery", orderId, null, "CancelOrder")`; a delivery `DeliveryCancelled` reply sends `AccountingCommand(eventId, "ReverseAuthorization", orderId, null, "CancelOrder")`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add matcher helpers**

This file has no existing helper methods to append after — add these as private static methods immediately after the `cancelPendingOrder()` helper (after line 26):

```java
    private static boolean isKitchenCommand(Object command, String commandType, Integer totalQuantity, String sagaType) {
        return command instanceof KitchenCommand kitchenCommand
                && commandType.equals(kitchenCommand.commandType())
                && java.util.Objects.equals(totalQuantity, kitchenCommand.totalQuantity())
                && sagaType.equals(kitchenCommand.sagaType());
    }

    private static boolean isDeliveryCommand(Object command, String commandType, Long restaurantId, String sagaType) {
        return command instanceof DeliveryCommand deliveryCommand
                && commandType.equals(deliveryCommand.commandType())
                && java.util.Objects.equals(restaurantId, deliveryCommand.restaurantId())
                && sagaType.equals(deliveryCommand.sagaType());
    }

    private static boolean isAccountingCommand(Object command, String commandType, Integer totalQuantity, String sagaType) {
        return command instanceof AccountingCommand accountingCommand
                && commandType.equals(accountingCommand.commandType())
                && java.util.Objects.equals(totalQuantity, accountingCommand.totalQuantity())
                && sagaType.equals(accountingCommand.sagaType());
    }
```

Add `import static org.mockito.ArgumentMatchers.argThat;` to the existing import block.

- [ ] **Step 2: Tighten the three positive assertions**

In `startSendsCancelTicketCommand`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L),
                argThat(command -> isKitchenCommand(command, "CancelTicket", null, "CancelOrder")));
```

In `kitchenConfirmedCancellableSendsReleaseDeliveryNotReverseAuthorization`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("delivery.commands"), any(), eq("ReleaseDelivery"), eq(42L),
                argThat(command -> isDeliveryCommand(command, "ReleaseDelivery", null, "CancelOrder")));
```
(Leave the following `never()` line on `accounting.commands` unchanged — already fully scoped.)

In `deliveryReleasedSendsReverseAuthorization`, replace:
```java
        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("ReverseAuthorization"), eq(42L), any());
```
with:
```java
        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("ReverseAuthorization"), eq(42L),
                argThat(command -> isAccountingCommand(command, "ReverseAuthorization", null, "CancelOrder")));
```

Leave `ticketCancellationRejectedUndoesCancelWithoutContactingAccounting`, `accountingReversedMarksOrderCancelled`, and `skipsDuplicateReplyDelivery` unchanged — none has a positive payload-bearing `publish` assertion to tighten.

- [ ] **Step 3: Run the tests**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestratorTest"`
Expected: all 6 tests pass.

- [ ] **Step 4: Commit**

```bash
git add ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/CancelOrderSagaOrchestratorTest.java
git commit -m "test: assert real command payloads in CancelOrderSagaOrchestratorTest"
```

---

### Task 3: Tighten `DeliveryServiceTest` payload/outbox assertions

**Files:**
- Modify: `ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryServiceTest.java`

**Interfaces:**
- Consumes: `DeliveryDomainEventPublisher.publish(Delivery delivery, List<DeliveryDomainEvent> events)` and `DeliveryScheduledEvent(Long orderId, Long courierId)` (both `ftgo-delivery-service/src/main/java/com/sanjay/ftgo/delivery/domain/`, confirmed by reading `Delivery.schedule()`, which returns `new DeliveryScheduleResult(delivery, List.of(new DeliveryScheduledEvent(orderId, courierId)))`); `OutboxEvent` getters `getEventType()`, `getAggregateId()`, `getTopic()` (`ftgo-common`'s `com.sanjay.ftgo.common.outbox.OutboxEvent`, already imported in this file). Confirmed by reading `DeliveryService.publishReply(...)`: every reply is saved as `new OutboxEvent(eventId, eventType, orderId, "saga.replies", ...)`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Tighten `handleOrderCreatedSchedulesWhenCourierAvailable`**

Replace:
```java
        verify(domainEventPublisher).publish(any(Delivery.class), any());
```
with:
```java
        verify(domainEventPublisher).publish(any(Delivery.class),
                eq(java.util.List.of(new DeliveryScheduledEvent(42L, courier.getId()))));
```
Add `import static org.mockito.ArgumentMatchers.eq;` to the existing import block (it's not currently imported in this file — check before adding to avoid a duplicate import).

- [ ] **Step 2: Tighten `handleScheduleDeliveryCommandRepliesDeliveryScheduled`**

Replace the whole test body after the `deliveryService.handleScheduleDeliveryCommand("evt-3", 42L, 7L);` line:
```java
        verify(outboxEventRepository, times(1)).save(any());
```
with:
```java
        org.mockito.ArgumentCaptor<com.sanjay.ftgo.common.outbox.OutboxEvent> captor =
                org.mockito.ArgumentCaptor.forClass(com.sanjay.ftgo.common.outbox.OutboxEvent.class);
        verify(outboxEventRepository, times(1)).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("DeliveryScheduled");
        assertThat(captor.getValue().getAggregateId()).isEqualTo(42L);
        assertThat(captor.getValue().getTopic()).isEqualTo("saga.replies");
```

- [ ] **Step 3: Tighten `handleReleaseDeliveryCommandRepliesDeliveryCancelled`**

Replace:
```java
        verify(outboxEventRepository, times(1)).save(any());
```
with:
```java
        org.mockito.ArgumentCaptor<com.sanjay.ftgo.common.outbox.OutboxEvent> captor =
                org.mockito.ArgumentCaptor.forClass(com.sanjay.ftgo.common.outbox.OutboxEvent.class);
        verify(outboxEventRepository, times(1)).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("DeliveryCancelled");
        assertThat(captor.getValue().getAggregateId()).isEqualTo(42L);
        assertThat(captor.getValue().getTopic()).isEqualTo("saga.replies");
```

Leave every other test in this file unchanged (`handleOrderCreatedPublishesSchedulingFailedWhenNoCourierAvailable`, `releaseCancelsDeliveryAndFreesCourier`, etc. already assert real payloads, per the audit).

- [ ] **Step 4: Run the tests**

Run: `./gradlew :ftgo-delivery-service:test --tests "com.sanjay.ftgo.delivery.domain.DeliveryServiceTest"`
Expected: all 9 tests pass.

- [ ] **Step 5: Commit**

```bash
git add ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/domain/DeliveryServiceTest.java
git commit -m "test: assert real event/outbox payloads in DeliveryServiceTest"
```

---

### Task 4: Add `OrderLineItemTest` (value-object worked example)

**Files:**
- Create: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderLineItemTest.java`

**Interfaces:**
- Consumes: `OrderLineItem(Long menuItemId, int quantity)` — an `@Embeddable` record with no behavior methods (`ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderLineItem.java`). Records get generated `equals()`/`hashCode()`/accessors (`menuItemId()`, `quantity()`) for free — this task tests that generated contract, matching the book's §9.2.2 framing of value objects as immutable, easy-to-test, dependency-free classes (the book's own `Money` example tests `add()`/`multiply()`; this domain's line item has no such behavior, so its test instead covers the record's identity/equality contract, which is exactly what §9.2.2 says to verify for a value object: "creates a value object in a particular state ... makes assertions about the return value").
- Produces: nothing consumed by other tasks — new standalone file.

- [ ] **Step 1: Write the test file**

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderLineItemTest {

    @Test
    void exposesMenuItemIdAndQuantity() {
        OrderLineItem lineItem = new OrderLineItem(10L, 3);

        assertThat(lineItem.menuItemId()).isEqualTo(10L);
        assertThat(lineItem.quantity()).isEqualTo(3);
    }

    @Test
    void twoLineItemsWithTheSameValuesAreEqual() {
        OrderLineItem a = new OrderLineItem(10L, 3);
        OrderLineItem b = new OrderLineItem(10L, 3);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void lineItemsDifferingByMenuItemIdAreNotEqual() {
        OrderLineItem a = new OrderLineItem(10L, 3);
        OrderLineItem b = new OrderLineItem(11L, 3);

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void lineItemsDifferingByQuantityAreNotEqual() {
        OrderLineItem a = new OrderLineItem(10L, 3);
        OrderLineItem b = new OrderLineItem(10L, 4);

        assertThat(a).isNotEqualTo(b);
    }
}
```

- [ ] **Step 2: Run the test**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderLineItemTest"`
Expected: all 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderLineItemTest.java
git commit -m "test: add OrderLineItemTest as this codebase's value-object worked example"
```

---

### Task 5: Full test suite regression run

**Files:** none (verification only)

**Interfaces:** none

- [ ] **Step 1: Run the full multi-module test suite**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL, 0 failures, across every module (this task touches no production code, so this is a pure regression check that nothing outside the four touched test files broke).

- [ ] **Step 2: If any failure is unrelated to Tasks 1–4's files, stop and report it rather than proceeding to Task 6** — it indicates a pre-existing issue, not something this plan caused, and shouldn't be silently absorbed into this chapter's scope.

---

### Task 6: Documentation — correct the mis-filing and record Ch.9

**Files:**
- Modify: `CONTEXT.md`

**Interfaces:** none (documentation only)

- [ ] **Step 1: Fix the patterns-reference mis-filing**

In the `### Testing` section (currently):
```markdown
### Testing
- [ ] Consumer-driven contract test (Ch. 9)
- [ ] Component test (Ch. 10)
- [ ] Service component test (Ch. 10)
```
replace with:
```markdown
### Testing
- [x] Unit testing patterns (Ch. 9) — entities/sagas/value objects (sociable), domain services/controllers/event handlers (solitary); this codebase's existing suite already matched 4 of the book's 6 techniques independently, tightened the remaining 2 (saga/event payload assertions, a standalone value-object test)
- [ ] Consumer-driven contract test (Ch. 10) — corrected from this table's earlier mis-filing under Ch.9; the book's actual contract-testing sections are §10.1.2–10.1.4
- [ ] Component test (Ch. 10)
- [ ] Service component test (Ch. 10)
```

- [ ] **Step 2: Flip Ch.9 to Done in the book-progress table**

Find the row starting `| 9  | Testing microservices: Part 1 | Not started | — | |` and replace with:
```markdown
| 9  | Testing microservices: Part 1 | Done | High | §9.1 (testing strategy/test pyramid) is conceptual, no code. §9.2 (unit-testing patterns for entities/value objects/sagas/domain services/controllers/event handlers) audited against the existing suite — 4 of 6 techniques already matched the book independently; tightened saga/event payload assertions in 2 order-service + 1 delivery-service test files and added a standalone `OrderLineItemTest`. Corrected a mis-filed patterns-reference entry: consumer-driven contract testing belongs to Ch.10 (§10.1.2–10.1.4), not Ch.9. |
```

- [ ] **Step 3: Update "Current position"**

Replace the `**Chapter**:` and `**Status**:` lines under `## Current position` to reflect Ch.9 done, next up Ch.10, following the same phrasing style as the existing Ch.8 entry (read the current text first — it's the block starting `- **Chapter**: 8 — External API patterns — **Done**.`). Update `**Last session**: 2026-07-29` to `**Last session**: 2026-07-30`.

- [ ] **Step 4: Add a "Concept understanding" entry**

Under `### Understood well`, add a new bullet (after the existing Ch.8 bullet):
```markdown
- Ch. 9 (testing strategies, unit testing): the test pyramid (unit at the base, fewest end-to-end tests at the top) and the solitary/sociable distinction — entities, value objects, and sagas get *sociable* unit tests (test the class together with its real, cheap-to-construct dependencies, e.g. an `Order` built via its own real constructor/methods, not mocked), while domain services, controllers, and event/message handlers get *solitary* unit tests (mock the collaborators — repositories, publishers, other services — and verify the class's own dispatch logic in isolation). The most useful finding wasn't a new technique to learn, it was discovering this codebase had already converged on 4 of the book's 6 named patterns without ever having read the chapter — evidence that the DDD-aggregate work from Ch.5 and the domain-service/controller/listener separation from every chapter since had already been shaping tests toward exactly this structure. The two real gaps found were both about assertion *depth*, not technique: several saga-orchestrator and message-handler tests verified that a call happened but not that its payload was correct (a trailing Mockito `any()` where a real value was available), which is the same class of blind spot a wrong-field regression could slip through silently.
```

- [ ] **Step 5: Add the session log entry**

Append to the end of the `## Session log` list (after the 2026-07-29 CQRS entry):
```markdown
- 2026-07-30 · Claude Code · Implemented Ch.9 (testing microservices: unit testing) via a full brainstorm → spec → plan cycle. Confirmed by reading the book PDF directly that Ch.9 covers only §9.1 (testing strategy/test pyramid, conceptual) and §9.2 (six unit-testing techniques: entities/value objects/sagas as sociable tests, domain services/controllers/event handlers as solitary tests) — consumer-driven contract testing, previously mis-filed under Ch.9 in this file's patterns-reference table, actually belongs to Ch.10 (§10.1.2–10.1.4), corrected in this session. Rather than assume new work was needed, opened with a gap audit of the existing test suite against the book's six techniques: 4 of 6 already matched closely (entity tests already assert on returned domain events per aggregate method, not just end state; domain-service and controller tests already follow the book's setup/execute/verify structure with real Mockito verification). Two real, narrow gaps found and fixed: loose `any()` payload assertions on saga commands/replies (tightened in `CreateOrderSagaOrchestratorTest`, `CancelOrderSagaOrchestratorTest`, and 2 of `DeliveryServiceTest`'s message-handler tests — `ReviseOrderSagaOrchestratorTest` was already tight and needed no change), and a missing standalone value-object test (`OrderLineItemTest`, new). No production code changed anywhere in this chapter — the existing implementation was already correct; only test assertion depth was insufficient to catch a hypothetical payload regression. No `docs/ARCHITECTURE.md` sweep, since no new pattern, saga, or service was introduced.
```

- [ ] **Step 6: Update the footer timestamp**

Replace the final `*Last updated: ...*` line with:
```markdown
*Last updated: 2026-07-30 — Ch.9 done: unit-testing patterns audited against the existing suite (4 of 6 book techniques already matched independently), saga/event payload assertions tightened in 3 test files, a standalone `OrderLineItemTest` added, and the Ch.9/Ch.10 contract-testing mis-filing corrected. Next up: Ch.10.*
```

- [ ] **Step 7: Commit**

```bash
git add CONTEXT.md
git commit -m "docs: record Ch.9 unit-testing session and fix Ch.9/Ch.10 contract-test mis-filing"
```

---

## Self-Review Notes

- **Spec coverage:** Spec's 3 scope items (tighten saga payload assertions, value-object worked example, documentation) map to Tasks 1–3 (payload tightening, split by file for reviewability), Task 4 (value object), and Task 6 (docs). Task 5 (full regression run) wasn't named in the spec's scope list but is required by the spec's own "Verification" section (`./gradlew test` across affected modules) — included as its own task since it's a distinct gate before docs.
- **Deviation from spec:** the spec listed `ReviseOrderSagaOrchestratorTest` as needing tightening alongside the other two orchestrator tests. Reading it during planning showed its `start`, `TicketQuantityRevised`, and `AuthorizationRevisionRejected` tests already use `argThat` with real field checks (see lines 36–40, 50–53, 82–84 of that file) — it was, in fact, the existing reference pattern the other two files fall short of. No task modifies it; this is called out explicitly in Task 1's file list omission rather than left as a silent gap, so a plan reviewer isn't left wondering why 2 files were touched instead of 3.
- **Placeholder scan:** no TBD/TODO markers; every step has literal file content, not a description of content.
- **Type consistency:** `KitchenCommand`/`DeliveryCommand`/`AccountingCommand`/`VerifyConsumerCommand` field names and helper method signatures are identical across Tasks 1 and 2 (both files get their own private copies, since the two test classes don't share a base class today and this plan doesn't introduce one — extracting a shared test-fixture helper class for ~7 lines of duplicated matcher logic would be scope creep beyond what this chapter's spec asked for).
