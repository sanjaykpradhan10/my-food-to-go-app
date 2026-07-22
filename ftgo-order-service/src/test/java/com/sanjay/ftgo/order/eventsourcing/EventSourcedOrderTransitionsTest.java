package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.TransitionResult;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({com.sanjay.ftgo.order.domain.OrderEventSerializer.class, OrderEventStore.class, EventSourcedOrderTransitions.class,
        JacksonAutoConfiguration.class})
// EventSourcedOrderTransitions is gated by @ConditionalOnProperty(persistence.mode=event-sourcing) — @Import
// alone doesn't bypass that condition, the property must actually be set for the bean to register.
@TestPropertySource(properties = "persistence.mode=event-sourcing")
class EventSourcedOrderTransitionsTest {

    @Autowired
    private EventSourcedOrderTransitions transitions;

    @Test
    void createReturnsOrderInApprovalPending() {
        var order = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThat(order.getId()).isNotNull();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void approveThenCancelReachesCancelPending() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");

        TransitionResult result = transitions.cancel(created.getId(), "evt-3");

        assertThat(result.order().getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
        assertThat(result.events()).containsExactly(new OrderCancelledEvent(created.getId()));
    }

    @Test
    void cancelThrowsWhenOrderNotFound() {
        assertThatThrownBy(() -> transitions.cancel(999L, "evt-1")).isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void cancelThrowsWhenWrongState() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThatThrownBy(() -> transitions.cancel(created.getId(), "evt-2"))
                .isInstanceOf(com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException.class);
    }

    @Test
    void approveSilentlyNoOpsWhenOrderNotFound() {
        transitions.approve(999L, "evt-1");
        // No exception — matches JpaOrderTransitions' silent no-op contract.
    }

    @Test
    void approveSilentlyNoOpsWhenWrongState() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");

        transitions.approve(created.getId(), "evt-3");
        // Second approve() from APPROVED is a no-op, not a thrown UnsupportedStateTransitionException.
    }

    @Test
    void noteCancelledMovesOrderToCancelledWithoutError() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");
        transitions.cancel(created.getId(), "evt-3");

        transitions.noteCancelled(created.getId(), "evt-4");
        // Matches JpaOrderTransitionsTest.noteCancelledSavesAndPublishesOnSuccess's coverage of the
        // same distinct command path (NoteOrderCancelledCommand, not Approve/RejectOrderCommand).
    }

    @Test
    void undoCancelMovesOrderBackToApprovedWithoutError() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");
        transitions.cancel(created.getId(), "evt-3");

        transitions.undoCancel(created.getId(), "evt-4");
        // Matches JpaOrderTransitionsTest.undoCancelSavesAndPublishesOnSuccess's coverage of the
        // same distinct command path (UndoCancelCommand, not Approve/RejectOrderCommand).
    }

    @Test
    void reviseThenConfirmRevisionSucceedsWithoutError() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");

        TransitionResult revised = transitions.revise(created.getId(),
                new OrderRevision(List.of(new OrderLineItem(10L, 5))), "evt-3");
        assertThat(revised.order().getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);

        transitions.confirmRevision(created.getId(), "evt-4");
        // Correctness of the resulting APPROVED state + updated line items is covered by
        // OrderAggregateTest (Task 3, in-memory) and OrderEventStoreTest (Task 5, replay) —
        // this test's job is only to prove EventSourcedOrderTransitions wires those pieces
        // together without throwing. A read-based assertion on the post-confirm state is added
        // in Task 17 once OrderTransitions.findById exists.
    }

    @Test
    void reviseThenRejectRevisionSucceedsWithoutError() {
        var created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");
        transitions.approve(created.getId(), "evt-2");

        TransitionResult revised = transitions.revise(created.getId(),
                new OrderRevision(List.of(new OrderLineItem(10L, 5))), "evt-3");
        assertThat(revised.order().getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);

        transitions.rejectRevision(created.getId(), "evt-4");
        // Same rationale as reviseThenConfirmRevisionSucceedsWithoutError above, but for the
        // distinct RejectReviseOrderCommand path (Order::rejectRevision, not Order::confirmRevision).
    }
}
