package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private Order orderIn(OrderStatus status) {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), status);
    }

    @Test
    void noteApprovedMovesFromApprovalPendingToApproved() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = order.noteApproved();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(events).containsExactly(new OrderApprovedEvent(42L));
    }

    @Test
    void noteApprovedFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::noteApproved).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void noteRejectedMovesFromApprovalPendingToRejected() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = order.noteRejected();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(events).containsExactly(new OrderRejectedEvent(42L));
    }

    @Test
    void noteRejectedFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.REJECTED);

        assertThatThrownBy(order::noteRejected).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromApprovedMovesToCancelPending() {
        Order order = orderIn(OrderStatus.APPROVED);

        List<OrderDomainEvent> events = order.cancel();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
        assertThat(events).containsExactly(new OrderCancelledEvent(42L));
    }

    @Test
    void cancelFromApprovalPendingThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromRejectedThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.REJECTED);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromCancelPendingThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.CANCEL_PENDING);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromCancelledThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.CANCELLED);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelFromRevisionPendingThrowsOrderCannotBeCancelled() {
        Order order = orderIn(OrderStatus.REVISION_PENDING);

        assertThatThrownBy(order::cancel).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void noteCancelledMovesFromCancelPendingToCancelled() {
        Order order = orderIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = order.noteCancelled();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(events).containsExactly(new OrderCancelConfirmedEvent(42L));
    }

    @Test
    void noteCancelledFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::noteCancelled).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void undoCancelMovesFromCancelPendingBackToApproved() {
        Order order = orderIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = order.undoCancel();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(events).containsExactly(new OrderCancelRejectedEvent(42L));
    }

    @Test
    void undoCancelFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::undoCancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseFromApprovedMovesToRevisionPending() {
        Order order = orderIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        List<OrderDomainEvent> events = order.revise(revision);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        assertThat(events).containsExactly(new OrderRevisionProposedEvent(42L, revision.revisedLineItems()));
    }

    @Test
    void reviseFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVAL_PENDING);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        assertThatThrownBy(() -> order.revise(revision)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void confirmRevisionMovesFromRevisionPendingToApprovedAndAppliesLineItems() {
        Order order = orderIn(OrderStatus.REVISION_PENDING);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        List<OrderDomainEvent> events = order.confirmRevision(revision);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).isEqualTo(revision.revisedLineItems());
        assertThat(events).containsExactly(new OrderRevisedEvent(42L, revision.revisedLineItems()));
    }

    @Test
    void confirmRevisionFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        assertThatThrownBy(() -> order.confirmRevision(revision))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void rejectRevisionMovesFromRevisionPendingToApprovedWithoutChangingLineItems() {
        Order order = orderIn(OrderStatus.REVISION_PENDING);
        List<OrderLineItem> originalLineItems = order.getLineItems();

        List<OrderDomainEvent> events = order.rejectRevision();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).isEqualTo(originalLineItems);
        assertThat(events).containsExactly(new OrderRevisionRejectedEvent(42L));
    }

    @Test
    void rejectRevisionFromWrongStatusThrows() {
        Order order = orderIn(OrderStatus.APPROVED);

        assertThatThrownBy(order::rejectRevision).isInstanceOf(UnsupportedStateTransitionException.class);
    }
}
