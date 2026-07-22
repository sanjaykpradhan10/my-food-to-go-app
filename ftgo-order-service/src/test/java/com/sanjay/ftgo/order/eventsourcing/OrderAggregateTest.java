package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCancelConfirmedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderCreatedEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderRevisedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderAggregateTest {

    private OrderAggregate aggregateIn(OrderStatus status) {
        OrderAggregate aggregate = new OrderAggregate();
        aggregate.apply(new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2))));
        if (status != OrderStatus.APPROVAL_PENDING) {
            reachStatus(aggregate, status);
        }
        return aggregate;
    }

    private void reachStatus(OrderAggregate aggregate, OrderStatus status) {
        switch (status) {
            case APPROVED -> aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
            case REJECTED -> aggregate.apply(new com.sanjay.ftgo.order.domain.OrderRejectedEvent(42L));
            case CANCEL_PENDING -> {
                aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
                aggregate.apply(new OrderCancelledEvent(42L));
            }
            case CANCELLED -> {
                aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
                aggregate.apply(new OrderCancelledEvent(42L));
                aggregate.apply(new OrderCancelConfirmedEvent(42L));
            }
            case REVISION_PENDING -> {
                aggregate.apply(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
                aggregate.apply(new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5))));
            }
            default -> throw new IllegalArgumentException("Unsupported target status in test: " + status);
        }
    }

    @Test
    void processCreateOrderCommandEmitsOrderCreatedEvent() {
        OrderAggregate aggregate = new OrderAggregate();
        CreateOrderCommand command = new CreateOrderCommand(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)));

        List<OrderDomainEvent> events = aggregate.process(command);

        assertThat(events).containsExactly(new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2))));
        assertThat(aggregate.getStatus()).isNull();
    }

    @Test
    void applyOrderCreatedEventInitializesState() {
        OrderAggregate aggregate = new OrderAggregate();

        aggregate.apply(new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2))));

        assertThat(aggregate.getId()).isEqualTo(42L);
        assertThat(aggregate.getConsumerId()).isEqualTo(1L);
        assertThat(aggregate.getRestaurantId()).isEqualTo(1L);
        assertThat(aggregate.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void processApproveOrderCommandFromApprovalPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new ApproveOrderCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new com.sanjay.ftgo.order.domain.OrderApprovedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void processApproveOrderCommandFromWrongStatusThrows() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVED);

        assertThatThrownBy(() -> aggregate.process(new ApproveOrderCommand()))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void processRejectOrderCommandFromApprovalPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVAL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new RejectOrderCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new com.sanjay.ftgo.order.domain.OrderRejectedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    void processCancelOrderCommandFromApproved() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVED);

        List<OrderDomainEvent> events = aggregate.process(new CancelOrderCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderCancelledEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
    }

    @Test
    void processCancelOrderCommandFromWrongStatusThrowsOrderCannotBeCancelled() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVAL_PENDING);

        assertThatThrownBy(() -> aggregate.process(new CancelOrderCommand()))
                .isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void processNoteOrderCancelledCommandFromCancelPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new NoteOrderCancelledCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderCancelConfirmedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void processUndoCancelCommandFromCancelPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.CANCEL_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new UndoCancelCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderCancelRejectedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void processReviseOrderCommandFromApproved() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.APPROVED);
        OrderRevision revision = new OrderRevision(List.of(new OrderLineItem(10L, 5)));

        List<OrderDomainEvent> events = aggregate.process(new ReviseOrderCommand(revision));
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5))));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        assertThat(aggregate.getPendingRevisedLineItems()).containsExactly(new OrderLineItem(10L, 5));
    }

    @Test
    void processConfirmRevisionCommandFromRevisionPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.REVISION_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new ConfirmRevisionCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 5))));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(aggregate.getLineItems()).containsExactly(new OrderLineItem(10L, 5));
    }

    @Test
    void processRejectRevisionCommandFromRevisionPending() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.REVISION_PENDING);

        List<OrderDomainEvent> events = aggregate.process(new RejectRevisionCommand());
        events.forEach(aggregate::apply);

        assertThat(events).containsExactly(new OrderRevisionRejectedEvent(42L));
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(aggregate.getPendingRevisedLineItems()).isNull();
    }

    @Test
    void snapshotRoundTripPreservesState() {
        OrderAggregate aggregate = aggregateIn(OrderStatus.REVISION_PENDING);

        OrderAggregate restored = OrderAggregate.fromSnapshot(aggregate.toSnapshotData());

        assertThat(restored.getId()).isEqualTo(aggregate.getId());
        assertThat(restored.getStatus()).isEqualTo(aggregate.getStatus());
        assertThat(restored.getLineItems()).isEqualTo(aggregate.getLineItems());
        assertThat(restored.getPendingRevisedLineItems()).isEqualTo(aggregate.getPendingRevisedLineItems());
    }

    @Test
    void genericApplyDispatchesToCorrectOverload() {
        OrderAggregate aggregate = new OrderAggregate();

        OrderDomainEvent event = new OrderCreatedEvent(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)));
        aggregate.apply(event);

        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }
}
