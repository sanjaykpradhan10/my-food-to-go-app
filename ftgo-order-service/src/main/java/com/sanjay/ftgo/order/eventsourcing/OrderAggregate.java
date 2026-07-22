package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCancelConfirmedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderApprovedEvent;
import com.sanjay.ftgo.order.domain.OrderCreatedEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionRejectedEvent;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;

import java.util.ArrayList;
import java.util.List;

public class OrderAggregate {

    private Long id;
    private Long consumerId;
    private Long restaurantId;
    private List<OrderLineItem> lineItems;
    private OrderStatus status;
    private List<OrderLineItem> pendingRevisedLineItems;

    public OrderAggregate() {
    }

    private OrderAggregate(OrderSnapshotData data) {
        this.id = data.id();
        this.consumerId = data.consumerId();
        this.restaurantId = data.restaurantId();
        this.lineItems = data.lineItems() == null ? null : new ArrayList<>(data.lineItems());
        this.status = data.status();
        this.pendingRevisedLineItems =
                data.pendingRevisedLineItems() == null ? null : new ArrayList<>(data.pendingRevisedLineItems());
    }

    public static OrderAggregate fromSnapshot(OrderSnapshotData data) {
        return new OrderAggregate(data);
    }

    public OrderSnapshotData toSnapshotData() {
        return new OrderSnapshotData(id, consumerId, restaurantId, lineItems, status, pendingRevisedLineItems);
    }

    public Long getId() {
        return id;
    }

    public Long getConsumerId() {
        return consumerId;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public List<OrderLineItem> getLineItems() {
        return lineItems;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public List<OrderLineItem> getPendingRevisedLineItems() {
        return pendingRevisedLineItems;
    }

    public List<OrderDomainEvent> process(CreateOrderCommand command) {
        return List.of(new OrderCreatedEvent(command.orderId(), command.consumerId(), command.restaurantId(),
                command.lineItems()));
    }

    public void apply(OrderCreatedEvent event) {
        this.id = event.orderId();
        this.consumerId = event.consumerId();
        this.restaurantId = event.restaurantId();
        this.lineItems = new ArrayList<>(event.lineItems());
        this.status = OrderStatus.APPROVAL_PENDING;
    }

    public List<OrderDomainEvent> process(ApproveOrderCommand command) {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderApprovedEvent(id));
    }

    public void apply(OrderApprovedEvent event) {
        this.status = OrderStatus.APPROVED;
    }

    public List<OrderDomainEvent> process(RejectOrderCommand command) {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRejectedEvent(id));
    }

    public void apply(OrderRejectedEvent event) {
        this.status = OrderStatus.REJECTED;
    }

    public List<OrderDomainEvent> process(CancelOrderCommand command) {
        if (status != OrderStatus.APPROVED) {
            throw new OrderCannotBeCancelledException(id);
        }
        return List.of(new OrderCancelledEvent(id));
    }

    public void apply(OrderCancelledEvent event) {
        this.status = OrderStatus.CANCEL_PENDING;
    }

    public List<OrderDomainEvent> process(NoteOrderCancelledCommand command) {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderCancelConfirmedEvent(id));
    }

    public void apply(OrderCancelConfirmedEvent event) {
        this.status = OrderStatus.CANCELLED;
    }

    public List<OrderDomainEvent> process(UndoCancelCommand command) {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderCancelRejectedEvent(id));
    }

    public void apply(OrderCancelRejectedEvent event) {
        this.status = OrderStatus.APPROVED;
    }

    public List<OrderDomainEvent> process(ReviseOrderCommand command) {
        if (status != OrderStatus.APPROVED) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRevisionProposedEvent(id, command.revision().revisedLineItems()));
    }

    public void apply(OrderRevisionProposedEvent event) {
        this.status = OrderStatus.REVISION_PENDING;
        this.pendingRevisedLineItems = new ArrayList<>(event.revisedLineItems());
    }

    public List<OrderDomainEvent> process(ConfirmRevisionCommand command) {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRevisedEvent(id, pendingRevisedLineItems));
    }

    public void apply(OrderRevisedEvent event) {
        this.status = OrderStatus.APPROVED;
        this.lineItems = new ArrayList<>(event.revisedLineItems());
        this.pendingRevisedLineItems = null;
    }

    public List<OrderDomainEvent> process(RejectRevisionCommand command) {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        return List.of(new OrderRevisionRejectedEvent(id));
    }

    public void apply(OrderRevisionRejectedEvent event) {
        this.status = OrderStatus.APPROVED;
        this.pendingRevisedLineItems = null;
    }

    // Generic dispatcher used by OrderEventStore during replay, where only the sealed
    // OrderDomainEvent supertype is known at the call site (not its concrete subtype).
    public void apply(OrderDomainEvent event) {
        switch (event) {
            case OrderCreatedEvent e -> apply(e);
            case OrderApprovedEvent e -> apply(e);
            case OrderRejectedEvent e -> apply(e);
            case OrderCancelledEvent e -> apply(e);
            case OrderCancelConfirmedEvent e -> apply(e);
            case OrderCancelRejectedEvent e -> apply(e);
            case OrderRevisionProposedEvent e -> apply(e);
            case OrderRevisedEvent e -> apply(e);
            case OrderRevisionRejectedEvent e -> apply(e);
        }
    }
}
