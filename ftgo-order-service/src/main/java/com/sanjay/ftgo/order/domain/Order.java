package com.sanjay.ftgo.order.domain;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long consumerId;

    private Long restaurantId;

    @ElementCollection
    @CollectionTable(name = "order_line_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderLineItem> lineItems;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    // Only populated between revise() and confirmRevision()/rejectRevision() - the confirming
    // reply arrives in a different transaction than the original /revise request, so the
    // OrderRevision object itself no longer exists to pass back in.
    @ElementCollection
    @CollectionTable(name = "order_pending_revised_line_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderLineItem> pendingRevisedLineItems;

    protected Order() {
    }

    public Order(Long id, Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this.id = id;
        this.consumerId = consumerId;
        this.restaurantId = restaurantId;
        this.lineItems = lineItems;
        this.status = status;
    }

    public Order(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this(null, consumerId, restaurantId, lineItems, status);
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

    public List<OrderDomainEvent> noteApproved() {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        return List.of(new OrderApprovedEvent(id));
    }

    public List<OrderDomainEvent> noteRejected() {
        if (status != OrderStatus.APPROVAL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.REJECTED;
        return List.of(new OrderRejectedEvent(id));
    }

    public List<OrderDomainEvent> cancel() {
        if (status != OrderStatus.APPROVED) {
            throw new OrderCannotBeCancelledException(id);
        }
        this.status = OrderStatus.CANCEL_PENDING;
        return List.of(new OrderCancelledEvent(id));
    }

    public List<OrderDomainEvent> noteCancelled() {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.CANCELLED;
        return List.of(new OrderCancelConfirmedEvent(id));
    }

    public List<OrderDomainEvent> undoCancel() {
        if (status != OrderStatus.CANCEL_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        return List.of(new OrderCancelRejectedEvent(id));
    }

    public List<OrderDomainEvent> revise(OrderRevision revision) {
        if (status != OrderStatus.APPROVED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.REVISION_PENDING;
        this.pendingRevisedLineItems = revision.revisedLineItems();
        return List.of(new OrderRevisionProposedEvent(id, revision.revisedLineItems()));
    }

    public List<OrderDomainEvent> confirmRevision() {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        // Copy contents rather than re-homing the same PersistentCollection instance into a
        // different @ElementCollection-mapped field - Hibernate rejects a shared collection
        // reference across two mapped roles once this Order is a managed entity.
        this.lineItems = new ArrayList<>(pendingRevisedLineItems);
        List<OrderDomainEvent> events = List.of(new OrderRevisedEvent(id, pendingRevisedLineItems));
        this.pendingRevisedLineItems = null;
        return events;
    }

    public List<OrderDomainEvent> rejectRevision() {
        if (status != OrderStatus.REVISION_PENDING) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = OrderStatus.APPROVED;
        this.pendingRevisedLineItems = null;
        return List.of(new OrderRevisionRejectedEvent(id));
    }
}
