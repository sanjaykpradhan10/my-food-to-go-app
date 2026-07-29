package com.sanjay.ftgo.orderhistory.domain;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "order_views")
public class OrderView {

    @Id
    private Long orderId;

    private Long consumerId;

    private Long restaurantId;

    private String orderStatus;

    private String ticketStatus;

    private String authorizationStatus;

    private String deliveryStatus;

    private Long courierId;

    @ElementCollection
    @CollectionTable(name = "order_view_line_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderViewLineItem> lineItems = new ArrayList<>();

    // This @Id is externally assigned (orderId), so every save() goes through
    // EntityManager.merge() rather than a true INSERT-then-dirty-checking path, and merge
    // always writes every column, not just the ones the current handler touched. Without
    // optimistic locking, two of the four @KafkaListener threads racing to update the same
    // row (e.g. OrderCreated and TicketCreated landing close together) can silently overwrite
    // each other's columns with a stale read snapshot - see SagaJoinState in
    // ftgo-accounting-service for this repo's existing precedent. KafkaConsumerConfig retries
    // the listener on OptimisticLockingFailureException so the loser re-reads the
    // already-committed write instead of clobbering it.
    @Version
    private Long version;

    protected OrderView() {
    }

    // No status is set here on purpose - every field starts unset (null/empty) regardless of
    // which of the 4 event sources creates this row first (see OrderViewService's upsert
    // pattern), since Kafka gives no cross-topic ordering guarantee.
    public OrderView(Long orderId) {
        this.orderId = orderId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(Long consumerId) {
        this.consumerId = consumerId;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public void setRestaurantId(Long restaurantId) {
        this.restaurantId = restaurantId;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getTicketStatus() {
        return ticketStatus;
    }

    public void setTicketStatus(String ticketStatus) {
        this.ticketStatus = ticketStatus;
    }

    public String getAuthorizationStatus() {
        return authorizationStatus;
    }

    public void setAuthorizationStatus(String authorizationStatus) {
        this.authorizationStatus = authorizationStatus;
    }

    public String getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(String deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public Long getCourierId() {
        return courierId;
    }

    public void setCourierId(Long courierId) {
        this.courierId = courierId;
    }

    public List<OrderViewLineItem> getLineItems() {
        return lineItems;
    }

    // Defensive copy: OrderEventListener builds this list via Stream.toList(), which is
    // immutable. Assigning it directly into this @ElementCollection field makes Hibernate
    // throw UnsupportedOperationException when it clears the collection during merge on
    // save() - same bug and fix as Order.java's revise()/confirmRevision() in ftgo-order-service.
    public void setLineItems(List<OrderViewLineItem> lineItems) {
        this.lineItems = lineItems == null ? new ArrayList<>() : new ArrayList<>(lineItems);
    }

    public Long getVersion() {
        return version;
    }
}
