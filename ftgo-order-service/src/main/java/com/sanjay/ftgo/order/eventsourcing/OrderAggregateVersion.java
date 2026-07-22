package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "order_aggregate_version")
public class OrderAggregateVersion {

    @Id
    private Long orderId;

    @Column(name = "last_event_id", nullable = false)
    private String lastEventId;

    @Version
    private Long version;

    protected OrderAggregateVersion() {
    }

    public OrderAggregateVersion(Long orderId, String lastEventId) {
        this.orderId = orderId;
        this.lastEventId = lastEventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getLastEventId() {
        return lastEventId;
    }

    public Long getVersion() {
        return version;
    }

    public void recordEvent(String eventId) {
        this.lastEventId = eventId;
    }
}
