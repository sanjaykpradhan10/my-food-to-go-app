package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_events")
public class OrderEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "triggering_event")
    private String triggeringEvent;

    protected OrderEventEntity() {
    }

    public OrderEventEntity(String eventId, String eventType, Long orderId, String payload, String triggeringEvent) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
        this.payload = payload;
        this.triggeringEvent = triggeringEvent;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getPayload() {
        return payload;
    }

    public String getTriggeringEvent() {
        return triggeringEvent;
    }
}
