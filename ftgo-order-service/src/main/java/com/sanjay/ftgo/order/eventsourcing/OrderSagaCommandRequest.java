package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "order_saga_command_requests")
public class OrderSagaCommandRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "command_type", nullable = false)
    private String commandType;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "target_topic", nullable = false)
    private String targetTopic;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OrderSagaCommandRequest() {
    }

    public OrderSagaCommandRequest(String eventId, String commandType, Long orderId, String targetTopic, String payload) {
        this.eventId = eventId;
        this.commandType = commandType;
        this.orderId = orderId;
        this.targetTopic = targetTopic;
        this.payload = payload;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getTargetTopic() {
        return targetTopic;
    }

    public String getPayload() {
        return payload;
    }

    public boolean isPublished() {
        return publishedAt != null;
    }

    public void markPublished() {
        this.publishedAt = Instant.now();
    }
}
