package com.sanjay.ftgo.common.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    // Column name (order_id) unchanged from before this class was shared across services —
    // renaming the DB column is a schema change out of scope for this extraction. The Java
    // field is named generically (aggregateId, not orderId) since every consumer's saga is
    // actually keyed by the order's id regardless of which service's aggregate it's reacting
    // to (Ticket, Authorization, etc.) — see OutboxPublisher, which uses it as the Kafka
    // partition key so all events for one saga land on the same partition.
    @Column(name = "order_id", nullable = false)
    private Long aggregateId;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "sent_at")
    private Instant sentAt;

    // W3C traceparent captured at creation time (whichever request/message thread is handling the
    // original operation), not at publish time — OutboxPublisher's @Scheduled poll runs on its own
    // thread with no trace context of its own, so without this every published event would start a
    // brand-new trace instead of continuing the one that actually caused it. Nullable: absent in a
    // plain unit test or any context with no tracing infrastructure configured (see
    // TraceContextCapture), in which case OutboxPublisher just sends untraced, same as before this
    // column existed.
    @Column(name = "traceparent")
    private String traceparent;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String eventType, Long aggregateId, String topic, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.payload = payload;
        this.traceparent = TraceContextCapture.captureCurrentTraceparent();
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

    public Long getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public String getTraceparent() {
        return traceparent;
    }

    public boolean isSent() {
        return sentAt != null;
    }

    public void markSent() {
        this.sentAt = Instant.now();
    }
}
