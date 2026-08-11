package com.sanjay.ftgo.auditlog.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "audit_log_entries")
public class AuditLogEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id")
    private String userId;

    // Stored as a comma-joined string rather than an @ElementCollection table: this field is
    // write-once (set at insert time from the Kafka event, never updated) and only ever read
    // back whole for display, so a normalized child table would be pure overhead - unlike
    // OrderView's line items in ftgo-order-history-service, which are genuinely relational.
    @Column(name = "roles")
    private String roles;

    @Column(name = "action", nullable = false)
    private String action;

    @Column(name = "entity_type")
    private String entityType;

    @Column(name = "entity_id")
    private String entityId;

    @Column(name = "outcome", nullable = false)
    private String outcome;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "service_name", nullable = false)
    private String serviceName;

    @Column(name = "occurred_at", nullable = false)
    private Instant timestamp;

    protected AuditLogEntry() {
    }

    public AuditLogEntry(String userId, String roles, String action, String entityType, String entityId,
                          String outcome, String failureReason, String serviceName, Instant timestamp) {
        this.userId = userId;
        this.roles = roles;
        this.action = action;
        this.entityType = entityType;
        this.entityId = entityId;
        this.outcome = outcome;
        this.failureReason = failureReason;
        this.serviceName = serviceName;
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public String getRoles() {
        return roles;
    }

    public String getAction() {
        return action;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getOutcome() {
        return outcome;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
}
