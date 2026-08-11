package com.sanjay.ftgo.auditlog.api;

import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;

import java.time.Instant;

public record AuditLogEntryResponse(
        Long id,
        String userId,
        String roles,
        String action,
        String entityType,
        String entityId,
        String outcome,
        String failureReason,
        String serviceName,
        Instant timestamp) {

    public static AuditLogEntryResponse from(AuditLogEntry entry) {
        return new AuditLogEntryResponse(entry.getId(), entry.getUserId(), entry.getRoles(), entry.getAction(),
                entry.getEntityType(), entry.getEntityId(), entry.getOutcome(), entry.getFailureReason(),
                entry.getServiceName(), entry.getTimestamp());
    }
}
