package com.sanjay.ftgo.common.audit;

import java.time.Instant;
import java.util.List;

public record AuditLogEntryEvent(
        String userId,
        List<String> roles,
        String action,
        String entityType,
        String entityId,
        String outcome,
        String failureReason,
        String serviceName,
        Instant timestamp) {

    public static final String SUCCESS = "SUCCESS";
    public static final String FAILURE = "FAILURE";
}
