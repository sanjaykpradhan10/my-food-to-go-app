package com.sanjay.ftgo.auditlog.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import com.sanjay.ftgo.common.audit.AuditLogEntryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuditLogEventListener {

    private static final Logger log = LoggerFactory.getLogger(AuditLogEventListener.class);

    private final AuditLogEntryRepository repository;
    private final ObjectMapper objectMapper;

    public AuditLogEventListener(AuditLogEntryRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "audit-log", groupId = "audit-log-service")
    public void onMessage(String payload) {
        AuditLogEntryEvent event;
        try {
            event = objectMapper.readValue(payload, AuditLogEntryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed audit log event: {}", payload, e);
            return;
        }
        String roles = event.roles() == null || event.roles().isEmpty() ? null : String.join(",", event.roles());
        AuditLogEntry entry = new AuditLogEntry(event.userId(), roles, event.action(), event.entityType(),
                event.entityId(), event.outcome(), event.failureReason(), event.serviceName(), event.timestamp());
        repository.save(entry);
    }
}
