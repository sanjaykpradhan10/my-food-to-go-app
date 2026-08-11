package com.sanjay.ftgo.auditlog.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface AuditLogEntryRepository extends JpaRepository<AuditLogEntry, Long> {

    List<AuditLogEntry> findByUserIdOrderByTimestampDesc(String userId);

    List<AuditLogEntry> findByEntityTypeAndEntityIdOrderByTimestampDesc(String entityType, String entityId);

    List<AuditLogEntry> findByTimestampBetweenOrderByTimestampDesc(Instant from, Instant to);

    List<AuditLogEntry> findAllByOrderByTimestampDesc();
}
