package com.sanjay.ftgo.auditlog.api;

import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/audit-log")
public class AuditLogController {

    private final AuditLogEntryRepository repository;

    public AuditLogController(AuditLogEntryRepository repository) {
        this.repository = repository;
    }

    @PreAuthorize("hasRole('ADMIN')")
    @GetMapping
    public ResponseEntity<List<AuditLogEntryResponse>> query(
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String entityType,
            @RequestParam(required = false) String entityId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to) {

        // entityType/entityId and from/to are each required in pairs (see repository method
        // signatures below); a lone half silently falling through to a broader query would hide a
        // caller's typo behind a result set they didn't ask for, so reject it instead of guessing.
        if ((entityType != null) != (entityId != null)) {
            return ResponseEntity.badRequest().build();
        }
        if ((from != null) != (to != null)) {
            return ResponseEntity.badRequest().build();
        }

        List<AuditLogEntry> entries;
        if (userId != null) {
            entries = repository.findByUserIdOrderByTimestampDesc(userId);
        } else if (entityType != null) {
            entries = repository.findByEntityTypeAndEntityIdOrderByTimestampDesc(entityType, entityId);
        } else if (from != null) {
            entries = repository.findByTimestampBetweenOrderByTimestampDesc(from, to);
        } else {
            entries = repository.findAllByOrderByTimestampDesc();
        }

        return ResponseEntity.ok(entries.stream().map(AuditLogEntryResponse::from).toList());
    }
}
