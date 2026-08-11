package com.sanjay.ftgo.auditlog.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import com.sanjay.ftgo.common.audit.AuditLogEntryEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AuditLogEventListenerTest {

    @Mock
    private AuditLogEntryRepository repository;

    @Test
    void persistsAuditLogEntryFromEvent() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        AuditLogEventListener listener = new AuditLogEventListener(repository, objectMapper);

        AuditLogEntryEvent event = new AuditLogEntryEvent("42", List.of("CONSUMER", "ADMIN"),
                "POST OrderController.cancel", "Order", "7", AuditLogEntryEvent.SUCCESS, null,
                "ftgo-order-service", Instant.parse("2026-08-11T10:00:00Z"));
        String payload = objectMapper.writeValueAsString(event);

        listener.onMessage(payload);

        ArgumentCaptor<AuditLogEntry> captor = ArgumentCaptor.forClass(AuditLogEntry.class);
        verify(repository).save(captor.capture());
        AuditLogEntry saved = captor.getValue();
        assertThat(saved.getUserId()).isEqualTo("42");
        assertThat(saved.getRoles()).isEqualTo("CONSUMER,ADMIN");
        assertThat(saved.getAction()).isEqualTo("POST OrderController.cancel");
        assertThat(saved.getEntityType()).isEqualTo("Order");
        assertThat(saved.getEntityId()).isEqualTo("7");
        assertThat(saved.getOutcome()).isEqualTo("SUCCESS");
        assertThat(saved.getServiceName()).isEqualTo("ftgo-order-service");
    }

    @Test
    void skipsMalformedPayloadWithoutThrowing() {
        AuditLogEventListener listener = new AuditLogEventListener(repository, new ObjectMapper());

        listener.onMessage("not valid json");

        // No exception, no save - see onMessage's catch block.
    }
}
