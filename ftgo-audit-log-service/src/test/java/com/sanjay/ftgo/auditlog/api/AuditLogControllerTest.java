package com.sanjay.ftgo.auditlog.api;

import com.sanjay.ftgo.auditlog.domain.AuditLogEntry;
import com.sanjay.ftgo.auditlog.domain.AuditLogEntryRepository;
import com.sanjay.ftgo.auditlog.security.SecurityConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = AuditLogController.class)
@org.springframework.context.annotation.Import(SecurityConfig.class)
class AuditLogControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AuditLogEntryRepository repository;

    @Test
    @WithMockUser(roles = "ADMIN")
    void returnsEntriesForUserId() throws Exception {
        AuditLogEntry entry = new AuditLogEntry("42", "CONSUMER", "POST OrderController.cancel", "Order", "7",
                "SUCCESS", null, "ftgo-order-service", Instant.parse("2026-08-11T10:00:00Z"));
        when(repository.findByUserIdOrderByTimestampDesc("42")).thenReturn(List.of(entry));

        mockMvc.perform(get("/audit-log").param("userId", "42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].userId").value("42"))
                .andExpect(jsonPath("$[0].action").value("POST OrderController.cancel"));
    }

    @Test
    @WithMockUser(roles = "CONSUMER")
    void forbidsNonAdmin() throws Exception {
        mockMvc.perform(get("/audit-log"))
                .andExpect(status().isForbidden());
    }
}
