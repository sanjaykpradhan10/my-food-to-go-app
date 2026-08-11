package com.sanjay.ftgo.common.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = AuditLoggingAspectTest.TestConfig.class)
class AuditLoggingAspectTest {

    @Autowired
    private OrderController fakeOrderController;

    @Autowired
    private AtomicReference<String> capturedTopic;

    @Autowired
    private AtomicReference<String> capturedPayload;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void publishesAuditEventOnSuccess() throws Exception {
        Jwt jwt = mock(Jwt.class);
        when(jwt.getSubject()).thenReturn("42");
        when(jwt.getClaimAsStringList("roles")).thenReturn(List.of("CONSUMER"));

        fakeOrderController.cancel(7L, jwt);

        assertThat(capturedTopic.get()).isEqualTo("audit-log");
        AuditLogEntryEvent event = objectMapper.readValue(capturedPayload.get(), AuditLogEntryEvent.class);
        assertThat(event.userId()).isEqualTo("42");
        assertThat(event.roles()).containsExactly("CONSUMER");
        assertThat(event.entityType()).isEqualTo("Order");
        assertThat(event.entityId()).isEqualTo("7");
        assertThat(event.outcome()).isEqualTo(AuditLogEntryEvent.SUCCESS);
        assertThat(event.action()).contains("cancel");
        assertThat(event.serviceName()).isEqualTo("test-service");
    }

    @Test
    void publishesFailureEventAndRethrowsWhenMethodThrows() throws Exception {
        Jwt jwt = mock(Jwt.class);
        when(jwt.getSubject()).thenReturn("99");
        when(jwt.getClaimAsStringList("roles")).thenReturn(List.of("ADMIN"));

        assertThatThrownBy(() -> fakeOrderController.explode(1L, jwt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");

        AuditLogEntryEvent event = objectMapper.readValue(capturedPayload.get(), AuditLogEntryEvent.class);
        assertThat(event.outcome()).isEqualTo(AuditLogEntryEvent.FAILURE);
        assertThat(event.failureReason()).contains("IllegalStateException");
    }

    @RestController
    @RequestMapping("/orders")
    static class OrderController {

        @PreAuthorize("hasAnyRole('CONSUMER', 'ADMIN')")
        @PostMapping("/{id}/cancel")
        public String cancel(@PathVariable Long id, Jwt jwt) {
            return "cancelled";
        }

        @PreAuthorize("hasRole('ADMIN')")
        @PostMapping("/{id}/explode")
        public String explode(@PathVariable Long id, Jwt jwt) {
            throw new IllegalStateException("boom");
        }
    }

    @Configuration
    @EnableAspectJAutoProxy
    static class TestConfig {

        @Bean
        AtomicReference<String> capturedTopic() {
            return new AtomicReference<>();
        }

        @Bean
        AtomicReference<String> capturedPayload() {
            return new AtomicReference<>();
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules();
        }

        @Bean
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> eventKafkaTemplate(AtomicReference<String> capturedTopic,
                                                           AtomicReference<String> capturedPayload) {
            KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
            when(template.send(anyString(), anyString())).thenAnswer(invocation -> {
                capturedTopic.set(invocation.getArgument(0));
                capturedPayload.set(invocation.getArgument(1));
                return CompletableFuture.completedFuture(mock(SendResult.class));
            });
            return template;
        }

        @Bean
        AuditLoggingAspect auditLoggingAspect(KafkaTemplate<String, String> eventKafkaTemplate,
                                               ObjectMapper objectMapper) {
            return new AuditLoggingAspect(eventKafkaTemplate, objectMapper, "test-service");
        }

        @Bean
        OrderController fakeOrderController() {
            return new OrderController();
        }
    }
}
