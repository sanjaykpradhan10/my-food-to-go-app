package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizationCancelServiceMetricsTest {

    @Mock
    private AuthorizationRepository authorizationRepository;
    @Mock
    private ProcessedEventRepository processedEventRepository;
    @Mock
    private AuthorizationDomainEventPublisher domainEventPublisher;
    @Mock
    private OutboxEventRepository outboxEventRepository;

    private SimpleMeterRegistry meterRegistry;
    private AuthorizationCancelService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new AuthorizationCancelService(authorizationRepository, processedEventRepository,
                domainEventPublisher, outboxEventRepository, new ObjectMapper(), meterRegistry);
    }

    @Test
    void reverseForChoreographyIncrementsAuthorizationsReversedCounter() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reverseForChoreography("evt-1", 42L);

        assertThat(meterRegistry.counter("authorizations_reversed").count()).isEqualTo(1.0);
    }

    @Test
    void reverseForCommandIncrementsAuthorizationsReversedCounter() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reverseForCommand("evt-1", 42L, "CancelOrder");

        assertThat(meterRegistry.counter("authorizations_reversed").count()).isEqualTo(1.0);
    }
}
