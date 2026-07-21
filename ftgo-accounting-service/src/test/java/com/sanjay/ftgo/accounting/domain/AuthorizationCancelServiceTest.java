package com.sanjay.ftgo.accounting.domain;

import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuthorizationCancelServiceTest {

    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final AuthorizationDomainEventPublisher domainEventPublisher = mock(AuthorizationDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AuthorizationCancelService service = new AuthorizationCancelService(
            authorizationRepository, processedEventRepository, domainEventPublisher);

    @Test
    void reversesAnAuthorizedAuthorization() {
        Authorization authorization = Authorization.authorize(42L).authorization();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reverse("e1", 42L, "CancelOrder");

        assertThat(authorization.getStatus()).isEqualTo(AuthorizationStatus.REVERSED);
        verify(domainEventPublisher).publish(java.util.List.of(new AuthorizationReversedEvent(42L)));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.reverse("e1", 42L, "CancelOrder");

        verify(authorizationRepository, never()).findByOrderId(any());
    }

    @Test
    void doesNothingWhenNoAuthorizationExistsForOrder() {
        when(processedEventRepository.existsById("e2")).thenReturn(false);
        when(authorizationRepository.findByOrderId(43L)).thenReturn(Optional.empty());

        service.reverse("e2", 43L, "CancelOrder");

        verify(authorizationRepository, never()).save(any());
    }
}
