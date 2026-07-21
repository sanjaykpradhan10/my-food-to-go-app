package com.sanjay.ftgo.accounting.domain;

import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
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
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AuthorizationCancelService service = new AuthorizationCancelService(
            authorizationRepository, processedEventRepository, domainEventPublisher, outboxEventRepository, objectMapper);

    @Test
    void reverseForChoreographyReversesAndPublishesDomainEvent() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reverseForChoreography("e1", 42L);

        assertThat(authorization.getStatus()).isEqualTo(AuthorizationStatus.REVERSED);
        verify(domainEventPublisher).publish(List.of(new AuthorizationReversedEvent(42L)));
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void reverseForChoreographySkipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.reverseForChoreography("e1", 42L);

        verify(authorizationRepository, never()).findByOrderId(any());
    }

    @Test
    void reverseForChoreographyDoesNothingWhenNoAuthorizationExistsForOrder() {
        when(processedEventRepository.existsById("e2")).thenReturn(false);
        when(authorizationRepository.findByOrderId(43L)).thenReturn(Optional.empty());

        service.reverseForChoreography("e2", 43L);

        verify(authorizationRepository, never()).save(any());
    }

    @Test
    void reverseForCommandReversesAndPublishesSagaReply() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e3")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reverseForCommand("e3", 42L, "CancelOrder");

        assertThat(authorization.getStatus()).isEqualTo(AuthorizationStatus.REVERSED);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "AuthorizationReversed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"CancelOrder\"")));
    }

    @Test
    void reverseForCommandSkipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e3")).thenReturn(true);

        service.reverseForCommand("e3", 42L, "CancelOrder");

        verify(authorizationRepository, never()).findByOrderId(any());
    }

    @Test
    void reverseForCommandDoesNothingWhenNoAuthorizationExistsForOrder() {
        when(processedEventRepository.existsById("e4")).thenReturn(false);
        when(authorizationRepository.findByOrderId(43L)).thenReturn(Optional.empty());

        service.reverseForCommand("e4", 43L, "CancelOrder");

        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }
}
