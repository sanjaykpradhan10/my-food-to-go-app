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

class AuthorizationReviseServiceTest {

    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final AuthorizationDomainEventPublisher domainEventPublisher = mock(AuthorizationDomainEventPublisher.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AuthorizationReviseService service = new AuthorizationReviseService(
            authorizationRepository, processedEventRepository, domainEventPublisher, outboxEventRepository, objectMapper);

    @Test
    void reviseForChoreographyWithinLimitReviseAuthorizationAndPublishesDomainEvent() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reviseForChoreography("e1", 42L, 8);

        assertThat(authorization.getTotalQuantity()).isEqualTo(8);
        verify(domainEventPublisher).publish(List.of(new AuthorizationRevisedEvent(42L, 8)));
    }

    @Test
    void reviseForChoreographyOverLimitPublishesRevisionRejectedWithoutMutating() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e2")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reviseForChoreography("e2", 42L, 15);

        assertThat(authorization.getTotalQuantity()).isEqualTo(3);
        verify(authorizationRepository, never()).save(any());
        verify(domainEventPublisher).publish(List.of(
                new AuthorizationRevisionRejectedEvent(42L, "order quantity exceeds authorization limit")));
    }

    @Test
    void reviseForChoreographySkipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.reviseForChoreography("e1", 42L, 8);

        verify(authorizationRepository, never()).findByOrderId(any());
    }

    @Test
    void reviseForCommandWithinLimitReplyAuthorizationRevised() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e3")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));
        when(authorizationRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.reviseForCommand("e3", 42L, 8, "ReviseOrder");

        assertThat(authorization.getTotalQuantity()).isEqualTo(8);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "AuthorizationRevised".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }

    @Test
    void reviseForCommandOverLimitRepliesAuthorizationRevisionRejectedWithoutMutating() {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(processedEventRepository.existsById("e4")).thenReturn(false);
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        service.reviseForCommand("e4", 42L, 15, "ReviseOrder");

        assertThat(authorization.getTotalQuantity()).isEqualTo(3);
        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "AuthorizationRevisionRejected".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void reviseForCommandSkipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e3")).thenReturn(true);

        service.reviseForCommand("e3", 42L, 8, "ReviseOrder");

        verify(authorizationRepository, never()).findByOrderId(any());
    }
}
