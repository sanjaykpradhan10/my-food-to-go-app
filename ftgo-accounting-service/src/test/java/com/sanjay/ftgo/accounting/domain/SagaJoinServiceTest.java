package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaJoinServiceTest {

    private final SagaJoinStateRepository sagaJoinStateRepository = mock(SagaJoinStateRepository.class);
    private final AuthorizationRepository authorizationRepository = mock(AuthorizationRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final SagaJoinService service = new SagaJoinService(
            sagaJoinStateRepository, authorizationRepository, processedEventRepository, outboxEventRepository, objectMapper);

    @Test
    void authorizesWhenConsumerVerifiedArrivesFirstThenTicketCreatedUnderLimit() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleConsumerEvent("e1", 42L, "ConsumerVerified");
        service.handleKitchenEvent("e2", 42L, "TicketCreated", 5);

        verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e -> "CardAuthorized".equals(e.getEventType())));
    }

    @Test
    void authorizesWhenTicketCreatedArrivesFirstThenConsumerVerified() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleKitchenEvent("e1", 42L, "TicketCreated", 5);
        service.handleConsumerEvent("e2", 42L, "ConsumerVerified");

        verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e -> "CardAuthorized".equals(e.getEventType())));
    }

    @Test
    void declinesWhenTotalQuantityExceedsLimit() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleConsumerEvent("e1", 42L, "ConsumerVerified");
        service.handleKitchenEvent("e2", 42L, "TicketCreated", 15);

        verify(authorizationRepository).save(argThat(a -> "DECLINED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e -> "CardAuthorizationFailed".equals(e.getEventType())));
    }

    @Test
    void abandonsJoinWhenConsumerVerificationFails() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleConsumerEvent("e1", 42L, "ConsumerVerificationFailed");
        service.handleKitchenEvent("e2", 42L, "TicketCreated", 5);

        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void abandonsJoinWhenTicketCreationFails() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleKitchenEvent("e1", 42L, "TicketCreationFailed", null);
        service.handleConsumerEvent("e2", 42L, "ConsumerVerified");

        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.handleConsumerEvent("e1", 42L, "ConsumerVerified");

        verify(sagaJoinStateRepository, never()).findById(any());
    }

    @Test
    void ignoresLateDuplicateEventAfterJoinAlreadyResolved() {
        SagaJoinState state = new SagaJoinState(42L);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaJoinStateRepository.findById(42L)).thenReturn(Optional.of(state));
        when(sagaJoinStateRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        service.handleConsumerEvent("e1", 42L, "ConsumerVerified");
        service.handleKitchenEvent("e2", 42L, "TicketCreated", 5);

        // Late/duplicate redelivery for the same order, after the join has already resolved.
        service.handleKitchenEvent("e3", 42L, "TicketCreated", 5);

        verify(authorizationRepository, times(1)).save(any());
        verify(outboxEventRepository, times(1)).save(any());
    }

    @Test
    void authorizesViaCommandWhenWithinLimit() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(false);

        service.handleAuthorizeCardCommand("cmd-1", 42L, 5);

        verify(authorizationRepository).save(argThat(a -> "AUTHORIZED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e ->
                "CardAuthorized".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void declinesViaCommandWhenOverLimit() {
        when(processedEventRepository.existsById("cmd-2")).thenReturn(false);

        service.handleAuthorizeCardCommand("cmd-2", 42L, 15);

        verify(authorizationRepository).save(argThat(a -> "DECLINED".equals(a.getStatus())));
        verify(outboxEventRepository).save(argThat(e ->
                "CardAuthorizationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void skipsDuplicateCommandDelivery() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(true);

        service.handleAuthorizeCardCommand("cmd-1", 42L, 5);

        verify(authorizationRepository, never()).save(any());
    }

    @Test
    void declineViaCommandWhenQuantityIsNull() {
        when(processedEventRepository.existsById("cmd-3")).thenReturn(false);

        service.handleAuthorizeCardCommand("cmd-3", 42L, null);

        verify(authorizationRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat(e ->
                "CardAuthorizationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }
}
