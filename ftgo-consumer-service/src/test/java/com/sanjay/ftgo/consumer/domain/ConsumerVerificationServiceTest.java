package com.sanjay.ftgo.consumer.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConsumerVerificationServiceTest {

    private final ConsumerRepository consumerRepository = mock(ConsumerRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ConsumerVerificationService service = new ConsumerVerificationService(
            consumerRepository, processedEventRepository, outboxEventRepository, objectMapper);

    private final OrderCreatedEvent event = new OrderCreatedEvent("event-1", "OrderCreated", 42L, 1L);

    @Test
    void publishesConsumerVerifiedWhenConsumerIsActive() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Sanjay", true)));

        service.handleOrderCreated(event);

        verify(outboxEventRepository).save(argThat(e -> "ConsumerVerified".equals(e.getEventType())));
    }

    @Test
    void publishesConsumerVerificationFailedWhenConsumerIsInactive() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Blocked Consumer", false)));

        service.handleOrderCreated(event);

        verify(outboxEventRepository).save(argThat(e -> "ConsumerVerificationFailed".equals(e.getEventType())));
    }

    @Test
    void publishesConsumerVerificationFailedWhenConsumerNotFound() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.empty());

        service.handleOrderCreated(event);

        verify(outboxEventRepository).save(argThat(e -> "ConsumerVerificationFailed".equals(e.getEventType())));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(true);

        service.handleOrderCreated(event);

        verify(outboxEventRepository, never()).save(any());
        verify(consumerRepository, never()).findById(any());
    }

    @Test
    void publishesConsumerVerifiedReplyWhenConsumerIsActiveViaCommand() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Sanjay", true)));

        service.handleVerifyConsumerCommand("cmd-1", 42L, 1L);

        verify(outboxEventRepository).save(argThat(e ->
                "ConsumerVerified".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void publishesConsumerVerificationFailedReplyWhenConsumerIsInactiveViaCommand() {
        when(processedEventRepository.existsById("cmd-2")).thenReturn(false);
        when(consumerRepository.findById(1L)).thenReturn(Optional.of(new Consumer(1L, "Blocked Consumer", false)));

        service.handleVerifyConsumerCommand("cmd-2", 42L, 1L);

        verify(outboxEventRepository).save(argThat(e ->
                "ConsumerVerificationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void skipsDuplicateCommandDelivery() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(true);

        service.handleVerifyConsumerCommand("cmd-1", 42L, 1L);

        verify(outboxEventRepository, never()).save(any());
        verify(consumerRepository, never()).findById(any());
    }
}
