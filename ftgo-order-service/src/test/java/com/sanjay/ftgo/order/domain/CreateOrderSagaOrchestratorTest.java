package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CreateOrderSagaOrchestratorTest {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository = mock(CreateOrderSagaInstanceRepository.class);
    private final OrderTransitions orderTransitions = mock(OrderTransitions.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final SagaCommandPublisher sagaCommandPublisher = mock(SagaCommandPublisher.class);

    private final CreateOrderSagaOrchestrator orchestrator = new CreateOrderSagaOrchestrator(
            sagaInstanceRepository, orderTransitions, processedEventRepository, sagaCommandPublisher);

    private Order pendingOrder() {
        return new Order(42L, 1L, 1L, java.util.List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void startSendsVerifyConsumerAndCreateTicketCommands() {
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.start(pendingOrder());

        verify(sagaInstanceRepository).save(any());
        verify(sagaCommandPublisher).publish(eq("consumer.commands"), any(), eq("VerifyConsumerCommand"), eq(42L), any());
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CreateTicket"), eq(42L), any());
    }

    @Test
    void authorizesOnceBothConsumerVerifiedAndTicketCreatedRegardlessOfOrder() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerified", 42L, null);
        orchestrator.handleReply("e2", "kitchen", "TicketCreated", 42L, null);

        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("AuthorizeCard"), eq(42L), any());
    }

    @Test
    void authorizesRegardlessOfReplyOrder() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "kitchen", "TicketCreated", 42L, null);
        orchestrator.handleReply("e2", "consumer", "ConsumerVerified", 42L, null);

        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("AuthorizeCard"), eq(42L), any());
    }

    @Test
    void approvesOrderDirectlyOnCardAuthorizedWithoutWaitingForConfirmation() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "accounting", "CardAuthorized", 42L, null);

        verify(orderTransitions).approve(eq(42L), any());
        verify(orderTransitions, never()).reject(any(), any());
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("ConfirmTicket"), eq(42L), any());
    }

    @Test
    void rejectsOrderAndCancelsTicketOnCardAuthorizationFailed() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "accounting", "CardAuthorizationFailed", 42L, "declined");

        verify(orderTransitions).reject(eq(42L), any());
        verify(orderTransitions, never()).approve(any(), any());
        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
    }

    @Test
    void rejectsOrderWithoutCompensatingWhenConsumerVerificationFailsBeforeTicketCreated() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerificationFailed", 42L, "not found");

        verify(orderTransitions).reject(eq(42L), any());
        verify(sagaCommandPublisher, never()).publish(eq("kitchen.commands"), any(), any(), any(), any());
    }

    @Test
    void compensatesLateTicketCreatedReplyAfterAlreadyFailed() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerificationFailed", 42L, "not found");
        orchestrator.handleReply("e2", "kitchen", "TicketCreated", 42L, null);

        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
    }

    @Test
    void rejectsOrderOnTicketCreationFailedWithNoCompensationNeeded() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "kitchen", "TicketCreationFailed", 42L, "capacity");

        verify(orderTransitions).reject(eq(42L), any());
        verify(sagaCommandPublisher, never()).publish(eq("kitchen.commands"), any(), any(), any(), any());
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "consumer", "ConsumerVerified", 42L, null);

        verify(sagaInstanceRepository, never()).findById(any());
    }
}
