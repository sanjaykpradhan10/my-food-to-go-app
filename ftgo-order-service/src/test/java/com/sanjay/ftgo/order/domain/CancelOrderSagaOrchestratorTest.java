package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CancelOrderSagaOrchestratorTest {

    private final OrderTransitions orderTransitions = mock(OrderTransitions.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final SagaCommandPublisher sagaCommandPublisher = mock(SagaCommandPublisher.class);

    private final CancelOrderSagaOrchestrator orchestrator = new CancelOrderSagaOrchestrator(
            orderTransitions, processedEventRepository, sagaCommandPublisher);

    private Order cancelPendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
    }

    @Test
    void startSendsCancelTicketCommand() {
        orchestrator.start(cancelPendingOrder());

        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("CancelTicket"), eq(42L), any());
    }

    @Test
    void ticketCancelledTriggersReverseAuthorization() {
        when(processedEventRepository.existsById(any())).thenReturn(false);

        orchestrator.handleReply("e1", "kitchen", "TicketCancelled", 42L, null);

        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("ReverseAuthorization"), eq(42L), any());
    }

    @Test
    void ticketCancellationRejectedUndoesCancelWithoutContactingAccounting() {
        when(processedEventRepository.existsById(any())).thenReturn(false);

        orchestrator.handleReply("e1", "kitchen", "TicketCancellationRejected", 42L, "cannot cancel once ready for pickup");

        verify(orderTransitions).undoCancel(eq(42L), any());
        verify(sagaCommandPublisher, never()).publish(eq("accounting.commands"), any(), any(), any(), any());
    }

    @Test
    void authorizationReversedConfirmsCancel() {
        when(processedEventRepository.existsById(any())).thenReturn(false);

        orchestrator.handleReply("e2", "accounting", "AuthorizationReversed", 42L, null);

        verify(orderTransitions).noteCancelled(eq(42L), any());
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "kitchen", "TicketCancelled", 42L, null);

        verify(sagaCommandPublisher, never()).publish(any(), any(), any(), any(), any());
        verify(orderTransitions, never()).undoCancel(any(), any());
        verify(orderTransitions, never()).noteCancelled(any(), any());
    }
}
