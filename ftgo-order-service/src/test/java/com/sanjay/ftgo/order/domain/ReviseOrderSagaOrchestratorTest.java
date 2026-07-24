package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReviseOrderSagaOrchestratorTest {

    private final OrderTransitions orderTransitions = mock(OrderTransitions.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final SagaCommandPublisher sagaCommandPublisher = mock(SagaCommandPublisher.class);

    private final ReviseOrderSagaOrchestrator orchestrator = new ReviseOrderSagaOrchestrator(
            orderTransitions, processedEventRepository, sagaCommandPublisher);

    private Order revisionPendingOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        order.revise(new OrderRevision(List.of(new OrderLineItem(10L, 8))));
        return order;
    }

    @Test
    void startSendsReviseTicketCommandWithPendingRevisedQuantity() {
        orchestrator.start(revisionPendingOrder());

        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("ReviseTicket"), eq(42L),
                argThat(command -> command instanceof KitchenCommand kitchenCommand
                        && Integer.valueOf(8).equals(kitchenCommand.totalQuantity())
                        && "ReviseOrder".equals(kitchenCommand.sagaType())));
    }

    @Test
    void ticketQuantityRevisedTriggersReviseAuthorizationWithPendingRevisedQuantity() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderTransitions.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketQuantityRevised", 42L, null);

        verify(sagaCommandPublisher).publish(eq("accounting.commands"), any(), eq("ReviseAuthorization"), eq(42L),
                argThat(command -> command instanceof AccountingCommand accountingCommand
                        && Integer.valueOf(8).equals(accountingCommand.totalQuantity())));
    }

    @Test
    void ticketRevisionRejectedRejectsRevisionWithoutContactingAccounting() {
        when(processedEventRepository.existsById(any())).thenReturn(false);

        orchestrator.handleReply("e1", "kitchen", "TicketRevisionRejected", 42L, "order exceeds kitchen capacity");

        verify(orderTransitions).rejectRevision(eq(42L), any());
        verify(sagaCommandPublisher, never()).publish(eq("accounting.commands"), any(), any(), any(), any());
    }

    @Test
    void authorizationRevisedConfirmsRevision() {
        when(processedEventRepository.existsById(any())).thenReturn(false);

        orchestrator.handleReply("e2", "accounting", "AuthorizationRevised", 42L, null);

        verify(orderTransitions).confirmRevision(eq(42L), any());
    }

    @Test
    void authorizationRevisionRejectedSendsUndoReviseTicketWithOriginalQuantity() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderTransitions.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e2", "accounting", "AuthorizationRevisionRejected", 42L, "order quantity exceeds authorization limit");

        verify(sagaCommandPublisher).publish(eq("kitchen.commands"), any(), eq("UndoReviseTicket"), eq(42L),
                argThat(command -> command instanceof KitchenCommand kitchenCommand
                        && Integer.valueOf(2).equals(kitchenCommand.totalQuantity())));
        verify(orderTransitions, never()).confirmRevision(any(), any());
        verify(orderTransitions, never()).rejectRevision(any(), any());
    }

    @Test
    void ticketRevisionUndoneFinalizesRejection() {
        when(processedEventRepository.existsById(any())).thenReturn(false);

        orchestrator.handleReply("e3", "kitchen", "TicketRevisionUndone", 42L, null);

        verify(orderTransitions).rejectRevision(eq(42L), any());
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "kitchen", "TicketQuantityRevised", 42L, null);

        verify(orderTransitions, never()).findById(any());
    }
}
