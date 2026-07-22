package com.sanjay.ftgo.order.domain;

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

class ReviseOrderSagaOrchestratorTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ReviseOrderSagaOrchestrator orchestrator = new ReviseOrderSagaOrchestrator(
            orderRepository, processedEventRepository, outboxEventRepository, domainEventPublisher, objectMapper);

    private Order revisionPendingOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        order.revise(new OrderRevision(List.of(new OrderLineItem(10L, 8))));
        return order;
    }

    @Test
    void startSendsReviseTicketCommandWithPendingRevisedQuantity() {
        orchestrator.start(revisionPendingOrder());

        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "ReviseTicket".equals(e.getEventType())
                && e.getPayload().contains("\"totalQuantity\":8")
                && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }

    @Test
    void ticketQuantityRevisedTriggersReviseAuthorizationWithPendingRevisedQuantity() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketQuantityRevised", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "accounting.commands".equals(e.getTopic())
                && "ReviseAuthorization".equals(e.getEventType())
                && e.getPayload().contains("\"totalQuantity\":8")));
    }

    @Test
    void ticketRevisionRejectedRejectsRevisionWithoutContactingAccounting() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketRevisionRejected", 42L, "order exceeds kitchen capacity");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
        verify(outboxEventRepository, never()).save(argThat(e -> "accounting.commands".equals(e.getTopic())));
    }

    @Test
    void authorizationRevisedConfirmsRevision() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e2", "accounting", "AuthorizationRevised", 42L, null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 8));
        verify(domainEventPublisher).publish(List.of(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 8)))));
    }

    @Test
    void authorizationRevisionRejectedSendsUndoReviseTicketWithOriginalQuantity() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e2", "accounting", "AuthorizationRevisionRejected", 42L, "order quantity exceeds authorization limit");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "UndoReviseTicket".equals(e.getEventType())
                && e.getPayload().contains("\"totalQuantity\":2")));
        verify(orderRepository, never()).save(any());
    }

    @Test
    void ticketRevisionUndoneFinalizesRejection() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e3", "kitchen", "TicketRevisionUndone", 42L, null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "kitchen", "TicketQuantityRevised", 42L, null);

        verify(orderRepository, never()).findById(any());
    }
}
