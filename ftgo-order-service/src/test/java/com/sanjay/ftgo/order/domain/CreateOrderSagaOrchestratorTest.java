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

class CreateOrderSagaOrchestratorTest {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository = mock(CreateOrderSagaInstanceRepository.class);
    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final CreateOrderSagaOrchestrator orchestrator = new CreateOrderSagaOrchestrator(
            sagaInstanceRepository, orderRepository, processedEventRepository, outboxEventRepository,
            domainEventPublisher, objectMapper);

    private Order pendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void startSendsVerifyConsumerAndCreateTicketCommands() {
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.start(pendingOrder());

        verify(sagaInstanceRepository).save(any());
        verify(outboxEventRepository).save(argThat(e -> "consumer.commands".equals(e.getTopic())));
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "CreateTicket".equals(e.getEventType())));
    }

    @Test
    void authorizesOnceBothConsumerVerifiedAndTicketCreatedRegardlessOfOrder() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerified", 42L, null);
        orchestrator.handleReply("e2", "kitchen", "TicketCreated", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "accounting.commands".equals(e.getTopic())));
    }

    @Test
    void authorizesRegardlessOfReplyOrder() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrator.handleReply("e1", "kitchen", "TicketCreated", 42L, null);
        orchestrator.handleReply("e2", "consumer", "ConsumerVerified", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "accounting.commands".equals(e.getTopic())));
    }

    @Test
    void approvesOrderDirectlyOnCardAuthorizedWithoutWaitingForConfirmation() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "accounting", "CardAuthorized", 42L, null);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(domainEventPublisher).publish(List.of(new OrderApprovedEvent(42L)));
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "ConfirmTicket".equals(e.getEventType())));
    }

    @Test
    void rejectsOrderAndCancelsTicketOnCardAuthorizationFailed() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "accounting", "CardAuthorizationFailed", 42L, "declined");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(domainEventPublisher).publish(List.of(new OrderRejectedEvent(42L)));
        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "CancelTicket".equals(e.getEventType())));
    }

    @Test
    void rejectsOrderWithoutCompensatingWhenConsumerVerificationFailsBeforeTicketCreated() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerificationFailed", 42L, "not found");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(outboxEventRepository, never()).save(argThat(e -> "kitchen.commands".equals(e.getTopic())));
    }

    @Test
    void compensatesLateTicketCreatedReplyAfterAlreadyFailed() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "consumer", "ConsumerVerificationFailed", 42L, "not found");
        orchestrator.handleReply("e2", "kitchen", "TicketCreated", 42L, null);

        verify(outboxEventRepository).save(argThat(e -> "kitchen.commands".equals(e.getTopic())
                && "CancelTicket".equals(e.getEventType())));
    }

    @Test
    void rejectsOrderOnTicketCreationFailedWithNoCompensationNeeded() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 5);
        Order order = pendingOrder();
        when(processedEventRepository.existsById(any())).thenReturn(false);
        when(sagaInstanceRepository.findById(42L)).thenReturn(Optional.of(instance));
        when(sagaInstanceRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orchestrator.handleReply("e1", "kitchen", "TicketCreationFailed", 42L, "capacity");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(outboxEventRepository, never()).save(argThat(e -> "kitchen.commands".equals(e.getTopic())));
    }

    @Test
    void skipsDuplicateReplyDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orchestrator.handleReply("e1", "consumer", "ConsumerVerified", 42L, null);

        verify(sagaInstanceRepository, never()).findById(any());
    }
}
