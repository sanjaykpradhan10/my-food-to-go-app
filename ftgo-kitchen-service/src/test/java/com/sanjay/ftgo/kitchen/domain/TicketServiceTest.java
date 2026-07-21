package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TicketServiceTest {

    private final TicketRepository ticketRepository = mock(TicketRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final FailedOrderRepository failedOrderRepository = mock(FailedOrderRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final TicketDomainEventPublisher domainEventPublisher = mock(TicketDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final TicketService ticketService = new TicketService(
            ticketRepository, processedEventRepository, failedOrderRepository,
            outboxEventRepository, domainEventPublisher, objectMapper);

    private final OrderCreatedEvent event = new OrderCreatedEvent(
            "event-1", "OrderCreated", 42L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 2)));

    @Test
    void createsTicketInCreatePendingOnFirstDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository).save(any());
        verify(ticketRepository).save(argThat(t -> t.getState() == TicketState.CREATE_PENDING));
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCreatedEvent));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(true);

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository, never()).save(any());
        verify(ticketRepository, never()).save(any());
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void createsTicketDirectlyAsCancelledWhenOrderAlreadyFailed() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(true);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(ticketRepository).save(argThat(t -> t.getState() == TicketState.CANCELLED));
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void rejectsTicketCreationWhenQuantityExceedsKitchenCapacity() {
        OrderCreatedEvent bigEvent = new OrderCreatedEvent(
                "event-2", "OrderCreated", 43L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 25)));
        when(processedEventRepository.existsById("event-2")).thenReturn(false);
        when(failedOrderRepository.existsById(43L)).thenReturn(false);

        ticketService.handleOrderCreated(bigEvent);

        verify(ticketRepository, never()).save(any());
        verify(domainEventPublisher).publishCreationFailed(argThat(e -> e.orderId().equals(43L)));
    }

    @Test
    void confirmsTicketWhenCardAuthorized() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("acct-event-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-1", 42L, "CardAuthorized");

        assertThat(ticket.getState()).isEqualTo(TicketState.AWAITING_ACCEPTANCE);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketConfirmedEvent));
    }

    @Test
    void cancelsTicketWhenCardAuthorizationFailed() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("acct-event-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-2", 42L, "CardAuthorizationFailed");

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancelledEvent));
    }

    @Test
    void cancelsExistingTicketWhenConsumerVerificationFails() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cons-event-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleConsumerVerificationFailed("cons-event-1", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancelledEvent));
        verify(failedOrderRepository, never()).save(any());
    }

    @Test
    void recordsFailedOrderWhenNoTicketExistsYet() {
        when(processedEventRepository.existsById("cons-event-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(43L)).thenReturn(Optional.empty());

        ticketService.handleConsumerVerificationFailed("cons-event-2", 43L);

        verify(failedOrderRepository).save(any());
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void createsTicketViaCommandWhenWithinCapacity() {
        when(processedEventRepository.existsById("cmd-1")).thenReturn(false);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleCreateTicketCommand("cmd-1", 42L, 5);

        verify(ticketRepository).save(argThat(t -> t.getState() == TicketState.CREATE_PENDING));
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCreated".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void repliesTicketCreationFailedViaCommandWhenOverCapacity() {
        when(processedEventRepository.existsById("cmd-2")).thenReturn(false);

        ticketService.handleCreateTicketCommand("cmd-2", 43L, 25);

        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCreationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void repliesTicketCreationFailedViaCommandWhenTotalQuantityIsNull() {
        when(processedEventRepository.existsById("cmd-5")).thenReturn(false);

        ticketService.handleCreateTicketCommand("cmd-5", 44L, null);

        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCreationFailed".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void confirmsTicketViaCommand() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-3")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleConfirmTicketCommand("cmd-3", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.AWAITING_ACCEPTANCE);
    }

    @Test
    void cancelsTicketViaCommandAndRepliesTicketCancelled() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-4")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleCancelTicketCommand("cmd-4", 42L, "CreateOrder");

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCancelled".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"CreateOrder\"")));
    }

    @Test
    void repliesTicketCancellationRejectedWhenTicketCannotBeCancelled() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        ticket.preparing();
        ticket.readyForPickup();
        when(processedEventRepository.existsById("cmd-5")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleCancelTicketCommand("cmd-5", 42L, "CancelOrder");

        assertThat(ticket.getState()).isEqualTo(TicketState.READY_FOR_PICKUP);
        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketCancellationRejected".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"CancelOrder\"")));
    }

    @Test
    void handlesOrderCancelledChoreographyAndPublishesTicketCancelled() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCancelled("evt-1", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancelledEvent));
    }

    @Test
    void handlesOrderCancelledChoreographyRejectionWithoutMutatingTicket() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        ticket.preparing();
        ticket.readyForPickup();
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleOrderCancelled("evt-2", 42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.READY_FOR_PICKUP);
        verify(ticketRepository, never()).save(any());
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketCancellationRejectedEvent));
    }

    @Test
    void handlesOrderRevisionProposedWithinCapacityAndPublishesQuantityRevised() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        OrderCreatedEvent event = new OrderCreatedEvent("evt-10", "OrderRevisionProposed", 42L, null,
                List.of(new OrderCreatedEvent.LineItem(10L, 8)));
        when(processedEventRepository.existsById("evt-10")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderRevisionProposed(event);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketQuantityRevisedEvent));
    }

    @Test
    void handlesOrderRevisionProposedOverCapacityAndPublishesRevisionRejectedWithoutMutating() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        OrderCreatedEvent event = new OrderCreatedEvent("evt-11", "OrderRevisionProposed", 42L, null,
                List.of(new OrderCreatedEvent.LineItem(10L, 25)));
        when(processedEventRepository.existsById("evt-11")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleOrderRevisionProposed(event);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(ticketRepository, never()).save(any());
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketRevisionRejectedEvent));
    }

    @Test
    void handlesOrderRevisionRejectedCompensationAndUndoesQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.reviseQuantity(8);
        OrderCreatedEvent event = new OrderCreatedEvent("evt-12", "OrderRevisionCompensationRequested", 42L, null,
                List.of(new OrderCreatedEvent.LineItem(10L, 2)));
        when(processedEventRepository.existsById("evt-12")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderRevisionRejected(event);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(domainEventPublisher).publish(any(Ticket.class), argThat(events ->
                events.size() == 1 && events.get(0) instanceof TicketRevisionUndoneEvent));
    }

    @Test
    void reviseTicketCommandWithinCapacityRepliesQuantityRevised() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-10")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleReviseTicketCommand("cmd-10", 42L, 8);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketQuantityRevised".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }

    @Test
    void reviseTicketCommandOverCapacityRepliesRevisionRejected() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        when(processedEventRepository.existsById("cmd-11")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));

        ticketService.handleReviseTicketCommand("cmd-11", 42L, 25);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketRevisionRejected".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())));
    }

    @Test
    void undoReviseTicketCommandRestoresOriginalQuantityAndReplies() {
        Ticket ticket = Ticket.createTicket(42L, 2).ticket();
        ticket.reviseQuantity(8);
        when(processedEventRepository.existsById("cmd-12")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleUndoReviseTicketCommand("cmd-12", 42L, 2);

        assertThat(ticket.getTotalQuantity()).isEqualTo(2);
        verify(outboxEventRepository).save(argThat((OutboxEvent e) ->
                "TicketRevisionUndone".equals(e.getEventType()) && "saga.replies".equals(e.getTopic())
                        && e.getPayload().contains("\"sagaType\":\"ReviseOrder\"")));
    }
}
