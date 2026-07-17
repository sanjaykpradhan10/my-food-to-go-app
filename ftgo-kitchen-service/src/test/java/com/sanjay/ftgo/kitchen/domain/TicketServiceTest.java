package com.sanjay.ftgo.kitchen.domain;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TicketServiceTest {

    private final TicketRepository ticketRepository = mock(TicketRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final FailedOrderRepository failedOrderRepository = mock(FailedOrderRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

    private final TicketService ticketService = new TicketService(
            ticketRepository, processedEventRepository, failedOrderRepository, outboxEventRepository, objectMapper);

    private final OrderCreatedEvent event = new OrderCreatedEvent(
            "event-1", "OrderCreated", 42L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 2)));

    @Test
    void createsTicketInCreatePendingOnFirstDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository).save(any());
        verify(ticketRepository).save(argThatStatusIs("CREATE_PENDING"));
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketCreated"));
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("event-1")).thenReturn(true);

        ticketService.handleOrderCreated(event);

        verify(processedEventRepository, never()).save(any());
        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void createsTicketDirectlyAsCancelledWhenOrderAlreadyFailed() {
        when(processedEventRepository.existsById("event-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(true);
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleOrderCreated(event);

        verify(ticketRepository).save(argThatStatusIs("CANCELLED"));
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void rejectsTicketCreationWhenQuantityExceedsKitchenCapacity() {
        OrderCreatedEvent bigEvent = new OrderCreatedEvent(
                "event-2", "OrderCreated", 43L, 1L, List.of(new OrderCreatedEvent.LineItem(10L, 25)));
        when(processedEventRepository.existsById("event-2")).thenReturn(false);
        when(failedOrderRepository.existsById(43L)).thenReturn(false);

        ticketService.handleOrderCreated(bigEvent);

        verify(ticketRepository, never()).save(any());
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketCreationFailed"));
    }

    @Test
    void confirmsTicketWhenCardAuthorized() {
        Ticket ticket = new Ticket(42L, "CREATE_PENDING");
        when(processedEventRepository.existsById("acct-event-1")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-1", 42L, "CardAuthorized");

        assertThat(ticket.getStatus()).isEqualTo("AWAITING_ACCEPTANCE");
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketConfirmed"));
    }

    @Test
    void cancelsTicketWhenCardAuthorizationFailed() {
        Ticket ticket = new Ticket(42L, "CREATE_PENDING");
        when(processedEventRepository.existsById("acct-event-2")).thenReturn(false);
        when(ticketRepository.findByOrderId(42L)).thenReturn(Optional.of(ticket));
        when(ticketRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        ticketService.handleAccountingEvent("acct-event-2", 42L, "CardAuthorizationFailed");

        assertThat(ticket.getStatus()).isEqualTo("CANCELLED");
        verify(outboxEventRepository).save(argThatEventTypeIs("TicketCancelled"));
    }

    private Ticket argThatStatusIs(String status) {
        return org.mockito.ArgumentMatchers.argThat(t -> t != null && status.equals(t.getStatus()));
    }

    private OutboxEvent argThatEventTypeIs(String eventType) {
        return org.mockito.ArgumentMatchers.argThat(e -> e != null && eventType.equals(e.getEventType()));
    }
}
