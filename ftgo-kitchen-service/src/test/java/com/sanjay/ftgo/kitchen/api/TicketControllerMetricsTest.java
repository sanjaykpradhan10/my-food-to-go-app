package com.sanjay.ftgo.kitchen.api;

import com.sanjay.ftgo.kitchen.domain.Ticket;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisher;
import com.sanjay.ftgo.kitchen.domain.TicketRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

// Uses a real SimpleMeterRegistry (not a Mockito mock) so counter().increment() calls behave
// exactly as they would in production, letting these tests assert real counter values.
@ExtendWith(MockitoExtension.class)
class TicketControllerMetricsTest {

    @Mock
    private TicketRepository ticketRepository;

    @Mock
    private TicketDomainEventPublisher domainEventPublisher;

    private SimpleMeterRegistry meterRegistry;
    private TicketController controller;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        controller = new TicketController(ticketRepository, domainEventPublisher, meterRegistry);
    }

    @Test
    void acceptIncrementsTicketsAcceptedCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.accept(1L, new AcceptTicketRequest(ZonedDateTime.now().plusMinutes(20)));

        assertThat(meterRegistry.counter("tickets_accepted").count()).isEqualTo(1.0);
    }

    @Test
    void preparingIncrementsTicketsPreparingCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(20));
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.preparing(1L);

        assertThat(meterRegistry.counter("tickets_preparing").count()).isEqualTo(1.0);
    }

    @Test
    void readyForPickupIncrementsTicketsReadyForPickupCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(20));
        ticket.preparing();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.readyForPickup(1L);

        assertThat(meterRegistry.counter("tickets_ready_for_pickup").count()).isEqualTo(1.0);
    }

    @Test
    void pickedUpIncrementsTicketsPickedUpCounter() {
        Ticket ticket = Ticket.createTicket(1L, 2).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(20));
        ticket.preparing();
        ticket.readyForPickup();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        controller.pickedUp(1L);

        assertThat(meterRegistry.counter("tickets_picked_up").count()).isEqualTo(1.0);
    }
}
