package com.sanjay.ftgo.kitchen.api;

import com.sanjay.ftgo.kitchen.domain.Ticket;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisher;
import com.sanjay.ftgo.kitchen.domain.TicketRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.ZonedDateTime;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(TicketController.class)
class TicketControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TicketRepository ticketRepository;

    @MockitoBean
    private TicketDomainEventPublisher domainEventPublisher;

    @Test
    void acceptsAwaitingAcceptanceTicket() throws Exception {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/accept")
                        .contentType("application/json")
                        .content("""
                                {"readyBy":"2026-07-20T18:00:00Z"}
                                """))
                .andExpect(status().isOk());
    }

    @Test
    void returns404WhenTicketNotFoundOnAccept() throws Exception {
        when(ticketRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/tickets/99/accept")
                        .contentType("application/json")
                        .content("""
                                {"readyBy":"2026-07-20T18:00:00Z"}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenAcceptingTicketInWrongState() throws Exception {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/accept")
                        .contentType("application/json")
                        .content("""
                                {"readyBy":"2026-07-20T18:00:00Z"}
                                """))
                .andExpect(status().isConflict());
    }

    @Test
    void movesAcceptedTicketToPreparing() throws Exception {
        Ticket ticket = acceptedTicket();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/preparing"))
                .andExpect(status().isOk());
    }

    @Test
    void returns409WhenMovingCreatePendingTicketToPreparing() throws Exception {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/preparing"))
                .andExpect(status().isConflict());
    }

    @Test
    void movesPreparingTicketToReadyForPickup() throws Exception {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/ready-for-pickup"))
                .andExpect(status().isOk());
    }

    @Test
    void movesReadyForPickupTicketToPickedUp() throws Exception {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();
        when(ticketRepository.findById(1L)).thenReturn(Optional.of(ticket));

        mockMvc.perform(post("/tickets/1/picked-up"))
                .andExpect(status().isOk());
    }

    @Test
    void returns404WhenTicketNotFoundOnPickedUp() throws Exception {
        when(ticketRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/tickets/99/picked-up"))
                .andExpect(status().isNotFound());
    }

    private Ticket acceptedTicket() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        return ticket;
    }
}
