package com.sanjay.ftgo.kitchen.api;

import com.sanjay.ftgo.kitchen.domain.Ticket;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEvent;
import com.sanjay.ftgo.kitchen.domain.TicketDomainEventPublisher;
import com.sanjay.ftgo.kitchen.domain.TicketNotFoundException;
import com.sanjay.ftgo.kitchen.domain.TicketRepository;
import com.sanjay.ftgo.kitchen.domain.UnsupportedStateTransitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/tickets")
@Transactional
public class TicketController {

    private final TicketRepository ticketRepository;
    private final TicketDomainEventPublisher domainEventPublisher;

    public TicketController(TicketRepository ticketRepository, TicketDomainEventPublisher domainEventPublisher) {
        this.ticketRepository = ticketRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @PostMapping("/{ticketId}/accept")
    public ResponseEntity<Void> accept(@PathVariable Long ticketId, @RequestBody AcceptTicketRequest request) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.accept(request.readyBy()));
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{ticketId}/preparing")
    public ResponseEntity<Void> preparing(@PathVariable Long ticketId) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.preparing());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{ticketId}/ready-for-pickup")
    public ResponseEntity<Void> readyForPickup(@PathVariable Long ticketId) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.readyForPickup());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{ticketId}/picked-up")
    public ResponseEntity<Void> pickedUp(@PathVariable Long ticketId) {
        Ticket ticket = findTicket(ticketId);
        apply(ticket, ticket.pickedUp());
        return ResponseEntity.ok().build();
    }

    private Ticket findTicket(Long ticketId) {
        return ticketRepository.findById(ticketId)
                .orElseThrow(() -> new TicketNotFoundException(ticketId));
    }

    private void apply(Ticket ticket, List<TicketDomainEvent> events) {
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @ExceptionHandler(TicketNotFoundException.class)
    public ResponseEntity<String> handleNotFound(TicketNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(UnsupportedStateTransitionException.class)
    public ResponseEntity<String> handleConflict(UnsupportedStateTransitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
    }
}
